#include "master.hpp"
#include "wordexp.h"
#include <stdio.h>
#include <readline/readline.h>
#include <readline/history.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/wait.h>
#include <filesystem>
#include <string_view>
#include <fstream>
#include <map>
#include <tclap/CmdLine.h>
#include <pybind11/stl.h>
#include <pybind11/embed.h>

namespace py = pybind11;
using namespace pybind11::literals;
namespace fs = std::filesystem;

struct Options {
    std::string       protocol;
    tl::logger::level masterLogLevel;
    tl::logger::level workerLogLevel;
    std::string       pythonFile;
    size_t            initClusterSize;
    std::string       logPath;
    std::string       logType;
    std::string       tracePath;
};

static void parseCommandLine(int argc, char** argv, Options& options);

PYBIND11_EMBEDDED_MODULE(raft, m) {

    py::enum_<mraft::Role>(m, "Role")
        .value("SPARE", mraft::Role::SPARE)
        .value("VOTER", mraft::Role::VOTER)
        .value("STANDBY", mraft::Role::STANDBY)
        .export_values();

    py::class_<WorkerHandle, std::shared_ptr<WorkerHandle>>(m, "Worker")
        .def_readonly("raft_id", &WorkerHandle::raftID)
        .def_readonly("address", &WorkerHandle::address)
        .def("__repr__", [](const WorkerHandle& w) -> std::string {
            return w.to_string();
        })
        .def("apply", [](const WorkerHandle& w, const std::string& command) -> bool {
            return w.master.lock()->apply(w, command);
        })
        .def("assign", [](const WorkerHandle& w, WorkerHandle& target, mraft::Role role) -> bool {
            return w.master.lock()->assign(w, target, role);
        })
        .def("add", [](const WorkerHandle& w, WorkerHandle& toAdd) -> bool {
            return w.master.lock()->add(w, toAdd);
        })
        .def("remove", [](const WorkerHandle& w, WorkerHandle& toRemove) -> bool {
            return w.master.lock()->remove(w, toRemove);
        })
        .def("remove", [](const WorkerHandle& w, raft_id id) -> bool {
            return w.master.lock()->remove(w, id);
        })
        .def("start", [](WorkerHandle& w) -> bool {
            return w.master.lock()->start(w);
        })
        .def("transfer", [](const WorkerHandle& w, WorkerHandle& target) -> bool {
            return w.master.lock()->transfer(w, target);
        })
        .def("barrier", [](const WorkerHandle& w) -> bool {
            return w.master.lock()->barrier(w);
        })
        .def("suspend", [](const WorkerHandle& w, unsigned msec) -> bool {
            return w.master.lock()->suspend(w, msec);
        })
        .def("isolate", [](const WorkerHandle& w, bool flag) -> bool {
            return w.master.lock()->isolate(w, flag);
        })
        .def("get_fsm_content", [](const WorkerHandle& w) -> std::optional<std::string> {
            return w.master.lock()->get_fsm_content(w);
        })
        .def("get_leader", [](const WorkerHandle& w) -> std::shared_ptr<WorkerHandle> {
            return w.master.lock()->get_leader(w);
        })
        .def("shutdown", [](WorkerHandle& w) -> bool {
            return w.master.lock()->shutdown(w);
        })
        .def("kill", [](WorkerHandle& w) -> bool {
            return w.master.lock()->kill(w);
        });

    py::class_<MasterContext, std::shared_ptr<MasterContext>>(m, "Cluster")
        .def("__repr__", [](const MasterContext& master) {
            return master.cluster->to_string();
        })
        .def("__len__", [](const MasterContext& master) {
            return master.cluster->workers.size();
        })
        .def("__getitem__", [](const MasterContext& master, raft_id id) -> std::shared_ptr<WorkerHandle> {
            if(master.cluster->workers.count(id) != 0)
                return master.cluster->workers[id];
            else
                return nullptr;
        })
        .def("spawn", [](MasterContext& master, raft_id id) -> std::shared_ptr<WorkerHandle> {
            return master.spawnWorker(id);
        }, "id"_a=0);
}

// ----------------------------------------------------------------------------
// Main function
// ----------------------------------------------------------------------------

int main(int argc, char** argv) {
    Options options;
    parseCommandLine(argc, argv, options);

    MasterOptions masterOptions;
    masterOptions.protocol = options.protocol;
    masterOptions.logLevel = options.masterLogLevel;

    WorkerOptions workerOptions;
    workerOptions.protocol  = options.protocol;
    workerOptions.logLevel  = options.workerLogLevel;
    workerOptions.logPath   = options.logPath;
    workerOptions.logType   = options.logType;
    workerOptions.tracePath = options.tracePath;

    auto master = std::make_shared<MasterContext>(masterOptions, workerOptions);

    py::scoped_interpreter guard{};

    py::module_ mod = py::module_::import("raft");
    mod.attr("cluster") = master;

    py::dict locals;
    py::dict globals = py::globals();

    auto masterPID = getpid();
    decltype(masterPID) currentPID;

    int ret = 0;

    // initial cluster setup
    for(size_t i = 0; i < options.initClusterSize; ++i) {
        std::shared_ptr<WorkerHandle> w;
        try {
            w = master->spawnWorker();
        } catch(const MasterContext::CatchMeIfYouCan&) {
            // prevent child from running master code when it leave worker code
            return 0;
        }
        if(!w) {
            auto mid = master->client->engine.get_margo_instance();
            margo_critical(mid, "Could not spawn initial worker %lu", i+1);
            ret = 1;
            goto cleanup;
        }
    }

    master->bootstrap();
    master->start();
    master->waitForLeader();

    if(options.pythonFile.empty()) {
        // interactive mode
        char* l;
        while((l = readline(">> ")) != nullptr) {
            if(*l) add_history(l);
            std::string line{l};
            while(!line.empty() && !::isalnum(line.front())) line = line.substr(1);
            if(line.rfind("exit", 0) == 0) break;
            try {
                py::exec(line, globals, locals);
            } catch(const py::error_already_set& e) {
                currentPID = getpid();
                if(currentPID != masterPID) return 0;
                auto mid = master->client->engine.get_margo_instance();
                margo_error(mid, "[master] python error: %s", e.what());
            }
            free(l);
        }
    } else {
        // file mode
        std::ifstream file(options.pythonFile);
        if(!file.good()) {
            auto mid = master->client->engine.get_margo_instance();
            margo_critical(mid, "[master] could not find file %s", options.pythonFile.c_str());
        }
        std::string content{
            (std::istreambuf_iterator<char>(file)),
            (std::istreambuf_iterator<char>())};
        file.close();
        try {
            py::exec(content.c_str(), globals, locals);
        } catch(const py::error_already_set& e) {
            currentPID = getpid();
            if(currentPID != masterPID) return 0;
            auto mid = master->client->engine.get_margo_instance();
            margo_error(mid, "[master] python error: %s", e.what());
            ret = 1;
        }
    }

cleanup:
    for(const auto& [raftID, worker] : *master->cluster) {
        auto addr = master->client->engine.lookup(worker->address);
        master->client->engine.shutdown_remote_engine(addr);
        auto mid = master->client->engine.get_margo_instance();
        margo_trace(mid,
                "[master] waiting for worker with pID=%u and raftID=%lu to terminate...",
                worker->pID, worker->raftID);
        waitpid(worker->pID, nullptr, 0);
    }
    return ret;
}

// ----------------------------------------------------------------------------
// Command line argument parsing
// ----------------------------------------------------------------------------

static void parseCommandLine(int argc, char** argv, Options& options) {
    try {
        TCLAP::CmdLine cmd("Mochi-Raft test framework", ' ', "0.1.0");

        TCLAP::UnlabeledValueArg<std::string> protocol(
                "protocol", "Protocol (e.g. ofi+tcp)", true, "na+sm", "protocol");
        TCLAP::ValueArg<std::string> masterLogLevel(
                "v", "verbosity",
                "Log level for the master (trace, debug, info, warning, error, critical, off)",
                false, "info", "level");
        TCLAP::ValueArg<std::string> workerLogLevel(
                "w", "worker-verbosity",
                "Log level for the worker (defaults to that of the master)",
                false, "", "level");
        TCLAP::ValueArg<std::string> pythonFile(
                "f", "python-file", "Python file for the master to execute", false,
                "", "filename");
        TCLAP::ValueArg<size_t> clusterSize(
                "n", "cluster-size", "Initial number of processes the cluster contains",
                false, 1, "size");
        TCLAP::ValueArg<std::string> logPath(
                "p", "log-path", "Path where the logs should be stored", false,
                ".", "path");
        TCLAP::ValueArg<std::string> tracePath(
                "t", "trace-path", "Path where the worker traces should be stored", false,
                "", "path");
        TCLAP::ValueArg<std::string> logType(
                "l", "log-type", "Type of log to use (\"abt-io\" or \"memory\")", false,
                "abt-io", "type");

        cmd.add(protocol);
        cmd.add(masterLogLevel);
        cmd.add(workerLogLevel);
        cmd.add(pythonFile);
        cmd.add(clusterSize);
        cmd.add(logPath);
        cmd.add(logType);
        cmd.add(tracePath);
        cmd.parse(argc, argv);

        static std::unordered_map<std::string, tl::logger::level> logLevelMap = {
            {"trace",    tl::logger::level::trace},
            {"debug",    tl::logger::level::debug},
            {"info",     tl::logger::level::info},
            {"warning",  tl::logger::level::warning},
            {"error",    tl::logger::level::error},
            {"critical", tl::logger::level::critical},
            {"off",      tl::logger::level::critical}
        };

        options.protocol        = protocol.getValue();
        options.pythonFile      = pythonFile.getValue();
        options.initClusterSize = clusterSize.getValue();
        options.logPath         = logPath.getValue();
        options.logType         = logType.getValue();
        options.tracePath       = tracePath.getValue();
        if(logLevelMap.count(masterLogLevel.getValue()))
            options.masterLogLevel = logLevelMap[masterLogLevel.getValue()];
        else
            options.masterLogLevel = tl::logger::level::info;
        if(logLevelMap.count(workerLogLevel.getValue()))
            options.workerLogLevel = logLevelMap[workerLogLevel.getValue()];
        else
            options.workerLogLevel = options.masterLogLevel;

        if(!options.pythonFile.empty() && !fs::is_regular_file(options.pythonFile)) {
            std::cerr << "error: " << options.pythonFile << " does not exist or is not a file" << std::endl;
            exit(-1);
        }
        if(!options.pythonFile.empty() && !fs::is_directory(options.logPath)) {
            std::cerr << "error: " << options.logPath << " does not exist or not a directory" << std::endl;
            exit(-1);
        }
        if(options.initClusterSize == 0) {
            std::cerr << "error: cannot start with a cluster of size 0" << std::endl;
            exit(-1);
        }
        if(options.logType != "abt-io" && options.logType != "memory") {
            std::cerr << "error: invalid log type (should be \"abt-io\" or \"memory\")" << std::endl;
            exit(-1);
        }

    } catch (TCLAP::ArgException& e) {
        std::cerr << "error: " << e.error() << " for arg " << e.argId()
            << std::endl;
        exit(-1);
    }
}
