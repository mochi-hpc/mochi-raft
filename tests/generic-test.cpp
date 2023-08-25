#include <readline/readline.h>
#include <readline/history.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/wait.h>
#include <filesystem>
#include <map>
#include <thallium.hpp>
#include <mochi-raft.hpp>
#include <tclap/CmdLine.h>
#define SOL_ALL_SAFETIES_ON 1
#include <sol/sol.hpp>

namespace fs = std::filesystem;
namespace tl = thallium;

struct Options {
    std::string       protocol;
    tl::logger::level masterLogLevel;
    tl::logger::level workerLogLevel;
    std::string       luaFile;
    size_t            initClusterSize;
    std::string       logPath;
};

static void parseCommandLine(int argc, char** argv, Options& options);
static void runMaster(const Options& options);
[[noreturn]] static void runWorker(const Options& options, tl::engine engine);

// ----------------------------------------------------------------------------
// Main function
// ----------------------------------------------------------------------------

int main(int argc, char** argv) {
    Options options;
    parseCommandLine(argc, argv, options);
    runMaster(options);
    return 0;
}

// ----------------------------------------------------------------------------
// Master functions and structure definitions
// ----------------------------------------------------------------------------

struct WorkerHandle {
    raft_id      raftID = 0;
    pid_t        pID    = -1;
    tl::endpoint address;

    auto to_string() const {
        std::stringstream stream;
        auto addr = (pID == -1) ? std::string{"?"} : static_cast<std::string>(address);
        stream << "WorkerHandle"
               << " {raftID=" << raftID
               << ", pID=" << pID
               << ", address=" << addr
               << "}";
        return stream.str();
    }
};

struct Cluster {

    std::map<raft_id, WorkerHandle> workers;

    auto to_string() const {
        std::stringstream stream;
        for(const auto& [raftID, worker] : workers) {
            stream << "- " << worker.to_string() << "\n";
        }
        auto result = stream.str();
        if(!result.empty()) result.resize(result.size()-1);
        return result;
    }
};

struct MasterContext {
    tl::engine               engine;
    sol::state               lua;
    std::shared_ptr<Cluster> cluster;
    raft_id                  nextRaftID = 1;
};

static std::optional<WorkerHandle> spawnWorker(MasterContext& master, const Options& options) {
    int pipefd[2];
    if (pipe(pipefd) == -1) {
        perror("Pipe creation failed");
        return std::nullopt;
    }

    pid_t pid = fork();
    if (pid < 0) {
        perror("Fork failed");
        return std::nullopt;
    } else if (pid == 0) {
        // Child process
        close(pipefd[0]);
        master.engine.finalize();

        auto engine = tl::engine(options.protocol, MARGO_SERVER_MODE);
        engine.enable_remote_shutdown();

        auto address = static_cast<std::string>(engine.self());
        address.resize(1024);
        ssize_t _ = write(pipefd[1], address.data(), 1024);
        close(pipefd[1]);

        runWorker(options, engine);
    } else {
        // Parent process
        close(pipefd[1]);
        char address[1024];
        ssize_t bytes_read = read(pipefd[0], address, 1024);
        close(pipefd[0]);
        auto raftID = master.nextRaftID++;
        master.cluster->workers[raftID] = WorkerHandle{
            raftID, pid, master.engine.lookup(address)
        };
        return master.cluster->workers[raftID];
    }
}

static void runMaster(const Options& options) {
    MasterContext master;
    master.engine = tl::engine{options.protocol, THALLIUM_CLIENT_MODE};
    master.engine.set_log_level(options.masterLogLevel);
    master.lua.open_libraries(sol::lib::base, sol::lib::string, sol::lib::math);
    master.cluster = std::make_shared<Cluster>();

    master.lua.new_usertype<Cluster>("Cluster",
        sol::meta_function::index, [](Cluster& cluster, raft_id id) -> std::optional<WorkerHandle> {
            if(cluster.workers.count(id) == 0) return std::nullopt;
            return cluster.workers[id];
        }
    );

    master.lua["cluster"] = master.cluster;

    master.lua.new_usertype<WorkerHandle>("Worker",
        "address", sol::property([](const WorkerHandle& w) { return static_cast<std::string>(w.address); }),
        "raft_id", sol::readonly(&WorkerHandle::raftID),

        // kill command (send SIGKILL to worker)
        "kill", [&master](WorkerHandle& self) mutable {
            static WorkerHandle none;
            auto& workers = master.cluster->workers;
            auto it = workers.find(self.raftID);
            if(it == workers.end()) {
                auto mid = master.engine.get_margo_instance();
                margo_error(mid, "[master] worker not found in the cluster");
                return;
            }
            kill(self.pID, SIGKILL);
            waitpid(self.pID, NULL, 0);
            workers.erase(it);
            self.address = tl::endpoint();
            self.pID = -1;
        }
    );

    master.lua.set_function("spawn", [&master, &options]() mutable {
        return spawnWorker(master, options);
    });

    master.lua.set_function("sleep", [](unsigned msec) {
        usleep(msec*1000);
    });

    if(options.luaFile.empty()) {
        char* l;
        while((l = readline(">> ")) != nullptr) {
            if(*l) add_history(l);
            std::string line{l};
            while(!line.empty() && !::isalnum(line.front())) line = line.substr(1);
            if(line.rfind("exit", 0) == 0) break;
            master.lua.script(line, [](lua_State*, sol::protected_function_result pfr) {
                sol::error err = pfr;
                std::cout << err.what() << std::endl;
                return pfr;
            });
            free(l);
        }
    } else {
        master.lua.script_file(options.luaFile);
    }

    for(const auto& [raftID, worker] : master.cluster->workers) {
        master.engine.shutdown_remote_engine(worker.address);
        waitpid(worker.pID, nullptr, 0);
    }
}

// ----------------------------------------------------------------------------
// Worker functionalities
// ----------------------------------------------------------------------------

[[noreturn]] static void runWorker(const Options& options, tl::engine engine) {
    (void)options;
    engine.wait_for_finalize();
    std::exit(0);
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
        TCLAP::ValueArg<std::string> luaFile(
                "j", "lua-file", "Lua file for the master to execute", false,
                "", "filename");
        TCLAP::ValueArg<size_t> clusterSize(
                "n", "cluster-size", "Initial number of processes the cluster contains",
                false, 1, "size");
        TCLAP::ValueArg<std::string> logPath(
                "p", "log-path", "Path where the logs should be stored", false,
                ".", "path");

        cmd.add(protocol);
        cmd.add(masterLogLevel);
        cmd.add(workerLogLevel);
        cmd.add(luaFile);
        cmd.add(clusterSize);
        cmd.add(logPath);
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
        options.luaFile         = luaFile.getValue();
        options.initClusterSize = clusterSize.getValue();
        options.logPath         = logPath.getValue();
        if(logLevelMap.count(masterLogLevel.getValue()))
            options.masterLogLevel = logLevelMap[masterLogLevel.getValue()];
        else
            options.masterLogLevel = tl::logger::level::info;
        if(logLevelMap.count(workerLogLevel.getValue()))
            options.workerLogLevel = logLevelMap[workerLogLevel.getValue()];
        else
            options.workerLogLevel = options.masterLogLevel;

        if(!options.luaFile.empty() && !fs::is_regular_file(options.luaFile)) {
            std::cerr << "error: " << options.luaFile << " does not exist or is not a file" << std::endl;
            exit(-1);
        }
        if(!options.luaFile.empty() && !fs::is_directory(options.logPath)) {
            std::cerr << "error: " << options.logPath << " does not exist or not a directory" << std::endl;
            exit(-1);
        }

    } catch (TCLAP::ArgException& e) {
        std::cerr << "error: " << e.error() << " for arg " << e.argId()
            << std::endl;
        exit(-1);
    }
}
