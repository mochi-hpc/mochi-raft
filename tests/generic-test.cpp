#include <readline/readline.h>
#include <readline/history.h>
#include <stdlib.h>
#include <filesystem>
#include <thallium.hpp>
#include <mochi-raft.hpp>
#include <tclap/CmdLine.h>
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
    tl::engine   engine;
    raft_id      raftID;
    int          pID;
    tl::endpoint address;

    auto to_string() const {
        std::stringstream stream;
        stream << "WorkerHandle@" << (void*)this
               << " {raftID=" << raftID
               << ", pID=" << pID
               << ", address=" << address
               << "}";
        return stream.str();
    }
};

struct MasterContext {
    tl::engine                                engine;
    sol::state                                lua;
    std::unordered_map<raft_id, WorkerHandle> workers;
    raft_id                                   nextRaftID = 1;
};

static void runMaster(const Options& options) {
    MasterContext master;
    master.engine = tl::engine{options.protocol, THALLIUM_CLIENT_MODE};
    master.engine.set_log_level(options.masterLogLevel);
    master.lua.open_libraries(sol::lib::base);

    master.lua.new_usertype<WorkerHandle>("Worker",
        "address", sol::property([](const WorkerHandle& w) { return static_cast<std::string>(w.address); }),
        "raft_id", sol::readonly(&WorkerHandle::raftID)
    );

    master.lua.set_function("spawn", [&master]() mutable {
        auto raftID = master.nextRaftID++;
        master.workers[raftID] = WorkerHandle{
            master.engine,
            raftID,
            1234,
            master.engine.self()};
        return master.workers[raftID];
    });

    if(options.luaFile.empty()) {
        char* l;
        while((l = readline(">> ")) != nullptr) {
            if(*l) add_history(l);
            std::string line{l};
            while(!line.empty() && !::isalnum(line.front())) line = line.substr(1);
            if(line.rfind("exit", 0) == 0) break;
            std::string r = master.lua.do_string(line);
            while(!r.empty() && r.back() == '\n') r.resize(r.size()-1);
            if(!r.empty()) std::cout << r << '\n';
            free(l);
        }
    } else {
        master.lua.script_file(options.luaFile);
    }
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
