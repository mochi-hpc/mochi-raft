#include <readline/readline.h>
#include <readline/history.h>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/wait.h>
#include <filesystem>
#include <string_view>
#include <map>
#include <thallium.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/string.hpp>
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
// Worker structure
// ----------------------------------------------------------------------------

struct FSM : public mraft::StateMachine {

    FSM(raft_id id)
    : id(id) {}

    void apply(const struct raft_buffer* buf, void** result) override {
        content.append(std::string_view{(const char*)buf->base, buf->len});
    }

    void snapshot(struct raft_buffer* bufs[], unsigned* n_bufs) override {
        *bufs   = static_cast<raft_buffer*>(raft_malloc(sizeof(**bufs)));
        *n_bufs = 1;
        (*bufs)[0].base = strdup(content.c_str());
        (*bufs)[0].len  = content.size() + 1;
    }

    void restore(struct raft_buffer* buf) override {
        content.assign(std::string_view{(const char*)buf->base, buf->len});
    }

    raft_id     id;
    std::string content;

};

struct Worker : public tl::provider<Worker> {

    Worker(tl::engine engine, raft_id raftID, const char* config = nullptr)
    : tl::provider<Worker>(engine, 0)
    , fsm{raftID}
    , abtlog{id, ABT_IO_INSTANCE_NULL, config, engine.get_margo_instance()}
    , raft{engine.get_margo_instance(), raftID, fsm, abtlog}
    , id{raftID} {
        #define DEFINE_WORKER_RPC(__rpc__) define("mraft_test_" #__rpc__, &Worker::__rpc__)
        DEFINE_WORKER_RPC(bootstrap);
        DEFINE_WORKER_RPC(start);
        DEFINE_WORKER_RPC(add);
        DEFINE_WORKER_RPC(assign);
        DEFINE_WORKER_RPC(remove);
        DEFINE_WORKER_RPC(apply);
        DEFINE_WORKER_RPC(get_leader);
        DEFINE_WORKER_RPC(transfer);
        DEFINE_WORKER_RPC(suspend);
        DEFINE_WORKER_RPC(get_fsm_content);
        #undef DEFINE_WORKER_RPC
    }

    #define WRAP_CALL(func, ...)                   \
    do {                                           \
        try {                                      \
            raft.func(__VA_ARGS__);                \
        } catch (const mraft::RaftException& ex) { \
            return ex.code();                      \
        }                                          \
        return 0;                                  \
    } while (0)

    int bootstrap(const std::vector<mraft::ServerInfo>& servers) {
        WRAP_CALL(bootstrap, servers);
    }

    int start() {
        WRAP_CALL(start);
    }

    int add(const raft_id& raftId, const std::string& addr) {
        WRAP_CALL(add, raftId, addr.c_str());
    }

    int assign(const raft_id& raftId, const mraft::Role& role) {
        WRAP_CALL(assign, raftId, role);
    }

    int remove(const raft_id& raftId) {
        WRAP_CALL(remove, raftId);
    }

    int apply(const std::string& buffer) {
        struct raft_buffer bufs = {
            .base = (void*)buffer.c_str(),
            .len  = buffer.size()
        };
        WRAP_CALL(apply, &bufs, 1U);
    }

    mraft::ServerInfo get_leader() const {
        mraft::ServerInfo leaderInfo{0, ""};
        try {
            leaderInfo = raft.get_leader();
        } catch (mraft::RaftException& ex) {}
        return leaderInfo;
    }

    int transfer(raft_id transferToId) {
        WRAP_CALL(transfer, transferToId);
    }

    void suspend(const tl::request& req, unsigned msec) const {
        req.respond(0);
        usleep(msec*1000);
    }

    const std::string& get_fsm_content() const {
        return fsm.content;
    }

    #undef WRAP_CALL

    raft_id         id;
    FSM             fsm;
    mraft::AbtIoLog abtlog;
    mraft::Raft     raft;
};

// ----------------------------------------------------------------------------
// Master functions and structure definitions
// ----------------------------------------------------------------------------

struct WorkerHandle {
    raft_id             raftID = 0;
    pid_t               pID    = -1;
    tl::provider_handle address;

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
    tl::engine                      engine;
    tl::remote_procedure            rpc_bootstrap;


    Cluster(tl::engine engine)
    : engine(engine)
    , rpc_bootstrap(engine.define("mraft_test_bootstrap"))
    {}

    auto to_string() const {
        std::stringstream stream;
        for(const auto& [raftID, worker] : workers) {
            stream << "- " << worker.to_string() << "\n";
        }
        auto result = stream.str();
        if(!result.empty()) result.resize(result.size()-1);
        return result;
    }

    void bootstrap() const {
        std::vector<mraft::ServerInfo> servers;
        servers.reserve(workers.size());
        for(auto& [id, worker] : workers)
            servers.push_back({id, worker.address});
        std::vector<tl::async_response> responses;
        responses.reserve(workers.size());
        for(auto& [id, worker] : workers)
            responses.push_back(rpc_bootstrap.on(worker.address).async(servers));
        for(auto& response : responses)
            response.wait();
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

    auto raftID = master.nextRaftID++;

    pid_t pid = fork();
    if (pid < 0) {
        perror("Fork failed");
        master.nextRaftID--;
        return std::nullopt;

    } else if (pid == 0) {
        // Child process
        close(pipefd[0]);
        master.engine.finalize();

        auto engine = tl::engine(options.protocol, MARGO_SERVER_MODE);
        engine.enable_remote_shutdown();

        auto worker = new Worker{engine, raftID};
        engine.push_finalize_callback([worker](){ delete worker; });

        auto address = static_cast<std::string>(engine.self());
        address.resize(1024);
        ssize_t _ = write(pipefd[1], address.data(), 1024);
        close(pipefd[1]);

        engine.wait_for_finalize();
        std::exit(0);

    } else {
        // Parent process
        close(pipefd[1]);
        char address[1024];
        ssize_t bytes_read = read(pipefd[0], address, 1024);
        close(pipefd[0]);
        master.cluster->workers[raftID] = WorkerHandle{
            raftID, pid, tl::provider_handle{master.engine.lookup(address), 0}
        };
        return master.cluster->workers[raftID];
    }
}

static void runMaster(const Options& options) {
    MasterContext master;
    master.engine = tl::engine{options.protocol, THALLIUM_CLIENT_MODE};
    master.engine.set_log_level(options.masterLogLevel);
    master.lua.open_libraries(sol::lib::base, sol::lib::string, sol::lib::math);
    master.cluster = std::make_shared<Cluster>(master.engine);

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

    // initial cluster setup
    for(size_t i = 0; i < options.initClusterSize; ++i) {
        auto w = spawnWorker(master, options);
        if(!w) {
            margo_critical(master.engine.get_margo_instance(),
                "Could not spawn initial worker %lu", i);
            goto cleanup;
        }
    }
    master.cluster->bootstrap();

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

cleanup:
    for(const auto& [raftID, worker] : master.cluster->workers) {
        master.engine.shutdown_remote_engine(worker.address);
        waitpid(worker.pID, nullptr, 0);
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
        if(options.initClusterSize == 0) {
            std::cerr << "error: cannot start with a cluster of size 0" << std::endl;
            exit(-1);
        }

    } catch (TCLAP::ArgException& e) {
        std::cerr << "error: " << e.error() << " for arg " << e.argId()
            << std::endl;
        exit(-1);
    }
}
