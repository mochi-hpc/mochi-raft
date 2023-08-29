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
#include <mochi-raft-test.h>
#include <tclap/CmdLine.h>
#define SOL_ALL_SAFETIES_ON 1
#include <sol/sol.hpp>
#include <chrono>

namespace fs = std::filesystem;
namespace tl = thallium;

template<typename T>
using sptr = std::shared_ptr<T>;

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
        std::unique_lock<tl::mutex> lock{content_mtx};
        content.append(std::string_view{(const char*)buf->base, buf->len});
    }

    void snapshot(struct raft_buffer* bufs[], unsigned* n_bufs) override {
        std::unique_lock<tl::mutex> lock{content_mtx};
        *bufs   = static_cast<raft_buffer*>(raft_malloc(sizeof(**bufs)));
        *n_bufs = 1;
        (*bufs)[0].base = strdup(content.c_str());
        (*bufs)[0].len  = content.size() + 1;
    }

    void restore(struct raft_buffer* buf) override {
        std::unique_lock<tl::mutex> lock{content_mtx};
        content.assign(std::string_view{(const char*)buf->base, buf->len});
    }

    raft_id           id;
    std::string       content;
    mutable tl::mutex content_mtx;

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
        DEFINE_WORKER_RPC(barrier);
        DEFINE_WORKER_RPC(isolate);
        DEFINE_WORKER_RPC(get_fsm_content);
        #undef DEFINE_WORKER_RPC
    }

    #define WRAP_CALL(func, ...)                          \
    do {                                                  \
        try {                                             \
            raft.func(__VA_ARGS__);                       \
        } catch (const mraft::RaftException& ex) {        \
            margo_error(get_engine().get_margo_instance(),\
                "[worker:%lu] %s failed: %s",             \
                id, #func, raft_strerror(ex.code()));     \
            return ex.code();                             \
        }                                                 \
        return 0;                                         \
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

    int barrier() {
        WRAP_CALL(barrier);
    }

    int isolate(bool flag) {
        auto raft_io = raft.get_raft_io();
        return mraft_io_simulate_dead(raft_io, flag);
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

    std::string get_fsm_content() const {
        std::unique_lock<tl::mutex> lock{fsm.content_mtx};
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

static inline auto& operator<<(std::ostream& stream, const mraft::Role& role) {
    switch(role) {
    case mraft::Role::SPARE:
        stream << "SPARE"; break;
    case mraft::Role::VOTER:
        stream << "VOTER"; break;
    case mraft::Role::STANDBY:
        stream << "STANDBY"; break;
    default:
        stream << "???";
    }
    return stream;
}

struct WorkerHandle {

    raft_id             raftID = 0;
    pid_t               pID    = -1;
    tl::provider_handle address;
    mraft::Role         knownRole = mraft::Role::SPARE;
    bool                knownRunning = false;

    WorkerHandle() = default;

    WorkerHandle(raft_id id, pid_t pid, tl::provider_handle addr)
    : raftID(id)
    , pID(pid)
    , address(addr) {}

    auto to_string() const {
        std::stringstream stream;
        auto addr = (pID == -1) ? std::string{"?"} : static_cast<std::string>(address);
        stream << "WorkerHandle"
               << " {raftID=" << raftID
               << ", pID=" << pID
               << ", address=" << addr
               << ", role=" << knownRole
               << ", started=" << std::boolalpha << knownRunning
               << "}";
        return stream.str();
    }

    bool operator==(const WorkerHandle& other) const {
        return raftID == other.raftID
            && pID == other.pID
            && address == other.address;
    }
};

struct Cluster {

    std::map<raft_id, sptr<WorkerHandle>> workers;
    tl::engine                            engine;
    tl::remote_procedure                  rpc_bootstrap;
    tl::remote_procedure                  rpc_start;
    tl::remote_procedure                  rpc_apply;
    tl::remote_procedure                  rpc_get_leader;
    tl::remote_procedure                  rpc_get_fsm_content;
    tl::remote_procedure                  rpc_barrier;
    tl::remote_procedure                  rpc_assign;
    tl::remote_procedure                  rpc_transfer;
    tl::remote_procedure                  rpc_add;
    tl::remote_procedure                  rpc_remove;
    tl::remote_procedure                  rpc_suspend;
    tl::remote_procedure                  rpc_isolate;

    Cluster(tl::engine engine)
    : engine(engine)
    , rpc_bootstrap(engine.define("mraft_test_bootstrap"))
    , rpc_start(engine.define("mraft_test_start"))
    , rpc_apply(engine.define("mraft_test_apply"))
    , rpc_get_leader(engine.define("mraft_test_get_leader"))
    , rpc_get_fsm_content(engine.define("mraft_test_get_fsm_content"))
    , rpc_barrier(engine.define("mraft_test_barrier"))
    , rpc_assign(engine.define("mraft_test_assign"))
    , rpc_transfer(engine.define("mraft_test_transfer"))
    , rpc_add(engine.define("mraft_test_add"))
    , rpc_remove(engine.define("mraft_test_remove"))
    , rpc_suspend(engine.define("mraft_test_suspend"))
    , rpc_isolate(engine.define("mraft_test_isolate"))
    {}

    auto to_string() const {
        std::stringstream stream;
        for(const auto& [raftID, worker] : workers) {
            stream << "- " << worker->to_string() << "\n";
        }
        auto result = stream.str();
        if(!result.empty()) result.resize(result.size()-1);
        return result;
    }

    void bootstrap() {
        std::vector<mraft::ServerInfo> servers;
        servers.reserve(workers.size());
        for(const auto& [id, worker] : workers)
            servers.push_back({id, worker->address, mraft::Role::VOTER});
        std::vector<tl::async_response> responses;
        responses.reserve(workers.size());
        for(const auto& [id, worker] : workers)
            responses.push_back(rpc_bootstrap.on(worker->address).async(servers));
        for(auto& response : responses)
            response.wait();
        for(auto& [id, worker] : workers)
            worker->knownRole = mraft::Role::VOTER;
    }

    void start() {
        std::vector<tl::async_response> responses;
        responses.reserve(workers.size());
        for(auto& [id, worker] : workers) {
            if(worker->knownRunning) continue;
            responses.push_back(rpc_start.on(worker->address).async());
        }
        for(auto& response : responses)
            response.wait();
        for(auto& [id, worker] : workers)
            worker->knownRunning = true;
    }
};

struct MasterContext {
    tl::engine               engine;
    sol::state               lua;
    std::shared_ptr<Cluster> cluster;
    raft_id                  nextRaftID = 1;
};

static sptr<WorkerHandle> spawnWorker(MasterContext& master, const Options& options) {
    int pipefd[2];
    if (pipe(pipefd) == -1) {
        perror("Pipe creation failed");
        return nullptr;
    }

    auto raftID = master.nextRaftID++;

    pid_t pid = fork();
    if (pid < 0) {
        perror("Fork failed");
        master.nextRaftID--;
        return nullptr;

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
        master.cluster->workers[raftID] = std::make_shared<WorkerHandle>(
            raftID, pid, tl::provider_handle{master.engine.lookup(address), 0});
        return master.cluster->workers[raftID];
    }
}

static void runMaster(const Options& options) {
    MasterContext master;
    master.engine = tl::engine{options.protocol, THALLIUM_CLIENT_MODE};
    master.engine.set_log_level(options.masterLogLevel);
    master.lua.open_libraries(sol::lib::base, sol::lib::string, sol::lib::math);
    master.cluster = std::make_shared<Cluster>(master.engine);

    master.lua.new_enum("Role",
        "STANDBY", mraft::Role::STANDBY,
        "VOTER",   mraft::Role::VOTER,
        "SPARE",   mraft::Role::SPARE);

    master.lua.new_usertype<Cluster>("Cluster",
        sol::meta_function::index, [](Cluster& cluster, raft_id id) -> std::shared_ptr<WorkerHandle> {
            if(cluster.workers.count(id) == 0) return nullptr;
            return cluster.workers[id];
        },
        "spawn", [&master, &options]() mutable {
            return spawnWorker(master, options);
        }
    );

    master.lua["cluster"] = master.cluster;

    using namespace std::literals::chrono_literals;

    master.lua.new_usertype<WorkerHandle>("Worker",
        "address", sol::property([](const WorkerHandle& w) { return static_cast<std::string>(w.address); }),
        "raft_id", sol::readonly(&WorkerHandle::raftID),
        // apply command (asks the worker to apply a command)
        "apply", [rpc=master.cluster->rpc_apply, mid=master.engine.get_margo_instance()]
                 (const WorkerHandle& w, const std::string& command) {
            if(!w.knownRunning) {
                margo_error(mid, "[master] worker is not running");
                return false;
            }
            try {
                rpc.on(w.address).timed(1s, command);
                return true;
            } catch(tl::timeout& ex) {
                margo_error(mid, "[master] apply timed out");
            } catch(const std::exception& ex) {
                margo_error(mid, "[master] apply failed: %s", ex.what());
            }
            return false;
        },
        // assign (asks the worker to assign another worker the specified role)
        "assign", [rpc=master.cluster->rpc_assign, mid=master.engine.get_margo_instance()]
                 (const WorkerHandle& w, WorkerHandle& target, mraft::Role role) {
            if(!w.knownRunning) {
                margo_error(mid, "[master] worker is not running");
                return false;
            }
            try {
                rpc.on(w.address).timed(1s, target.raftID, role);
                target.knownRole = role;
                return true;
            } catch(tl::timeout& ex) {
                margo_error(mid, "[master] assign timed out");
            } catch(const std::exception& ex) {
                margo_error(mid, "[master] assign failed: %s", ex.what());
            }
            return false;
        },
        // add (asks the worker to ask the leader to add the given process)
        "add", [rpc=master.cluster->rpc_add, mid=master.engine.get_margo_instance()]
                 (const WorkerHandle& w, const WorkerHandle& toAdd) {
            if(!w.knownRunning) {
                margo_error(mid, "[master] worker is not running");
                return false;
            }
            try {
                rpc.on(w.address).timed(1s,
                    toAdd.raftID, static_cast<std::string>(toAdd.address));
                return true;
            } catch(tl::timeout& ex) {
                margo_error(mid, "[master] add timed out");
            } catch(const std::exception& ex) {
                margo_error(mid, "[master] add failed: %s", ex.what());
            }
            return false;
        },
        // remove (asks the worker to ask the leader to remove the given process)
        "remove", [rpc=master.cluster->rpc_remove, mid=master.engine.get_margo_instance()]
                 (const WorkerHandle& w, const WorkerHandle& toRemove) {
            if(!w.knownRunning) {
                margo_error(mid, "[master] worker is not running");
                return false;
            }
            try {
                rpc.on(w.address).timed(1s, toRemove.raftID);
                return true;
            } catch(tl::timeout& ex) {
                margo_error(mid, "[master] remove timed out");
            } catch(const std::exception& ex) {
                margo_error(mid, "[master] remove failed: %s", ex.what());
            }
            return false;
        },
        // start (start the worker, if it is not started already)
        "start", [&master, rpc=master.cluster->rpc_start, mid=master.engine.get_margo_instance()]
                 (WorkerHandle& w) {
            if(w.knownRunning) {
                margo_error(mid, "[master] worker is already running");
                return false;
            }
            try {
                rpc.on(w.address).timed(1s);
                w.knownRunning = true;
                return true;
            } catch(tl::timeout& ex) {
                margo_error(mid, "[master] start timed out");
            } catch(const std::exception& ex) {
                margo_error(mid, "[master] start failed: %s", ex.what());
            }
            return false;
        },
        // transfer (asks the worker to request leadership be transferred to another worker)
        "transfer", [rpc=master.cluster->rpc_transfer, mid=master.engine.get_margo_instance()]
                 (const WorkerHandle& w, raft_id id) {
            if(!w.knownRunning) {
                margo_error(mid, "[master] worker is not running");
                return false;
            }
            try {
                rpc.on(w.address).timed(1s, id);
                return true;
            } catch(tl::timeout& ex) {
                margo_error(mid, "[master] transfer timed out");
            }  catch(const std::exception& ex) {
                margo_error(mid, "[master] transfer failed: %s", ex.what());
            }
            return false;
        },
        // barrier (wait for the commands to be committed)
        "barrier", [rpc=master.cluster->rpc_barrier, mid=master.engine.get_margo_instance()]
                 (const WorkerHandle& w) -> bool {
            if(!w.knownRunning) {
                margo_error(mid, "[master] worker is not running");
                return false;
            }
            try {
                rpc.on(w.address).timed(1s);
                return true;
            } catch(tl::timeout& ex) {
                margo_error(mid, "[master] barrier timed out");
            } catch(const std::exception& ex) {
                margo_error(mid, "[master] barrier failed: %s", ex.what());
            }
            return false;
        },
        // suspend (put the process to sleep for the target number of milliseconds)
        "suspend", [rpc=master.cluster->rpc_suspend, mid=master.engine.get_margo_instance()]
                 (const WorkerHandle& w, unsigned msec) -> bool {
            if(!w.knownRunning) {
                margo_error(mid, "[master] worker is not running");
                return false;
            }
            try {
                rpc.on(w.address).timed(1s, msec);
                return true;
            } catch(tl::timeout& ex) {
                margo_error(mid, "[master] suspend timed out");
            } catch(const std::exception& ex) {
                margo_error(mid, "[master] suspend failed: %s", ex.what());
            }
            return false;
        },
        // isolate (isolate the process by making it ignore all RPCs)
        "isolate", [rpc=master.cluster->rpc_isolate, mid=master.engine.get_margo_instance()]
                 (const WorkerHandle& w, bool flag) -> bool {
            if(!w.knownRunning) {
                margo_error(mid, "[master] worker is not running");
                return false;
            }
            try {
                rpc.on(w.address).timed(1s, flag);
                return true;
            } catch(tl::timeout& ex) {
                margo_error(mid, "[master] isolate timed out");
            } catch(const std::exception& ex) {
                margo_error(mid, "[master] isolate failed: %s", ex.what());
            }
            return false;
        },
        // get_fsm_content (asks the worker for the content of its FSM)
        "get_fsm_content", [rpc=master.cluster->rpc_get_fsm_content, mid=master.engine.get_margo_instance()]
                 (const WorkerHandle& w) -> std::optional<std::string> {
            if(!w.knownRunning) {
                margo_error(mid, "[master] worker is not running");
                return std::nullopt;
            }
            try {
                return static_cast<std::string>(rpc.on(w.address).timed(1s));
            } catch(tl::timeout& ex) {
                margo_error(mid, "[master] get_fsm_content timed out");
            } catch(const std::exception& ex) {
                margo_error(mid, "[master] get_fsm_content failed: %s", ex.what());
            }
            return std::nullopt;
        },
        // get_leader (asks the worker who the leader currently is)
        "get_leader", [&master, rpc=master.cluster->rpc_get_leader, mid=master.engine.get_margo_instance()]
                 (const WorkerHandle& w) -> std::shared_ptr<WorkerHandle> {
            if(!w.knownRunning) {
                margo_error(mid, "[master] worker is not running");
                return nullptr;
            }
            try {
                mraft::ServerInfo info = rpc.on(w.address).timed(1s);
                auto& workers = master.cluster->workers;
                auto it = workers.find(info.id);
                if(it == workers.end()) return nullptr;
                return it->second;
            } catch(tl::timeout& ex) {
                margo_error(mid, "[master] get_leader timed out");
            } catch(const std::exception& ex) {
                margo_error(mid, "[master] get_leader failed: %s", ex.what());
            }
            return nullptr;
        },
        // shutdown command (sends a remote shutdown RPC to the worker's margo instance
        "shutdown", [&master](WorkerHandle& self) mutable {
            static WorkerHandle none;
            auto& workers = master.cluster->workers;
            auto it = workers.find(self.raftID);
            if(it == workers.end()) {
                auto mid = master.engine.get_margo_instance();
                margo_error(mid, "[master] worker not found in the cluster");
                return;
            }
            master.engine.shutdown_remote_engine(it->second->address);
            while(waitpid(self.pID, NULL, WNOHANG) != self.pID) {
                usleep(100);
            }
            workers.erase(it);
            self = none;
        },
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
            self.address = tl::endpoint();
            self.pID = -1;
            workers.erase(it);
        }
    );

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
    master.cluster->start();

    // execution of lua code
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
        master.engine.shutdown_remote_engine(worker->address);
        waitpid(worker->pID, nullptr, 0);
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
