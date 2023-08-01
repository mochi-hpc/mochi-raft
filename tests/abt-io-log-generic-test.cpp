#include <cstdlib>
#include <fstream>
#include <getopt.h>
#include <iostream>
#include <map>
#include <mochi-raft.hpp>
#include <set>
#include <signal.h>
#include <sstream>
#include <string>
#include <sys/types.h>
#include <sys/wait.h>
#include <thallium.hpp>
#include <thallium/serialization/stl/array.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <unistd.h>

namespace tl = thallium;

#define margo_assert(__mid__, __expr__)                                      \
    do {                                                                     \
        if (!(__expr__)) {                                                   \
            margo_critical(__mid__,                                          \
                           "[test] assert failed at %s line %d: " #__expr__, \
                           __FILE__, __LINE__);                              \
            exit(-1);                                                        \
        }                                                                    \
    } while (0)

namespace {
class MyFSM : public mraft::StateMachine {
    friend class WorkerProvider;

  private:
    std::string content;

    void apply(const struct raft_buffer* buf, void** result) override
    {
        std::cerr << "[test] [debug] applying '"
                  << static_cast<char*>(buf->base) << "'\n";
        content.append(static_cast<char*>(buf->base));
    }

    void snapshot(struct raft_buffer* bufs[], unsigned* n_bufs) override
    {
        *bufs   = static_cast<raft_buffer*>(raft_malloc(sizeof(**bufs)));
        *n_bufs = 1;
        (*bufs)[0].base = strdup(content.c_str());
        (*bufs)[0].len  = content.size() + 1;
    }

    void restore(struct raft_buffer* buf) override
    {
        std::cout << "[test] [debug] restoring\n";
        content.clear();
        if (!buf || !buf->len) return;
        std::cout << "content='" << static_cast<char*>(buf->base) << "'\n";
        content.append(std::string(static_cast<char*>(buf->base)));
    }
};

class WorkerProvider : public tl::provider<WorkerProvider> {
  public:
    WorkerProvider(tl::engine& e, uint16_t provider_id)
        : tl::provider<WorkerProvider>(e, 0), myfsm{}, abtlog{provider_id},
          raft(e.get_margo_instance(), provider_id, myfsm, abtlog)
    {
        // raft.enable_tracer(true);
        raft.set_tracer([](const char* file, int line, const char* message) {
            std::cerr << "[" << file << ":" << line << "] " << message
                      << std::endl;
        });

#define DEFINE_WORKER_RPC(__rpc__) define(#__rpc__, &WorkerProvider::__rpc__)
        DEFINE_WORKER_RPC(start);
        DEFINE_WORKER_RPC(bootstrap);
        DEFINE_WORKER_RPC(add);
        DEFINE_WORKER_RPC(assign);
        DEFINE_WORKER_RPC(remove);
        DEFINE_WORKER_RPC(apply);
        DEFINE_WORKER_RPC(get_leader);
        DEFINE_WORKER_RPC(transfer);
        DEFINE_WORKER_RPC(shutdown);
        DEFINE_WORKER_RPC(suspend);
        DEFINE_WORKER_RPC(get_fsm_content);
#undef DEFINE_WORKER_RPC
    }

  private:
    MyFSM           myfsm;
    mraft::AbtIoLog abtlog;
    mraft::Raft     raft;

#define MRAFT_WRAP_CPP_CALL(func, ...)             \
    do {                                           \
        try {                                      \
            raft.func(__VA_ARGS__);                \
        } catch (const mraft::RaftException& ex) { \
            return ex.code();                      \
        }                                          \
        return 0;                                  \
    } while (0)

    int start(void) { MRAFT_WRAP_CPP_CALL(start); }

    int bootstrap(const std::array<mraft::ServerInfo, 1>& serverList)
    {
        MRAFT_WRAP_CPP_CALL(bootstrap, serverList);
    }

    int add(const raft_id& raftId, const std::string& addr)
    {
        MRAFT_WRAP_CPP_CALL(add, raftId, addr.c_str());
    }

    int assign(const raft_id& raftId, const mraft::Role& role)
    {
        MRAFT_WRAP_CPP_CALL(assign, raftId, role);
    }

    int remove(const raft_id& raftId) { MRAFT_WRAP_CPP_CALL(remove, raftId); }

    int apply(const std::string& buffer)
    {
        struct raft_buffer bufs = {
            .base = (void*)buffer.c_str(),
            .len  = buffer.size() + 1, // Include '\0'
        };
        MRAFT_WRAP_CPP_CALL(apply, &bufs, 1U);
    }

    mraft::ServerInfo get_leader(void) const
    {
        margo_trace(get_engine().get_margo_instance(),
                    "[worker] received get_leader");
        mraft::ServerInfo leaderInfo;
        try {
            leaderInfo = raft.get_leader();
        } catch (mraft::RaftException& ex) {
            leaderInfo = (mraft::ServerInfo){
                .id      = 0,
                .address = std::string(""),
            };
        }
        margo_trace(get_engine().get_margo_instance(),
                    "[worker] completed get_leader");
        return leaderInfo;
    }

    int transfer(raft_id transferToId)
    {
        MRAFT_WRAP_CPP_CALL(transfer, transferToId);
    }

    int shutdown(void) const
    {
        margo_trace(get_engine().get_margo_instance(),
                    "[worker] received shutdown");
        try {
            get_engine().finalize();
        } catch (thallium::exception& ex) {
            std::cerr << ex.what() << "\n";
            return 1;
        }
        margo_trace(get_engine().get_margo_instance(),
                    "[worker] completed shutdown");
        return 0;
    }

    int suspend(double timeout_ms) const
    {
        try {
            get_engine().get_progress_pool().make_thread(
                [timeout_ms]() { sleep(timeout_ms); }, tl::anonymous{});
        } catch (thallium::exception& ex) {
            std::cerr << ex.what() << "\n";
            return 1;
        }
        return 0;
    }

    std::string get_fsm_content(void) const { return myfsm.content; }

#undef MRAFT_WRAP_CPP_CALL
};

class Master {
  public:
    Master(tl::engine& e) : engine(e)
    {
#define DEFINE_MASTER_RPC(__rpc__) \
    rpcs.insert(std::make_pair(#__rpc__, engine.define(#__rpc__)))
        DEFINE_MASTER_RPC(start);
        DEFINE_MASTER_RPC(bootstrap);
        DEFINE_MASTER_RPC(add);
        DEFINE_MASTER_RPC(assign);
        DEFINE_MASTER_RPC(remove);
        DEFINE_MASTER_RPC(apply);
        DEFINE_MASTER_RPC(transfer);
        DEFINE_MASTER_RPC(shutdown);
        DEFINE_MASTER_RPC(suspend);
        DEFINE_MASTER_RPC(get_leader);
        DEFINE_MASTER_RPC(get_fsm_content);
#undef DEFINE_MASTER_RPC
    }

    ~Master()
    {
        // Request to all workers to shutdown
        while (!cluster.empty()) shutdownWorker(cluster.cbegin()->first);
        engine.finalize();
    }

    void readInput(std::istream& input)
    {
        std::cout << "==================================================\n";
        // Read from input stream until EOF
        std::string line;
        while (std::getline(input, line)) {
            margo_trace(engine.get_margo_instance(), "[master] read line: '%s'",
                        line.c_str());
            std::istringstream iss(line);
            std::string        command;
            iss >> command;
            if (command == "add") {
                raft_id raftId;
                iss >> raftId;
                addWorker(raftId);
            } else if (command == "remove") {
                raft_id raftId;
                iss >> raftId;
                removeWorker(raftId);
            } else if (command == "shutdown") {
                raft_id raftId;
                iss >> raftId;
                shutdownWorker(raftId);
            } else if (command == "kill") {
                pid_t pid;
                iss >> pid;
                killWorker(pid);
            } else if (command == "apply") {
                std::string data;
                std::getline(iss >> std::ws, data);
                expectedFsmContent.append(data);
                sendDataToRaftCluster(data);
            } else if (command == "sleep") {
                double timeout_ms;
                iss >> timeout_ms;
                margo_thread_sleep(engine.get_margo_instance(), timeout_ms);
            } else if (command == "suspend") {
                raft_id raftId;
                double  timeout_ms;
                iss >> raftId >> timeout_ms;
                suspendWorker(raftId, timeout_ms);
            } else {
                std::cerr << "[master] unrecognized command in line: '" << line
                          << "'... ingoring line\n";
            }
            std::cout << "==================================================\n";
        }
        margo_trace(engine.get_margo_instance(),
                    "[master] finished reading input stream");
    }

  private:
    tl::engine                                  engine;
    std::map<std::string, tl::remote_procedure> rpcs;
    std::set<raft_id>                           seenRaftIds;
    std::map<raft_id, std::string>              cluster;
    std::map<pid_t, raft_id>                    pidToRaftId;
    std::string                                 expectedFsmContent;

    /**
     * Spawn a worker process to add to the raft cluster
     * @param [in] raftId Raft ID to give the spawned worker in the raft cluster
     */
    void addWorker(raft_id raftId)
    {
        int  ret;
        int  pipeFd[2]; // Pipe for worker to communicate self_addr to master
        char workerAddrPtr[256];

        if (pipe(pipeFd) == -1) {
            margo_critical(engine.get_margo_instance(),
                           "failed creating communication pipe");
            std::exit(1);
        }

        pid_t pid = fork();

        if (pid < 0) {
            margo_critical(engine.get_margo_instance(), "failed forking child");
            std::exit(1);
        } else if (pid == 0) {
            // Worker (child) process
            margo_trace(engine.get_margo_instance(),
                        "[worker] spawned with PID: %d", getpid());

            // Close the read end of the pipe
            close(pipeFd[0]);

            // Here, the child has a copy of the parent engine, so we call
            // finalize on it
            engine.finalize();

            // The child creates its own engine
            engine = tl::engine("tcp", THALLIUM_SERVER_MODE);

            // Get the self_addr and resize it to size 256 with '\0'
            std::string self_addr = engine.self();
            self_addr.resize(256, '\0');

            // Create provider
            WorkerProvider* provider = new WorkerProvider(engine, raftId);
            auto            finalize_callback = [provider, raftId]() mutable {
                std::cout << "inside finalize_callback of raftId=" << raftId
                          << "\n";
                delete provider;
                provider = nullptr;
            };
            engine.push_finalize_callback(finalize_callback);

            // Send self_addr to master
            ssize_t _ = write(pipeFd[1], self_addr.c_str(), self_addr.size());
            close(pipeFd[1]);
            margo_trace(engine.get_margo_instance(),
                        "[worker] wrote self address to pipe: %s",
                        self_addr.c_str());

            engine.wait_for_finalize();
            std::exit(0);
        } else if (pid > 0) {
            // Master (parent) process
            // Close the write end of the pipe
            close(pipeFd[1]);

            // Process the self_addr received from the worker
            ssize_t _ = read(pipeFd[0], workerAddrPtr, sizeof(workerAddrPtr));
            close(pipeFd[0]);
            std::string workerAddrStr(workerAddrPtr);
            workerAddrStr.resize(256, '\0');
            margo_trace(engine.get_margo_instance(),
                        "[master] read address from pipe: %s", workerAddrPtr);

            // Get handle for the spawned worker to make RPC requests to worker
            tl::provider_handle handle(engine.lookup(workerAddrStr), 0);

            // If its the first spawned worker, send it an RPC requesting to
            // bootstrap the raft cluster
            if (cluster.size() == 0) {
                margo_trace(engine.get_margo_instance(),
                            "[master] requesting bootstrap RPC to worker: "
                            "id=%llu, address=%s",
                            raftId, workerAddrPtr);
                std::array<mraft::ServerInfo, 1> servers = {mraft::ServerInfo{
                    .id      = raftId,
                    .address = workerAddrStr,
                    .role    = mraft::Role::VOTER,
                }};
                try {
                    ret = rpcs["bootstrap"].on(handle)(servers);
                } catch (const thallium::margo_exception& e) {
                    std::cerr << e.what() << std::endl;
                }
                margo_assert(engine.get_margo_instance(), ret == 0);
            }

            // Send RPC requesting the spawned worked to call mraft_start
            margo_trace(engine.get_margo_instance(),
                        "[master] requesting start RPC to worker: "
                        "id=%llu, address=%s",
                        raftId, workerAddrPtr);
            try {
                ret = rpcs["start"].on(handle)();
            } catch (const thallium::margo_exception& e) {
                std::cerr << e.what() << std::endl;
            }
            margo_assert(engine.get_margo_instance(), ret == 0);

            // If we didn't bootstrap, send an RPC to the leader of the cluster
            // asking to add the spawned worker and assign it to raft voter
            if (cluster.size() == 0) goto end;

            // Find leader of the cluster
            {
                mraft::ServerInfo leader;
                ret = getLeaderInfo(leader, handle);
                margo_assert(engine.get_margo_instance(), ret == 0);
                margo_trace(
                    engine.get_margo_instance(),
                    "[master] found leader of cluster: id=%llu, address=%s",
                    leader.id, leader.address.c_str());

                // Request add RPC to leader
                margo_trace(engine.get_margo_instance(),
                            "[master] requesting start RPC to leader");
                try {
                    ret = rpcs["add"].on(handle)(raftId, workerAddrStr);
                } catch (const thallium::margo_exception& e) {
                    std::cerr << e.what() << std::endl;
                }
                margo_assert(engine.get_margo_instance(), ret == 0);

                // Request assign RPC to leader
                const auto role = mraft::Role::VOTER;
                margo_trace(engine.get_margo_instance(),
                            "requesting assign RPC to leader: assigning "
                            "id=%llu to role=%i",
                            raftId, static_cast<int>(role));
                try {
                    ret = rpcs["assign"].on(handle)(raftId, role);
                } catch (const thallium::margo_exception& e) {
                    std::cerr << e.what() << std::endl;
                }
                margo_assert(engine.get_margo_instance(), ret == 0);
            }
end:
            cluster.insert({raftId, workerAddrStr});
            pidToRaftId.insert({pid, raftId});
            seenRaftIds.insert(raftId);
        }
    }

    void removeWorker(int raftId)
    {
        // Request the worker to remove itself from the cluster
        mraft::ServerInfo   leader;
        tl::provider_handle handle;
        int                 ret = getLeaderInfo(leader, handle);
        margo_assert(engine.get_margo_instance(), ret == 0);

        // If the worker is the leader, it must first transfer leadership
        if (leader.id == raftId) {
            margo_trace(engine.get_margo_instance(),
                        "[master] leadership needs to be transfered before "
                        "removal of id: %llu",
                        raftId);

            raft_id transferToId;
            for (auto it = cluster.cbegin(); it != cluster.cend(); it++) {
                if (it->first != raftId) {
                    transferToId = it->first;
                    break;
                }
            }

            margo_trace(
                engine.get_margo_instance(),
                "[master] requesting transfer RPC from id=%llu to id=%llu",
                raftId, transferToId);
            try {
                ret = rpcs["transfer"].on(handle)(transferToId);
            } catch (const thallium::margo_exception& e) {
                std::cerr << e.what() << std::endl;
            }
            margo_assert(engine.get_margo_instance(), ret == 0);
        }

        // Request the worker to remove itself from the cluster
        std::string workerAddr = cluster[raftId];
        handle = tl::provider_handle(engine.lookup(workerAddr), 0);
        margo_trace(
            engine.get_margo_instance(),
            "[master] requesting remove RPC to worker: id=%llu, address=%s",
            raftId, workerAddr.c_str());
        try {
            ret = rpcs["remove"].on(handle)(raftId);
        } catch (const thallium::margo_exception& e) {
            std::cerr << e.what() << std::endl;
        }
        margo_assert(engine.get_margo_instance(), ret == 0);

        // Request worker to remove the shutdown process from the cluster
        margo_trace(
            engine.get_margo_instance(),
            "[master] requesting shutdown RPC to worker: id=%llu, address=%s",
            raftId, workerAddr.c_str());
        try {
            ret = rpcs["shutdown"].on(handle)();
        } catch (const thallium::margo_exception& e) {
            std::cerr << e.what() << std::endl;
        }
        margo_assert(engine.get_margo_instance(), ret == 0);

        cluster.erase(raftId);
    }

    void shutdownWorker(int raftId)
    {
        // Request the worker to shutdown
        int                 ret;
        std::string         workerAddr = cluster[raftId];
        tl::provider_handle handle
            = tl::provider_handle(engine.lookup(workerAddr), 0);

        margo_trace(
            engine.get_margo_instance(),
            "[master] requesting shutdown RPC to worker: id=%llu, address=%s",
            raftId, workerAddr.c_str());
        try {
            ret = rpcs["shutdown"].on(handle)();
            for (const auto& pair : pidToRaftId) {
                if (pair.second == raftId) {
                    std::cout << "[test] [debug] waiting on PID=" << pair.first
                              << "\n";
                    waitpid(pair.first, NULL, 0);
                    std::cout << "[test] [debug] finished waiting on PID="
                              << pair.first << "\n";
                }
            }
        } catch (const thallium::margo_exception& e) {
            std::cerr << e.what() << std::endl;
        }
        margo_assert(engine.get_margo_instance(), ret == 0);

        // Find leader of the cluster and ask it to remove the dead worker.
        // We need to wrap this in a while loop because one of the other workers
        // may still believe that the dead process is still leader and return it
        // as the leader, which will cause a timeout.
        // So we keep asking until the call to remove succeeds.
        while (true) {
            // TODO: integrate with Matthieu's mraft timeout system
            mraft::ServerInfo leader;
            ret = getLeaderInfo(leader, handle);
            // margo_assert(engine.get_margo_instance(), ret == 0);
            margo_trace(engine.get_margo_instance(),
                        "[master] found leader of cluster: id=%llu, address=%s",
                        leader.id, leader.address.c_str());
            if (ret != 0) {
                if (cluster.size() == 1)
                    break;
                margo_trace(engine.get_margo_instance(),
                            "[master] no leader found... trying again");
                margo_thread_sleep(engine.get_margo_instance(), 2000);
                continue;
            }

            margo_trace(
                engine.get_margo_instance(),
                "[master] requesting remove RPC to leader: remove id=%llu",
                raftId);

            const auto start_time = std::chrono::steady_clock::now();
            const auto end_time
                = start_time + std::chrono::duration<double, std::milli>(500);

            const auto current_timeout_ms
                = std::chrono::duration<double, std::micro>(
                    end_time - std::chrono::steady_clock::now());

            try {
                ret = rpcs["remove"]
                          .on(handle)
                          .timed_async(current_timeout_ms, raftId)
                          .wait();
                margo_assert(engine.get_margo_instance(), ret == 0);
                break;
            } catch (thallium::timeout& e) {
                margo_trace(
                    engine.get_margo_instance(),
                    "[master] timeout on remove RPC to id=%llu, address=%s",
                    leader.id, leader.address.c_str());
                margo_thread_sleep(engine.get_margo_instance(), 2000);
            }
        }

        cluster.erase(raftId);
    }

    void killWorker(pid_t workerPid)
    {
        int ret;

        // Kill the worker
        kill(workerPid, SIGTERM);

        // Request leader to remove the killed worker from the cluster
        raft_id killedWorkerRaftId = pidToRaftId[workerPid];

        // Find leader of the cluster and ask it to remove the dead worker.
        // We need to wrap this in a while loop because one of the other workers
        // may still believe that the dead process is still leader and return it
        // as the leader, which will cause a timeout.
        // So we keep asking until the call to remove succeeds.
        while (true) {
            // TODO: integrate with Matthieu's mraft timeout system
            mraft::ServerInfo   leader;
            tl::provider_handle handle;
            ret = getLeaderInfo(leader, handle);
            margo_assert(engine.get_margo_instance(), ret == 0);
            margo_trace(engine.get_margo_instance(),
                        "[master] found leader of cluster: id=%llu, address=%s",
                        leader.id, leader.address.c_str());

            margo_trace(
                engine.get_margo_instance(),
                "[master] requesting remove RPC to leader: remove id=%llu",
                killedWorkerRaftId);

            const auto start_time = std::chrono::steady_clock::now();
            const auto end_time
                = start_time + std::chrono::duration<double, std::milli>(500);

            const auto current_timeout_ms
                = std::chrono::duration<double, std::micro>(
                    end_time - std::chrono::steady_clock::now());

            try {
                ret = rpcs["remove"]
                          .on(handle)
                          .timed_async(current_timeout_ms, killedWorkerRaftId)
                          .wait();
                margo_assert(engine.get_margo_instance(), ret == 0);
                break;
            } catch (thallium::timeout& e) {
                margo_trace(
                    engine.get_margo_instance(),
                    "[master] timeout on remove RPC to id=%llu, address=%s",
                    leader.id, leader.address.c_str());
                margo_thread_sleep(engine.get_margo_instance(), 2000);
            }
        }

        pidToRaftId.erase(workerPid);
        cluster.erase(killedWorkerRaftId);
    }

    void sendDataToRaftCluster(const std::string& data)
    {
        int ret;

        // Find leader of the cluster
        mraft::ServerInfo   leader;
        tl::provider_handle handle;
        ret = getLeaderInfo(leader, handle);
        margo_assert(engine.get_margo_instance(), ret == 0);
        margo_trace(engine.get_margo_instance(),
                    "[master] found leader of cluster: id=%llu, address=%s",
                    leader.id, leader.address.c_str());

        margo_trace(engine.get_margo_instance(),
                    "[master] requesting apply RPC to leader: apply data='%s'",
                    data.c_str());
        try {
            ret = rpcs["apply"].on(handle)(data);
        } catch (const thallium::margo_exception& e) {
            std::cerr << e.what() << std::endl;
        }
        margo_assert(engine.get_margo_instance(), ret == 0);
    }

    void suspendWorker(raft_id raftId, double timeout_ms)
    {
        int                 ret;
        std::string         workerAddr = cluster[raftId];
        tl::provider_handle handle(engine.lookup(workerAddr), 0);
        margo_trace(
            engine.get_margo_instance(),
            "[master] requesting suspend RPC to worker id=%llu: timeout_ms=%lf",
            raftId, timeout_ms);
        try {
            ret = rpcs["suspend"].on(handle)(timeout_ms);
        } catch (const thallium::margo_exception& e) {
            std::cerr << e.what() << std::endl;
        }
        margo_assert(engine.get_margo_instance(), ret == 0);
    }

    int getLeaderInfo(mraft::ServerInfo& leader, tl::provider_handle& handle)
    {
        for (auto it = cluster.cbegin(); it != cluster.cend(); it++) {
            margo_trace(engine.get_margo_instance(),
                        "[master] requesting get_leader RPC to worker: "
                        "id=%llu, address=%s",
                        it->first, it->second.c_str());
            handle = tl::provider_handle(engine.lookup(it->second), 0);

            // TODO: integrate with Matthieu's mraft timeout system
            const auto start_time = std::chrono::steady_clock::now();
            const auto end_time
                = start_time + std::chrono::duration<double, std::milli>(500);

            const auto current_timeout_ms
                = std::chrono::duration<double, std::micro>(
                    end_time - std::chrono::steady_clock::now());
            try {
                leader
                    = rpcs["get_leader"].on(handle).timed(current_timeout_ms);
                if (leader.id != 0) return 0;
            } catch (thallium::timeout& e) {
                margo_trace(
                    engine.get_margo_instance(),
                    "[master] timeout on get_leader RPC to id=%llu, address=%s",
                    it->first, it->second.c_str());
            } catch (const thallium::exception& e) {
                margo_trace(engine.get_margo_instance(),
                            "[master] getLeader request threw exception...");
                std::cerr << e.what() << std::endl;
            }
        }
        return -1;
    };
};

/**
 * @brief Parse the command line arguments
 *
 * @param [in] argc argument count
 * @param [in] argv command line arguments
 * @param [out] filename pointer to store the filename given in the command
 * line arguments
 * @return 0 on success, -1 on failure
 */
int parseCommandLineArgs(int argc, char* argv[], char** filename)
{
    static const char* const shortOpts = "f:";
    static const option      longOpts[]
        = {{"file", required_argument, nullptr, 'f'}, {nullptr, 0, nullptr, 0}};

    int option;
    while ((option = getopt_long(argc, argv, shortOpts, longOpts, nullptr))
           != -1) {
        switch (option) {
        case 'f':
            *filename = optarg;
            break;
        case '?':
        default:
            // Handle unrecognized options or missing arguments
            std::cerr << "usage: " << argv[0]
                      << " [-f <file> | --file=<file>]\n";
            std::exit(1);
        }
    }
    return 0;
}
} // namespace

int main(int argc, char* argv[])
{
    tl::engine engine("tcp", THALLIUM_SERVER_MODE);
    margo_set_log_level(engine.get_margo_instance(), MARGO_LOG_TRACE);

    // Parse command-line arguments
    char* filename = nullptr;
    parseCommandLineArgs(argc, argv, &filename);
    std::ifstream inputFile;
    std::istream* input
        = (!filename) ? &std::cin : (inputFile.open(filename), &inputFile);

    Master master(engine);

    margo_trace(engine.get_margo_instance(), "[master] reading input stream");
    master.readInput(*input);

    margo_trace(engine.get_margo_instance(), "[master] exiting program");
    return 0;
}
