#include <cstdlib>
#include <fstream>
#include <getopt.h>
#include <iostream>
#include <map>
#include <mochi-raft.hpp>
#include <signal.h>
#include <sstream>
#include <string>
#include <sys/types.h>
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

#define worker_trace(__message__)                                \
    do {                                                         \
        std::cerr << "[trace] [worker] " << __message__ << "\n"; \
    } while (0)

#define master_trace(__message__)                                \
    do {                                                         \
        std::cerr << "[trace] [master] " << __message__ << "\n"; \
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
        content.clear();
        content.append(std::string(static_cast<char*>(buf->base)));
    }
};

class WorkerProvider : public tl::provider<WorkerProvider> {
  public:
    WorkerProvider(tl::engine& e, uint16_t provider_id)
        : tl::provider<WorkerProvider>(e, provider_id), myfsm{}, mlog{},
          raft(e.get_margo_instance(), provider_id, myfsm, mlog, provider_id)
    {
#define DEFINE_WORKER_RPC(__rpc__) define(#__rpc__, &WorkerProvider::__rpc__)
        DEFINE_WORKER_RPC(start);
        DEFINE_WORKER_RPC(bootstrap);
        DEFINE_WORKER_RPC(add);
        DEFINE_WORKER_RPC(assign);
        DEFINE_WORKER_RPC(remove);
        DEFINE_WORKER_RPC(apply);
        DEFINE_WORKER_RPC(get_leader);
        DEFINE_WORKER_RPC(shutdown);
        DEFINE_WORKER_RPC(suspend);
        DEFINE_WORKER_RPC(get_fsm_content);
#undef DEFINE_WORKER_RPC
    }

  private:
    MyFSM            myfsm;
    mraft::MemoryLog mlog;
    mraft::Raft      raft;

    void start(void)
    {
        worker_trace("received start RPC request from master");
        raft.start();
        worker_trace("completed start RPC request");
    }

    void bootstrap(const std::array<mraft::ServerInfo, 1>& serverList)
    {
        worker_trace("received bootstrap RPC request from master");
        raft.bootstrap(serverList);
        worker_trace("completed bootstrap RPC request");
    }

    void add(const raft_id& raftId, const std::string& addr)
    {
        worker_trace("received add(raft_id=" << raftId << ", addr=" << addr
                                             << ") RPC request from master");
        raft.add(raftId, addr.c_str());
        worker_trace("completed add RPC request");
    }

    void assign(const raft_id& raftId, const mraft::Role& role)
    {
        worker_trace("received assign(raft_id=" << raftId << ", role="
                                                << static_cast<int>(role)
                                                << ") RPC request from master");
        raft.assign(raftId, role);
        worker_trace("completed assign RPC request");
    }

    void remove(const raft_id& raftId)
    {
        worker_trace("received remove(raft_id=" << raftId
                                                << ") RPC request from master");
        raft.remove(raftId);
        worker_trace("completed remove RPC request");
    }

    void apply(const std::string& buffer)
    {
        worker_trace("received apply(buffer=" << buffer
                                              << ") RPC request from master");
        struct raft_buffer bufs = {
            .base = (void*)buffer.c_str(),
            .len  = buffer.size() + 1, // Include '\0'
        };
        raft.apply(&bufs, 1U);
        worker_trace("completed apply RPC request");
    }

    mraft::ServerInfo get_leader(void) const
    {
        worker_trace("received get_leader RPC request from master");
        mraft::ServerInfo leader = raft.get_leader();
        worker_trace("completed get_leader RPC request");
        return leader;
    }

    void shutdown(void) const
    {
        worker_trace("received shutdown RPC request from master");
        get_engine().finalize();
        worker_trace("completed shutdown RPC request");
    }

    void suspend(double timeout_ms) const
    {
        worker_trace("received suspend(timeout_ms="
                     << timeout_ms << ") RPC request from master");
        get_engine().get_progress_pool().make_thread(
            [timeout_ms]() { sleep(timeout_ms); }, tl::anonymous{});
        worker_trace("completed suspend RPC request");
    }

    std::string get_fsm_content(void) const
    {
        worker_trace("received get_fsm_content RPC request from master");
        worker_trace("completed suspend RPC request");
        return myfsm.content;
    }
}; // namespace

class Master {
  public:
    Master(tl::engine& e) : engine(e)
    {
#define DEFINE_MASTER_RPC(__rpc__) \
    rpcs.insert(                   \
        std::make_pair(#__rpc__, engine.define(#__rpc__).disable_response()))
        DEFINE_MASTER_RPC(start);
        DEFINE_MASTER_RPC(bootstrap);
        DEFINE_MASTER_RPC(add);
        DEFINE_MASTER_RPC(assign);
        DEFINE_MASTER_RPC(remove);
        DEFINE_MASTER_RPC(apply);
        DEFINE_MASTER_RPC(transfer);
        DEFINE_MASTER_RPC(shutdown);
        DEFINE_MASTER_RPC(suspend);
#undef DEFINE_MASTER_RPC
        // This RPC respond with data, so we don't call disable_response
        rpcs.insert(std::make_pair("get_leader", engine.define("get_leader")));
        rpcs.insert(std::make_pair("get_fsm_content",
                                   engine.define("get_fsm_content")));
    }

    ~Master()
    {
        // Request to all workers to shutdown
        while (!raftAddrs.empty()) shutdownWorker(raftAddrs.cbegin()->first);
        engine.finalize();
    }

    void readInput(std::istream& input)
    {
        // Read from input stream until EOF
        std::string line;
        master_trace("reading input stream until EOF...");
        while (std::getline(input, line)) {
            std::cout << "=================================================\n";
            master_trace("reading line=" << line);
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
                expected_fsm_content.append(data);
                sendDataToRaftCluster(data);
            } else if (command == "sleep") {
                double timeout_ms;
                iss >> timeout_ms;
                master_trace("sleeping for " << timeout_ms << " ms");
                margo_thread_sleep(engine.get_margo_instance(), timeout_ms);
            } else if (command == "suspend") {
                raft_id raftId;
                double  timeout_ms;
                iss >> raftId >> timeout_ms;
                suspendWorker(raftId, timeout_ms);
            } else {
                master_trace("unrecognized command: " << command);
            }
        }
        master_trace("done reading input stream");
    }

  private:
    std::map<std::string, tl::remote_procedure> rpcs;
    tl::engine                                  engine;
    std::map<raft_id, std::string>              raftAddrs;
    std::map<pid_t, raft_id>                    pidToRaftId;
    std::string                                 expected_fsm_content;

    /**
     * Spawn a worker process to add to the raft cluster
     * @param [in] raftId Raft ID to give the spawned worker in the raft cluster
     */
    void addWorker(raft_id raftId)
    {
        master_trace(
            "preparing to request to add worker with raft_id=" << raftId);
        int  pipeFd[2]; // Pipe for worker to communicate self_addr to master
        char worker_addr[256];

        if (pipe(pipeFd) == -1) {
            master_trace("failed creating communication pipe... exiting");
            std::exit(1);
        }

        pid_t pid = fork();

        if (pid < 0) {
            master_trace("failed fork call... exiting");
            std::exit(1);
        } else if (pid == 0) {
            // Worker (child) process
            worker_trace("spawned with pid=" << getpid());

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

            // Send self_addr to master
            ssize_t _ = write(pipeFd[1], self_addr.c_str(), self_addr.size());
            close(pipeFd[1]);
            worker_trace("wrote addr=" << self_addr
                                       << " to communication pipe");

            // Create provider
            WorkerProvider* provider = new WorkerProvider(engine, raftId);
            auto            finalize_callback = [provider]() mutable {
                delete provider;
                provider = nullptr;
            };
            engine.push_finalize_callback(finalize_callback);
            worker_trace("provider created... waiting for finalize call");
            engine.wait_for_finalize();
            std::exit(0);
        } else if (pid > 0) {
            // Master (parent) process
            // Close the write end of the pipe
            close(pipeFd[1]);

            // Process the self_addr received from the worker
            ssize_t _ = read(pipeFd[0], worker_addr, sizeof(worker_addr));
            close(pipeFd[0]);
            std::string worker_addr_str(worker_addr);
            worker_addr_str.resize(256, '\0');
            master_trace("found worker addr=" << worker_addr_str);
            margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);

            // Add raft-id -> worker_addr to mapping
            raftAddrs.insert({raftId, worker_addr_str});
            pidToRaftId.insert({pid, raftId});

            // Get handle for the spawned worker to make RPC requests to worker
            tl::provider_handle handle(engine.lookup(worker_addr_str), raftId);

            // If its the first spawned worker, send it an RPC requesting to
            // bootstrap the raft cluster
            if (raftAddrs.size() == 1) {
                master_trace("requesting bootstrap RPC to worker with raft_id="
                             << raftId << ", addr=" << worker_addr_str);
                std::array<mraft::ServerInfo, 1> servers = {mraft::ServerInfo{
                    .id      = raftId,
                    .address = worker_addr_str,
                    .role    = mraft::Role::VOTER,
                }};
                rpcs["bootstrap"].on(handle)(servers);
                margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);
            }

            // Send RPC requesting the spawned worked to call mraft_start
            master_trace("requesting start RPC to worker with raft_id="
                         << raftId << ", addr=" << worker_addr_str);
            rpcs["start"].on(handle)();
            margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);

            // If we didn't bootstrap, send an RPC to the leader of the cluster
            // asking to add the spawned worker and assign it to raft voter
            if (raftAddrs.size() == 1) return;

            // Find leader of the cluster
            mraft::ServerInfo leader;
            int               ret = getLeaderInfo(leader, handle);
            if (ret == -1) {
                master_trace("failed getting leader from cluster... exiting");
                std::exit(1);
            }
            master_trace("found leader(raft_id=" << leader.id << ", addr="
                                                 << leader.address << ")");
            margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);

            // Request add RPC to leader
            master_trace("requesting add(raft_id=" << raftId << ", addr="
                                                   << worker_addr_str
                                                   << ") to leader");
            rpcs["add"].on(handle)(raftId, worker_addr_str);
            margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);

            // Request assign RPC to leader
            master_trace("requesting assign(raft_id="
                         << raftId
                         << ", role=" << static_cast<int>(mraft::Role::VOTER)
                         << ") to leader");
            rpcs["assign"].on(handle)(raftId, mraft::Role::VOTER);
            margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);
        }
    }

    void removeWorker(int raftId)
    {
        master_trace(
            "preparing to request to remove worker with raft_id=" << raftId);

        // Request the worker to remove itself from the cluster
        std::string         worker_addr = raftAddrs[raftId];
        tl::provider_handle handle(engine.lookup(worker_addr), raftId);
        master_trace("requesting remove to worker(raft_id="
                     << raftId << ", addr=" << worker_addr << ")");
        rpcs["remove"].on(handle)(raftId);
        margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);

        // Request leader to remove the shutdown process from the cluster
        master_trace("requesting shutdown to worker(raft_id=" << raftId << ")");
        rpcs["shutdown"].on(handle)();
        raftAddrs.erase(raftId);
        margo_thread_sleep(engine.get_margo_instance(), 500);
    }

    void shutdownWorker(int raftId)
    {
        master_trace(
            "preparing to request to shutdown worker with raft_id=" << raftId);

        // Find leader of the cluster
        mraft::ServerInfo   leader;
        tl::provider_handle handle;
        int                 ret = getLeaderInfo(leader, handle);
        if (ret == -1) {
            master_trace("failed getting leader from cluster... exiting");
            std::exit(1);
        }
        master_trace("found leader(raft_id=" << leader.id << ", addr="
                                             << leader.address << ")");
        margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);

        // Request leader to remove the shutdown process from the cluster
        master_trace("requesting remove(raft_id=" << raftId << ") to leader");
        rpcs["remove"].on(handle)(raftId);
        margo_thread_sleep(engine.get_margo_instance(), 500);

        // Check that the worker's FSM is up to date with the correct values
        std::string worker_addr = raftAddrs[raftId];
        handle = tl::provider_handle(engine.lookup(worker_addr), raftId);
        master_trace("requesting get_fsm_content to worker(raft_id="
                     << raftId << ", addr=" << worker_addr << ")");
        std::string worker_fsm_content = rpcs["get_fsm_content"].on(handle)();
        margo_assert(engine.get_margo_instance(),
                     worker_fsm_content == expected_fsm_content);
        margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);

        // Request the worker to shutdown
        master_trace("requesting shutdown(raft_id="
                     << raftId << ", addr=" << worker_addr << ") to leader");
        rpcs["shutdown"].on(handle)();
        raftAddrs.erase(raftId);
        margo_thread_sleep(engine.get_margo_instance(), 500);
    }

    void killWorker(pid_t workerPid)
    {
        // Find leader of the cluster
        mraft::ServerInfo   leader;
        tl::provider_handle handle;
        int                 ret = getLeaderInfo(leader, handle);
        if (ret == -1) {
            master_trace("failed getting leader from cluster... exiting");
            std::exit(1);
        }
        master_trace("found leader(raft_id=" << leader.id << ", addr="
                                             << leader.address << ")");
        margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);

        // Request leader to remove the killed worker from the cluster
        raft_id worker_raft_id = pidToRaftId[workerPid];
        master_trace("requesting remove(raft_id=" << worker_raft_id
                                                  << ") to leader");
        rpcs["remove"].on(handle)(worker_raft_id);
        pidToRaftId.erase(workerPid);
        raftAddrs.erase(worker_raft_id);
        margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);

        // Kill the worker
        master_trace("killing worker with pid=" << workerPid);
        kill(workerPid, SIGTERM);
        margo_thread_sleep(engine.get_margo_instance(), 500);
    }

    void sendDataToRaftCluster(const std::string& data)
    {
        master_trace("preparing to request to apply with data=" << data);

        // Find leader of the cluster
        mraft::ServerInfo   leader;
        tl::provider_handle handle;
        int                 ret = getLeaderInfo(leader, handle);
        if (ret == -1) {
            master_trace("failed getting leader from cluster... exiting");
            std::exit(1);
        }
        master_trace("found leader(raft_id=" << leader.id << ", addr="
                                             << leader.address << ")");
        margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);

        master_trace("requesting apply(data=" << data << ") to leader");
        rpcs["apply"].on(handle)(data);
        margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);
    }

    void suspendWorker(raft_id raftId, double timeout_ms)
    {
        master_trace("preparing to request to suspend worker with raft_id="
                     << raftId << " for " << timeout_ms << " ms");

        std::string worker_addr = raftAddrs[raftId];
        master_trace("requesting suspend(timeout_ms="
                     << timeout_ms << ") to worker(raft_id=" << raftId
                     << ", addr=" << worker_addr << ")");
        tl::provider_handle handle(engine.lookup(worker_addr), raftId);
        rpcs["suspend"].on(handle)(timeout_ms);

        margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);
    }

    int getLeaderInfo(mraft::ServerInfo& leader, tl::provider_handle& handle)
    {
        for (auto it = raftAddrs.cbegin(); it != raftAddrs.cend(); it++) {
            master_trace("requesting get_leader RPC to worker with raft_id="
                         << it->first << ", addr=" << it->second);
            handle = tl::provider_handle(engine.lookup(it->second), it->first);
            leader = rpcs["get_leader"].on(handle)();
            if (leader.id != 0) return 0;
            master_trace("the worker couldn't find a leader... trying again");
            margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);
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
            master_trace("unrecognized option or missing argument");
            std::cerr << "usage: " << argv[0] << " -f <file> | --file=<file>\n";
            std::exit(1);
        }
    }
    return 0;
}
} // namespace

int main(int argc, char* argv[])
{
    tl::engine engine("tcp", THALLIUM_SERVER_MODE);

    // Parse command-line arguments
    char* filename = nullptr;
    parseCommandLineArgs(argc, argv, &filename);
    std::ifstream inputFile;
    std::istream* input
        = (!filename) ? &std::cin : (inputFile.open(filename), &inputFile);

    Master master(engine);

    master.readInput(*input);

    master_trace("sleeping...");
    margo_thread_sleep(engine.get_margo_instance(), 3 * 1000);

    master_trace("exiting program");
    return 0;
}