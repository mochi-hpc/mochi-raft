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

namespace {
class MyFSM : public mraft::StateMachine {
    friend class WorkerProvider;

  public:
    const std::string& getContent() { return content; }

  private:
    std::string content;

    void apply(const struct raft_buffer* buf, void** result) override
    {
        fprintf(stderr, "[test] [debug] [MYFSM] applying '%s'\n",
                static_cast<char*>(buf->base));
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
        content = std::string(static_cast<char*>(buf->base));
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
        DEFINE_WORKER_RPC(transfer);
        DEFINE_WORKER_RPC(leader);
        DEFINE_WORKER_RPC(shutdown);
#undef DEFINE_WORKER_RPC
    }

    ~WorkerProvider()
    {
        // std::cout << "[test] [debug] [worker] calling wait_for_finalize()
        // from "
        //              "~WorkerProvider\n";
        // get_engine().wait_for_finalize();
        // std::cout
        //     << "[test] [debug] [worker] unfroze from wait_for_finalize()\n";
        std::cout
            << "[test] [debug] [worker] in ~WorkerProvider, myfsmcontents = '"
            << myfsm.content << "'\n";
    }

  private:
    MyFSM            myfsm;
    mraft::MemoryLog mlog;
    mraft::Raft      raft;

    void start(void)
    {
        std::cout << "[test] [debug] [worker] received start RPC request\n";
        raft.start();
        std::cout << "[test] [debug] [worker] completed start RPC request\n";
    }

    // TODO: see about std::array<T, 1> ? std::vector<> ?
    // template <typename ServerInfoContainer>
    void bootstrap(const std::array<mraft::ServerInfo, 1>& serverList)
    {
        std::cout << "[test] [debug] [worker] received bootstrap RPC request\n";
        raft.bootstrap(serverList);
        std::cout
            << "[test] [debug] [worker] completed bootstrap RPC request\n";
    }

    void add(const raft_id& raftId, const std::string& addr)
    {
        std::cout << "[test] [debug] [worker] received add(raftid=" << raftId
                  << ", addr='" << addr << "') RPC request\n";
        raft.add(raftId, addr.c_str());
        std::cout << "[test] [debug] [worker] completed add(raftid=" << raftId
                  << ", addr='" << addr << "') RPC request\n";
    }

    void assign(const raft_id& raftId, const mraft::Role& role)
    {
        std::cout << "[test] [debug] [worker] received assign(raftid=" << raftId
                  << ", role=" << static_cast<int>(role) << ") RPC request\n";
        raft.assign(raftId, role);
        std::cout << "[test] [debug] [worker] completed assign(raftid="
                  << raftId << ", role=" << static_cast<int>(role)
                  << ") RPC request\n";
    }

    void remove(const raft_id& raftId)
    {
        std::cout << "[test] [debug] [worker] received remove(raftid=" << raftId
                  << ") RPC request\n";
        raft.remove(raftId);
        std::cout << "[test] [debug] [worker] completed remove(raftid="
                  << raftId << ") RPC request\n";
    }

    void apply(const std::string& buffer)
    {
        std::cout << "[test] [debug] [worker] received apply(buffer=" << buffer
                  << ") RPC request\n";
        std::cout << "[test] [debug] [worker] c_str='" << buffer.c_str()
                  << "', size=" << buffer.size() << "\n";
        struct raft_buffer bufs = {
            .base = (void*)buffer.c_str(),
            .len  = buffer.size() + 1, // Include '\0'
        };
        raft.apply(&bufs, 1U);
        std::cout << "[test] [debug] [worker] completed apply(buffer=" << buffer
                  << ") RPC request\n";
    }

    void transfer(const raft_id& raftId)
    {
        std::cout << "[test] [debug] [worker] received transfer(raftId="
                  << raftId << ") RPC request\n";
        raft.transfer(raftId);
        std::cout << "[test] [debug] [worker] completed transfer(raftId="
                  << raftId << ") RPC request\n";
    }

    mraft::ServerInfo leader(void)
    {
        std::cout << "[test] [debug] [worker] received leader RPC request\n";
        mraft::ServerInfo leader = raft.get_leader();
        std::cout << "[test] [debug] [worker] completed leader RPC request\n";
        return leader;
    }

    void shutdown(void)
    {
        std::cout << "[test] [debug] [worker] received shutdown RPC request\n";
        get_engine().finalize();
        std::cout << "[test] [debug] [worker] completed shutdown RPC request\n";
    }
};

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
#undef DEFINE_MASTER_RPC
        // This RPC response with data
        rpcs.insert(std::make_pair("leader", engine.define("leader")));
    }

    void readInput(std::istream& input)
    {
        // Read from input stream until EOF
        std::string line;
        while (std::getline(input, line)) {
            std::cout << "[test] [debug] [master] reading line: '" << line
                      << "'\n";
            std::istringstream iss(line);
            std::string        command;
            iss >> command;
            if (command == "add") {
                raft_id raftId;
                iss >> raftId;
                std::cout << "[test] [debug] [master] adding raftId=" << raftId
                          << "\n";
                addWorker(raftId);
            } else if (command == "remove") {
                raft_id raftId;
                iss >> raftId;
                std::cout << "[test] [debug] [master] removing raftId="
                          << raftId << "\n";
                removeWorker(raftId);
            } else if (command == "shutdown") {
                raft_id raftId;
                iss >> raftId;
                std::cout << "[test] [debug] [master] shutting down raftId="
                          << raftId << "\n";
                shutdownWorker(raftId);
            } else if (command == "kill") {
                pid_t pid;
                iss >> pid;
                std::cout << "[test] [debug] [master] killing child pid=" << pid
                          << "\n";
                killWorker(pid);
            } else if (command == "apply") {
                std::string data;
                iss >> data;
                std::cout << "[test] [debug] [master] sending data '" << data
                          << "' to FSM\n";
                sendDataToRaftCluster(data);
            } else if (command == "sleep") {
                double timeout_ms;
                iss >> timeout_ms;
                std::cout << "[test] [debug] [master] sleeping for "
                          << timeout_ms << " ms\n";
                margo_thread_sleep(engine.get_margo_instance(), timeout_ms);
            } else {
                std::cerr
                    << "[test] [debug] [master] invalid command in line: '"
                    << line << "'\n";
            }
        }
        std::cout << "[test] [debug] [master] finished reading input stream\n";
    }

  private:
    std::map<std::string, tl::remote_procedure> rpcs;
    tl::engine                                  engine;
    std::map<raft_id, std::string>              raftAddrs;

    /**
     * Spawn a worker process to add to the raft cluster
     * @param [in] raftId Raft ID to give the spawned worker in the raft cluster
     */
    void addWorker(raft_id raftId)
    {
        std::cout << "[test] [debug] [master] trying to add raft-id=" << raftId
                  << " to raft-cluster\n";
        int  pipeFd[2]; // Pipe for worker to communicate self_addr to master
        char worker_addr[256];

        if (pipe(pipeFd) == -1) {
            std::cerr << "[test] [debug] [master] error creating pipe for "
                         "worker of raft id="
                      << raftId << std::endl;
            std::exit(-1);
        }

        pid_t pid = fork();

        if (pid < 0) {
            std::cerr
                << "[test] [debug] [master] error forking worker with raft id="
                << raftId << std::endl;
            std::exit(-1);
        } else if (pid == 0) {
            // Worker (child) process
            std::cout << "[test] [debug] [worker] spawned with pid=" << getpid()
                      << "\n";

            // Close the read end of the pipe
            close(pipeFd[0]);

            // Here, the child has a copy of the parent engine, so we call
            // finalize on it
            engine.finalize();

            // The child creates its own engine
            engine = tl::engine("tcp", THALLIUM_SERVER_MODE);
            std::cout << "[test] [debug] [worker] created tl::engine of mid="
                      << engine.get_margo_instance() << "\n";

            // Get the self_addr and resize it to size 256 with '\0'
            std::string self_addr = engine.self();
            self_addr.resize(256, '\0');

            // Send self_addr to master
            ssize_t _ = write(pipeFd[1], self_addr.c_str(), self_addr.size());
            close(pipeFd[1]);
            std::cout << "[test] [debug] [worker] raftId=" << raftId
                      << ", addr='" << self_addr
                      << "' wrote self_addr=" << self_addr << " to pipe\n";

            // Create provider
            std::cout << "[test] [debug] [worker] raftId=" << raftId
                      << ", addr='" << self_addr << "' initializing provider\n";
            WorkerProvider* provider = new WorkerProvider(engine, raftId);
            std::cout << "[test] [debug] [worker] heap allocated provider\n";
            engine.push_finalize_callback(
                [provider]() mutable { delete provider; });
            std::cout << "[test] [debug] [worker] added finalize callback\n";
            engine.wait_for_finalize();
            std::cout << "[test] [debug] [worker] code after wait for "
                         "finalize... exiting\n";
            // WorkerProvider provider(engine, raftId);
            // provider.~WorkerProvider(); // FIXME: this shouldn't be necessary
            // ? std::cout << "=============================================
            // [test] "
            //              "[debug] [worker] SHOULD NOT REACH THIS\n";
            std::exit(0);
        } else if (pid > 0) {
            // Master (parent) process
            // Close the write end of the pipe
            close(pipeFd[1]);

            // Process the self_addr received from the worker
            ssize_t _ = read(pipeFd[0], worker_addr, sizeof(worker_addr));
            close(pipeFd[0]);
            std::cout << "[test] [debug] [master] read worker_addr="
                      << worker_addr << " from pipe\n";

            std::string worker_addr_str(worker_addr);
            worker_addr_str.resize(256, '\0');
            margo_thread_sleep(engine.get_margo_instance(), 4 * 1000);

            // Add raft-id -> worker_addr to mapping
            raftAddrs.insert({raftId, worker_addr_str});
            std::cout << "[test] [debug] [master] lookup addr="
                      << worker_addr_str
                      << " found: " << engine.lookup(worker_addr_str) << "\n";
            tl::provider_handle handle(engine.lookup(worker_addr_str), raftId);
            std::cout << "[test] [debug] [master] created provider handle\n";

            // If its the first spawned worker, send it an RPC requesting to
            // bootstrap the raft cluster
            if (raftAddrs.size() == 1) {
                std::cout << "[test] [debug] [master] sending bootstrap RPC "
                             "request to"
                             "raftId="
                          << raftId << ", addr='" << worker_addr_str << "'\n";
                mraft::ServerInfo server = {
                    .id      = raftId,
                    .address = worker_addr_str,
                    .role    = mraft::Role::VOTER,
                };
                std::array<mraft::ServerInfo, 1> servers = {server};
                rpcs["bootstrap"].on(handle)(servers);
                std::cout
                    << "[test] [debug] [master] bootstrap RPC requested\n";
                margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);
            }

            // Send RPC requesting the spawned worked to call mraft_start
            std::cout << "[test] [debug] [master] sending start RPC request to "
                         "raftId="
                      << raftId << ", addr='" << worker_addr_str << "'\n";
            rpcs["start"].on(handle)();
            std::cout << "[test] [debug] [master] start RPC requested\n";
            margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);

            // If we didn't bootstrap, send an RPC to the leader of the cluster
            // asking to add the spawned worker and assign it to raft voter
            // TODO: think about edge case if the only process in the cluster
            // has bootstrap, but leaves
            if (raftAddrs.size() > 1) {
                // Send RPCs to any spawned worker and ask for the leader ID.
                // Stop after the first response, because we might ask the
                // worker we just spawned, in which case he doesn't have a
                // leader, so we need to make it ask somebody else.
                mraft::ServerInfo   leader;
                tl::provider_handle handle;
                int                 ret = getLeaderInfo(leader, handle);
                if (ret == -1) {
                    std::cerr << "[test] [debug] [master] failed getting "
                                 "leader from cluster\n";
                    exit(1);
                }
                std::cout << "[test] [debug] [master] RPC leader found raftId="
                          << leader.id << ", addr='" << leader.address
                          << "'. Provider found is " << handle << "\n";
                margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);
                std::cout << "[test] [debug] [master] sending add(raftId="
                          << raftId << ", addr='" << worker_addr_str
                          << "') RPC request to "
                             "raftId="
                          << leader.id << ", addr='" << leader.address << "'\n";
                rpcs["add"].on(handle)(raftId, worker_addr_str);
                std::cout << "[test] [debug] [master] add RPC requested\n";
                margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);

                std::cout
                    << "[test] [debug] [master] sending assign RPC request to "
                       "raftId="
                    << leader.id << ", addr='" << leader.address << "'\n";
                rpcs["assign"].on(handle)(raftId, mraft::Role::VOTER);
                std::cout << "[test] [debug] [master] assign RPC requested\n";
                margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);
            }
        }
    }

    void removeWorker(int raftId)
    {
        mraft::ServerInfo   leader;
        tl::provider_handle handle;
        int                 ret = getLeaderInfo(leader, handle);
        if (ret == -1) {
            std::cerr << "[test] [debug] [master] failed getting leader from "
                         "cluster\n";
            exit(1);
        }
        std::cout << "[test] [debug] [master] RPC leader found raftId="
                  << leader.id << ", addr='" << leader.address
                  << "'. Provider found is " << handle << "\n";
        margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);
        std::cout << "[test] [debug] [master] sending remove RPC request to "
                     "raftId="
                  << leader.id << ", addr='" << leader.address << "'\n";
        rpcs["remove"].on(handle)(raftId);
        std::cout << "[test] [debug] [master] completed remove RPC request\n";
        margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);

        shutdownWorker(raftId);
    }

    void shutdownWorker(int raftId)
    {
        std::string worker_addr = raftAddrs[raftId];
        std::cout << "[test] [debug] [master] sending shutdown RPC request to "
                     "raftId="
                  << raftId << ", addr='" << worker_addr << "'\n";
        tl::provider_handle handle(engine.lookup(worker_addr), raftId);
        rpcs["shutdown"].on(handle)();
        std::cout << "[test] [debug] [master] completed shutdown RPC request\n";
        raftAddrs.erase(raftId);
        margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);
    }

    void killWorker(pid_t workerPid)
    {
        kill(workerPid, SIGTERM);
        // TODO: Remove from raftAddrs ???
    }

    void sendDataToRaftCluster(const std::string& data)
    {
        std::cout << "[test] [debug] [master] sending data=" << data << "\n";
        mraft::ServerInfo   leader;
        tl::provider_handle handle;
        int                 ret = getLeaderInfo(leader, handle);
        if (ret == -1) {
            std::cerr << "[test] [debug] [master] failed getting leader from "
                         "cluster\n";
            std::exit(1);
        }
        std::cout << "[test] [debug] [master] RPC leader found raftId="
                  << leader.id << ", addr='" << leader.address
                  << "'. Provider found is " << handle << "\n";
        margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);
        std::cout << "[test] [debug] [master] sending apply RPC request to "
                     "raftId="
                  << leader.id << ", addr='" << leader.address << "'\n";
        rpcs["apply"].on(handle)(data);
        std::cout << "[test] [debug] [master] completed apply RPC request\n";
        margo_thread_sleep(engine.get_margo_instance(), 2 * 1000);
    }

    int getLeaderInfo(mraft::ServerInfo& leader, tl::provider_handle& handle)
    {
        for (auto it = raftAddrs.cbegin(); it != raftAddrs.cend(); it++) {
            std::cout << "[test] [debug] [master] requested leader RPC to "
                         "raftId="
                      << it->first << ", addr='" << it->second << "'\n";
            handle = tl::provider_handle(engine.lookup(it->second), it->first);
            leader = rpcs["leader"].on(handle)();
            std::cout
                << "[test] [debug] [master] completed leader RPC request\n";
            if (leader.id != 0) return 0;
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
            std::cerr << "[test] [debug] [master] unrecognized option or "
                         "missing argument\n";
            return -1;
        }
    }
    return 0;
}
} // namespace

int main(int argc, char* argv[])
{
    tl::engine engine("tcp", THALLIUM_SERVER_MODE);
    std::cout << "[test] [debug] [master] created tl::engine of mid="
              << engine.get_margo_instance() << "\n";

    // Parse command-line arguments
    char* filename = nullptr;
    if (parseCommandLineArgs(argc, argv, &filename) < 0) {
        std::cerr
            << "[test] [debug] [master] error parsing command-line arguments"
            << std::endl;
        return 1;
    };
    std::ifstream inputFile;
    std::istream* input
        = (!filename) ? &std::cin : (inputFile.open(filename), &inputFile);

    std::cout << "[test] [debug] [master] parsed command-line arguments"
              << "\n";

    Master master(engine);

    master.readInput(*input);

    std::cout << "[test] [debug] [master] sleeping for 10 seconds"
              << "\n";
    margo_thread_sleep(engine.get_margo_instance(), 10 * 1000);

    engine.finalize();

    std::cout << "[test] [debug] [master] engine finalized\n";

    return 0;
}