#include <cstdlib>
#include <fstream>
#include <getopt.h>
#include <iostream>
#include <map>
#include <mochi-raft.hpp>
#include <sstream>
#include <string>
#include <thallium.hpp>
#include <thallium/serialization/stl/array.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <unistd.h>
#include <sys/types.h>

namespace tl = thallium;

namespace {
class MyFSM : public mraft::StateMachine {
  public:
  private:
    std::string content;

    void apply(const struct raft_buffer* buf, void** result) override
    {
        fprintf(stderr, "[test] applying %s\n", (char*)buf->base);
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
        : tl::provider<WorkerProvider>(e, provider_id),
          raft(e.get_margo_instance(),
               provider_id,
               *std::make_unique<MyFSM>(),
               *std::make_unique<mraft::MemoryLog>(),
               provider_id)
    {
#define DEFINE_WORKER_RPC(__rpc__) define(#__rpc__, &WorkerProvider::__rpc__)
        DEFINE_WORKER_RPC(start);
        DEFINE_WORKER_RPC(bootstrap);
        DEFINE_WORKER_RPC(add);
        DEFINE_WORKER_RPC(assign);
        DEFINE_WORKER_RPC(remove);
        // TODO: work the serialize DEFINE_WORKER_RPC(apply);
        DEFINE_WORKER_RPC(transfer);
#undef DEFINE_WORKER_RPC
    }

    ~WorkerProvider()
    {
        std::cout << "[test] [debug] [worker] calling wait_for_finalize() from "
                     "~WorkerProvider\n";
        get_engine().wait_for_finalize();
    }

  private:
    mraft::Raft raft;

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

    // void apply(std::shared_ptr<mraft::RaftBufferWrapper> bufs,
    //            const unsigned&                           n)
    // {
    //     std::cout << "[test] [debug] [worker] received apply(n=" << n
    //               << ") RPC request\n";
    //     // TODO: review this wrapper and serialize problem
    //     // raft.apply(bufs.get()->buffer, n);
    //     std::cout << "[test] [debug] [worker] completed apply(n=" << n
    //               << ") RPC request\n";
    // }

    void transfer(const raft_id& raftId)
    {
        std::cout << "[test] [debug] [worker] received transfer(raftId="
                  << raftId << ") RPC request\n";
        raft.transfer(raftId);
        std::cout << "[test] [debug] [worker] completed transfer(raftId="
                  << raftId << ") RPC request\n";
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
#undef DEFINE_MASTER_RPC
    }

    void readInput(std::istream& input)
    {
        // Read from input stream until EOF
        std::string line;
        while (std::getline(input, line)) {
            std::cout << "[test] [debug] [master] reading line: '" << line
                      << "'\n";
            if (line.find("add") == 0) {
                std::cout
                    << "[test] [debug] [master] found call to ADD process\n";
                raft_id raftId = extractRaftId(line);
                std::cout << "[test] [debug] [master] extracted raft-id="
                          << raftId << "from line\n";
                addToRaftCluster(raftId);
            } else if (line.find("remove") == 0) {
                raft_id raftId = extractRaftId(line);
                removeFromRaftCluster(raftId);
            } else if (line.find("send") == 0) {
            } else if (line.find("exit") == 0) {
            } else {
                std::cerr << "[test] [debug] [master] invalid command: " << line
                          << "\n";
            }
        }
        std::cout << "[test] [debug] [master] finished reading input stream\n";
    }

  private:
    std::map<std::string, tl::remote_procedure> rpcs;
    tl::engine                                  engine;
    std::map<raft_id, std::string>              raftAddrs;

    /**
     * @brief Extract the <raft-id> value from a line in the form
     * "add raft-id=<raft-id>""
     * @param [in] line the line to extract the raft-id from
     * @return 0 on error, the value of the raft-id on success
     */
    int extractRaftId(const std::string& line)
    {
        const auto& pos = line.find("=");
        if (pos == std::string::npos) {
            std::cerr << "[test] [debug] [maste] extractRaftId invalid line: "
                      << line << std::endl;
            std::exit(1);
        };

        // Trim any leading or trailing whitespace characters
        auto        raftIdStr          = line.substr(pos + 1);
        const auto& firstNonWhitespace = raftIdStr.find_first_not_of(" \t");
        const auto& lastNonWhitespace  = raftIdStr.find_last_not_of(" \t");
        if (firstNonWhitespace != std::string::npos
            && lastNonWhitespace != std::string::npos)
            raftIdStr = raftIdStr.substr(
                firstNonWhitespace, lastNonWhitespace - firstNonWhitespace + 1);

        // Read the given raft ID
        std::istringstream iss(raftIdStr);
        raft_id            raftId;
        iss >> raftId;

        return raftId;
    }

    std::string extractData(const std::string& line) { return line.substr(10); }

    /**
     * Spawn a worker process to add to the raft cluster
     * @param [in] raftId Raft ID to give the spawned worker in the raft cluster
     */
    void addToRaftCluster(raft_id raftId)
    {
        std::cout << "[test] [debug] [master] trying to add raft-id=" << raftId
                  << " to raft-cluster\n";
        int  pipeFd[2]; /* Pipe for worker to communicate self_addr to master */
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
            WorkerProvider provider(engine, raftId);
            provider.~WorkerProvider(); // FIXME: this shouldn't be necessary ?
            std::cout << "============================================= [test] "
                         "[debug] [worker] SHOULD "
                         "REACH THIS\n";
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

            // Add raft-id -> worker_addr to mapping
            raftAddrs.insert({raftId, worker_addr});
            std::cout << "[test] [debug] [master] lookup addr=" << worker_addr
                      << " found: " << engine.lookup(worker_addr) << "\n";
            tl::provider_handle handler(engine.lookup(worker_addr), raftId);
            std::cout << "[test] [debug] [master] created provider handler\n";

            // If its the first spawned worker, send it an RPC requesting to
            // bootstrap the raft cluster
            if (raftAddrs.size() == 1) {
                std::cout << "[test] [debug] [master] sending bootstrap RPC "
                             "request to"
                             "raftId="
                          << raftId << ", addr='" << worker_addr << "'\n";
                mraft::ServerInfo server = {
                    .id      = raftId,
                    .address = worker_addr,
                    .role    = mraft::Role::VOTER,
                };
                std::array<mraft::ServerInfo, 1> servers = {server};
                rpcs["bootstrap"].on(handler)(servers);
                std::cout
                    << "[test] [debug] [master] bootstrap RPC requested\n";
            }

            // Send RPC requesting the spawned worked to call mraft_start
            std::cout << "[test] [debug] [master] sending start RPC request to "
                         "raftId="
                      << raftId << ", addr='" << worker_addr << "'\n";
            rpcs["start"].on(handler)();
            std::cout << "[test] [debug] [master] start RPC requested\n";
        }
    }

    void removeFromRaftCluster(int raftId)
    {
        /* Send rpc to server with the given raft id. */
        /* That RPC will send an RPC to the leader to remove him from the
         * cluster */
    }

    void sendData(const std::string& data)
    {
        /* Send rpc to the leader asking him to send data to the FSM*/
    }
};

/**
 * @brief Parse the command line arguments
 *
 * @param [in] argc argument count
 * @param [in] argv command line arguments
 * @param [out] filename pointer to store the filename given in the command line
 * arguments
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
                         "missing argument"
                      << std::endl;
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

    return 0;
}