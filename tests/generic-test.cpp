#include <fstream>
#include <future>
#include <iostream>
#include <map>
#include <mochi-raft.hpp>
#include <mutex>
#include <sstream>
#include <string>
#include <thallium.hpp>
#include <thread>

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

void requestToJoin(const tl::request& req, const raft_id& raftId, const std::string& addr)
{

}

class Master {
  public:
    void readInput(std::istream* input)
    {
        // Read from input stream until EOF
        std::string line;
        while (std::getline(*input, line)) {
            if (line.find("add") == 0) {
                // Spawn a process, and add it to the raft cluster
                raft_id raftId = extractRaftId(line);
                if (raftId == 0) {
                    std::cerr << "[test] [debug] invalid raft-id: " << line
                              << "\n";
                    return;
                }
                addToRaftCluster(raftId);
            } else if (line.find("remove") == 0) {
            } else if (line.find("send") == 0) {
            } else if (line.find("exit") == 0) {
            } else {
                std::cerr << "[test] [debug] invalid command: " << line << "\n";
            }
        }
        std::string line;
        while (std::getline(*input, line)) {
            if (line.find("add raft-id=") == 0) {
                int raftId = extractRaftId(line);
                addToRaftCluster(raftId);
            } else if (line.find("remove raft-id=") == 0) {
                int raftId = extractRaftId(line);
                removeFromRaftCluster(raftId);
            } else if (line.find("send data=") == 0) {
                std::string data = extractData(line);
                sendData(data);
            }
        }
    }

  private:
    static unsigned                totalWorkers = 0U;

    std::map<raft_id, std::string> raftAddrs;
    std::vector<std::thread> raftWorkerThreads;
    std::mutex                     mutex;

    int extractRaftId(const std::string& line)
    {
        const auto& pos = line.find("=");
        if (pos == std::string::npos) return 0;

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

    void addToRaftCluster(int raftId)
    {
        std::lock_guard<std::mutex> lock(mutex);
        raftWorkerThreads.emplace_back([this, &raftId]() {
            tl::engine       engine("tcp", THALLIUM_SERVER_MODE);
            mraft::MemoryLog mlog;
            MyFSM            myfsm;
            mraft::Raft raft(engine.get_margo_instance(), raftId, myfsm, mlog);

            if (totalWorkers == 0) {
                // First spawned, bootstrap raft cluster
                mraft::ServerInfo server = {
                    .address = static_cast<std::string>(engine.self()),
                    .id = raftId,
                    .role = mraft::Role::VOTER,
                };
                std::array<mraft::ServerInfo, 1> servers = {server};
                raft.bootstrap(servers);
            }

            raft.start();

            if (totalWorkers != 0) {
                // Send RPC to leader requesting to be added to the raft cluster
            }

            mutex.lock();
            raftAddrs[raftId] = static_cast<std::string>(engine.self());
            totalWorkers++;
            mutex.unlock();

            /* Wait for master process to call engine.finalize() */
            engine.wait_for_finalize();
        }));
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
} // namespace

int main(int argc, char* argv[])
{
    tl::engine myEngine("tcp", THALLIUM_SERVER_MODE);

    // Parse command-line arguments
    std::ifstream inputFile;
    std::istream* input = nullptr;
    for (int i = 1; i < argc; ++i) {
        if (argv[i] == "--file" && i + 1 < argc) {
            inputFile.open(argv[i + 1]);
            ++i; // Skip the next argument
            if (inputFile.is_open()) {
                std::cerr << "[test] [debug] error opening " << argv[i + 1]
                          << std::endl;
                return 1;
            }
            input = &inputFile;
        } else {
            std::cerr << "[test] [debug] invalid option or flag: " << argv[i]
                      << std::endl;
            return 1;
        }
    }
    // If no file was given on the command line, read commands from stdin
    if (!input) input = &std::cin;

    Master master;
    master.readInput(input);
    myEngine.finalize();

    return 0;
}