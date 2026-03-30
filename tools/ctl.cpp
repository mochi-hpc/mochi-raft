#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <map>
#include <memory>
#include <cstdlib>
#include <cstring>
#include <csignal>
#include <filesystem>

#include <unistd.h>
#include <sys/wait.h>
#include <poll.h>

#include "pipe_protocol.hpp"

namespace fs = std::filesystem;

// We use raft_id as a plain uint64_t here (no c-raft header in master)
using raft_id = uint64_t;

struct ChildProcess {
    pid_t pid = -1;
    int to_child = -1;    // write end → child's stdin
    int from_child = -1;  // read end ← child's stdout
    raft_id id = 0;
    std::string address;
    bool alive = false;
};

static std::string worker_path;
static std::string base_dir;
static std::string transport = "na+sm";
static std::map<raft_id, ChildProcess> children;

// Send a command to a child and read one response line.
// Returns the full response line, or "" on failure.
static std::string send_cmd(ChildProcess& child, const std::string& cmd) {
    if (!child.alive) return "ERR dead";

    std::string msg = cmd + "\n";
    ssize_t nw = write(child.to_child, msg.data(), msg.size());
    if (nw <= 0) {
        child.alive = false;
        return "ERR write failed";
    }

    // Read response line (with timeout)
    std::string response;
    struct pollfd pfd;
    pfd.fd = child.from_child;
    pfd.events = POLLIN;

    auto deadline_ms = 15000; // 15 second timeout
    while (deadline_ms > 0) {
        int ret = poll(&pfd, 1, deadline_ms);
        if (ret <= 0) {
            return "ERR timeout";
        }

        char buf[4096];
        ssize_t nr = read(child.from_child, buf, sizeof(buf));
        if (nr <= 0) {
            child.alive = false;
            return "ERR read failed";
        }

        response.append(buf, nr);
        // Check for newline
        auto nl = response.find('\n');
        if (nl != std::string::npos) {
            return response.substr(0, nl);
        }

        deadline_ms -= 100; // rough approximation
    }
    return "ERR timeout";
}

static std::string find_worker_binary(const char* argv0) {
    // Look for mochi-raft-worker in the same directory as ctl
    fs::path ctl_path = fs::canonical(fs::path(argv0));
    fs::path dir = ctl_path.parent_path();
    fs::path worker = dir / "mochi-raft-worker";
    if (fs::exists(worker)) return worker.string();

    // Try current directory
    if (fs::exists("mochi-raft-worker")) return "./mochi-raft-worker";

    // Try PATH
    return "mochi-raft-worker";
}

static bool spawn_worker(raft_id id) {
    if (children.count(id) && children[id].alive) {
        std::cerr << "Node " << id << " is already running" << std::endl;
        return false;
    }

    int to_child[2], from_child[2];
    if (pipe(to_child) != 0 || pipe(from_child) != 0) {
        perror("pipe");
        return false;
    }

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return false;
    }

    if (pid == 0) {
        // Child process
        close(to_child[1]);    // close write end of to_child
        close(from_child[0]);  // close read end of from_child

        dup2(to_child[0], STDIN_FILENO);
        dup2(from_child[1], STDOUT_FILENO);

        close(to_child[0]);
        close(from_child[1]);

        execl(worker_path.c_str(), "mochi-raft-worker", nullptr);
        perror("execl");
        _exit(1);
    }

    // Parent process
    close(to_child[0]);    // close read end
    close(from_child[1]);  // close write end

    ChildProcess child;
    child.pid = pid;
    child.to_child = to_child[1];
    child.from_child = from_child[0];
    child.id = id;
    child.alive = true;

    // Send INIT command
    std::string data_dir = base_dir + "/node-" + std::to_string(id);
    std::string init_cmd = "INIT " + std::to_string(id) + " " +
                           transport + " " + data_dir;
    children[id] = child;

    std::string resp = send_cmd(children[id], init_cmd);
    auto tokens = proto::split(resp);
    if (tokens.empty() || tokens[0] != "OK" || tokens.size() < 2) {
        std::cerr << "Failed to initialize node " << id << ": " << resp << std::endl;
        kill(pid, SIGKILL);
        waitpid(pid, nullptr, 0);
        close(child.to_child);
        close(child.from_child);
        children.erase(id);
        return false;
    }

    children[id].address = tokens[1];
    std::cout << "  Node " << id << " initialized at " << tokens[1] << std::endl;
    return true;
}

static void cleanup_child(ChildProcess& child) {
    if (child.to_child >= 0) close(child.to_child);
    if (child.from_child >= 0) close(child.from_child);
    child.to_child = -1;
    child.from_child = -1;
    child.alive = false;
}

// Reap any zombie children
static void reap_children() {
    int status;
    pid_t pid;
    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        for (auto& [id, child] : children) {
            if (child.pid == pid) {
                child.alive = false;
                break;
            }
        }
    }
}

// Find the leader among live children
static raft_id find_leader() {
    for (auto& [id, child] : children) {
        if (!child.alive) continue;
        std::string resp = send_cmd(child, "STATUS");
        auto tokens = proto::split(resp);
        if (tokens.size() >= 2 && tokens[0] == "OK" && tokens[1] == "leader") {
            return id;
        }
    }
    return 0;
}

static void cmd_spawn(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cout << "Usage: spawn <id1> [id2 ...]" << std::endl;
        return;
    }

    // Parse IDs
    std::vector<raft_id> ids;
    for (size_t i = 1; i < args.size(); i++) {
        ids.push_back(std::stoull(args[i]));
    }

    // Spawn all workers
    std::cout << "Spawning " << ids.size() << " worker(s)..." << std::endl;
    for (auto id : ids) {
        if (!spawn_worker(id)) return;
    }

    // Collect all addresses for bootstrap
    // Include ALL alive children (not just newly spawned ones)
    // But only bootstrap children that haven't been bootstrapped yet
    // For simplicity: bootstrap all alive children with full cluster config
    std::string bootstrap_args;
    for (auto& [id, child] : children) {
        if (!child.alive) continue;
        if (!bootstrap_args.empty()) bootstrap_args += " ";
        bootstrap_args += std::to_string(id) + " " + child.address;
    }

    // Bootstrap all newly spawned nodes
    std::cout << "Bootstrapping..." << std::endl;
    for (auto id : ids) {
        auto& child = children[id];
        std::string resp = send_cmd(child, "BOOTSTRAP " + bootstrap_args);
        if (resp.substr(0, 2) != "OK") {
            std::cerr << "  Node " << id << " bootstrap failed: " << resp << std::endl;
            return;
        }
    }

    // Start all newly spawned nodes
    std::cout << "Starting..." << std::endl;
    for (auto id : ids) {
        auto& child = children[id];
        std::string resp = send_cmd(child, "START");
        if (resp.substr(0, 2) != "OK") {
            std::cerr << "  Node " << id << " start failed: " << resp << std::endl;
            return;
        }
    }

    // Wait briefly for leader election
    std::cout << "Waiting for leader election..." << std::endl;
    for (int attempt = 0; attempt < 50; attempt++) {
        usleep(200000); // 200ms
        raft_id leader = find_leader();
        if (leader > 0) {
            std::cout << "Leader elected: node " << leader << std::endl;
            return;
        }
    }
    std::cout << "No leader elected yet (may still be in progress)" << std::endl;
}

static void cmd_shutdown(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cout << "Usage: shutdown <id>" << std::endl;
        return;
    }

    raft_id id = std::stoull(args[1]);
    auto it = children.find(id);
    if (it == children.end() || !it->second.alive) {
        std::cout << "Node " << id << " is not running" << std::endl;
        return;
    }

    std::string resp = send_cmd(it->second, "SHUTDOWN");
    std::cout << "Node " << id << ": " << resp << std::endl;

    waitpid(it->second.pid, nullptr, 0);
    cleanup_child(it->second);
}

static void cmd_kill(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cout << "Usage: kill <id>" << std::endl;
        return;
    }

    raft_id id = std::stoull(args[1]);
    auto it = children.find(id);
    if (it == children.end() || !it->second.alive) {
        std::cout << "Node " << id << " is not running" << std::endl;
        return;
    }

    ::kill(it->second.pid, SIGKILL);
    waitpid(it->second.pid, nullptr, 0);
    cleanup_child(it->second);
    std::cout << "Node " << id << " killed" << std::endl;
}

static void cmd_list(const std::vector<std::string>&) {
    reap_children();

    if (children.empty()) {
        std::cout << "No nodes" << std::endl;
        return;
    }

    std::cout << "ID  STATE       TERM  COMMIT  ADDRESS" << std::endl;
    std::cout << "--  -----       ----  ------  -------" << std::endl;

    for (auto& [id, child] : children) {
        if (!child.alive) {
            printf("%-3lu %-11s %-5s %-7s %s\n",
                   (unsigned long)id, "dead", "-", "-",
                   child.address.c_str());
            continue;
        }

        std::string resp = send_cmd(child, "STATUS");
        auto tokens = proto::split(resp);
        if (tokens.size() >= 4 && tokens[0] == "OK") {
            printf("%-3lu %-11s %-5s %-7s %s\n",
                   (unsigned long)id,
                   tokens[1].c_str(),
                   tokens[2].c_str(),
                   tokens[3].c_str(),
                   child.address.c_str());
        } else {
            printf("%-3lu %-11s %-5s %-7s %s\n",
                   (unsigned long)id, "error", "-", "-",
                   child.address.c_str());
        }
    }
}

static void cmd_isolate(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cout << "Usage: isolate <id> [inbound|outbound|both]" << std::endl;
        return;
    }

    raft_id id = std::stoull(args[1]);
    auto it = children.find(id);
    if (it == children.end() || !it->second.alive) {
        std::cout << "Node " << id << " is not running" << std::endl;
        return;
    }

    std::string mode = "both";
    if (args.size() >= 3) mode = args[2];

    std::string resp = send_cmd(it->second, "ISOLATE " + mode);
    std::cout << "Node " << id << " isolated (" << mode << "): " << resp << std::endl;
}

static void cmd_deisolate(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cout << "Usage: deisolate <id>" << std::endl;
        return;
    }

    raft_id id = std::stoull(args[1]);
    auto it = children.find(id);
    if (it == children.end() || !it->second.alive) {
        std::cout << "Node " << id << " is not running" << std::endl;
        return;
    }

    std::string resp = send_cmd(it->second, "DEISOLATE");
    std::cout << "Node " << id << " deisolated: " << resp << std::endl;
}

static void cmd_transfer(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cout << "Usage: transfer <target_id>" << std::endl;
        return;
    }

    raft_id target = std::stoull(args[1]);

    // Find the current leader
    raft_id leader = find_leader();
    if (leader == 0) {
        std::cout << "No leader found" << std::endl;
        return;
    }

    auto& child = children[leader];
    std::string resp = send_cmd(child, "TRANSFER " + std::to_string(target));
    if (resp.substr(0, 2) != "OK") {
        std::cout << "Transfer failed: " << resp << std::endl;
        return;
    }

    // Wait for transfer to complete
    std::cout << "Transferring leadership to node " << target << "..." << std::endl;
    for (int i = 0; i < 50; i++) {
        usleep(200000);
        raft_id new_leader = find_leader();
        if (new_leader == target) {
            std::cout << "Node " << target << " is now leader" << std::endl;
            return;
        }
    }
    std::cout << "Transfer may not have completed (check with 'list')" << std::endl;
}

static void cmd_put(const std::vector<std::string>& args) {
    if (args.size() < 3) {
        std::cout << "Usage: put <key> <value>" << std::endl;
        return;
    }

    // Find the leader
    raft_id leader = find_leader();
    if (leader == 0) {
        std::cout << "No leader found" << std::endl;
        return;
    }

    // Reconstruct value (may contain spaces)
    std::string key = args[1];
    std::string value;
    for (size_t i = 2; i < args.size(); i++) {
        if (!value.empty()) value += " ";
        value += args[i];
    }

    auto& child = children[leader];
    std::string resp = send_cmd(child, "PUT " + key + " " + value);
    if (resp.substr(0, 2) == "OK") {
        std::cout << "OK" << std::endl;
    } else {
        std::cout << resp << std::endl;
    }
}

static void cmd_get(const std::vector<std::string>& args) {
    if (args.size() < 2) {
        std::cout << "Usage: get <key> [id]" << std::endl;
        return;
    }

    std::string key = args[1];
    raft_id target = 0;

    if (args.size() >= 3) {
        // Target specific node
        target = std::stoull(args[2]);
    } else {
        // Default to leader for freshest data
        target = find_leader();
        if (target == 0) {
            // Fall back to any alive node
            for (auto& [id, child] : children) {
                if (child.alive) { target = id; break; }
            }
        }
    }

    if (target == 0 || !children.count(target) || !children[target].alive) {
        std::cout << "No available node" << std::endl;
        return;
    }

    auto& child = children[target];
    std::string resp = send_cmd(child, "GET " + key);
    auto tokens = proto::split(resp, 2);
    if (tokens.size() >= 2 && tokens[0] == "OK") {
        std::cout << tokens[1] << std::endl;
    } else if (tokens.size() >= 2 && tokens[1] == "NOTFOUND") {
        std::cout << "(not found)" << std::endl;
    } else {
        std::cout << resp << std::endl;
    }
}

static void cmd_help(const std::vector<std::string>&) {
    std::cout << "Commands:" << std::endl;
    std::cout << "  spawn <id1> [id2 ...]     Spawn and start raft nodes" << std::endl;
    std::cout << "  shutdown <id>             Cleanly shut down a node" << std::endl;
    std::cout << "  kill <id>                 Force-kill a node" << std::endl;
    std::cout << "  isolate <id> [mode]       Isolate node (inbound|outbound|both)" << std::endl;
    std::cout << "  deisolate <id>            Remove isolation" << std::endl;
    std::cout << "  list                      List all nodes with status" << std::endl;
    std::cout << "  transfer <id>             Transfer leadership to node" << std::endl;
    std::cout << "  put <key> <value>         Store a key-value pair" << std::endl;
    std::cout << "  get <key> [id]            Fetch a value (from any node, or specific)" << std::endl;
    std::cout << "  help                      Show this help" << std::endl;
    std::cout << "  quit / exit               Shutdown all and exit" << std::endl;
}

static void shutdown_all() {
    for (auto& [id, child] : children) {
        if (!child.alive) continue;
        // Try clean shutdown first
        send_cmd(child, "SHUTDOWN");
        waitpid(child.pid, nullptr, WNOHANG);
        // If still alive, kill
        if (::kill(child.pid, 0) == 0) {
            ::kill(child.pid, SIGKILL);
            waitpid(child.pid, nullptr, 0);
        }
        cleanup_child(child);
    }
}

int main(int argc, char* argv[]) {
    // Parse CLI args
    for (int i = 1; i < argc; i++) {
        if (std::string(argv[i]) == "--transport" && i + 1 < argc) {
            transport = argv[++i];
        }
    }

    // Find worker binary
    worker_path = find_worker_binary(argv[0]);

    // Create temp base directory
    char tpl[] = "/tmp/mochi-raft-ctl-XXXXXX";
    char* dir = mkdtemp(tpl);
    if (!dir) {
        perror("mkdtemp");
        return 1;
    }
    base_dir = dir;

    std::cout << "mochi-raft-ctl (data: " << base_dir << ")" << std::endl;
    std::cout << "Type 'help' for available commands." << std::endl;

    std::string line;
    while (true) {
        std::cout << "> " << std::flush;
        if (!std::getline(std::cin, line)) break;

        auto args = proto::split(line);
        if (args.empty()) continue;

        const std::string& cmd = args[0];

        if (cmd == "spawn") {
            cmd_spawn(args);
        } else if (cmd == "shutdown") {
            cmd_shutdown(args);
        } else if (cmd == "kill") {
            cmd_kill(args);
        } else if (cmd == "list" || cmd == "ls") {
            cmd_list(args);
        } else if (cmd == "isolate") {
            cmd_isolate(args);
        } else if (cmd == "deisolate") {
            cmd_deisolate(args);
        } else if (cmd == "transfer") {
            cmd_transfer(args);
        } else if (cmd == "put") {
            cmd_put(args);
        } else if (cmd == "get") {
            cmd_get(args);
        } else if (cmd == "help") {
            cmd_help(args);
        } else if (cmd == "quit" || cmd == "exit") {
            break;
        } else {
            std::cout << "Unknown command: " << cmd << " (type 'help')" << std::endl;
        }
    }

    std::cout << "Shutting down all nodes..." << std::endl;
    shutdown_all();

    // Clean up temp directory
    fs::remove_all(base_dir);
    return 0;
}
