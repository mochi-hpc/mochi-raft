#include <iostream>
#include <string>
#include <memory>
#include <cstdio>
#include <filesystem>
#include <unistd.h>

extern "C" {
#include <raft.h>
}

#include <abt.h>
#include <abt-io.h>
#include <thallium.hpp>
#include <mochi-raft.hpp>

#include "kv_fsm.hpp"
#include "pipe_protocol.hpp"

namespace tl = thallium;
namespace fs = std::filesystem;

enum class WorkerState {
    UNINITIALIZED,
    INITIALIZED,
    BOOTSTRAPPED,
    RUNNING,
    SHUTDOWN
};

static void respond(const std::string& line) {
    std::cout << line << std::endl;
    std::cout.flush();
}

int main() {
    // Ensure stdout is line-buffered for pipe communication
    setvbuf(stdout, nullptr, _IOLBF, 0);

    ABT_init(0, nullptr);

    WorkerState state = WorkerState::UNINITIALIZED;
    std::unique_ptr<tl::engine> engine;
    abt_io_instance_id abt_io = ABT_IO_INSTANCE_NULL;
    std::unique_ptr<mraft::MochiRaftServer> server;
    KeyValueFsm fsm;
    raft_id my_id = 0;
    std::string my_address;
    std::string data_dir;

    std::string line;
    while (std::getline(std::cin, line)) {
        auto tokens = proto::split(line, 0);
        if (tokens.empty()) continue;

        const std::string& cmd = tokens[0];

        if (cmd == "INIT") {
            if (state != WorkerState::UNINITIALIZED) {
                respond(proto::err("already initialized"));
                continue;
            }
            if (tokens.size() < 4) {
                respond(proto::err("usage: INIT <id> <transport> <data_dir>"));
                continue;
            }

            my_id = static_cast<raft_id>(std::stoull(tokens[1]));
            std::string transport = tokens[2];
            data_dir = tokens[3];

            // Create data directory
            fs::create_directories(data_dir);

            // Initialize ABT-IO
            abt_io = abt_io_init(1);
            if (abt_io == ABT_IO_INSTANCE_NULL) {
                respond(proto::err("abt_io_init failed"));
                continue;
            }

            // Initialize Thallium engine
            try {
                engine = std::make_unique<tl::engine>(
                    transport, THALLIUM_SERVER_MODE, true, 1);
                my_address = static_cast<std::string>(engine->self());
            } catch (const std::exception& e) {
                respond(proto::err(std::string("engine init: ") + e.what()));
                abt_io_finalize(abt_io);
                abt_io = ABT_IO_INSTANCE_NULL;
                continue;
            }

            state = WorkerState::INITIALIZED;
            respond(proto::ok(my_address));

        } else if (cmd == "BOOTSTRAP") {
            if (state != WorkerState::INITIALIZED) {
                respond(proto::err("must be in INITIALIZED state"));
                continue;
            }
            // Parse pairs: id1 addr1 id2 addr2 ...
            if (tokens.size() < 3 || (tokens.size() - 1) % 2 != 0) {
                respond(proto::err("usage: BOOTSTRAP <id1> <addr1> ..."));
                continue;
            }

            std::vector<std::pair<raft_id, std::string>> cluster;
            for (size_t i = 1; i < tokens.size(); i += 2) {
                raft_id sid = static_cast<raft_id>(std::stoull(tokens[i]));
                cluster.push_back({sid, tokens[i + 1]});
            }

            // Create server
            auto pool = engine->get_handler_pool();
            server = std::make_unique<mraft::MochiRaftServer>(
                *engine, abt_io, my_id, pool, pool, data_dir, fsm);

            int rv = server->bootstrap(cluster);
            if (rv != 0) {
                respond(proto::err("bootstrap failed: " + std::to_string(rv)));
                server.reset();
                continue;
            }

            state = WorkerState::BOOTSTRAPPED;
            respond(proto::ok());

        } else if (cmd == "START") {
            if (state != WorkerState::BOOTSTRAPPED) {
                respond(proto::err("must be in BOOTSTRAPPED state"));
                continue;
            }

            int rv = server->start();
            if (rv != 0) {
                respond(proto::err("start failed: " + std::to_string(rv)));
                continue;
            }

            state = WorkerState::RUNNING;
            respond(proto::ok());

        } else if (cmd == "STATUS") {
            if (state != WorkerState::RUNNING) {
                respond(proto::err("not running"));
                continue;
            }

            unsigned short s = server->state();
            const char* state_str = "unknown";
            switch (s) {
                case RAFT_UNAVAILABLE: state_str = "unavailable"; break;
                case RAFT_FOLLOWER:    state_str = "follower"; break;
                case RAFT_CANDIDATE:   state_str = "candidate"; break;
                case RAFT_LEADER:      state_str = "leader"; break;
            }

            std::string info = std::string(state_str) +
                " " + std::to_string(server->current_term()) +
                " " + std::to_string(server->commit_index());
            respond(proto::ok(info));

        } else if (cmd == "PUT") {
            if (state != WorkerState::RUNNING) {
                respond(proto::err("not running"));
                continue;
            }
            if (tokens.size() < 3) {
                respond(proto::err("usage: PUT <key> <value>"));
                continue;
            }

            // Reconstruct "PUT key value" as the entry data
            // tokens[0] = "PUT", tokens[1] = key, rest = value
            // Re-split with max_tokens to preserve value spaces
            auto put_tokens = proto::split(line, 3);
            std::string entry = "PUT " + put_tokens[1] + " " + put_tokens[2];

            auto commit_before = server->commit_index();
            int rv = server->submit(mraft::MochiRaftBuffer{entry});
            if (rv != 0) {
                if (rv == RAFT_NOTLEADER) {
                    respond(proto::err("NOTLEADER"));
                } else {
                    respond(proto::err("submit failed: " + std::to_string(rv)));
                }
                continue;
            }

            // Wait for the entry to be committed and applied.
            // Use a real-time deadline so we don't block the OS thread
            // indefinitely, but yield to let Argobots ULTs run.
            auto deadline = std::chrono::steady_clock::now() +
                            std::chrono::seconds(5);
            while (server->commit_index() <= commit_before &&
                   std::chrono::steady_clock::now() < deadline) {
                usleep(10000); // 10ms — main thread is separate from handler pool
            }
            respond(proto::ok());

        } else if (cmd == "GET") {
            if (state != WorkerState::RUNNING) {
                respond(proto::err("not running"));
                continue;
            }
            if (tokens.size() < 2) {
                respond(proto::err("usage: GET <key>"));
                continue;
            }

            auto val = fsm.get(tokens[1]);
            if (val.has_value()) {
                respond(proto::ok(val.value()));
            } else {
                respond(proto::err("NOTFOUND"));
            }

        } else if (cmd == "ISOLATE") {
            if (state != WorkerState::RUNNING) {
                respond(proto::err("not running"));
                continue;
            }

            mraft::IsolationMode mode = mraft::IsolationMode::BOTH;
            if (tokens.size() >= 2) {
                if (tokens[1] == "inbound") {
                    mode = mraft::IsolationMode::INBOUND;
                } else if (tokens[1] == "outbound") {
                    mode = mraft::IsolationMode::OUTBOUND;
                } else if (tokens[1] == "both") {
                    mode = mraft::IsolationMode::BOTH;
                }
            }

            server->set_isolation(mode);
            respond(proto::ok());

        } else if (cmd == "DEISOLATE") {
            if (state != WorkerState::RUNNING) {
                respond(proto::err("not running"));
                continue;
            }
            server->set_isolation(mraft::IsolationMode::NONE);
            respond(proto::ok());

        } else if (cmd == "TRANSFER") {
            if (state != WorkerState::RUNNING) {
                respond(proto::err("not running"));
                continue;
            }
            if (tokens.size() < 2) {
                respond(proto::err("usage: TRANSFER <target_id>"));
                continue;
            }

            raft_id target = static_cast<raft_id>(std::stoull(tokens[1]));
            int rv = server->transfer(target);
            if (rv != 0) {
                respond(proto::err("transfer failed: " + std::to_string(rv)));
                continue;
            }
            respond(proto::ok());

        } else if (cmd == "SHUTDOWN") {
            if (server) {
                server->shutdown();
                server.reset();
            }
            respond(proto::ok());
            state = WorkerState::SHUTDOWN;
            break;

        } else {
            respond(proto::err("unknown command: " + cmd));
        }
    }

    // Cleanup
    server.reset();
    if (engine) {
        engine->finalize();
        engine.reset();
    }
    if (abt_io != ABT_IO_INSTANCE_NULL) {
        abt_io_finalize(abt_io);
    }
    ABT_finalize();
    return 0;
}
