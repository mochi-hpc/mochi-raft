#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>

#include <abt.h>
#include <abt-io.h>
#include <thallium.hpp>

#include <mochi-raft.hpp>

namespace tl = thallium;
namespace fs = std::filesystem;

// A simple FSM that prints each applied entry to stdout.
class PrintFsm : public mraft::Fsm {
public:
    int apply(std::string_view data) override {
        std::cout << "FSM apply: " << data << std::endl;
        return 0;
    }
};

// Yield the current ULT for the given number of milliseconds.
static void yield_ms(int ms) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < deadline)
        ABT_thread_yield();
}

int main() {
    ABT_init(0, nullptr);

    // Create a temporary data directory for the Raft log.
    char tpl[] = "/tmp/mochi-raft-demo-XXXXXX";
    char* dir  = mkdtemp(tpl);

    // Initialize ABT-IO for persistent log and metadata I/O.
    abt_io_instance_id abt_io = abt_io_init(1);

    // Create a Thallium engine using shared-memory transport.
    tl::engine engine("na+sm", THALLIUM_SERVER_MODE, true, 1);
    std::string address = static_cast<std::string>(engine.self());
    std::cout << "Server address: " << address << std::endl;

    PrintFsm fsm;
    auto pool = engine.get_handler_pool();

    // Construct the Raft server.  The engine and FSM must outlive the server.
    mraft::MochiRaftServer server(engine, abt_io, /*id=*/1,
                                  pool, pool, dir, fsm);

    // Bootstrap with a single-member cluster configuration.
    server.bootstrap({{1, address}});

    // Start the event loop; the server now participates in elections.
    server.start();

    // A single-node cluster elects itself leader immediately.
    while (server.state() != RAFT_LEADER)
        yield_ms(10);
    std::cout << "Leader elected (term " << server.current_term() << ")"
              << std::endl;

    // Submit a log entry.
    const char* msg = "hello mochi-raft";
    server.submit(mraft::MochiRaftBuffer{msg, strlen(msg)});

    // Wait for the entry to be committed (and applied to the FSM).
    raft_index initial = server.commit_index();
    while (server.commit_index() <= initial)
        yield_ms(10);
    std::cout << "Committed at index " << server.commit_index() << std::endl;

    // Cleanly shut down before destroying the engine.
    server.shutdown();
    engine.finalize();
    abt_io_finalize(abt_io);
    fs::remove_all(dir);

    ABT_finalize();
    return 0;
}
