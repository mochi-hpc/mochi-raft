#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>

#include <abt.h>
#include <abt-io.h>
#include <thallium.hpp>

#include "kv_fsm.hpp"

namespace tl = thallium;
namespace fs = std::filesystem;

static void yield_ms(int ms) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < deadline)
        ABT_thread_yield();
}

int main() {
    ABT_init(0, nullptr);

    char tpl[] = "/tmp/mochi-raft-kv-XXXXXX";
    char* dir  = mkdtemp(tpl);

    abt_io_instance_id abt_io = abt_io_init(1);
    tl::engine engine("na+sm", THALLIUM_SERVER_MODE, true, 1);
    std::string address = static_cast<std::string>(engine.self());

    KvFsm fsm;
    auto pool = engine.get_handler_pool();
    mraft::MochiRaftServer server(engine, abt_io, 1, pool, pool, dir, fsm);

    server.bootstrap({{1, address}});
    server.start();

    while (server.state() != RAFT_LEADER)
        yield_ms(10);

    // Submit a batch of SET commands using completion callbacks.
    // The callback increments an atomic counter so we can wait for all three.
    const char* cmds[] = {"SET foo bar", "SET hello world", "SET count 42"};
    std::atomic<int> applied{0};

    for (const char* cmd : cmds) {
        server.submit(
            mraft::MochiRaftBuffer{cmd, strlen(cmd)},
            /*forward=*/true,
            [&](int) { applied.fetch_add(1); });
    }

    // Wait until all three entries have been applied to the FSM.
    while (applied.load() < 3)
        yield_ms(10);

    // Read back state from the FSM.
    std::cout << "foo   = " << fsm.get("foo")   << std::endl;
    std::cout << "hello = " << fsm.get("hello") << std::endl;
    std::cout << "count = " << fsm.get("count") << std::endl;

    server.shutdown();
    engine.finalize();
    abt_io_finalize(abt_io);
    fs::remove_all(dir);

    ABT_finalize();
    return 0;
}
