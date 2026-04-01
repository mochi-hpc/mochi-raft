#include <atomic>
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

class NoOpFsm : public mraft::Fsm {
public:
    int apply(std::string_view) override { return 0; }
};

static void yield_ms(int ms) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < deadline)
        ABT_thread_yield();
}

int main() {
    ABT_init(0, nullptr);

    char tpl[] = "/tmp/mochi-raft-cb-XXXXXX";
    char* dir  = mkdtemp(tpl);

    abt_io_instance_id abt_io = abt_io_init(1);
    tl::engine engine("na+sm", THALLIUM_SERVER_MODE, true, 1);
    std::string address = static_cast<std::string>(engine.self());

    NoOpFsm fsm;
    auto pool = engine.get_handler_pool();
    mraft::MochiRaftServer server(engine, abt_io, 1, pool, pool, dir, fsm);

    server.bootstrap({{1, address}});
    server.start();

    while (server.state() != RAFT_LEADER)
        yield_ms(10);

    // submit() with an on_applied callback.
    //
    // The callback fires from the event loop ULT, not the caller's thread,
    // so shared state must be protected with atomic types or tl::mutex.
    std::atomic<int> result{-1};

    const char* data = "entry-with-callback";
    server.submit(
        mraft::MochiRaftBuffer{data, strlen(data)},
        /*forward=*/true,
        [&](int rv) {
            // rv == 0  → applied successfully
            // rv != 0  → entry was rolled back (e.g. RAFT_NOTLEADER)
            result.store(rv);
        });

    // Wait for the callback to fire.
    while (result.load() == -1)
        yield_ms(10);

    if (result.load() == 0)
        std::cout << "Entry applied successfully." << std::endl;
    else
        std::cout << "Entry failed (rv=" << result.load() << ")." << std::endl;

    server.shutdown();
    engine.finalize();
    abt_io_finalize(abt_io);
    fs::remove_all(dir);

    ABT_finalize();
    return 0;
}
