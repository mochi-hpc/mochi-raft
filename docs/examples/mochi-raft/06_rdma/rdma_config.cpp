#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <vector>

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

    char tpl[] = "/tmp/mochi-raft-rdma-XXXXXX";
    char* dir  = mkdtemp(tpl);

    abt_io_instance_id abt_io = abt_io_init(1);
    tl::engine engine("na+sm", THALLIUM_SERVER_MODE, true, 1);
    std::string address = static_cast<std::string>(engine.self());

    NoOpFsm fsm;
    auto pool = engine.get_handler_pool();
    mraft::MochiRaftServer server(engine, abt_io, 1, pool, pool, dir, fsm);

    // Route every non-empty AppendEntries payload through RDMA bulk transfer.
    // The effective timeout per transfer is:
    //   max(min_timeout_ms, timeout_factor * payload_bytes) / 1000  seconds
    server.set_rdma_config(
        /*rdma_threshold=*/0,       // RDMA for all non-empty payloads
        /*min_timeout_ms=*/5000.0,  // at least 5 s per transfer
        /*timeout_factor=*/0.001);  // +1 ms per KiB of payload

    server.bootstrap({{1, address}});
    server.start();

    while (server.state() != RAFT_LEADER)
        yield_ms(10);

    // Create a 4 KiB payload and submit it.
    std::vector<char> payload(4096, 'x');
    server.submit(mraft::MochiRaftBuffer{payload.data(), payload.size()});

    raft_index initial = server.commit_index();
    while (server.commit_index() <= initial)
        yield_ms(10);

    std::cout << "4 KiB entry committed via RDMA at index "
              << server.commit_index() << std::endl;

    server.shutdown();
    engine.finalize();
    abt_io_finalize(abt_io);
    fs::remove_all(dir);

    ABT_finalize();
    return 0;
}
