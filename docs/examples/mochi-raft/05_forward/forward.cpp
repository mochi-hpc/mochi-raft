#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <iostream>
#include <memory>
#include <vector>

#include <abt.h>
#include <abt-io.h>
#include <thallium.hpp>

#include <mochi-raft.hpp>

namespace tl = thallium;
namespace fs = std::filesystem;

static constexpr int N = 3;

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

    std::array<std::string, N> dirs;
    for (int i = 0; i < N; i++) {
        char tpl[] = "/tmp/mochi-raft-fwd-XXXXXX";
        dirs[i] = mkdtemp(tpl);
    }

    abt_io_instance_id abt_io = abt_io_init(1);

    std::array<std::unique_ptr<tl::engine>, N> engines;
    std::array<std::string, N> addrs;
    for (int i = 0; i < N; i++) {
        engines[i] = std::make_unique<tl::engine>(
            "na+sm", THALLIUM_SERVER_MODE, true, 1);
        addrs[i] = static_cast<std::string>(engines[i]->self());
    }

    std::vector<std::pair<raft_id, std::string>> cluster;
    for (int i = 0; i < N; i++)
        cluster.push_back({static_cast<raft_id>(i + 1), addrs[i]});

    NoOpFsm fsms[N];
    std::array<std::unique_ptr<mraft::MochiRaftServer>, N> servers;
    for (int i = 0; i < N; i++) {
        auto pool = engines[i]->get_handler_pool();
        servers[i] = std::make_unique<mraft::MochiRaftServer>(
            *engines[i], abt_io, static_cast<raft_id>(i + 1),
            pool, pool, dirs[i], fsms[i]);
        servers[i]->bootstrap(cluster);
    }
    for (int i = 0; i < N; i++)
        servers[i]->start();

    // Wait for a leader to emerge.
    int leader = -1;
    while (leader < 0) {
        for (int i = 0; i < N; i++)
            if (servers[i]->state() == RAFT_LEADER) { leader = i; break; }
        yield_ms(10);
    }

    // Pick any follower.
    int follower = (leader + 1) % N;
    std::cout << "Leader: server "   << (leader   + 1) << std::endl;
    std::cout << "Follower: server " << (follower + 1) << std::endl;

    // submit() from the follower with forward=true (the default).
    // The library forwards the entry to the leader transparently; the
    // completion callback fires on this server once the leader has applied it.
    std::atomic<int> result{-1};
    const char* data = "forwarded-entry";

    servers[follower]->submit(
        mraft::MochiRaftBuffer{data, strlen(data)},
        /*forward=*/true,
        [&](int rv) { result.store(rv); });

    // Wait for the forwarded entry to complete.
    while (result.load() == -1)
        yield_ms(10);

    std::cout << "Forward result: " << result.load()
              << (result.load() == 0 ? " (success)" : " (error)") << std::endl;

    for (int i = 0; i < N; i++) servers[i]->shutdown();
    for (int i = 0; i < N; i++) { engines[i]->finalize(); engines[i].reset(); }
    abt_io_finalize(abt_io);
    for (int i = 0; i < N; i++) fs::remove_all(dirs[i]);

    ABT_finalize();
    return 0;
}
