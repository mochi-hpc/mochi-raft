#include <array>
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

    // Create a temporary data directory for each server.
    std::array<std::string, N> dirs;
    for (int i = 0; i < N; i++) {
        char tpl[] = "/tmp/mochi-raft-cluster-XXXXXX";
        dirs[i] = mkdtemp(tpl);
    }

    abt_io_instance_id abt_io = abt_io_init(1);

    // Create one Thallium engine per server using shared-memory transport.
    // In production, each server is a separate process on its own machine.
    std::array<std::unique_ptr<tl::engine>, N> engines;
    std::array<std::string, N> addrs;
    for (int i = 0; i < N; i++) {
        engines[i] = std::make_unique<tl::engine>(
            "na+sm", THALLIUM_SERVER_MODE, true, 1);
        addrs[i] = static_cast<std::string>(engines[i]->self());
    }

    // Build the initial cluster configuration: (server_id, address) pairs.
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
    }

    // All servers must call bootstrap() with the same cluster configuration
    // before start().  On a restart this call is a no-op.
    for (int i = 0; i < N; i++)
        servers[i]->bootstrap(cluster);

    // Start all servers; they will hold an election and choose a leader.
    for (int i = 0; i < N; i++)
        servers[i]->start();

    // Wait for a leader to emerge.
    int leader = -1;
    while (leader < 0) {
        for (int i = 0; i < N; i++) {
            if (servers[i]->state() == RAFT_LEADER) { leader = i; break; }
        }
        yield_ms(10);
    }
    std::cout << "Leader is server " << (leader + 1)
              << " (term " << servers[leader]->current_term() << ")"
              << std::endl;

    // Submit a log entry on the leader.
    const char* msg = "cluster-entry";
    servers[leader]->submit(mraft::MochiRaftBuffer{msg, strlen(msg)});

    // Wait for the entry to be committed across the cluster.
    raft_index initial = servers[leader]->commit_index();
    while (servers[leader]->commit_index() <= initial)
        yield_ms(10);
    std::cout << "Replicated and committed at index "
              << servers[leader]->commit_index() << std::endl;

    // Print cluster membership as seen by the leader.
    std::cout << "Cluster members:" << std::endl;
    for (auto& [id, addr] : servers[leader]->members())
        std::cout << "  server " << id << " at " << addr << std::endl;

    // Shut everything down in order.
    for (int i = 0; i < N; i++) servers[i]->shutdown();
    for (int i = 0; i < N; i++) { engines[i]->finalize(); engines[i].reset(); }
    abt_io_finalize(abt_io);
    for (int i = 0; i < N; i++) fs::remove_all(dirs[i]);

    ABT_finalize();
    return 0;
}
