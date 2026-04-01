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

static void yield_ms(int ms) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < deadline)
        ABT_thread_yield();
}

// A minimal FSM that counts applied entries and supports snapshotting.
// The entire state is a single integer, serialised as four raw bytes.
class CountingFsm : public mraft::Fsm {
public:
    int apply(std::string_view) override {
        ++count_;
        return 0;
    }

    int snapshot(std::string& out) override {
        out.resize(sizeof(int));
        std::memcpy(out.data(), &count_, sizeof(int));
        return 0;
    }

    int restore(std::string_view in) override {
        if (in.size() < sizeof(int)) return -1;
        std::memcpy(&count_, in.data(), sizeof(int));
        return 0;
    }

    int count() const { return count_; }

private:
    int count_ = 0;
};

// Helper: submit `n` entries and wait until all are applied.
static void submit_entries(mraft::MochiRaftServer& server, int n) {
    std::atomic<int> done{0};
    for (int i = 0; i < n; i++) {
        const char msg[] = "tick";
        server.submit(
            mraft::MochiRaftBuffer{msg, sizeof(msg) - 1},
            /*forward=*/true,
            [&](int) { done.fetch_add(1); });
    }
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (done.load() < n && std::chrono::steady_clock::now() < deadline)
        yield_ms(10);
}

int main() {
    ABT_init(0, nullptr);

    // Use a persistent directory so we can restart and load the snapshot.
    const char* data_dir = "/tmp/mochi-raft-snapshot-example";
    fs::remove_all(data_dir);
    fs::create_directory(data_dir);

    // ── Phase 1: run the server, apply entries, take a snapshot ─────────────

    std::cout << "Phase 1: submitting entries..." << std::endl;
    {
        abt_io_instance_id abt_io = abt_io_init(1);
        tl::engine engine("na+sm", THALLIUM_SERVER_MODE, true, 1);
        std::string address = static_cast<std::string>(engine.self());

        CountingFsm fsm;
        auto pool = engine.get_handler_pool();
        mraft::MochiRaftServer server(engine, abt_io, 1, pool, pool, data_dir, fsm);

        // Take a snapshot every 5 entries, keeping 2 trailing entries.
        server.set_snapshot_threshold(5);
        server.set_snapshot_trailing(2);

        server.bootstrap({{1, address}});
        server.start();

        while (server.state() != RAFT_LEADER)
            yield_ms(10);

        // Submit 10 entries — this crosses the threshold twice.
        submit_entries(server, 10);

        // Give the event loop time to apply entries and flush the snapshot.
        yield_ms(200);

        std::cout << "  FSM count after 10 entries: " << fsm.count() << std::endl;
        std::cout << "  First log index still available: "
                  << server.log_start_index() << std::endl;

        server.shutdown();
        engine.finalize();
        abt_io_finalize(abt_io);
    }

    // ── Phase 2: restart, load snapshot, verify restored state ──────────────

    std::cout << "Phase 2: restarting from snapshot..." << std::endl;
    {
        abt_io_instance_id abt_io = abt_io_init(1);
        // Use a different address so the engine picks a fresh port.
        tl::engine engine("na+sm", THALLIUM_SERVER_MODE, true, 1);

        CountingFsm fsm;
        auto pool = engine.get_handler_pool();
        mraft::MochiRaftServer server(engine, abt_io, 1, pool, pool, data_dir, fsm);
        server.set_snapshot_threshold(5);

        // No bootstrap() — the existing log and snapshot are loaded by start().
        server.start();

        while (server.state() != RAFT_LEADER)
            yield_ms(10);

        std::cout << "  FSM count after restart: " << fsm.count() << std::endl;
        std::cout << "  First log index still available: "
                  << server.log_start_index() << std::endl;

        // When creating an iterator after a snapshot, always check
        // log_start_index() so you don't request entries that were discarded.
        raft_index from = 1;
        raft_index safe_from = std::max(from, server.log_start_index());
        if (safe_from > from)
            std::cout << "  Note: entries before index " << safe_from
                      << " were compacted; iterator starts there." << std::endl;

        server.shutdown();
        engine.finalize();
        abt_io_finalize(abt_io);
    }

    fs::remove_all(data_dir);
    ABT_finalize();
    return 0;
}
