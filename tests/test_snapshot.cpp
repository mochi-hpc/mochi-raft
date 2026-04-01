#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <sys/stat.h>

extern "C" {
#include <raft.h>
}

#include <abt.h>
#include <abt-io.h>
#include <thallium.hpp>

#include "../include/mochi-raft.hpp"

namespace tl = thallium;
namespace fs = std::filesystem;

// FSM that counts the number of applied entries and supports snapshot/restore.
class CountingFsm : public mraft::Fsm {
public:
    int apply(std::string_view) override {
        count_++;
        return 0;
    }

    int snapshot(std::string& out) override {
        out.resize(sizeof(int));
        memcpy(out.data(), &count_, sizeof(int));
        return 0;
    }

    int restore(std::string_view in) override {
        if (in.size() < sizeof(int)) return -1;
        memcpy(&count_, in.data(), sizeof(int));
        return 0;
    }

    int count() const { return count_; }

private:
    int count_ = 0;
};

static void yield_ms(int ms) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < deadline)
        ABT_thread_yield();
}

class SnapshotTest : public ::testing::Test {
protected:
    void SetUp() override {
        char tpl[] = "/tmp/mochi-raft-snap-XXXXXX";
        char* dir  = mkdtemp(tpl);
        ASSERT_NE(dir, nullptr);
        test_dir_ = dir;

        abt_io_ = abt_io_init(1);
        ASSERT_NE(abt_io_, ABT_IO_INSTANCE_NULL);

        engine_ = std::make_unique<tl::engine>(
            "na+sm", THALLIUM_SERVER_MODE, true, 1);
        address_ = static_cast<std::string>(engine_->self());
    }

    void TearDown() override {
        server_.reset();
        engine_->finalize();
        engine_.reset();
        if (abt_io_ != ABT_IO_INSTANCE_NULL)
            abt_io_finalize(abt_io_);
        fs::remove_all(test_dir_);
    }

    // Start a fresh server with the given FSM and snapshot threshold.
    // Calls bootstrap() on the first call (when bootstrap=true).
    void start_server(CountingFsm& fsm, unsigned threshold, bool do_bootstrap = true) {
        auto pool = engine_->get_handler_pool();
        server_   = std::make_unique<mraft::MochiRaftServer>(
            *engine_, abt_io_, 1, pool, pool, test_dir_, fsm);
        server_->set_snapshot_threshold(threshold);
        if (do_bootstrap)
            ASSERT_EQ(server_->bootstrap({{1, address_}}), 0);
        ASSERT_EQ(server_->start(), 0);

        while (server_->state() != RAFT_LEADER)
            yield_ms(10);
    }

    void submit_entries(int count) {
        std::atomic<int> done{0};
        for (int i = 0; i < count; i++) {
            std::string payload = "entry-" + std::to_string(i);
            server_->submit(
                mraft::MochiRaftBuffer{payload.data(), payload.size()},
                /*forward=*/true,
                [&](int) { done.fetch_add(1); });
        }
        auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(10);
        while (done.load() < count &&
               std::chrono::steady_clock::now() < deadline)
            yield_ms(10);
        ASSERT_EQ(done.load(), count);
    }

    std::string                             test_dir_;
    abt_io_instance_id                      abt_io_ = ABT_IO_INSTANCE_NULL;
    std::unique_ptr<tl::engine>             engine_;
    std::string                             address_;
    std::unique_ptr<mraft::MochiRaftServer> server_;
};

// ── Snapshot is written to disk after threshold is crossed ───────────────────

TEST_F(SnapshotTest, SnapshotTaken) {
    CountingFsm fsm;
    start_server(fsm, /*threshold=*/5);

    submit_entries(10);

    // Give the event loop time to apply entries and take the snapshot.
    yield_ms(200);

    server_->shutdown();

    std::string snap_path = test_dir_ + "/snapshot";
    struct stat st;
    ASSERT_EQ(stat(snap_path.c_str(), &st), 0)
        << "snapshot file not found at " << snap_path;
    EXPECT_GT(st.st_size, 0);
}

// ── FSM state is correctly restored from snapshot on restart ─────────────────

TEST_F(SnapshotTest, RestoreFromSnapshot) {
    // Phase 1: start, submit entries, shut down.
    {
        CountingFsm fsm;
        start_server(fsm, /*threshold=*/5);
        submit_entries(10);
        yield_ms(200);           // let snapshot be written
        EXPECT_EQ(fsm.count(), 10);
        server_->shutdown();
    }

    // Phase 2: restart from the same data directory.
    server_.reset();
    CountingFsm fsm2;
    start_server(fsm2, /*threshold=*/5, /*do_bootstrap=*/false);

    // The snapshot should have been loaded and restore() called.
    EXPECT_EQ(fsm2.count(), 10);

    server_->shutdown();
}

// ── No snapshot is written when count is below threshold ─────────────────────

TEST_F(SnapshotTest, SnapshotThresholdRespected) {
    CountingFsm fsm;
    start_server(fsm, /*threshold=*/20);

    submit_entries(5);
    yield_ms(200);

    server_->shutdown();

    std::string snap_path = test_dir_ + "/snapshot";
    struct stat st;
    EXPECT_NE(stat(snap_path.c_str(), &st), 0)
        << "snapshot file should not exist yet";
}

int main(int argc, char** argv) {
    ABT_init(0, NULL);
    ::testing::InitGoogleTest(&argc, argv);
    int result = RUN_ALL_TESTS();
    ABT_finalize();
    return result;
}
