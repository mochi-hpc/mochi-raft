#include <gtest/gtest.h>
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <vector>

extern "C" {
#include <raft.h>
}

#include <abt.h>
#include <abt-io.h>
#include <thallium.hpp>

#include "../include/mochi-raft.hpp"

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

class LogIteratorTest : public ::testing::Test {
protected:
    void SetUp() override {
        char tpl[] = "/tmp/mochi-raft-iter-XXXXXX";
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

    void start_server() {
        auto pool = engine_->get_handler_pool();
        server_   = std::make_unique<mraft::MochiRaftServer>(
            *engine_, abt_io_, 1, pool, pool, test_dir_, fsm_);
        ASSERT_EQ(server_->bootstrap({{1, address_}}), 0);
        ASSERT_EQ(server_->start(), 0);

        // Wait for self-election.
        while (server_->state() != RAFT_LEADER)
            yield_ms(10);
    }

    // Submit `count` entries with payloads "entry-0", "entry-1", etc.
    // Returns when all entries have been committed.
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

    std::string                              test_dir_;
    abt_io_instance_id                       abt_io_ = ABT_IO_INSTANCE_NULL;
    std::unique_ptr<tl::engine>              engine_;
    std::string                              address_;
    std::unique_ptr<mraft::MochiRaftServer>  server_;
    NoOpFsm                                  fsm_;
};

// ── Basic replay of already-committed entries ────────────────────────────────

TEST_F(LogIteratorTest, ReplayCommittedEntries) {
    start_server();
    submit_entries(5);

    // The iterator starts after the server is already running and entries
    // are committed; it should replay them all.
    auto it = server_->entries(/*from=*/1);
    std::vector<std::string> received;

    // Read exactly 5 RAFT_COMMAND entries (entry-0 … entry-4).
    // Index 1 is the bootstrap RAFT_CHANGE entry, which is skipped.
    for (int i = 0; i < 5; i++) {
        ASSERT_TRUE(it.advance()) << "advance() returned false at i=" << i;
        received.emplace_back(it.current().data);
    }

    EXPECT_EQ(received[0], "entry-0");
    EXPECT_EQ(received[4], "entry-4");

    it.stop();
    server_->shutdown();
}

// ── Bounded range ────────────────────────────────────────────────────────────

TEST_F(LogIteratorTest, BoundedRange) {
    start_server();
    submit_entries(10);

    // Wait until all 10 entries are committed.
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (server_->commit_index() < 11 &&
           std::chrono::steady_clock::now() < deadline)
        yield_ms(10);

    raft_index ci = server_->commit_index();
    ASSERT_GE(ci, 11u);  // 1 config + 10 command entries

    // Ask for a bounded range: only the first 3 COMMAND entries.
    // Find the index of the 3rd COMMAND entry by counting.
    // Since index 1 = RAFT_CHANGE, command entries start at index 2.
    raft_index to = 4;  // entries at indices 2, 3, 4 → 3 command entries
    auto it = server_->entries(/*from=*/2, to);

    int count = 0;
    while (it.advance()) ++count;

    EXPECT_EQ(count, 3);

    server_->shutdown();
}

// ── Live tail: entries committed after iterator creation ─────────────────────

TEST_F(LogIteratorTest, LiveTail) {
    start_server();

    std::atomic<int> received{0};
    std::atomic<bool> iter_done{false};

    // Create the iterator BEFORE submitting anything.
    auto it = server_->entries();

    // Consume entries from a background ULT.
    auto pool   = engine_->get_handler_pool();
    auto thread = pool.make_thread([&]() {
        while (it.advance()) {
            received.fetch_add(1);
            if (received.load() >= 3) {
                it.stop();
                break;
            }
        }
        iter_done.store(true);
    });

    // Give the ULT time to start and block in advance().
    yield_ms(50);

    // Now submit entries; the iterator should receive them live.
    submit_entries(3);

    // Wait for the consumer ULT.
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!iter_done.load() &&
           std::chrono::steady_clock::now() < deadline)
        yield_ms(10);

    EXPECT_TRUE(iter_done.load());
    EXPECT_EQ(received.load(), 3);

    thread->join();
    server_->shutdown();
}

// ── stop() unblocks a waiting advance() ──────────────────────────────────────

TEST_F(LogIteratorTest, StopUnblocksAdvance) {
    start_server();

    auto it = server_->entries();
    std::atomic<bool> advance_returned{false};

    auto pool   = engine_->get_handler_pool();
    auto thread = pool.make_thread([&]() {
        // advance() will block because nothing is committed yet.
        bool result = it.advance();
        EXPECT_FALSE(result);  // stop() should cause it to return false
        advance_returned.store(true);
    });

    // Give the ULT time to enter advance() and block.
    yield_ms(100);
    EXPECT_FALSE(advance_returned.load());

    it.stop();

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!advance_returned.load() &&
           std::chrono::steady_clock::now() < deadline)
        yield_ms(10);

    EXPECT_TRUE(advance_returned.load());

    thread->join();
    server_->shutdown();
}

// ── shutdown() unblocks all waiting iterators ────────────────────────────────

TEST_F(LogIteratorTest, ShutdownUnblocksIterators) {
    start_server();

    auto it = server_->entries();
    std::atomic<bool> advance_returned{false};

    auto pool   = engine_->get_handler_pool();
    auto thread = pool.make_thread([&]() {
        bool result = it.advance();
        EXPECT_FALSE(result);  // shutdown marks EOF
        advance_returned.store(true);
    });

    yield_ms(100);
    EXPECT_FALSE(advance_returned.load());

    // shutdown() should mark all active iterators as EOF.
    server_->shutdown();

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!advance_returned.load() &&
           std::chrono::steady_clock::now() < deadline)
        yield_ms(10);

    EXPECT_TRUE(advance_returned.load());
    thread->join();
}

int main(int argc, char** argv) {
    ABT_init(0, NULL);
    ::testing::InitGoogleTest(&argc, argv);
    int result = RUN_ALL_TESTS();
    ABT_finalize();
    return result;
}
