#include <gtest/gtest.h>
#include <cstring>
#include <chrono>
#include <thread>
#include <filesystem>
#include <atomic>

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
    int apply(std::string_view) override {
        applied_count_.fetch_add(1);
        return 0;
    }

    int applied_count() const { return applied_count_.load(); }

private:
    std::atomic<int> applied_count_{0};
};

class SingleNodeTest : public ::testing::Test {
protected:
    void SetUp() override {
        char tpl[] = "/tmp/mochi-raft-single-XXXXXX";
        char* dir = mkdtemp(tpl);
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

        if (abt_io_ != ABT_IO_INSTANCE_NULL) {
            abt_io_finalize(abt_io_);
        }
        fs::remove_all(test_dir_);
    }

    std::string test_dir_;
    abt_io_instance_id abt_io_ = ABT_IO_INSTANCE_NULL;
    std::unique_ptr<tl::engine> engine_;
    std::string address_;
    std::unique_ptr<mraft::MochiRaftServer> server_;
    NoOpFsm fsm_;
};

static void yield_ms(int ms) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < deadline) {
        ABT_thread_yield();
    }
}

TEST_F(SingleNodeTest, BootstrapStartAndBecomeLeader) {
    auto pool = engine_->get_handler_pool();
    server_ = std::make_unique<mraft::MochiRaftServer>(
        *engine_, abt_io_, 1, pool, pool, test_dir_, fsm_);

    // Bootstrap with single server
    ASSERT_EQ(server_->bootstrap({{1, address_}}), 0);

    // Start
    ASSERT_EQ(server_->start(), 0);

    // Wait for leader election (single node should self-elect quickly)
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::seconds(5);
    while (server_->state() != RAFT_LEADER &&
           std::chrono::steady_clock::now() < deadline) {
        yield_ms(10);
    }

    EXPECT_EQ(server_->state(), RAFT_LEADER);
    EXPECT_GE(server_->current_term(), 1u);
    EXPECT_GE(server_->commit_index(), 1u);

    server_->shutdown();
}

TEST_F(SingleNodeTest, SubmitEntry) {
    auto pool = engine_->get_handler_pool();
    server_ = std::make_unique<mraft::MochiRaftServer>(
        *engine_, abt_io_, 1, pool, pool, test_dir_, fsm_);

    ASSERT_EQ(server_->bootstrap({{1, address_}}), 0);
    ASSERT_EQ(server_->start(), 0);

    // Wait for leader
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::seconds(5);
    while (server_->state() != RAFT_LEADER &&
           std::chrono::steady_clock::now() < deadline) {
        yield_ms(10);
    }
    ASSERT_EQ(server_->state(), RAFT_LEADER);

    // Submit an entry
    const char* data = "hello mochi-raft";
    ASSERT_EQ(server_->submit(mraft::MochiRaftBuffer{data, strlen(data)}), 0);

    // Wait for commit index to advance (entry should be committed quickly)
    raft_index initial_commit = server_->commit_index();
    deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (server_->commit_index() <= initial_commit &&
           std::chrono::steady_clock::now() < deadline) {
        yield_ms(10);
    }

    EXPECT_GT(server_->commit_index(), initial_commit);

    // Wait briefly for FSM application
    yield_ms(100);
    EXPECT_GE(fsm_.applied_count(), 1)
        << "FSM::apply() was not called for committed entry";

    server_->shutdown();
}

TEST_F(SingleNodeTest, SubmitWithCallback) {
    auto pool = engine_->get_handler_pool();
    server_ = std::make_unique<mraft::MochiRaftServer>(
        *engine_, abt_io_, 1, pool, pool, test_dir_, fsm_);

    ASSERT_EQ(server_->bootstrap({{1, address_}}), 0);
    ASSERT_EQ(server_->start(), 0);

    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (server_->state() != RAFT_LEADER &&
           std::chrono::steady_clock::now() < deadline)
        yield_ms(10);
    ASSERT_EQ(server_->state(), RAFT_LEADER);

    std::atomic<int> callback_rv{-1};
    const char* data = "callback-test";
    ASSERT_EQ(server_->submit(
        mraft::MochiRaftBuffer{data, strlen(data)},
        true,
        [&](int rv) { callback_rv.store(rv); }), 0);

    deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (callback_rv.load() == -1 &&
           std::chrono::steady_clock::now() < deadline)
        yield_ms(10);

    EXPECT_EQ(callback_rv.load(), 0);
    server_->shutdown();
}

int main(int argc, char** argv) {
    ABT_init(0, NULL);
    ::testing::InitGoogleTest(&argc, argv);
    int result = RUN_ALL_TESTS();
    ABT_finalize();
    return result;
}
