#include <gtest/gtest.h>
#include <cstring>
#include <atomic>
#include <chrono>
#include <thread>

#include <thallium.hpp>

extern "C" {
#include <raft.h>
}

#include "../src/network.hpp"

namespace tl = thallium;

class NetworkTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create two engines using na+sm (shared memory transport)
        engine1_ = std::make_unique<tl::engine>(
            "na+sm", THALLIUM_SERVER_MODE, true, 1);
        engine2_ = std::make_unique<tl::engine>(
            "na+sm", THALLIUM_SERVER_MODE, true, 1);

        addr1_ = static_cast<std::string>(engine1_->self());
        addr2_ = static_cast<std::string>(engine2_->self());
    }

    void TearDown() override {
        net1_.reset();
        net2_.reset();
        engine1_->finalize();
        engine2_->finalize();
    }

    std::unique_ptr<tl::engine> engine1_;
    std::unique_ptr<tl::engine> engine2_;
    std::unique_ptr<mraft::Network> net1_;
    std::unique_ptr<mraft::Network> net2_;
    std::string addr1_;
    std::string addr2_;
};

TEST_F(NetworkTest, SendRequestVote) {
    struct raft_message received_msg;
    memset(&received_msg, 0, sizeof(received_msg));
    std::atomic<bool> received{false};

    net1_ = std::make_unique<mraft::Network>(
        *engine1_, 0, 1, addr1_,
        [](struct raft_message* msg) { delete msg; });

    net2_ = std::make_unique<mraft::Network>(
        *engine2_, 0, 2, addr2_,
        [&](struct raft_message* msg) {
            received_msg = *msg;
            // Free address copy
            if (msg->server_address)
                raft_free(const_cast<char*>(msg->server_address));
            delete msg;
            received.store(true);
        });

    // Send RequestVote from engine1 to engine2
    struct raft_message msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = RAFT_REQUEST_VOTE;
    msg.server_id = 2;  // destination server id
    msg.server_address = addr2_.c_str();
    msg.request_vote.version = 1;
    msg.request_vote.term = 5;
    msg.request_vote.candidate_id = 1;
    msg.request_vote.last_log_index = 10;
    msg.request_vote.last_log_term = 4;
    msg.request_vote.disrupt_leader = false;
    msg.request_vote.pre_vote = true;

    net1_->send(&msg, 1);

    // Wait for delivery
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::seconds(5);
    while (!received.load() &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_TRUE(received.load()) << "Message was not received within timeout";
    EXPECT_EQ(received_msg.type, RAFT_REQUEST_VOTE);
    // server_id should be the SENDER's id (net1_ has local_id=1)
    EXPECT_EQ(received_msg.server_id, 1u);
    EXPECT_EQ(received_msg.request_vote.term, 5u);
    EXPECT_EQ(received_msg.request_vote.candidate_id, 1u);
    EXPECT_EQ(received_msg.request_vote.last_log_index, 10u);
    EXPECT_EQ(received_msg.request_vote.last_log_term, 4u);
    EXPECT_TRUE(received_msg.request_vote.pre_vote);
}

TEST_F(NetworkTest, SendAppendEntriesWithEntries) {
    struct raft_message received_msg;
    memset(&received_msg, 0, sizeof(received_msg));
    std::atomic<bool> received{false};

    net1_ = std::make_unique<mraft::Network>(
        *engine1_, 0, 1, addr1_,
        [](struct raft_message* msg) { delete msg; });

    net2_ = std::make_unique<mraft::Network>(
        *engine2_, 0, 2, addr2_,
        [&](struct raft_message* msg) {
            received_msg = *msg;
            if (msg->server_address)
                raft_free(const_cast<char*>(msg->server_address));
            delete msg;
            received.store(true);
        });

    // Create entries
    struct raft_entry entries[2];
    memset(entries, 0, sizeof(entries));
    const char* d1 = "data1";
    entries[0].term = 3;
    entries[0].type = RAFT_COMMAND;
    entries[0].buf.base = const_cast<char*>(d1);
    entries[0].buf.len = strlen(d1);
    const char* d2 = "data2data2";
    entries[1].term = 3;
    entries[1].type = RAFT_COMMAND;
    entries[1].buf.base = const_cast<char*>(d2);
    entries[1].buf.len = strlen(d2);

    struct raft_message msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = RAFT_APPEND_ENTRIES;
    msg.server_id = 1;
    msg.server_address = addr2_.c_str();
    msg.append_entries.term = 3;
    msg.append_entries.prev_log_index = 5;
    msg.append_entries.prev_log_term = 2;
    msg.append_entries.leader_commit = 4;
    msg.append_entries.entries = entries;
    msg.append_entries.n_entries = 2;

    net1_->send(&msg, 1);

    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::seconds(5);
    while (!received.load() &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_TRUE(received.load());
    EXPECT_EQ(received_msg.type, RAFT_APPEND_ENTRIES);
    EXPECT_EQ(received_msg.append_entries.term, 3u);
    EXPECT_EQ(received_msg.append_entries.prev_log_index, 5u);
    ASSERT_EQ(received_msg.append_entries.n_entries, 2u);
    EXPECT_EQ(received_msg.append_entries.entries[0].term, 3u);
    EXPECT_EQ(received_msg.append_entries.entries[0].buf.len, 5u);
    EXPECT_EQ(memcmp(received_msg.append_entries.entries[0].buf.base,
                     "data1", 5), 0);
    EXPECT_EQ(received_msg.append_entries.entries[1].buf.len, 10u);

    // Clean up received entries
    mraft::free_message_entries(received_msg);
}

TEST_F(NetworkTest, SendRequestVoteResult) {
    struct raft_message received_msg;
    memset(&received_msg, 0, sizeof(received_msg));
    std::atomic<bool> received{false};

    net1_ = std::make_unique<mraft::Network>(
        *engine1_, 0, 1, addr1_,
        [&](struct raft_message* msg) {
            received_msg = *msg;
            if (msg->server_address)
                raft_free(const_cast<char*>(msg->server_address));
            delete msg;
            received.store(true);
        });

    net2_ = std::make_unique<mraft::Network>(
        *engine2_, 0, 2, addr2_,
        [](struct raft_message* msg) { delete msg; });

    struct raft_message msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = RAFT_REQUEST_VOTE_RESULT;
    msg.server_id = 1;  // destination server id
    msg.server_address = addr1_.c_str();
    msg.request_vote_result.term = 5;
    msg.request_vote_result.vote_granted = true;
    msg.request_vote_result.features = 0x01;
    msg.request_vote_result.capacity = 42;

    net2_->send(&msg, 1);

    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::seconds(5);
    while (!received.load() &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_TRUE(received.load());
    EXPECT_EQ(received_msg.type, RAFT_REQUEST_VOTE_RESULT);
    // server_id should be the SENDER's id (net2_ has local_id=2)
    EXPECT_EQ(received_msg.server_id, 2u);
    EXPECT_EQ(received_msg.request_vote_result.term, 5u);
    EXPECT_TRUE(received_msg.request_vote_result.vote_granted);
}

TEST_F(NetworkTest, SendTimeoutNow) {
    struct raft_message received_msg;
    memset(&received_msg, 0, sizeof(received_msg));
    std::atomic<bool> received{false};

    net1_ = std::make_unique<mraft::Network>(
        *engine1_, 0, 1, addr1_,
        [](struct raft_message* msg) { delete msg; });

    net2_ = std::make_unique<mraft::Network>(
        *engine2_, 0, 2, addr2_,
        [&](struct raft_message* msg) {
            received_msg = *msg;
            if (msg->server_address)
                raft_free(const_cast<char*>(msg->server_address));
            delete msg;
            received.store(true);
        });

    struct raft_message msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = RAFT_TIMEOUT_NOW;
    msg.server_id = 2;  // destination server id
    msg.server_address = addr2_.c_str();
    msg.timeout_now.term = 7;
    msg.timeout_now.last_log_index = 50;
    msg.timeout_now.last_log_term = 6;

    net1_->send(&msg, 1);

    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::seconds(5);
    while (!received.load() &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    ASSERT_TRUE(received.load());
    EXPECT_EQ(received_msg.type, RAFT_TIMEOUT_NOW);
    EXPECT_EQ(received_msg.timeout_now.term, 7u);
    EXPECT_EQ(received_msg.timeout_now.last_log_index, 50u);
    EXPECT_EQ(received_msg.timeout_now.last_log_term, 6u);
}
