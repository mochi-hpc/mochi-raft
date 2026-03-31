#include <gtest/gtest.h>
#include <cstring>
#include <sstream>

#include <cereal/archives/binary.hpp>

extern "C" {
#include <raft.h>
}

#include "../src/serialization.hpp"

using namespace mraft;

// Round-trip helper: serialize then deserialize a MessageData
static MessageData round_trip(const MessageData& in) {
    std::stringstream ss;
    {
        cereal::BinaryOutputArchive oar(ss);
        MessageData copy = in;
        oar(copy);
    }
    MessageData out;
    {
        cereal::BinaryInputArchive iar(ss);
        iar(out);
    }
    return out;
}

TEST(Serialization, RequestVote) {
    struct raft_message msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = RAFT_REQUEST_VOTE;
    msg.server_id = 42;
    msg.server_address = "addr42";
    msg.request_vote.version = 1;
    msg.request_vote.term = 5;
    msg.request_vote.candidate_id = 42;
    msg.request_vote.last_log_index = 10;
    msg.request_vote.last_log_term = 4;
    msg.request_vote.disrupt_leader = true;
    msg.request_vote.pre_vote = false;

    MessageData data = MessageData::from_raft(msg);
    MessageData out = round_trip(data);

    EXPECT_EQ(out.type, RAFT_REQUEST_VOTE);
    EXPECT_EQ(out.server_id, 42u);
    EXPECT_EQ(out.server_address, "addr42");
    EXPECT_EQ(out.request_vote.version, 1);
    EXPECT_EQ(out.request_vote.term, 5u);
    EXPECT_EQ(out.request_vote.candidate_id, 42u);
    EXPECT_EQ(out.request_vote.last_log_index, 10u);
    EXPECT_EQ(out.request_vote.last_log_term, 4u);
    EXPECT_TRUE(out.request_vote.disrupt_leader);
    EXPECT_FALSE(out.request_vote.pre_vote);

    // Verify conversion back to raft struct
    struct raft_message restored;
    out.to_raft(restored);
    EXPECT_EQ(restored.type, RAFT_REQUEST_VOTE);
    EXPECT_EQ(restored.request_vote.term, 5u);
    EXPECT_EQ(restored.request_vote.candidate_id, 42u);
    EXPECT_EQ(restored.request_vote.last_log_index, 10u);
    EXPECT_EQ(restored.request_vote.last_log_term, 4u);
    EXPECT_TRUE(restored.request_vote.disrupt_leader);
}

TEST(Serialization, RequestVoteResult) {
    struct raft_message msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = RAFT_REQUEST_VOTE_RESULT;
    msg.server_id = 7;
    msg.server_address = "addr7";
    msg.request_vote_result.version = 1;
    msg.request_vote_result.term = 3;
    msg.request_vote_result.vote_granted = true;
    msg.request_vote_result.pre_vote = true;
    msg.request_vote_result.features = 0x01;
    msg.request_vote_result.capacity = 100;

    MessageData data = MessageData::from_raft(msg);
    MessageData out = round_trip(data);

    EXPECT_EQ(out.type, RAFT_REQUEST_VOTE_RESULT);
    EXPECT_EQ(out.request_vote_result.term, 3u);
    EXPECT_TRUE(out.request_vote_result.vote_granted);
    EXPECT_TRUE(out.request_vote_result.pre_vote);
    EXPECT_EQ(out.request_vote_result.features, 0x01);
    EXPECT_EQ(out.request_vote_result.capacity, 100);

    struct raft_message restored;
    out.to_raft(restored);
    EXPECT_EQ(restored.request_vote_result.term, 3u);
    EXPECT_TRUE(restored.request_vote_result.vote_granted);
}

TEST(Serialization, AppendEntriesEmpty) {
    struct raft_message msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = RAFT_APPEND_ENTRIES;
    msg.server_id = 1;
    msg.server_address = "leader";
    msg.append_entries.version = 0;
    msg.append_entries.term = 2;
    msg.append_entries.prev_log_index = 5;
    msg.append_entries.prev_log_term = 2;
    msg.append_entries.leader_commit = 4;
    msg.append_entries.entries = nullptr;
    msg.append_entries.n_entries = 0;

    MessageData data = MessageData::from_raft(msg);
    MessageData out = round_trip(data);

    EXPECT_EQ(out.type, RAFT_APPEND_ENTRIES);
    EXPECT_EQ(out.append_entries.term, 2u);
    EXPECT_EQ(out.append_entries.prev_log_index, 5u);
    EXPECT_EQ(out.append_entries.prev_log_term, 2u);
    EXPECT_EQ(out.append_entries.leader_commit, 4u);
    EXPECT_TRUE(out.append_entries.entries.empty());

    struct raft_message restored;
    out.to_raft(restored);
    EXPECT_EQ(restored.append_entries.n_entries, 0u);
    EXPECT_EQ(restored.append_entries.entries, nullptr);
}

TEST(Serialization, AppendEntriesWithEntries) {
    // Create entries
    struct raft_entry entries[2];
    memset(entries, 0, sizeof(entries));

    const char* data1 = "hello";
    entries[0].term = 2;
    entries[0].type = RAFT_COMMAND;
    entries[0].buf.base = const_cast<char*>(data1);
    entries[0].buf.len = strlen(data1);

    const char* data2 = "world!!";
    entries[1].term = 3;
    entries[1].type = RAFT_COMMAND;
    entries[1].buf.base = const_cast<char*>(data2);
    entries[1].buf.len = strlen(data2);

    struct raft_message msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = RAFT_APPEND_ENTRIES;
    msg.server_id = 1;
    msg.server_address = "leader";
    msg.append_entries.term = 3;
    msg.append_entries.prev_log_index = 10;
    msg.append_entries.prev_log_term = 2;
    msg.append_entries.leader_commit = 8;
    msg.append_entries.entries = entries;
    msg.append_entries.n_entries = 2;

    MessageData data = MessageData::from_raft(msg);
    MessageData out = round_trip(data);

    ASSERT_EQ(out.append_entries.entries.size(), 2u);
    EXPECT_EQ(out.append_entries.entries[0].term, 2u);
    EXPECT_EQ(out.append_entries.entries[0].type, RAFT_COMMAND);
    EXPECT_EQ(std::string(out.append_entries.entries[0].data.begin(),
                           out.append_entries.entries[0].data.end()),
              "hello");
    EXPECT_EQ(out.append_entries.entries[1].term, 3u);
    EXPECT_EQ(std::string(out.append_entries.entries[1].data.begin(),
                           out.append_entries.entries[1].data.end()),
              "world!!");

    // Convert back and verify
    struct raft_message restored;
    out.to_raft(restored);
    ASSERT_EQ(restored.append_entries.n_entries, 2u);
    EXPECT_EQ(restored.append_entries.entries[0].term, 2u);
    EXPECT_EQ(restored.append_entries.entries[0].buf.len, 5u);
    EXPECT_EQ(memcmp(restored.append_entries.entries[0].buf.base, "hello", 5),
              0);
    EXPECT_EQ(restored.append_entries.entries[1].term, 3u);
    EXPECT_EQ(restored.append_entries.entries[1].buf.len, 7u);
    EXPECT_EQ(
        memcmp(restored.append_entries.entries[1].buf.base, "world!!", 7), 0);

    free_message_entries(restored);
}

TEST(Serialization, AppendEntriesResult) {
    struct raft_message msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = RAFT_APPEND_ENTRIES_RESULT;
    msg.server_id = 3;
    msg.server_address = "follower";
    msg.append_entries_result.version = 1;
    msg.append_entries_result.term = 5;
    msg.append_entries_result.rejected = 0;
    msg.append_entries_result.last_log_index = 15;
    msg.append_entries_result.features = 0x03;
    msg.append_entries_result.capacity = 200;

    MessageData data = MessageData::from_raft(msg);
    MessageData out = round_trip(data);

    EXPECT_EQ(out.type, RAFT_APPEND_ENTRIES_RESULT);
    EXPECT_EQ(out.append_entries_result.term, 5u);
    EXPECT_EQ(out.append_entries_result.rejected, 0u);
    EXPECT_EQ(out.append_entries_result.last_log_index, 15u);
    EXPECT_EQ(out.append_entries_result.features, 0x03);
    EXPECT_EQ(out.append_entries_result.capacity, 200);
}

TEST(Serialization, InstallSnapshot) {
    struct raft_message msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = RAFT_INSTALL_SNAPSHOT;
    msg.server_id = 1;
    msg.server_address = "leader";
    msg.install_snapshot.version = 0;
    msg.install_snapshot.term = 10;
    msg.install_snapshot.last_index = 100;
    msg.install_snapshot.last_term = 9;
    msg.install_snapshot.conf_index = 50;

    raft_configuration_init(&msg.install_snapshot.conf);
    raft_configuration_add(&msg.install_snapshot.conf, 1, "addr1",
                           RAFT_VOTER);
    raft_configuration_add(&msg.install_snapshot.conf, 2, "addr2",
                           RAFT_VOTER);
    raft_configuration_add(&msg.install_snapshot.conf, 3, "addr3",
                           RAFT_STANDBY);

    const char* snap_data = "snapshot-data-here";
    msg.install_snapshot.data.base = const_cast<char*>(snap_data);
    msg.install_snapshot.data.len = strlen(snap_data);

    MessageData data = MessageData::from_raft(msg);
    MessageData out = round_trip(data);

    EXPECT_EQ(out.type, RAFT_INSTALL_SNAPSHOT);
    EXPECT_EQ(out.install_snapshot.term, 10u);
    EXPECT_EQ(out.install_snapshot.last_index, 100u);
    EXPECT_EQ(out.install_snapshot.last_term, 9u);
    EXPECT_EQ(out.install_snapshot.conf_index, 50u);
    ASSERT_EQ(out.install_snapshot.conf_servers.size(), 3u);
    EXPECT_EQ(out.install_snapshot.conf_servers[0].id, 1u);
    EXPECT_EQ(out.install_snapshot.conf_servers[0].address, "addr1");
    EXPECT_EQ(out.install_snapshot.conf_servers[0].role, RAFT_VOTER);
    EXPECT_EQ(out.install_snapshot.conf_servers[2].role, RAFT_STANDBY);
    EXPECT_EQ(std::string(out.install_snapshot.data.begin(),
                           out.install_snapshot.data.end()),
              "snapshot-data-here");

    // Convert back
    struct raft_message restored;
    out.to_raft(restored);
    EXPECT_EQ(restored.install_snapshot.conf.n, 3u);
    EXPECT_STREQ(restored.install_snapshot.conf.servers[0].address, "addr1");
    EXPECT_EQ(restored.install_snapshot.data.len, strlen(snap_data));

    free_message_entries(restored);
    raft_configuration_close(&msg.install_snapshot.conf);
}

TEST(Serialization, TimeoutNow) {
    struct raft_message msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = RAFT_TIMEOUT_NOW;
    msg.server_id = 1;
    msg.server_address = "leader";
    msg.timeout_now.version = 0;
    msg.timeout_now.term = 7;
    msg.timeout_now.last_log_index = 50;
    msg.timeout_now.last_log_term = 6;

    MessageData data = MessageData::from_raft(msg);
    MessageData out = round_trip(data);

    EXPECT_EQ(out.type, RAFT_TIMEOUT_NOW);
    EXPECT_EQ(out.timeout_now.term, 7u);
    EXPECT_EQ(out.timeout_now.last_log_index, 50u);
    EXPECT_EQ(out.timeout_now.last_log_term, 6u);

    struct raft_message restored;
    out.to_raft(restored);
    EXPECT_EQ(restored.timeout_now.term, 7u);
    EXPECT_EQ(restored.timeout_now.last_log_index, 50u);
}
