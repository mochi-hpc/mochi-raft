#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <chrono>

extern "C" {
#include <raft.h>
#include <abt.h>
#include <abt-io.h>
}

#include "../src/storage.hpp"

namespace fs = std::filesystem;

static raft_time now_ms() {
    auto tp = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(tp).count();
}

class RaftStepTest : public ::testing::Test {
protected:
    void SetUp() override {
        char tpl[] = "/tmp/mochi-raft-step-XXXXXX";
        char* dir = mkdtemp(tpl);
        ASSERT_NE(dir, nullptr);
        test_dir_ = dir;

        abt_io_ = abt_io_init(1);
        ASSERT_NE(abt_io_, ABT_IO_INSTANCE_NULL);
    }

    void TearDown() override {
        if (abt_io_ != ABT_IO_INSTANCE_NULL) {
            abt_io_finalize(abt_io_);
        }
        fs::remove_all(test_dir_);
    }

    std::string test_dir_;
    abt_io_instance_id abt_io_ = ABT_IO_INSTANCE_NULL;
};

TEST_F(RaftStepTest, SingleNodeElectionAndSubmit) {
    // 1. Bootstrap storage with a single server
    mochi_raft::Storage storage(abt_io_, test_dir_);
    ASSERT_EQ(storage.init(), 0);

    struct raft_configuration conf;
    raft_configuration_init(&conf);
    ASSERT_EQ(raft_configuration_add(&conf, 1, "addr1", RAFT_VOTER), 0);
    ASSERT_EQ(storage.bootstrap(&conf), 0);
    raft_configuration_close(&conf);

    // 2. Load state
    raft_term term;
    raft_id voted_for;
    raft_index start_index;
    struct raft_entry* entries = nullptr;
    size_t n_entries = 0;

    ASSERT_EQ(storage.load(&term, &voted_for, &start_index,
                           &entries, &n_entries), 0);
    EXPECT_EQ(term, 1u);
    EXPECT_EQ(start_index, 1u);
    ASSERT_EQ(n_entries, 1u); // The RAFT_CHANGE entry from bootstrap

    // 3. Initialize raft
    struct raft r;
    memset(&r, 0, sizeof(r));
    int rv = raft_init(&r, NULL, NULL, 1, "addr1");
    ASSERT_EQ(rv, 0);

    raft_seed(&r, 12345);
    raft_set_election_timeout(&r, 200);
    raft_set_heartbeat_timeout(&r, 50);

    // 4. RAFT_START event
    struct raft_event event;
    struct raft_update update;
    memset(&event, 0, sizeof(event));
    memset(&update, 0, sizeof(update));

    event.type = RAFT_START;
    event.time = now_ms();
    event.start.term = term;
    event.start.voted_for = voted_for;
    event.start.metadata = nullptr; // No snapshot
    event.start.start_index = start_index;
    event.start.entries = entries;
    event.start.n_entries = static_cast<unsigned>(n_entries);

    rv = raft_step(&r, &event, &update);
    ASSERT_EQ(rv, 0);

    // Helper lambda to process an update's persistence requirements
    auto process_update = [&](struct raft_update& upd) {
        if (upd.flags & RAFT_UPDATE_CURRENT_TERM) {
            storage.set_term(raft_current_term(&r));
        }
        if (upd.flags & RAFT_UPDATE_VOTED_FOR) {
            storage.set_vote(raft_voted_for(&r));
        }
        if (upd.flags & RAFT_UPDATE_ENTRIES) {
            storage.append(upd.entries.index, upd.entries.batch,
                           upd.entries.n);

            memset(&event, 0, sizeof(event));
            event.type = RAFT_PERSISTED_ENTRIES;
            event.time = now_ms();
            event.persisted_entries.index =
                upd.entries.index + upd.entries.n - 1;

            memset(&upd, 0, sizeof(upd));
            rv = raft_step(&r, &event, &upd);
            ASSERT_EQ(rv, 0);

            // Recursively handle persistence from the persisted-entries response
            if (upd.flags & RAFT_UPDATE_CURRENT_TERM) {
                storage.set_term(raft_current_term(&r));
            }
            if (upd.flags & RAFT_UPDATE_VOTED_FOR) {
                storage.set_vote(raft_voted_for(&r));
            }
        }
    };

    process_update(update);

    // 5. If not yet leader, fire RAFT_TIMEOUT to trigger election
    if (raft_state(&r) != RAFT_LEADER) {
        raft_time timeout = raft_timeout(&r);
        memset(&event, 0, sizeof(event));
        event.type = RAFT_TIMEOUT;
        event.time = timeout;

        memset(&update, 0, sizeof(update));
        rv = raft_step(&r, &event, &update);
        ASSERT_EQ(rv, 0);

        process_update(update);
    }

    // The single node should now be leader
    ASSERT_EQ(raft_state(&r), RAFT_LEADER)
        << "State is " << raft_state(&r)
        << " (expected RAFT_LEADER=" << RAFT_LEADER << ")";

    // 6. RAFT_SUBMIT — submit a new entry
    struct raft_entry submit_entry;
    memset(&submit_entry, 0, sizeof(submit_entry));
    submit_entry.term = raft_current_term(&r);
    submit_entry.type = RAFT_COMMAND;
    const char* data = "hello raft";
    submit_entry.buf.base = raft_malloc(strlen(data));
    memcpy(submit_entry.buf.base, data, strlen(data));
    submit_entry.buf.len = strlen(data);
    submit_entry.batch = submit_entry.buf.base;

    memset(&event, 0, sizeof(event));
    event.type = RAFT_SUBMIT;
    event.time = now_ms();
    event.submit.entries = &submit_entry;
    event.submit.n = 1;

    memset(&update, 0, sizeof(update));
    rv = raft_step(&r, &event, &update);
    ASSERT_EQ(rv, 0);

    // Should have entries to persist
    EXPECT_TRUE(update.flags & RAFT_UPDATE_ENTRIES);

    if (update.flags & RAFT_UPDATE_ENTRIES) {
        storage.append(update.entries.index, update.entries.batch,
                       update.entries.n);

        // Persist and notify
        memset(&event, 0, sizeof(event));
        event.type = RAFT_PERSISTED_ENTRIES;
        event.time = now_ms();
        event.persisted_entries.index =
            update.entries.index + update.entries.n - 1;
        memset(&update, 0, sizeof(update));
        rv = raft_step(&r, &event, &update);
        ASSERT_EQ(rv, 0);

        // Single node — persisted entries should immediately be committed
        EXPECT_TRUE(update.flags & RAFT_UPDATE_COMMIT_INDEX);
        if (update.flags & RAFT_UPDATE_COMMIT_INDEX) {
            EXPECT_GE(raft_commit_index(&r), 2u);
        }
    }

    raft_close(&r, NULL);
}

int main(int argc, char** argv) {
    ABT_init(0, NULL);
    ::testing::InitGoogleTest(&argc, argv);
    int result = RUN_ALL_TESTS();
    ABT_finalize();
    return result;
}
