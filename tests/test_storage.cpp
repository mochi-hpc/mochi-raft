#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <filesystem>

extern "C" {
#include <raft.h>
#include <abt.h>
#include <abt-io.h>
}

#include "../src/storage.hpp"

namespace fs = std::filesystem;

class StorageTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create temporary directory
        char tpl[] = "/tmp/mochi-raft-test-XXXXXX";
        char* dir = mkdtemp(tpl);
        ASSERT_NE(dir, nullptr);
        test_dir_ = dir;

        // Initialize ABT-IO
        abt_io_ = abt_io_init(1);
        ASSERT_NE(abt_io_, ABT_IO_INSTANCE_NULL);
    }

    void TearDown() override {
        if (abt_io_ != ABT_IO_INSTANCE_NULL) {
            abt_io_finalize(abt_io_);
        }
        // Clean up temp directory
        fs::remove_all(test_dir_);
    }

    std::string test_dir_;
    abt_io_instance_id abt_io_ = ABT_IO_INSTANCE_NULL;
};

TEST_F(StorageTest, InitCreatesDirectory) {
    std::string subdir = test_dir_ + "/data";
    mraft::Storage storage(abt_io_, subdir);
    ASSERT_EQ(storage.init(), 0);
    EXPECT_TRUE(fs::is_directory(subdir));
}

TEST_F(StorageTest, InitExistingDirectorySucceeds) {
    mraft::Storage storage(abt_io_, test_dir_);
    ASSERT_EQ(storage.init(), 0);
}

TEST_F(StorageTest, LoadMetadataEmptyDir) {
    mraft::Storage storage(abt_io_, test_dir_);
    ASSERT_EQ(storage.init(), 0);

    raft_term term;
    raft_id voted_for;
    ASSERT_EQ(storage.load_metadata(&term, &voted_for), 0);
    EXPECT_EQ(term, 0u);
    EXPECT_EQ(voted_for, 0u);
}

TEST_F(StorageTest, SetTermAndLoad) {
    mraft::Storage storage(abt_io_, test_dir_);
    ASSERT_EQ(storage.init(), 0);

    ASSERT_EQ(storage.set_term(5), 0);

    // Load in a new Storage instance to verify persistence
    mraft::Storage storage2(abt_io_, test_dir_);
    ASSERT_EQ(storage2.init(), 0);

    raft_term term;
    raft_id voted_for;
    ASSERT_EQ(storage2.load_metadata(&term, &voted_for), 0);
    EXPECT_EQ(term, 5u);
    EXPECT_EQ(voted_for, 0u);
}

TEST_F(StorageTest, SetVoteAndLoad) {
    mraft::Storage storage(abt_io_, test_dir_);
    ASSERT_EQ(storage.init(), 0);

    ASSERT_EQ(storage.set_vote(3), 0);

    mraft::Storage storage2(abt_io_, test_dir_);
    ASSERT_EQ(storage2.init(), 0);

    raft_term term;
    raft_id voted_for;
    ASSERT_EQ(storage2.load_metadata(&term, &voted_for), 0);
    EXPECT_EQ(term, 0u);
    EXPECT_EQ(voted_for, 3u);
}

TEST_F(StorageTest, SetTermThenVoteAndLoad) {
    mraft::Storage storage(abt_io_, test_dir_);
    ASSERT_EQ(storage.init(), 0);

    ASSERT_EQ(storage.set_term(5), 0);
    ASSERT_EQ(storage.set_vote(3), 0);

    mraft::Storage storage2(abt_io_, test_dir_);
    ASSERT_EQ(storage2.init(), 0);

    raft_term term;
    raft_id voted_for;
    ASSERT_EQ(storage2.load_metadata(&term, &voted_for), 0);
    EXPECT_EQ(term, 5u);
    EXPECT_EQ(voted_for, 3u);
}

TEST_F(StorageTest, MultipleTermUpdates) {
    mraft::Storage storage(abt_io_, test_dir_);
    ASSERT_EQ(storage.init(), 0);

    ASSERT_EQ(storage.set_term(1), 0);
    ASSERT_EQ(storage.set_term(2), 0);
    ASSERT_EQ(storage.set_term(3), 0);
    ASSERT_EQ(storage.set_vote(7), 0);

    mraft::Storage storage2(abt_io_, test_dir_);
    ASSERT_EQ(storage2.init(), 0);

    raft_term term;
    raft_id voted_for;
    ASSERT_EQ(storage2.load_metadata(&term, &voted_for), 0);
    EXPECT_EQ(term, 3u);
    EXPECT_EQ(voted_for, 7u);
}

TEST_F(StorageTest, DualFileRotation) {
    mraft::Storage storage(abt_io_, test_dir_);
    ASSERT_EQ(storage.init(), 0);

    // First write goes to metadata1 (odd version)
    ASSERT_EQ(storage.set_term(10), 0);
    EXPECT_TRUE(fs::exists(test_dir_ + "/metadata1"));

    // Second write goes to metadata2 (even version)
    ASSERT_EQ(storage.set_term(20), 0);
    EXPECT_TRUE(fs::exists(test_dir_ + "/metadata2"));

    // Both files should exist now
    EXPECT_TRUE(fs::exists(test_dir_ + "/metadata1"));
    EXPECT_TRUE(fs::exists(test_dir_ + "/metadata2"));

    // Loading should return the latest value
    mraft::Storage storage2(abt_io_, test_dir_);
    ASSERT_EQ(storage2.init(), 0);

    raft_term term;
    raft_id voted_for;
    ASSERT_EQ(storage2.load_metadata(&term, &voted_for), 0);
    EXPECT_EQ(term, 20u);
}

// --- Log entry tests ---

static struct raft_entry make_entry(raft_term term, const std::string& data) {
    struct raft_entry e;
    memset(&e, 0, sizeof(e));
    e.term = term;
    e.type = RAFT_COMMAND;
    e.buf.len = data.size();
    if (!data.empty()) {
        e.buf.base = raft_malloc(data.size());
        memcpy(e.buf.base, data.data(), data.size());
    }
    e.batch = nullptr;
    return e;
}

static void free_entries(struct raft_entry* entries, size_t n) {
    for (size_t i = 0; i < n; i++) {
        if (entries[i].buf.base) raft_free(entries[i].buf.base);
    }
    if (entries) raft_free(entries);
}

TEST_F(StorageTest, AppendAndLoadEntries) {
    mraft::Storage storage(abt_io_, test_dir_);
    ASSERT_EQ(storage.init(), 0);

    // Create and append 10 entries
    std::vector<struct raft_entry> entries;
    for (int i = 0; i < 10; i++) {
        entries.push_back(make_entry(1, "entry-" + std::to_string(i)));
    }

    ASSERT_EQ(storage.append(1, entries.data(), entries.size()), 0);

    // Free our local copies
    for (auto& e : entries) raft_free(e.buf.base);

    // Load back in a new instance
    mraft::Storage storage2(abt_io_, test_dir_);
    ASSERT_EQ(storage2.init(), 0);

    raft_term term;
    raft_id voted_for;
    raft_index start_index;
    struct raft_entry* loaded = nullptr;
    size_t n_loaded = 0;

    ASSERT_EQ(storage2.load(&term, &voted_for, &start_index,
                            &loaded, &n_loaded), 0);
    EXPECT_EQ(start_index, 1u);
    ASSERT_EQ(n_loaded, 10u);

    for (size_t i = 0; i < n_loaded; i++) {
        EXPECT_EQ(loaded[i].term, 1u);
        EXPECT_EQ(loaded[i].type, RAFT_COMMAND);
        std::string expected = "entry-" + std::to_string(i);
        EXPECT_EQ(loaded[i].buf.len, expected.size());
        EXPECT_EQ(memcmp(loaded[i].buf.base, expected.data(), expected.size()), 0);
    }

    free_entries(loaded, n_loaded);
}

TEST_F(StorageTest, LoadFromEmptyDir) {
    mraft::Storage storage(abt_io_, test_dir_);
    ASSERT_EQ(storage.init(), 0);

    raft_term term;
    raft_id voted_for;
    raft_index start_index;
    struct raft_entry* loaded = nullptr;
    size_t n_loaded = 0;

    ASSERT_EQ(storage.load(&term, &voted_for, &start_index,
                           &loaded, &n_loaded), 0);
    EXPECT_EQ(start_index, 1u);
    EXPECT_EQ(n_loaded, 0u);
    EXPECT_EQ(loaded, nullptr);
}

TEST_F(StorageTest, TruncateEntries) {
    mraft::Storage storage(abt_io_, test_dir_);
    ASSERT_EQ(storage.init(), 0);

    std::vector<struct raft_entry> entries;
    for (int i = 0; i < 10; i++) {
        entries.push_back(make_entry(1, "entry-" + std::to_string(i)));
    }
    ASSERT_EQ(storage.append(1, entries.data(), entries.size()), 0);
    for (auto& e : entries) raft_free(e.buf.base);

    // Truncate from index 6 (keep indices 1-5)
    ASSERT_EQ(storage.truncate(6), 0);

    // Append new entries starting at index 6
    std::vector<struct raft_entry> new_entries;
    for (int i = 0; i < 3; i++) {
        new_entries.push_back(make_entry(2, "new-" + std::to_string(i)));
    }
    ASSERT_EQ(storage.append(6, new_entries.data(), new_entries.size()), 0);
    for (auto& e : new_entries) raft_free(e.buf.base);

    // Load and verify
    mraft::Storage storage2(abt_io_, test_dir_);
    ASSERT_EQ(storage2.init(), 0);

    raft_term term;
    raft_id voted_for;
    raft_index start_index;
    struct raft_entry* loaded = nullptr;
    size_t n_loaded = 0;

    ASSERT_EQ(storage2.load(&term, &voted_for, &start_index,
                            &loaded, &n_loaded), 0);
    ASSERT_EQ(n_loaded, 8u); // 5 old + 3 new

    // First 5 entries should be the original ones
    for (size_t i = 0; i < 5; i++) {
        std::string expected = "entry-" + std::to_string(i);
        EXPECT_EQ(loaded[i].buf.len, expected.size());
    }

    // Last 3 entries should be the new ones with term 2
    for (size_t i = 5; i < 8; i++) {
        EXPECT_EQ(loaded[i].term, 2u);
        std::string expected = "new-" + std::to_string(i - 5);
        EXPECT_EQ(loaded[i].buf.len, expected.size());
    }

    free_entries(loaded, n_loaded);
}

TEST_F(StorageTest, Bootstrap) {
    mraft::Storage storage(abt_io_, test_dir_);
    ASSERT_EQ(storage.init(), 0);

    // Create a configuration with 1 server
    struct raft_configuration conf;
    raft_configuration_init(&conf);
    ASSERT_EQ(raft_configuration_add(&conf, 1, "addr1", RAFT_VOTER), 0);

    ASSERT_EQ(storage.bootstrap(&conf), 0);
    raft_configuration_close(&conf);

    // Load and verify
    mraft::Storage storage2(abt_io_, test_dir_);
    ASSERT_EQ(storage2.init(), 0);

    raft_term term;
    raft_id voted_for;
    raft_index start_index;
    struct raft_entry* loaded = nullptr;
    size_t n_loaded = 0;

    ASSERT_EQ(storage2.load(&term, &voted_for, &start_index,
                            &loaded, &n_loaded), 0);
    EXPECT_EQ(term, 1u);
    EXPECT_EQ(start_index, 1u);
    ASSERT_EQ(n_loaded, 1u);
    EXPECT_EQ(loaded[0].type, RAFT_CHANGE);
    EXPECT_EQ(loaded[0].term, 1u);

    // Decode the configuration
    struct raft_configuration loaded_conf;
    raft_configuration_init(&loaded_conf);
    ASSERT_EQ(raft_configuration_decode(&loaded[0].buf, &loaded_conf), 0);
    EXPECT_EQ(loaded_conf.n, 1u);
    EXPECT_EQ(loaded_conf.servers[0].id, 1u);
    EXPECT_STREQ(loaded_conf.servers[0].address, "addr1");

    raft_configuration_close(&loaded_conf);
    free_entries(loaded, n_loaded);
}

// Main needs ABT_init before any ABT-IO calls
int main(int argc, char** argv) {
    ABT_init(0, NULL);
    ::testing::InitGoogleTest(&argc, argv);
    int result = RUN_ALL_TESTS();
    ABT_finalize();
    return result;
}
