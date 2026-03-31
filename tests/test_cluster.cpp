#include <gtest/gtest.h>
#include <cstring>
#include <chrono>
#include <filesystem>
#include <atomic>
#include <array>

extern "C" {
#include <raft.h>
}

#include <abt.h>
#include <abt-io.h>
#include <thallium.hpp>

#include "../include/mochi-raft.hpp"

namespace tl = thallium;
namespace fs = std::filesystem;

static void yield_ms(int ms) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(ms);
    while (std::chrono::steady_clock::now() < deadline) {
        ABT_thread_yield();
    }
}

class NoOpFsm : public mraft::Fsm {
public:
    int apply(const struct raft_buffer& buf) override {
        applied_count_.fetch_add(1);
        return 0;
    }
    int applied_count() const { return applied_count_.load(); }

private:
    std::atomic<int> applied_count_{0};
};

static constexpr int N = 3;

class ClusterTest : public ::testing::Test {
protected:
    void SetUp() override {
        for (int i = 0; i < N; i++) {
            char tpl[] = "/tmp/mochi-raft-cluster-XXXXXX";
            char* dir = mkdtemp(tpl);
            ASSERT_NE(dir, nullptr);
            dirs_[i] = dir;
        }

        abt_io_ = abt_io_init(1);
        ASSERT_NE(abt_io_, ABT_IO_INSTANCE_NULL);

        for (int i = 0; i < N; i++) {
            engines_[i] = std::make_unique<tl::engine>(
                "na+sm", THALLIUM_SERVER_MODE, true, 1);
            addrs_[i] = static_cast<std::string>(engines_[i]->self());
        }
    }

    void TearDown() override {
        for (int i = 0; i < N; i++) {
            servers_[i].reset();
        }
        for (int i = 0; i < N; i++) {
            engines_[i]->finalize();
            engines_[i].reset();
        }
        if (abt_io_ != ABT_IO_INSTANCE_NULL) {
            abt_io_finalize(abt_io_);
        }
        for (int i = 0; i < N; i++) {
            fs::remove_all(dirs_[i]);
        }
    }

    void create_and_bootstrap() {
        std::vector<std::pair<raft_id, std::string>> cluster;
        for (int i = 0; i < N; i++) {
            cluster.push_back({static_cast<raft_id>(i + 1), addrs_[i]});
        }

        for (int i = 0; i < N; i++) {
            servers_[i] = std::make_unique<mraft::MochiRaftServer>(
                *engines_[i], abt_io_, static_cast<raft_id>(i + 1),
                addrs_[i], dirs_[i], fsms_[i]);
        }

        // Bootstrap all nodes with the same cluster configuration
        for (int i = 0; i < N; i++) {
            ASSERT_EQ(servers_[i]->bootstrap(cluster), 0);
        }
    }

    int find_leader() {
        for (int i = 0; i < N; i++) {
            if (servers_[i] && servers_[i]->state() == RAFT_LEADER) {
                return i;
            }
        }
        return -1;
    }

    bool wait_for_leader(int timeout_ms = 10000) {
        auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(timeout_ms);
        while (std::chrono::steady_clock::now() < deadline) {
            if (find_leader() >= 0) return true;
            yield_ms(10);
        }
        return false;
    }

    std::string dirs_[N];
    abt_io_instance_id abt_io_ = ABT_IO_INSTANCE_NULL;
    std::unique_ptr<tl::engine> engines_[N];
    std::string addrs_[N];
    std::unique_ptr<mraft::MochiRaftServer> servers_[N];
    NoOpFsm fsms_[N];
};

TEST_F(ClusterTest, ThreeNodeElection) {
    create_and_bootstrap();

    // Start all servers
    for (int i = 0; i < N; i++) {
        ASSERT_EQ(servers_[i]->start(), 0);
    }

    // Wait for a leader to emerge
    ASSERT_TRUE(wait_for_leader())
        << "No leader elected within timeout";

    int leader = find_leader();
    EXPECT_GE(leader, 0);
    EXPECT_LE(leader, N - 1);

    // Verify followers
    int follower_count = 0;
    for (int i = 0; i < N; i++) {
        if (i != leader) {
            EXPECT_EQ(servers_[i]->state(), RAFT_FOLLOWER)
                << "Server " << i << " is not a follower";
            follower_count++;
        }
    }
    EXPECT_EQ(follower_count, N - 1);

    for (int i = 0; i < N; i++) {
        servers_[i]->shutdown();
    }
}

TEST_F(ClusterTest, SubmitAndReplicate) {
    create_and_bootstrap();

    for (int i = 0; i < N; i++) {
        ASSERT_EQ(servers_[i]->start(), 0);
    }

    ASSERT_TRUE(wait_for_leader());
    int leader = find_leader();
    ASSERT_GE(leader, 0);

    // Submit an entry on the leader
    const char* data = "replicated-data";
    ASSERT_EQ(servers_[leader]->submit(data, strlen(data)), 0);

    // Wait for commit index to advance on the leader
    raft_index initial = servers_[leader]->commit_index();
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::seconds(10);
    while (servers_[leader]->commit_index() <= initial &&
           std::chrono::steady_clock::now() < deadline) {
        yield_ms(10);
    }

    EXPECT_GT(servers_[leader]->commit_index(), initial)
        << "Leader's commit index did not advance";

    // Wait a bit for FSM application to propagate
    yield_ms(500);

    // Verify FSM::apply() was called on the leader
    EXPECT_GE(fsms_[leader].applied_count(), 1)
        << "Leader FSM did not apply any entries";

    for (int i = 0; i < N; i++) {
        servers_[i]->shutdown();
    }
}

TEST_F(ClusterTest, IsolateLeaderNewElection) {
    create_and_bootstrap();

    for (int i = 0; i < N; i++) {
        ASSERT_EQ(servers_[i]->start(), 0);
    }

    ASSERT_TRUE(wait_for_leader());
    int old_leader = find_leader();
    ASSERT_GE(old_leader, 0);

    // Isolate the leader (both directions)
    servers_[old_leader]->set_isolation(mraft::IsolationMode::BOTH);

    // Wait for a new leader to emerge among the remaining nodes
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::seconds(15);
    int new_leader = -1;
    while (std::chrono::steady_clock::now() < deadline) {
        for (int i = 0; i < N; i++) {
            if (i != old_leader && servers_[i]->state() == RAFT_LEADER) {
                new_leader = i;
                break;
            }
        }
        if (new_leader >= 0) break;
        yield_ms(10);
    }

    ASSERT_GE(new_leader, 0)
        << "No new leader elected after isolating old leader";
    EXPECT_NE(new_leader, old_leader);

    // Deisolate the old leader — it should rejoin as follower
    servers_[old_leader]->set_isolation(mraft::IsolationMode::NONE);

    deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
        if (servers_[old_leader]->state() == RAFT_FOLLOWER) break;
        yield_ms(10);
    }

    EXPECT_EQ(servers_[old_leader]->state(), RAFT_FOLLOWER)
        << "Old leader did not rejoin as follower after deisolation";

    for (int i = 0; i < N; i++) {
        servers_[i]->shutdown();
    }
}

TEST_F(ClusterTest, LeadershipTransfer) {
    create_and_bootstrap();

    for (int i = 0; i < N; i++) {
        ASSERT_EQ(servers_[i]->start(), 0);
    }

    ASSERT_TRUE(wait_for_leader());
    int leader = find_leader();
    ASSERT_GE(leader, 0);

    // Pick a follower to transfer to
    int target = -1;
    for (int i = 0; i < N; i++) {
        if (i != leader) { target = i; break; }
    }
    ASSERT_GE(target, 0);

    raft_id target_id = static_cast<raft_id>(target + 1);
    ASSERT_EQ(servers_[leader]->transfer(target_id), 0);

    // Wait for the target to become leader
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
        if (servers_[target]->state() == RAFT_LEADER) break;
        yield_ms(10);
    }

    EXPECT_EQ(servers_[target]->state(), RAFT_LEADER)
        << "Target node did not become leader after transfer";

    for (int i = 0; i < N; i++) {
        servers_[i]->shutdown();
    }
}

int main(int argc, char** argv) {
    ABT_init(0, NULL);
    ::testing::InitGoogleTest(&argc, argv);
    int result = RUN_ALL_TESTS();
    ABT_finalize();
    return result;
}
