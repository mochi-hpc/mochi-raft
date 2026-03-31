#include <gtest/gtest.h>
#include <cstring>
#include <chrono>

extern "C" {
#include <raft.h>
#include <abt.h>
}

#include "../src/event_queue.hpp"

class EventQueueTest : public ::testing::Test {
protected:
    void SetUp() override {}
    void TearDown() override {}
};

static struct raft_event make_timeout_event(raft_time time) {
    struct raft_event e;
    memset(&e, 0, sizeof(e));
    e.type = RAFT_TIMEOUT;
    e.time = time;
    return e;
}

TEST_F(EventQueueTest, PushAndPop) {
    mraft::EventQueue q;

    q.push(make_timeout_event(100));
    q.push(make_timeout_event(200));
    q.push(make_timeout_event(300));

    auto e1 = q.pop(0);
    ASSERT_NE(e1, nullptr);
    EXPECT_EQ(e1->event.type, RAFT_TIMEOUT);
    EXPECT_EQ(e1->event.time, 100u);

    auto e2 = q.pop(0);
    ASSERT_NE(e2, nullptr);
    EXPECT_EQ(e2->event.time, 200u);

    auto e3 = q.pop(0);
    ASSERT_NE(e3, nullptr);
    EXPECT_EQ(e3->event.time, 300u);
}

TEST_F(EventQueueTest, PopEmptyNonBlocking) {
    mraft::EventQueue q;
    auto e = q.pop(0);
    EXPECT_EQ(e, nullptr);
}

TEST_F(EventQueueTest, PopTimesOut) {
    mraft::EventQueue q;

    auto start = std::chrono::steady_clock::now();
    auto e = q.pop(100); // 100ms timeout
    auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_EQ(e, nullptr);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed)
                  .count();
    EXPECT_GE(ms, 80); // Allow some slack
}

TEST_F(EventQueueTest, PushWakesUpPop) {
    mraft::EventQueue q;

    ABT_xstream xstream;
    ABT_xstream_create(ABT_SCHED_NULL, &xstream);
    ABT_pool pool;
    ABT_xstream_get_main_pools(xstream, 1, &pool);

    ABT_thread thread;
    ABT_thread_create(
        pool,
        [](void* arg) {
            auto* queue = static_cast<mraft::EventQueue*>(arg);
            struct timespec ts = {0, 50000000}; // 50ms
            nanosleep(&ts, nullptr);
            queue->push(make_timeout_event(42));
        },
        &q, ABT_THREAD_ATTR_NULL, &thread);

    auto start = std::chrono::steady_clock::now();
    auto e = q.pop(5000); // 5 second timeout
    auto elapsed = std::chrono::steady_clock::now() - start;

    ASSERT_NE(e, nullptr);
    EXPECT_EQ(e->event.time, 42u);

    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(elapsed)
                  .count();
    EXPECT_LT(ms, 2000);

    ABT_thread_free(&thread);
    ABT_xstream_join(xstream);
    ABT_xstream_free(&xstream);
}

int main(int argc, char** argv) {
    ABT_init(0, NULL);
    ::testing::InitGoogleTest(&argc, argv);
    int result = RUN_ALL_TESTS();
    ABT_finalize();
    return result;
}
