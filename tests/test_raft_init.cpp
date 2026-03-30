#include <gtest/gtest.h>

extern "C" {
#include <raft.h>
}

TEST(RaftInit, InitAndClose) {
    struct raft r;
    memset(&r, 0, sizeof(r));
    int rv = raft_init(&r, NULL, NULL, 1, "test-address");
    ASSERT_EQ(rv, 0);
    raft_close(&r, NULL);
}

TEST(RaftInit, StepAPIAvailable) {
    struct raft r;
    memset(&r, 0, sizeof(r));
    int rv = raft_init(&r, NULL, NULL, 1, "test-address");
    ASSERT_EQ(rv, 0);

    // Verify raft_step exists and can be called (will fail with invalid event,
    // but the point is that it links)
    struct raft_event event;
    struct raft_update update;
    memset(&event, 0, sizeof(event));
    memset(&update, 0, sizeof(update));

    // RAFT_START requires valid data, so just verify it doesn't crash
    // with an invalid event type (returns RAFT_INVALID)
    event.type = (enum raft_event_type)99;
    rv = raft_step(&r, &event, &update);
    EXPECT_EQ(rv, RAFT_INVALID);

    raft_close(&r, NULL);
}
