#pragma once

extern "C" {
#include <raft.h>
}

#include <thallium.hpp>
#include <deque>
#include <functional>
#include <optional>
#include <cstdint>
#include <memory>
#include <cstring>
#include <string>
#include <vector>

namespace tl = thallium;

namespace mraft {

// Wraps a raft_event together with the heap-allocated data it points into.
//
// A bare raft_event is a plain C struct that holds raw pointers but owns
// nothing — the caller must keep the referenced memory alive. OwnedEvent
// adds unique_ptr members that express ownership of that memory, so the
// data is freed automatically when the OwnedEvent is destroyed.
//
// For RAFT_SUBMIT events, owns the entry and its buffer data.
// For RAFT_RECEIVE events, the message is already owned by Network and
// freed by c-raft after processing — no extra ownership needed here.
struct OwnedEvent {
    struct raft_event event;

    // For RAFT_SUBMIT: heap-allocated entry
    std::unique_ptr<struct raft_entry> submit_entry;

    // For RAFT_RECEIVE: heap-allocated message (already owned by Network)
    // Just passed through — not freed here (freed by raft after processing)

    // Optional completion callback for RAFT_SUBMIT events.
    std::function<void(int)> on_applied;

    // For forward-submit events (is_forward=true): these bypass raft_step()
    // and are dispatched as RPCs to the leader from inside the event loop ULT.
    bool                 is_forward      = false;
    std::string          forward_dest;
    std::vector<uint8_t> forward_data;
    uint64_t             forward_corr_id = 0;

    OwnedEvent() { memset(&event, 0, sizeof(event)); }
};

// Thread-safe event queue for raft events.
// Uses Thallium mutex + condition variable for efficient blocking.
class EventQueue {
public:
    EventQueue() = default;
    ~EventQueue() = default;

    // Push an event to the back of the queue. Wakes up any waiting pop().
    void push(const struct raft_event& event);

    // Push an owned event (takes ownership of the OwnedEvent).
    void push(std::unique_ptr<OwnedEvent> event);

    // Pop the front event. Blocks up to timeout_ms milliseconds.
    // Returns nullptr if the timeout expires before an event is available.
    // timeout_ms == 0 means non-blocking check.
    std::unique_ptr<OwnedEvent> pop(double timeout_ms);

private:
    tl::mutex mutex_;
    tl::condition_variable cond_;
    std::deque<std::unique_ptr<OwnedEvent>> queue_;
};

} // namespace mraft
