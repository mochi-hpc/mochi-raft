#pragma once

#include <thallium.hpp>

extern "C" {
#include <raft.h>
}

#include <atomic>
#include <deque>
#include <string>
#include <string_view>
#include <utility>

#include "../include/mochi-raft.hpp"

namespace tl = thallium;

namespace mraft {

// Internal implementation of LogIterator.
//
// The event loop ULT is the sole writer: it calls push() for each committed
// RAFT_COMMAND entry and mark_eof() when the bounded range is exhausted or the
// server is shutting down.  The user's ULT calls advance()/stop() which read
// from the same queue via the condition variable.
struct LogIterator::Impl {
    raft_index        from_;
    raft_index        to_;       // 0 = live tail; >0 = inclusive upper bound
    LogIteratorConfig config_;

    std::atomic<bool> stopped_{false};

    // Queue of pending entries: protected by mu_.
    tl::mutex              mu_;
    tl::condition_variable cv_;
    std::deque<std::pair<raft_index, std::string>> queue_;
    bool eof_{false};   // set by event loop; once true, no more entries will arrive

    // Current entry exposed by current().  Set inside advance() before returning.
    std::string current_data_;
    LogEntry    current_entry_{};

    // Called from the event loop ULT only.
    void push(raft_index idx, const char* base, size_t len) {
        {
            std::lock_guard<tl::mutex> lk(mu_);
            queue_.emplace_back(idx, std::string(base, len));
        }
        cv_.notify_one();
    }

    // Called from the event loop ULT only.
    void mark_eof() {
        {
            std::lock_guard<tl::mutex> lk(mu_);
            eof_ = true;
        }
        cv_.notify_all();
    }
};

} // namespace mraft
