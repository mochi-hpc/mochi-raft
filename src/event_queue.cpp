#include "event_queue.hpp"
#include <chrono>

namespace mraft {

void EventQueue::push(const struct raft_event& event) {
    auto owned = std::make_unique<OwnedEvent>();
    owned->event = event;
    push(std::move(owned));
}

void EventQueue::push(std::unique_ptr<OwnedEvent> event) {
    std::unique_lock<tl::mutex> lock(mutex_);
    queue_.push_back(std::move(event));
    cond_.notify_one();
}

std::unique_ptr<OwnedEvent> EventQueue::pop(double timeout_ms) {
    std::unique_lock<tl::mutex> lock(mutex_);

    if (queue_.empty() && timeout_ms > 0) {
        // ABT_cond_timedwait uses CLOCK_REALTIME, so compute a realtime
        // deadline to avoid clock mismatch with steady_clock.
        auto deadline = std::chrono::system_clock::now() +
                        std::chrono::microseconds(
                            static_cast<long long>(timeout_ms * 1000.0));
        cond_.wait_until<std::chrono::system_clock>(lock, deadline);
    }

    std::unique_ptr<OwnedEvent> result;
    if (!queue_.empty()) {
        result = std::move(queue_.front());
        queue_.pop_front();
    }

    return result;
}

} // namespace mraft
