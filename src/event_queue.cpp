#include "event_queue.hpp"
#include <ctime>

namespace mraft {

EventQueue::EventQueue() {
    ABT_mutex_create(&mutex_);
    ABT_cond_create(&cond_);
}

EventQueue::~EventQueue() {
    if (cond_ != ABT_COND_NULL) ABT_cond_free(&cond_);
    if (mutex_ != ABT_MUTEX_NULL) ABT_mutex_free(&mutex_);
}

void EventQueue::push(const struct raft_event& event) {
    auto owned = std::make_unique<OwnedEvent>();
    owned->event = event;
    push(std::move(owned));
}

void EventQueue::push(std::unique_ptr<OwnedEvent> event) {
    ABT_mutex_lock(mutex_);
    queue_.push_back(std::move(event));
    ABT_cond_signal(cond_);
    ABT_mutex_unlock(mutex_);
}

std::unique_ptr<OwnedEvent> EventQueue::pop(double timeout_ms) {
    ABT_mutex_lock(mutex_);

    if (queue_.empty() && timeout_ms > 0) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);

        long long ns = static_cast<long long>(timeout_ms * 1000000.0);
        ts.tv_sec += ns / 1000000000LL;
        ts.tv_nsec += ns % 1000000000LL;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec += 1;
            ts.tv_nsec -= 1000000000L;
        }

        ABT_cond_timedwait(cond_, mutex_, &ts);
    }

    std::unique_ptr<OwnedEvent> result;
    if (!queue_.empty()) {
        result = std::move(queue_.front());
        queue_.pop_front();
    }

    ABT_mutex_unlock(mutex_);
    return result;
}

} // namespace mraft
