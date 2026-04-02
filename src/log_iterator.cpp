#include "log_iterator_impl.hpp"

namespace mraft {

LogIterator::LogIterator(std::shared_ptr<Impl> impl)
    : impl_(std::move(impl)) {}

LogIterator::LogIterator(LogIterator&&) noexcept = default;
LogIterator& LogIterator::operator=(LogIterator&&) noexcept = default;

LogIterator::~LogIterator() {
    if (impl_) stop();
}

bool LogIterator::advance() {
    auto& m = *impl_;
    std::unique_lock<tl::mutex> lk(m.mu_);

    while (true) {
        if (m.stopped_.load(std::memory_order_relaxed))
            return false;

        if (!m.queue_.empty()) {
            auto& front = m.queue_.front();
            m.current_data_          = std::move(front.second);
            m.current_entry_.index   = front.first;
            m.current_entry_.data    = std::string_view(m.current_data_);
            m.queue_.pop_front();
            return true;
        }

        if (m.eof_)
            return false;

        m.cv_.wait(lk);
    }
}

const LogEntry& LogIterator::current() const {
    return impl_->current_entry_;
}

void LogIterator::stop() {
    impl_->stopped_.store(true, std::memory_order_relaxed);
    impl_->cv_.notify_all();
}

} // namespace mraft
