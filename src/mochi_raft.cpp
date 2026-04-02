#include "storage.hpp"
#include "network.hpp"
#include "event_queue.hpp"
#include "log_iterator_impl.hpp"

#include <chrono>
#include <cstring>

namespace mraft {

static raft_time now_ms() {
    auto tp = std::chrono::steady_clock::now().time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(tp).count();
}

MochiRaftServer::MochiRaftServer(tl::engine& engine,
                                 abt_io_instance_id abt_io,
                                 raft_id id,
                                 tl::pool loop_pool,
                                 tl::pool rpc_pool,
                                 const std::string& data_dir,
                                 Fsm& fsm,
                                 uint16_t provider_id)
    : engine_(engine)
    , fsm_(fsm)
    , id_(id)
    , address_(static_cast<std::string>(engine.self()))
    , loop_pool_(std::move(loop_pool))
{
    memset(&raft_, 0, sizeof(raft_));

    storage_ = std::make_unique<Storage>(abt_io, data_dir);
    queue_ = std::make_unique<EventQueue>();
    network_ = std::make_unique<Network>(
        engine, provider_id, id, address_,
        [this](struct raft_message* msg) { on_receive(msg); },
        std::move(rpc_pool));

    network_->set_forward_submit_cb(
        [this](std::vector<uint8_t> data, uint64_t corr_id, std::string caller) {
            on_forward_submit_received(std::move(data), corr_id,
                                       std::move(caller));
        });
    network_->set_forward_result_cb([this](uint64_t corr_id, int rv) {
        on_forward_result_received(corr_id, rv);
    });
}

MochiRaftServer::~MochiRaftServer() {
    shutdown();
}

int MochiRaftServer::bootstrap(
    const std::vector<std::pair<raft_id, std::string>>& servers)
{
    int rv = storage_->init();
    if (rv != 0) return rv;

    struct raft_configuration conf;
    raft_configuration_init(&conf);

    for (auto& [sid, saddr] : servers) {
        rv = raft_configuration_add(&conf, sid, saddr.c_str(), RAFT_VOTER);
        if (rv != 0) {
            raft_configuration_close(&conf);
            return rv;
        }
    }

    rv = storage_->bootstrap(&conf);
    raft_configuration_close(&conf);
    return rv;
}

int MochiRaftServer::start() {
    int rv = storage_->init();
    if (rv != 0) return rv;

    // Load persistent state
    raft_term term;
    raft_id voted_for;
    raft_index start_index;
    struct raft_entry* entries = nullptr;
    size_t n_entries = 0;

    rv = storage_->load(&term, &voted_for, &start_index,
                        &entries, &n_entries);
    if (rv != 0) return rv;

    // Load snapshot if one exists on disk.
    struct raft_snapshot_metadata snap_meta{};
    std::string snap_data;
    bool has_snap = (storage_->snapshot_get(snap_meta, snap_data) == 0);
    if (has_snap) {
        fsm_.restore(std::string_view(snap_data));
        last_applied_        = snap_meta.index;
        last_snapshot_index_ = snap_meta.index;
        snap_data.clear();
    }

    // Initialize raft
    rv = raft_init(&raft_, NULL, NULL, id_, address_.c_str());
    if (rv != 0) {
        if (entries) {
            for (size_t i = 0; i < n_entries; i++)
                if (entries[i].buf.base) raft_free(entries[i].buf.base);
            raft_free(entries);
        }
        return rv;
    }

    // Randomize election timer
    unsigned seed = static_cast<unsigned>(now_ms() ^ (id_ * 7919));
    raft_seed(&raft_, seed);

    // Create RAFT_START event
    struct raft_event event;
    struct raft_update update;
    memset(&event, 0, sizeof(event));
    memset(&update, 0, sizeof(update));

    event.type = RAFT_START;
    event.time = now_ms();
    event.start.term        = term;
    event.start.voted_for   = voted_for;
    event.start.metadata    = has_snap ? &snap_meta : nullptr;
    event.start.start_index = start_index;
    event.start.entries     = entries;
    event.start.n_entries   = static_cast<unsigned>(n_entries);

    rv = raft_step(&raft_, &event, &update);
    // c-raft deep-copies snap_meta.configuration internally; release our copy.
    if (has_snap) raft_configuration_close(&snap_meta.configuration);
    if (rv != 0) {
        raft_close(&raft_, NULL);
        return rv;
    }

    handle_update(update);

    // Start the event loop on the engine's handler pool
    running_.store(true);

    loop_thread_ = loop_pool_.make_thread([this]() { event_loop(); });

    return 0;
}

void MochiRaftServer::shutdown() {
    if (!running_.load()) return;
    running_.store(false);

    // Push a dummy event to wake the event loop
    struct raft_event wake;
    memset(&wake, 0, sizeof(wake));
    wake.type = RAFT_TIMEOUT;
    wake.time = now_ms();
    queue_->push(wake);

    loop_thread_->join();
    loop_thread_ = tl::managed<tl::thread>();

    // Wake up any iterators that are blocked waiting for new commits.
    for (auto& impl : active_iterators_)
        impl->mark_eof();
    active_iterators_.clear();

    raft_close(&raft_, NULL);
}

void MochiRaftServer::event_loop() {
    while (running_.load()) {
        // Calculate how long to wait
        raft_time timeout_at = raft_timeout(&raft_);
        raft_time current = now_ms();
        double wait_ms = 10; // Default small wait
        if (timeout_at > current) {
            wait_ms = static_cast<double>(timeout_at - current);
            if (wait_ms > 100) wait_ms = 100; // Cap to check running_ periodically
        }

        // Wait for an event or timeout
        auto owned = queue_->pop(wait_ms);

        if (!running_.load()) break;

        // Forward-submit events bypass raft_step() entirely.
        if (owned && owned->is_forward) {
            handle_forward(*owned);
            continue;
        }

        if (owned && owned->is_iter_register) {
            if (owned->iter_register_fn) owned->iter_register_fn();
            continue;
        }

        struct raft_event event;
        if (owned) {
            event = owned->event;
            if (event.time == 0) event.time = now_ms();
        } else {
            // Pop timed out — check if raft timeout has actually passed
            current = now_ms();
            if (current < raft_timeout(&raft_)) {
                continue;
            }
            memset(&event, 0, sizeof(event));
            event.type = RAFT_TIMEOUT;
            event.time = current;
        }

        struct raft_update update;
        memset(&update, 0, sizeof(update));

        int rv = raft_step(&raft_, &event, &update);
        if (rv != 0) {
            continue;
        }

        if (update.flags & RAFT_UPDATE_ENTRIES) {
            // Cancel any pending callbacks whose index is being overwritten.
            auto it = pending_callbacks_.lower_bound(update.entries.index);
            while (it != pending_callbacks_.end()) {
                it->second.fn(RAFT_NOTLEADER);
                it = pending_callbacks_.erase(it);
            }
            // Register a new callback if this was a submit with one.
            if (owned && owned->event.type == RAFT_SUBMIT && owned->on_applied) {
                raft_term term = update.entries.batch[0].term;
                pending_callbacks_[update.entries.index] =
                    {term, std::move(owned->on_applied)};
            }
        }

        handle_update(update);
    }
}

void MochiRaftServer::handle_update(struct raft_update& update) {
    // 1. Persist term and vote BEFORE sending any messages (Raft safety)
    if (update.flags & RAFT_UPDATE_CURRENT_TERM) {
        storage_->set_term(raft_current_term(&raft_));
    }
    if (update.flags & RAFT_UPDATE_VOTED_FOR) {
        storage_->set_vote(raft_voted_for(&raft_));
    }

    // 2. Persist entries
    if (update.flags & RAFT_UPDATE_ENTRIES) {
        storage_->append(update.entries.index, update.entries.batch,
                         update.entries.n);

        // Notify raft that entries are persisted
        struct raft_event persisted;
        memset(&persisted, 0, sizeof(persisted));
        persisted.type = RAFT_PERSISTED_ENTRIES;
        persisted.time = now_ms();
        persisted.persisted_entries.index =
            update.entries.index + update.entries.n - 1;

        struct raft_update update2;
        memset(&update2, 0, sizeof(update2));
        int rv = raft_step(&raft_, &persisted, &update2);
        if (rv == 0) {
            handle_update(update2);
        }
    }

    // 3. Send messages (after term/vote are persisted)
    if (update.flags & RAFT_UPDATE_MESSAGES) {
        // Fill in AppendEntries entries from storage cache, then dispatch
        // each message via the appropriate transport (RDMA or inline RPC).
        for (unsigned i = 0; i < update.messages.n; i++) {
            struct raft_message& msg = update.messages.batch[i];
            if (msg.type == RAFT_APPEND_ENTRIES &&
                msg.append_entries.n_entries > 0) {
                const struct raft_entry* cached = nullptr;
                raft_index first = msg.append_entries.prev_log_index + 1;
                int rv = storage_->get_entries(
                    first, msg.append_entries.n_entries, &cached);
                if (rv == 0 && cached) {
                    msg.append_entries.entries =
                        const_cast<struct raft_entry*>(cached);
                } else {
                    // Can't find entries — send as heartbeat instead
                    msg.append_entries.n_entries = 0;
                    msg.append_entries.entries = nullptr;
                }
            }

            // Decide transport: RDMA if total entry payload exceeds threshold.
            size_t data_size = 0;
            if (msg.type == RAFT_APPEND_ENTRIES) {
                for (unsigned j = 0; j < msg.append_entries.n_entries; j++)
                    data_size += msg.append_entries.entries[j].buf.len;
            }
            bool use_rdma = data_size > rdma_threshold_;
            double timeout_s = std::max(min_timeout_ms_,
                                        timeout_factor_ * static_cast<double>(data_size))
                               / 1000.0;

            if (use_rdma)
                network_->send_rdma(&msg, timeout_s);
            else
                network_->send(&msg, 1);
        }
    }

    // 4. Handle incoming snapshot chunk (InstallSnapshot from leader)
    if (update.flags & RAFT_UPDATE_SNAPSHOT) {
        auto& snap = update.snapshot;

        // Accumulate chunk; snap.chunk.base is owned by c-raft, just copy bytes.
        const char* base = static_cast<const char*>(snap.chunk.base);
        incoming_snapshot_buf_.append(base, snap.chunk.len);

        if (snap.last) {
            // All chunks received — persist to disk and restore the FSM.
            storage_->snapshot_put(
                snap.metadata.index, snap.metadata.term,
                snap.metadata.configuration_index, snap.metadata.configuration,
                incoming_snapshot_buf_.data(), incoming_snapshot_buf_.size());

            fsm_.restore(std::string_view(incoming_snapshot_buf_));

            last_applied_        = snap.metadata.index;
            last_snapshot_index_ = snap.metadata.index;

            raft_index keep_from = (snap.metadata.index > snapshot_trailing_)
                ? (snap.metadata.index - snapshot_trailing_ + 1) : 1;
            storage_->discard_before(keep_from);

            incoming_snapshot_buf_.clear();
            incoming_snapshot_buf_.shrink_to_fit();
        }

        // Acknowledge the chunk to c-raft.
        struct raft_event persisted{};
        persisted.type                        = RAFT_PERSISTED_SNAPSHOT;
        persisted.time                        = now_ms();
        persisted.persisted_snapshot.metadata = snap.metadata;
        persisted.persisted_snapshot.offset   = snap.offset;
        persisted.persisted_snapshot.last     = snap.last;

        struct raft_update update2{};
        memset(&update2, 0, sizeof(update2));
        int rv2 = raft_step(&raft_, &persisted, &update2);
        if (rv2 == 0) handle_update(update2);
    }

    // 5. Apply committed entries
    if (update.flags & RAFT_UPDATE_COMMIT_INDEX) {
        apply_committed_entries();
    }

}

void MochiRaftServer::on_receive(struct raft_message* msg) {
    struct raft_event event;
    memset(&event, 0, sizeof(event));
    event.type = RAFT_RECEIVE;
    event.time = now_ms();
    event.receive.message = msg;
    queue_->push(event);
}

void MochiRaftServer::apply_committed_entries() {
    raft_index commit = raft_commit_index(const_cast<struct raft*>(&raft_));

    while (last_applied_ < commit) {
        raft_index idx = last_applied_ + 1;

        const struct raft_entry* entry = nullptr;
        int rv = storage_->get_entries(idx, 1, &entry);
        if (rv != 0 || !entry) break;

        // Only apply RAFT_COMMAND entries to the FSM;
        // skip RAFT_CHANGE (configuration) and RAFT_BARRIER entries.
        if (entry->type == RAFT_COMMAND && entry->buf.base && entry->buf.len > 0) {
            const char* base = static_cast<const char*>(entry->buf.base);
            size_t      len  = entry->buf.len;
            fsm_.apply({base, len});
            push_to_iterators(idx, base, len);
        }

        auto it = pending_callbacks_.find(idx);
        if (it != pending_callbacks_.end()) {
            if (it->second.term == entry->term)
                it->second.fn(0);
            pending_callbacks_.erase(it);
        }

        last_applied_ = idx;
    }

    maybe_take_snapshot();
}

void MochiRaftServer::maybe_take_snapshot() {
    if (snapshot_threshold_ == 0) return;
    if (last_applied_ == 0) return;
    if (last_applied_ - last_snapshot_index_ < snapshot_threshold_) return;
    do_take_snapshot();
}

void MochiRaftServer::do_take_snapshot() {
    // Ask the FSM to serialise its state.
    std::string snap_data;
    if (fsm_.snapshot(snap_data) != 0) return;  // FSM opted out or failed

    raft_index snap_index = last_applied_;

    // Get the term for snap_index from the in-memory entry cache.
    const struct raft_entry* entry = nullptr;
    storage_->get_entries(snap_index, 1, &entry);
    raft_term snap_term = entry ? entry->term : raft_current_term(&raft_);

    // Use the committed configuration (configuration_committed /
    // configuration_committed_index are public fields of struct raft).
    const struct raft_configuration& conf = raft_.configuration_committed;
    raft_index conf_index = raft_.configuration_committed_index;

    int rv = storage_->snapshot_put(snap_index, snap_term, conf_index, conf,
                                    snap_data.data(), snap_data.size());
    if (rv != 0) return;

    // Discard log entries before the trailing window.
    raft_index keep_from = (snap_index > snapshot_trailing_)
        ? (snap_index - snapshot_trailing_ + 1) : 1;
    storage_->discard_before(keep_from);

    // Notify c-raft about the snapshot so it can advance its log trail.
    // metadata.configuration is a shallow copy into raft_.configuration_committed;
    // c-raft deep-copies it internally, so no allocation or free is needed here.
    struct raft_snapshot_metadata metadata{};
    metadata.index               = snap_index;
    metadata.term                = snap_term;
    metadata.configuration       = conf;
    metadata.configuration_index = conf_index;

    struct raft_event event{};
    event.type              = RAFT_SNAPSHOT;
    event.time              = now_ms();
    event.snapshot.metadata = metadata;
    event.snapshot.trailing = snapshot_trailing_;

    struct raft_update update{};
    memset(&update, 0, sizeof(update));
    rv = raft_step(&raft_, &event, &update);
    if (rv == 0) handle_update(update);

    last_snapshot_index_ = snap_index;
}

void MochiRaftServer::set_snapshot_threshold(unsigned n) {
    snapshot_threshold_ = n;
}

void MochiRaftServer::set_snapshot_trailing(unsigned n) {
    snapshot_trailing_ = n;
}

int MochiRaftServer::submit(MochiRaftBuffer&& buf,
                             bool forward,
                             std::function<void(int)> on_applied) {
    bool is_leader =
        (raft_state(const_cast<struct raft*>(&raft_)) == RAFT_LEADER);

    if (!is_leader) {
        if (!forward) return RAFT_NOTLEADER;

        auto [leader_id, leader_addr] = leader();
        if (leader_addr.empty()) return RAFT_NOTLEADER;

        uint64_t corr_id = next_corr_id_.fetch_add(1);
        if (on_applied) {
            std::lock_guard<std::mutex> lock(forward_mutex_);
            forward_pending_[corr_id] = std::move(on_applied);
        }

        auto owned           = std::make_unique<OwnedEvent>();
        owned->is_forward    = true;
        owned->forward_dest  = leader_addr;
        owned->forward_data.assign(
            static_cast<const uint8_t*>(buf.data()),
            static_cast<const uint8_t*>(buf.data()) + buf.size());
        owned->forward_corr_id = corr_id;
        queue_->push(std::move(owned));
        return 0;
    }

    size_t len  = buf.size();
    void*  base = buf.release();   // take ownership; buf will no longer free it
    if (!base) return RAFT_NOMEM;

    auto owned = std::make_unique<OwnedEvent>();
    owned->submit_entry = std::make_unique<struct raft_entry>();
    memset(owned->submit_entry.get(), 0, sizeof(struct raft_entry));

    owned->submit_entry->term      = raft_current_term(&raft_);
    owned->submit_entry->type      = RAFT_COMMAND;
    owned->submit_entry->buf.base  = base;
    owned->submit_entry->buf.len   = len;
    owned->submit_entry->batch     = base;  // c-raft frees via batch pointer

    owned->event.type = RAFT_SUBMIT;
    owned->event.time = now_ms();
    owned->event.submit.entries = owned->submit_entry.get();
    owned->event.submit.n = 1;
    owned->on_applied = std::move(on_applied);

    queue_->push(std::move(owned));
    return 0;
}

void MochiRaftServer::set_rdma_config(size_t rdma_threshold,
                                       double min_timeout_ms,
                                       double timeout_factor) {
    rdma_threshold_ = rdma_threshold;
    min_timeout_ms_ = min_timeout_ms;
    timeout_factor_ = timeout_factor;
}

raft_id MochiRaftServer::id() const {
    return id_;
}

unsigned short MochiRaftServer::state() const {
    return static_cast<unsigned short>(
        raft_state(const_cast<struct raft*>(&raft_)));
}

raft_term MochiRaftServer::current_term() const {
    return raft_current_term(const_cast<struct raft*>(&raft_));
}

raft_index MochiRaftServer::log_start_index() const {
    return storage_->first_cached_index();
}

raft_index MochiRaftServer::commit_index() const {
    return raft_commit_index(const_cast<struct raft*>(&raft_));
}

std::vector<std::pair<raft_id, std::string>> MochiRaftServer::members() const {
    std::vector<std::pair<raft_id, std::string>> result;
    const auto& conf = raft_.configuration;
    for (unsigned i = 0; i < conf.n; i++) {
        result.push_back({conf.servers[i].id,
                          conf.servers[i].address ? conf.servers[i].address : ""});
    }
    return result;
}

std::pair<raft_id, std::string> MochiRaftServer::leader() const {
    raft_id id = 0;
    const char* address = nullptr;
    raft_leader(const_cast<struct raft*>(&raft_), &id, &address);
    return {id, address ? address : ""};
}

void MochiRaftServer::set_isolation(IsolationMode mode) {
    network_->set_isolation(mode);
}

int MochiRaftServer::transfer(raft_id target_id) {
    struct raft_event event;
    memset(&event, 0, sizeof(event));
    event.type = RAFT_TRANSFER;
    event.time = now_ms();
    event.transfer.server_id = target_id;
    queue_->push(event);
    return 0;
}

void MochiRaftServer::handle_forward(OwnedEvent& owned) {
    try {
        network_->send_forward_submit(owned.forward_dest, owned.forward_data,
                                      owned.forward_corr_id, address_);
    } catch (...) {
        // Send failed — fire the pending callback with an error.
        std::function<void(int)> cb;
        {
            std::lock_guard<std::mutex> lock(forward_mutex_);
            auto it = forward_pending_.find(owned.forward_corr_id);
            if (it != forward_pending_.end()) {
                cb = std::move(it->second);
                forward_pending_.erase(it);
            }
        }
        if (cb) cb(RAFT_NOTLEADER);
    }
}

void MochiRaftServer::on_forward_submit_received(
        std::vector<uint8_t> data, uint64_t corr_id, std::string caller_addr) {
    MochiRaftBuffer buf(reinterpret_cast<const char*>(data.data()), data.size());
    int rv = submit(std::move(buf), false,
        [this, corr_id, caller_addr](int result) {
            network_->send_forward_result(caller_addr, corr_id, result);
        });
    if (rv != 0)
        network_->send_forward_result(caller_addr, corr_id, rv);
}

void MochiRaftServer::on_forward_result_received(uint64_t corr_id, int rv) {
    std::function<void(int)> cb;
    {
        std::lock_guard<std::mutex> lock(forward_mutex_);
        auto it = forward_pending_.find(corr_id);
        if (it != forward_pending_.end()) {
            cb = std::move(it->second);
            forward_pending_.erase(it);
        }
    }
    if (cb) cb(rv);
}

LogIterator MochiRaftServer::entries(raft_index from, raft_index to,
                                      LogIteratorConfig config) {
    auto impl      = std::make_shared<LogIterator::Impl>();
    impl->from_    = (from == 0) ? 1 : from;
    impl->to_      = to;
    impl->config_  = config;

    // Post a registration event so the event loop replays history and
    // enrolls the iterator for future pushes — all from a single ULT.
    std::weak_ptr<LogIterator::Impl> weak = impl;
    auto owned                = std::make_unique<OwnedEvent>();
    owned->is_iter_register   = true;
    owned->iter_register_fn   = [this, weak]() {
        auto p = weak.lock();
        if (p) handle_iter_register(std::move(p));
    };
    queue_->push(std::move(owned));

    return LogIterator(std::move(impl));
}

void MochiRaftServer::handle_iter_register(
        std::shared_ptr<LogIterator::Impl> impl) {
    // Called from the event loop ULT — safe to read raft_ and storage_.
    raft_index ci   = raft_commit_index(const_cast<struct raft*>(&raft_));
    raft_index from = impl->from_;

    // Replay historical entries in batches.
    for (raft_index idx = from; idx <= ci && !impl->stopped_.load(); ) {
        unsigned n = static_cast<unsigned>(
            std::min<raft_index>(impl->config_.batch_size, ci - idx + 1));

        const struct raft_entry* batch = nullptr;
        int rv = storage_->get_entries(idx, n, &batch);
        if (rv != 0 || !batch) break;

        for (unsigned i = 0; i < n; i++) {
            raft_index eidx = idx + i;
            const auto& e   = batch[i];
            if (e.type == RAFT_COMMAND && e.buf.base && e.buf.len > 0) {
                impl->push(eidx,
                           static_cast<const char*>(e.buf.base), e.buf.len);
            }
            if (impl->to_ > 0 && eidx >= impl->to_) {
                impl->mark_eof();
                return;   // do not enroll; bounded range is already done
            }
        }
        idx += n;
    }

    if (impl->stopped_.load()) return;

    // Bounded range already satisfied by history.
    if (impl->to_ > 0 && ci >= impl->to_) {
        impl->mark_eof();
        return;
    }

    // Enroll for future pushes from apply_committed_entries().
    active_iterators_.push_back(std::move(impl));
}

void MochiRaftServer::push_to_iterators(raft_index idx,
                                         const char* base, size_t len) {
    // Called from apply_committed_entries(), which runs in the event loop ULT.
    auto it = active_iterators_.begin();
    while (it != active_iterators_.end()) {
        auto& impl = *it;
        if (impl->stopped_.load()) {
            it = active_iterators_.erase(it);
            continue;
        }
        impl->push(idx, base, len);
        if (impl->to_ > 0 && idx >= impl->to_) {
            impl->mark_eof();
            it = active_iterators_.erase(it);
        } else {
            ++it;
        }
    }
}

} // namespace mraft
