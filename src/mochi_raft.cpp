#include "storage.hpp"
#include "network.hpp"
#include "event_queue.hpp"

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
                                 const std::string& address,
                                 const std::string& data_dir,
                                 Fsm& fsm,
                                 uint16_t provider_id)
    : engine_(engine)
    , fsm_(fsm)
    , id_(id)
    , address_(address)
{
    memset(&raft_, 0, sizeof(raft_));

    storage_ = std::make_unique<Storage>(abt_io, data_dir);
    queue_ = std::make_unique<EventQueue>();
    network_ = std::make_unique<Network>(
        engine, provider_id, id, address,
        [this](struct raft_message* msg) { on_receive(msg); });
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
    event.start.term = term;
    event.start.voted_for = voted_for;
    event.start.metadata = nullptr;
    event.start.start_index = start_index;
    event.start.entries = entries;
    event.start.n_entries = static_cast<unsigned>(n_entries);

    rv = raft_step(&raft_, &event, &update);
    if (rv != 0) {
        raft_close(&raft_, NULL);
        return rv;
    }

    handle_update(update);

    // Start the event loop on the engine's handler pool
    running_.store(true);

    loop_thread_ = engine_.get_handler_pool().make_thread(
        [this]() { event_loop(); });

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

        // If this was an RDMA submit that produced new log entries, record
        // the per-entry metadata so handle_update() can pick the right transport.
        if (owned && owned->event.type == RAFT_SUBMIT && owned->use_rdma
                && (update.flags & RAFT_UPDATE_ENTRIES)) {
            for (unsigned j = 0; j < update.entries.n; j++)
                entry_meta_[update.entries.index + j] =
                    {owned->use_rdma, owned->rdma_timeout_s};
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

            // Choose RDMA path if any entry in this message was submitted
            // with use_rdma=true.
            bool   use_rdma  = false;
            double timeout_s = 5.0;
            if (msg.type == RAFT_APPEND_ENTRIES &&
                    msg.append_entries.n_entries > 0) {
                raft_index first = msg.append_entries.prev_log_index + 1;
                for (unsigned j = 0; j < msg.append_entries.n_entries; j++) {
                    auto it = entry_meta_.find(first + j);
                    if (it != entry_meta_.end() && it->second.use_rdma) {
                        use_rdma  = true;
                        timeout_s = std::min(timeout_s, it->second.timeout_s);
                    }
                }
            }

            if (use_rdma)
                network_->send_rdma(&msg, timeout_s);
            else
                network_->send(&msg, 1);
        }
    }

    // 4. Apply committed entries
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
            fsm_.apply({static_cast<const char*>(entry->buf.base), entry->buf.len});
        }

        entry_meta_.erase(idx);
        last_applied_ = idx;
    }
}

int MochiRaftServer::submit(const void* data, size_t len,
                             const SubmitOptions& opts) {
    // Create an owned event with heap-allocated entry data
    auto owned = std::make_unique<OwnedEvent>();
    owned->submit_entry = std::make_unique<struct raft_entry>();
    memset(owned->submit_entry.get(), 0, sizeof(struct raft_entry));

    owned->submit_entry->term = raft_current_term(&raft_);
    owned->submit_entry->type = RAFT_COMMAND;
    owned->submit_entry->buf.base = raft_malloc(len);
    if (!owned->submit_entry->buf.base) return RAFT_NOMEM;
    memcpy(owned->submit_entry->buf.base, data, len);
    owned->submit_entry->buf.len = len;
    owned->submit_entry->batch = owned->submit_entry->buf.base;

    owned->event.type = RAFT_SUBMIT;
    owned->event.time = now_ms();
    owned->event.submit.entries = owned->submit_entry.get();
    owned->event.submit.n = 1;

    owned->use_rdma       = opts.use_rdma;
    owned->rdma_timeout_s = opts.timeout_s;

    queue_->push(std::move(owned));
    return 0;
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

raft_index MochiRaftServer::commit_index() const {
    return raft_commit_index(const_cast<struct raft*>(&raft_));
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

} // namespace mraft
