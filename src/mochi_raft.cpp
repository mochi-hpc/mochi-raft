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

        auto it = pending_callbacks_.find(idx);
        if (it != pending_callbacks_.end()) {
            if (it->second.term == entry->term)
                it->second.fn(0);
            pending_callbacks_.erase(it);
        }

        last_applied_ = idx;
    }
}

int MochiRaftServer::submit(MochiRaftBuffer&& buf,
                             std::function<void(int)> on_applied) {
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

} // namespace mraft
