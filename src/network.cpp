#include "network.hpp"
#include <thallium/provider_handle.hpp>
#include <chrono>
#include <cstdio>

namespace mraft {

Network::Network(tl::engine& engine, uint16_t provider_id,
                 raft_id local_id, const std::string& local_address,
                 receive_cb_t on_receive)
    : tl::provider<Network>(engine, provider_id)
    , local_id_(local_id)
    , local_address_(local_address)
    , on_receive_(std::move(on_receive))
    , rpc_(define("mochi_raft_message", &Network::on_message_rpc))
    , rpc_rdma_(define("mochi_raft_message_rdma", &Network::on_message_rdma_rpc))
{
}

Network::~Network() = default;

void Network::set_isolation(IsolationMode mode) {
    isolation_.store(static_cast<uint8_t>(mode));
}

IsolationMode Network::get_isolation() const {
    return static_cast<IsolationMode>(isolation_.load());
}

void Network::on_message_rpc(MessageData& data) {
    auto mode = static_cast<IsolationMode>(isolation_.load());
    if (mode == IsolationMode::INBOUND || mode == IsolationMode::BOTH) {
        return;  // Silently drop incoming message
    }

    auto msg = new struct raft_message;
    data.to_raft(*msg);

    // Store the address string — the raft_message needs a stable pointer
    // We allocate a copy that the callback takes ownership of
    if (!data.server_address.empty()) {
        char* addr_copy = static_cast<char*>(
            raft_malloc(data.server_address.size() + 1));
        memcpy(addr_copy, data.server_address.c_str(),
               data.server_address.size() + 1);
        msg->server_address = addr_copy;
    }

    on_receive_(msg);
}

tl::endpoint Network::lookup_endpoint(const std::string& address) {
    std::lock_guard<std::mutex> lock(cache_mutex_);
    auto it = endpoint_cache_.find(address);
    if (it != endpoint_cache_.end()) {
        return it->second;
    }
    auto ep = get_engine().lookup(address);
    endpoint_cache_.emplace(address, ep);
    return ep;
}

void Network::send(const struct raft_message* messages, unsigned n) {
    auto mode = static_cast<IsolationMode>(isolation_.load());
    if (mode == IsolationMode::OUTBOUND || mode == IsolationMode::BOTH) {
        return;  // Silently drop all outgoing messages
    }

    for (unsigned i = 0; i < n; i++) {
        const struct raft_message& msg = messages[i];
        MessageData data = MessageData::from_raft(msg);

        // c-raft puts the DESTINATION server in server_id/server_address
        // for outgoing messages. We use that for endpoint lookup, but
        // replace it with the SENDER's identity in the serialized data,
        // because the receiver expects server_id/server_address to
        // identify the sender (see uv_recv.c:276-277).
        std::string dest_address = data.server_address;
        data.server_id = local_id_;
        data.server_address = local_address_;

        try {
            auto ep = lookup_endpoint(dest_address);
            tl::provider_handle ph(ep, get_provider_id());
            rpc_.on(ph)(data);
        } catch (const std::exception& e) {
            // Network errors are non-fatal — raft handles retries
        }
    }
}

void Network::send_rdma(const struct raft_message* msg, double timeout_s) {
    auto mode = static_cast<IsolationMode>(isolation_.load());
    if (mode == IsolationMode::OUTBOUND || mode == IsolationMode::BOTH)
        return;

    const auto& ae = msg->append_entries;

    RdmaAppendEntriesArgs args;
    args.server_id      = local_id_;
    args.server_address = local_address_;
    args.version        = ae.version;
    args.term           = ae.term;
    args.prev_log_index = ae.prev_log_index;
    args.prev_log_term  = ae.prev_log_term;
    args.leader_commit  = ae.leader_commit;
    args.timeout_s      = timeout_s;

    std::vector<std::pair<void*, std::size_t>> segments;
    for (unsigned i = 0; i < ae.n_entries; i++) {
        args.entries_meta.push_back(
            {ae.entries[i].term, ae.entries[i].type, ae.entries[i].buf.len});
        if (ae.entries[i].buf.len > 0)
            segments.push_back({ae.entries[i].buf.base, ae.entries[i].buf.len});
    }

    if (segments.empty()) {
        // Nothing to transfer via RDMA — fall back to regular path
        send(msg, 1);
        return;
    }

    auto local_bulk =
        get_engine().expose(segments, tl::bulk_mode::read_only);

    try {
        auto ep = lookup_endpoint(msg->server_address);
        tl::provider_handle ph(ep, get_provider_id());
        // Record sent_at as late as possible, right before the call
        args.sent_at = std::chrono::duration<double>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        // Blocking, timed RPC — buffer stays pinned until response or timeout.
        // timed() takes the duration first, then the RPC arguments.
        rpc_rdma_.on(ph).timed(
            std::chrono::duration<double>(timeout_s), args, local_bulk);
    } catch (const std::exception& e) {
        fprintf(stderr, "[mochi-raft] RDMA send to %s failed: %s\n",
                msg->server_address, e.what());
    }
    // local_bulk destroyed here — memory unpinned
}

void Network::on_message_rdma_rpc(const tl::request& req,
                                   RdmaAppendEntriesArgs& args,
                                   tl::bulk& sender_bulk) {
    auto mode = static_cast<IsolationMode>(isolation_.load());
    if (mode == IsolationMode::INBOUND || mode == IsolationMode::BOTH) {
        req.respond(false);
        return;
    }

    // Compute how much time is left before the sender's RPC times out
    double now_s = std::chrono::duration<double>(
        std::chrono::system_clock::now().time_since_epoch()).count();
    double remaining = args.timeout_s - (now_s - args.sent_at);
    if (remaining <= 0) {
        fprintf(stderr, "[mochi-raft] RDMA receive: sender already timed out\n");
        req.respond(false);
        return;
    }

    // Allocate one contiguous raft_malloc'd batch for all entry data and
    // expose it directly for RDMA writing — no intermediate copy needed.
    uint64_t total = 0;
    for (auto& em : args.entries_meta) total += em.size;

    void* batch_base = raft_malloc(total);
    if (!batch_base) {
        fprintf(stderr, "[mochi-raft] RDMA receive: allocation failed\n");
        req.respond(false);
        return;
    }

    std::vector<std::pair<void*, std::size_t>> seg = {{batch_base, total}};
    auto local_bulk = get_engine().expose(seg, tl::bulk_mode::write_only);
    auto caller     = req.get_endpoint();

    // Timed pull: use Y/2 of the remaining window.
    // remote_bulk::timed(ms) >> bulk_segment calls margo_bulk_transfer_timed();
    // tl::timeout is thrown if the deadline expires.
    double timeout_half_ms = (remaining / 2.0) * 1000.0;
    try {
        sender_bulk.on(caller).timed(timeout_half_ms) >>
            local_bulk.select(0, total);
    } catch (const tl::timeout&) {
        fprintf(stderr, "[mochi-raft] RDMA pull timed out (%.0f ms budget)\n",
                timeout_half_ms);
        raft_free(batch_base);
        req.respond(false);
        return;
    } catch (const std::exception& e) {
        fprintf(stderr, "[mochi-raft] RDMA pull failed: %s\n", e.what());
        raft_free(batch_base);
        req.respond(false);
        return;
    }

    // Reconstruct the raft_message and hand it to the event queue.
    // All entries share the single batch allocation: buf.base/len point into
    // sub-regions of it, and batch points to the allocation base so c-raft
    // can free the whole thing at once.
    auto msg = new struct raft_message;
    memset(msg, 0, sizeof(*msg));
    msg->type      = RAFT_APPEND_ENTRIES;
    msg->server_id = static_cast<raft_id>(args.server_id);

    if (!args.server_address.empty()) {
        char* addr_copy = static_cast<char*>(
            raft_malloc(args.server_address.size() + 1));
        memcpy(addr_copy, args.server_address.c_str(),
               args.server_address.size() + 1);
        msg->server_address = addr_copy;
    }

    auto& ae          = msg->append_entries;
    ae.version        = args.version;
    ae.term           = static_cast<raft_term>(args.term);
    ae.prev_log_index = static_cast<raft_index>(args.prev_log_index);
    ae.prev_log_term  = static_cast<raft_term>(args.prev_log_term);
    ae.leader_commit  = static_cast<raft_index>(args.leader_commit);
    ae.n_entries      = static_cast<unsigned>(args.entries_meta.size());
    ae.entries        = static_cast<struct raft_entry*>(
        raft_malloc(ae.n_entries * sizeof(struct raft_entry)));

    uint64_t offset = 0;
    for (unsigned i = 0; i < ae.n_entries; i++) {
        auto& em = args.entries_meta[i];
        memset(&ae.entries[i], 0, sizeof(struct raft_entry));
        ae.entries[i].term       = static_cast<raft_term>(em.term);
        ae.entries[i].type       = static_cast<enum raft_entry_type>(em.type);
        ae.entries[i].buf.base   = static_cast<char*>(batch_base) + offset;
        ae.entries[i].buf.len    = em.size;
        ae.entries[i].batch      = batch_base;
        offset += em.size;
    }

    on_receive_(msg);
    req.respond(true);
}

} // namespace mraft
