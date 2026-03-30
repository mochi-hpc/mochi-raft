#include "network.hpp"
#include <thallium/provider_handle.hpp>

namespace mochi_raft {

Network::Network(tl::engine& engine, uint16_t provider_id,
                 raft_id local_id, const std::string& local_address,
                 receive_cb_t on_receive)
    : tl::provider<Network>(engine, provider_id)
    , local_id_(local_id)
    , local_address_(local_address)
    , on_receive_(std::move(on_receive))
    , rpc_(define("mochi_raft_message", &Network::on_message_rpc))
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

} // namespace mochi_raft
