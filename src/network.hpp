#pragma once

#include <thallium.hpp>
#include <thallium/provider.hpp>
#include <thallium/bulk.hpp>
#include <thallium/timeout.hpp>

extern "C" {
#include <raft.h>
}

#include "serialization.hpp"

#include <functional>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <string>

#include "mochi-raft.hpp"

namespace tl = thallium;

namespace mraft {

class Network : public tl::provider<Network> {
public:
    // Callback invoked when a message is received from a peer.
    // The callback takes ownership of the message and must free
    // any allocated entries (e.g. from AppendEntries).
    using receive_cb_t = std::function<void(struct raft_message*)>;

    Network(tl::engine& engine, uint16_t provider_id,
            raft_id local_id, const std::string& local_address,
            receive_cb_t on_receive);
    ~Network();

    // Send messages returned by raft_step's update.
    // Each message's server_address is used to look up the destination endpoint.
    // The sender's id/address are stamped from local_id_/local_address_.
    void send(const struct raft_message* messages, unsigned n);

    // Send a single AppendEntries message using RDMA bulk transfer.
    // The sender exposes entry data as a read-only bulk handle and issues a
    // blocking timed RPC; the receiver pulls the data and responds.
    void send_rdma(const struct raft_message* msg, double timeout_s);

    // Set/get network isolation mode (thread-safe)
    void set_isolation(IsolationMode mode);
    IsolationMode get_isolation() const;

private:
    void on_message_rpc(MessageData& data);
    void on_message_rdma_rpc(const tl::request& req,
                              RdmaAppendEntriesArgs& args,
                              tl::bulk& sender_bulk);

    raft_id local_id_;
    std::string local_address_;
    receive_cb_t on_receive_;
    tl::remote_procedure rpc_;
    tl::remote_procedure rpc_rdma_;

    // Endpoint cache: address -> endpoint
    std::mutex cache_mutex_;
    std::unordered_map<std::string, tl::endpoint> endpoint_cache_;

    tl::endpoint lookup_endpoint(const std::string& address);

    std::atomic<uint8_t> isolation_{0};
};

} // namespace mraft
