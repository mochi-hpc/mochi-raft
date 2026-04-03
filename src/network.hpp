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

    // Callbacks for the forward-submit / forward-result RPCs.
    using forward_submit_cb_t =
        std::function<void(std::vector<uint8_t>, uint64_t, std::string)>;
    using forward_result_cb_t = std::function<void(uint64_t, int)>;

    Network(tl::engine& engine, uint16_t provider_id,
            raft_id local_id, const std::string& local_address,
            receive_cb_t on_receive, tl::pool rpc_pool);
    ~Network();

    // Send messages returned by raft_step's update.
    // Each message's server_address is used to look up the destination endpoint.
    // The sender's id/address are stamped from local_id_/local_address_.
    void send(const struct raft_message* messages, unsigned n);

    // Send a single AppendEntries message using RDMA bulk transfer.
    // The sender exposes entry data as a read-only bulk handle and issues a
    // blocking timed RPC; the receiver pulls the data and responds.
    void send_rdma(const struct raft_message* msg, double timeout_s);

    // Forward a submit payload to a remote leader node.
    void send_forward_submit(const std::string& dest,
                             const std::vector<uint8_t>& data,
                             uint64_t corr_id,
                             const std::string& caller_address);

    // Send the forwarded-submit result back to the originating follower.
    void send_forward_result(const std::string& dest, uint64_t corr_id, int rv);

    // Register callbacks for the two forward RPCs.
    void set_forward_submit_cb(forward_submit_cb_t cb);
    void set_forward_result_cb(forward_result_cb_t cb);

    // Set/get network isolation mode (thread-safe)
    void set_isolation(IsolationMode mode);
    IsolationMode get_isolation() const;

private:
    void on_message_rpc(MessageData& data);
    void on_message_rdma_rpc(const tl::request& req,
                              RdmaAppendEntriesArgs& args,
                              tl::bulk& sender_bulk);
    void on_forward_submit_rpc(ForwardSubmitArgs& args);
    void on_forward_result_rpc(ForwardResultArgs& args);

    raft_id local_id_;
    std::string local_address_;
    receive_cb_t on_receive_;
    tl::auto_remote_procedure rpc_;
    tl::auto_remote_procedure rpc_rdma_;
    tl::auto_remote_procedure rpc_forward_submit_;
    tl::auto_remote_procedure rpc_forward_result_;

    forward_submit_cb_t forward_submit_cb_;
    forward_result_cb_t forward_result_cb_;

    // Endpoint cache: address -> endpoint
    std::mutex cache_mutex_;
    std::unordered_map<std::string, tl::endpoint> endpoint_cache_;

    tl::endpoint lookup_endpoint(const std::string& address);

    std::atomic<uint8_t> isolation_{0};
};

} // namespace mraft
