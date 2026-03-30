#pragma once

#include <thallium.hpp>
#include <abt-io.h>

extern "C" {
#include <raft.h>
}

#include <memory>
#include <string>
#include <vector>
#include <atomic>

namespace tl = thallium;

namespace mochi_raft {

// Network isolation modes for testing/simulation
enum class IsolationMode : uint8_t {
    NONE     = 0,
    INBOUND  = 1,  // Drop incoming messages
    OUTBOUND = 2,  // Drop outgoing messages
    BOTH     = 3   // Drop all messages
};

class Storage;
class Network;
class EventQueue;

// User-provided finite state machine
struct Fsm {
    virtual ~Fsm() = default;
    virtual int apply(const struct raft_buffer& buf) = 0;
};

class MochiRaftServer {
public:
    MochiRaftServer(tl::engine& engine,
                    abt_io_instance_id abt_io,
                    raft_id id,
                    const std::string& address,
                    const std::string& data_dir,
                    Fsm& fsm,
                    uint16_t provider_id = 0);
    ~MochiRaftServer();

    // Bootstrap a new cluster with initial configuration.
    // Must be called before start() on exactly one node.
    int bootstrap(
        const std::vector<std::pair<raft_id, std::string>>& servers);

    // Start the server (loads state, begins event loop).
    int start();

    // Submit a new entry for replication (leader only).
    // Returns 0 on success, RAFT_NOTLEADER if not leader.
    int submit(const void* data, size_t len);

    // Get current state
    raft_id id() const;
    unsigned short state() const;  // not thread-safe, poll only
    raft_term current_term() const;  // not thread-safe, poll only
    raft_index commit_index() const;  // not thread-safe, poll only

    // Set network isolation mode (thread-safe)
    void set_isolation(IsolationMode mode);

    // Initiate leadership transfer to target node.
    // Pushes a RAFT_TRANSFER event; asynchronous.
    int transfer(raft_id target_id);

    // Shutdown the server
    void shutdown();

private:
    void event_loop();
    void handle_update(struct raft_update& update);
    void on_receive(struct raft_message* msg);
    void apply_committed_entries();

    struct raft raft_;
    tl::engine& engine_;
    Fsm& fsm_;
    raft_id id_;
    std::string address_;

    std::unique_ptr<Storage> storage_;
    std::unique_ptr<Network> network_;
    std::unique_ptr<EventQueue> queue_;

    ABT_thread loop_thread_ = ABT_THREAD_NULL;
    std::atomic<bool> running_{false};

    // Track last applied index for FSM
    raft_index last_applied_ = 0;
};

} // namespace mochi_raft
