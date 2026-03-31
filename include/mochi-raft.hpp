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

namespace mraft {

/**
 * @brief Network isolation modes for testing and partition simulation.
 */
enum class IsolationMode : uint8_t {
    NONE     = 0, ///< No isolation (default).
    INBOUND  = 1, ///< Drop all incoming messages.
    OUTBOUND = 2, ///< Drop all outgoing messages.
    BOTH     = 3  ///< Drop all messages in both directions.
};

class Storage;
class Network;
class EventQueue;

/**
 * @brief User-provided finite state machine.
 *
 * Implement this interface to define how committed log entries are applied
 * to your application state. The apply() method is called exactly once per
 * committed RAFT_COMMAND entry, in log order.
 */
struct Fsm {
    virtual ~Fsm() = default;

    /**
     * @brief Apply a committed log entry to the state machine.
     *
     * Called in log order for each committed RAFT_COMMAND entry.
     * RAFT_CHANGE (configuration) and RAFT_BARRIER entries are skipped.
     *
     * @param buf The entry payload. buf.base points to the data, buf.len
     *            is its size in bytes.
     * @return 0 on success, non-zero on error.
     */
    virtual int apply(const struct raft_buffer& buf) = 0;
};

/**
 * @brief A Raft server backed by Thallium RPCs and ABT-IO storage.
 *
 * Wraps c-raft's step-based API (raft_step()) with Mochi components:
 * Thallium for network transport and ABT-IO for persistent I/O.
 * An internal event loop runs as an Argobots ULT on the engine's handler
 * pool, driving elections, replication, and log persistence.
 *
 * Typical lifecycle:
 * @code
 *   MochiRaftServer server(engine, abt_io, id, addr, dir, fsm);
 *   server.bootstrap({{1, addr1}, {2, addr2}, {3, addr3}});
 *   server.start();
 *   // ... submit(), state(), commit_index() ...
 *   server.shutdown();
 * @endcode
 */
class MochiRaftServer {
public:
    /**
     * @brief Construct a Raft server.
     *
     * Allocates internal storage, network, and event queue components but
     * does not start the event loop. Call bootstrap() then start().
     *
     * @param engine      Thallium engine (must outlive the server).
     * @param abt_io      ABT-IO instance for persistent I/O.
     * @param id          Unique Raft server ID (must be > 0).
     * @param address     This server's Thallium address (e.g. "na+sm://...").
     * @param data_dir    Directory for persistent storage (created if absent).
     * @param fsm         User-provided state machine (must outlive the server).
     * @param provider_id Thallium provider ID, allowing multiple Raft groups
     *                    on the same engine (default 0).
     */
    MochiRaftServer(tl::engine& engine,
                    abt_io_instance_id abt_io,
                    raft_id id,
                    const std::string& address,
                    const std::string& data_dir,
                    Fsm& fsm,
                    uint16_t provider_id = 0);
    ~MochiRaftServer();

    /**
     * @brief Bootstrap a new cluster with an initial configuration.
     *
     * Writes the cluster configuration as the first log entry (RAFT_CHANGE).
     * Must be called on all nodes before start(), and only once per cluster.
     *
     * @param servers List of (server_id, thallium_address) pairs for all
     *                initial cluster members.
     * @return 0 on success, or a raft error code.
     */
    int bootstrap(
        const std::vector<std::pair<raft_id, std::string>>& servers);

    /**
     * @brief Start the server.
     *
     * Loads persisted state (term, vote, log entries) and starts the event
     * loop as an Argobots ULT. After this call, the server participates in
     * elections and replication.
     *
     * @return 0 on success, or a raft error code.
     */
    int start();

    /**
     * @brief Submit a new entry for replication.
     *
     * The entry is enqueued asynchronously; this call returns immediately.
     * The entry will be committed once a majority of the cluster has
     * persisted it, then applied to the FSM.
     *
     * @param data Pointer to the entry payload.
     * @param len  Size of the payload in bytes.
     * @return 0 on success, RAFT_NOTLEADER if this node is not the leader.
     */
    int submit(const void* data, size_t len);

    /**
     * @brief Get this server's Raft ID.
     * @return The server ID passed to the constructor.
     */
    raft_id id() const;

    /**
     * @brief Get the current Raft state.
     * @return One of RAFT_FOLLOWER, RAFT_CANDIDATE, or RAFT_LEADER.
     * @note Not thread-safe; intended for polling from outside the event loop.
     */
    unsigned short state() const;

    /**
     * @brief Get the current Raft term.
     * @return The term number.
     * @note Not thread-safe; intended for polling from outside the event loop.
     */
    raft_term current_term() const;

    /**
     * @brief Get the current commit index.
     * @return The highest log index known to be committed.
     * @note Not thread-safe; intended for polling from outside the event loop.
     */
    raft_index commit_index() const;

    /**
     * @brief Set the network isolation mode.
     *
     * Used to simulate network partitions for testing. The change takes
     * effect immediately and is thread-safe.
     *
     * @param mode The desired isolation mode.
     */
    void set_isolation(IsolationMode mode);

    /**
     * @brief Initiate leadership transfer to a specific node.
     *
     * Pushes a RAFT_TRANSFER event into the event queue. The current leader
     * will send a TimeoutNow RPC to the target, causing it to start an
     * immediate election. This call is asynchronous.
     *
     * @param target_id The Raft ID of the node to transfer leadership to.
     * @return 0 on success.
     */
    int transfer(raft_id target_id);

    /**
     * @brief Shut down the server.
     *
     * Stops the event loop and releases internal resources. Safe to call
     * multiple times. Blocks until the event loop ULT has exited.
     */
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

    raft_index last_applied_ = 0; ///< Tracks the last log index applied to the FSM.
};

} // namespace mraft
