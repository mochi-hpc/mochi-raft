#pragma once

#include <thallium.hpp>
#include <abt-io.h>

extern "C" {
#include <raft.h>
}

#include <climits>
#include <cstring>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <string_view>
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
     * @param data The entry payload as a string view.
     * @return 0 on success, non-zero on error.
     */
    virtual int apply(std::string_view data) = 0;
};

/**
 * @brief A buffer allocated with raft_malloc, for zero-copy submit().
 *
 * Callers can allocate a MochiRaftBuffer, fill it directly, and pass it
 * to submit() as an rvalue. submit() steals the raft_malloc allocation
 * without copying, so the payload travels from the caller's buffer straight
 * into the raft log entry.
 *
 * The size-only constructor allocates an uninitialized buffer that the caller
 * fills in place. The (data, size) and string_view constructors copy their
 * input into a fresh raft_malloc buffer, useful when the source is not
 * already in a raft_malloc region.
 */
class MochiRaftBuffer {
public:
    explicit MochiRaftBuffer(size_t size)
        : data_(raft_malloc(size)), size_(size) {}

    MochiRaftBuffer(const char* data, size_t size)
        : MochiRaftBuffer(size) {
        if (data_) memcpy(data_, data, size);
    }

    MochiRaftBuffer(std::string_view sv)
        : MochiRaftBuffer(sv.data(), sv.size()) {}

    ~MochiRaftBuffer() { raft_free(data_); }

    MochiRaftBuffer(const MochiRaftBuffer&)            = delete;
    MochiRaftBuffer& operator=(const MochiRaftBuffer&) = delete;

    MochiRaftBuffer(MochiRaftBuffer&& other) noexcept
        : data_(other.data_), size_(other.size_) {
        other.data_ = nullptr;
        other.size_ = 0;
    }

    MochiRaftBuffer& operator=(MochiRaftBuffer&& other) noexcept {
        if (this != &other) {
            raft_free(data_);
            data_ = other.data_;
            size_ = other.size_;
            other.data_ = nullptr;
            other.size_ = 0;
        }
        return *this;
    }

    void*       data()       { return data_; }
    const void* data() const { return data_; }
    size_t      size() const { return size_; }

    /// Transfer ownership of the buffer to the caller.
    /// After this call the MochiRaftBuffer is empty and will not free anything.
    void* release() noexcept {
        void* p = data_;
        data_   = nullptr;
        size_   = 0;
        return p;
    }

private:
    void*  data_;
    size_t size_;
};

/**
 * @brief A Raft server backed by Thallium RPCs and ABT-IO storage.
 *
 * Wraps c-raft's step-based API (raft_step()) with Mochi components:
 * Thallium for network transport and ABT-IO for persistent I/O.
 * An internal event loop runs as an Argobots ULT on the supplied loop pool,
 * driving elections, replication, and log persistence.
 *
 * Typical lifecycle:
 * @code
 *   auto pool = engine.get_handler_pool();
 *   MochiRaftServer server(engine, abt_io, id, pool, pool, dir, fsm);
 *   server.bootstrap({{1, engine1.self()}, {2, engine2.self()}, ...});
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
     * The server's Thallium address is derived automatically from
     * `engine.self()`.
     *
     * @param engine      Thallium engine (must outlive the server).
     * @param abt_io      ABT-IO instance for persistent I/O.
     * @param id          Unique Raft server ID (must be > 0).
     * @param loop_pool   Argobots pool on which the event loop ULT runs.
     * @param rpc_pool    Argobots pool on which incoming RPC handlers execute.
     * @param data_dir    Directory for persistent storage (created if absent).
     * @param fsm         User-provided state machine (must outlive the server).
     * @param provider_id Thallium provider ID, allowing multiple Raft groups
     *                    on the same engine (default 0).
     */
    MochiRaftServer(tl::engine& engine,
                    abt_io_instance_id abt_io,
                    raft_id id,
                    tl::pool loop_pool,
                    tl::pool rpc_pool,
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
     * If on_applied is provided, it is called exactly once:
     * - with 0 when the entry has been applied to the FSM, or
     * - with RAFT_NOTLEADER if the entry was rolled back due to a leadership
     *   change before it could be committed.
     *
     * The callback is invoked from the event loop ULT, not the caller's thread.
     *
     * @param buf        The entry payload.
     * @param on_applied Optional completion callback.
     * @return 0 on success, RAFT_NOTLEADER if this node is not the leader.
     */
    int submit(MochiRaftBuffer&& buf,
               std::function<void(int)> on_applied = {});

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
     * @brief Get the current cluster membership as seen by this server.
     *
     * Returns all servers in the committed configuration.
     *
     * @return Vector of (server_id, address) pairs.
     * @note Not thread-safe; intended for polling from outside the event loop.
     */
    std::vector<std::pair<raft_id, std::string>> members() const;

    /**
     * @brief Get the current leader as seen by this server.
     *
     * Returns the ID and address of the node this server believes to be the
     * current leader. If no leader is known, id is 0 and address is empty.
     *
     * @return (leader_id, leader_address) pair.
     * @note Not thread-safe; intended for polling from outside the event loop.
     */
    std::pair<raft_id, std::string> leader() const;

    /**
     * @brief Configure the RDMA transport policy.
     *
     * Entries whose total payload exceeds rdma_threshold bytes are sent via
     * RDMA bulk transfer; smaller entries use inline RPC. The timeout for an
     * RDMA send is max(min_timeout_ms, timeout_factor * data_size_bytes),
     * converted to seconds. Defaults: RDMA disabled (threshold = SIZE_MAX),
     * min_timeout_ms = 5000, timeout_factor = 0.
     *
     * @param rdma_threshold  Byte threshold above which RDMA is used.
     * @param min_timeout_ms  Minimum RDMA timeout in milliseconds.
     * @param timeout_factor  Milliseconds of timeout added per byte of data.
     */
    void set_rdma_config(size_t rdma_threshold,
                         double min_timeout_ms,
                         double timeout_factor);

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
    std::string address_;  ///< Derived from engine.self() at construction time.
    tl::pool loop_pool_;   ///< Pool on which the event loop ULT runs.

    std::unique_ptr<Storage> storage_;
    std::unique_ptr<Network> network_;
    std::unique_ptr<EventQueue> queue_;

    tl::managed<tl::thread> loop_thread_;
    std::atomic<bool> running_{false};

    raft_index last_applied_ = 0; ///< Tracks the last log index applied to the FSM.

    size_t rdma_threshold_ = SIZE_MAX; ///< Byte threshold above which RDMA is used (default: disabled).
    double min_timeout_ms_ = 5000.0;   ///< Minimum RDMA timeout in milliseconds.
    double timeout_factor_ = 0.0;      ///< Additional ms of timeout per byte of data.

    struct PendingCallback {
        raft_term             term;
        std::function<void(int)> fn;
    };
    std::map<raft_index, PendingCallback> pending_callbacks_;
};

} // namespace mraft
