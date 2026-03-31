# mochi-raft

A C++ library that integrates the [c-raft](https://github.com/cowsql/raft.git) consensus protocol with the [Mochi](https://mochi.readthedocs.io/en/latest/) microservice ecosystem, using [Thallium](https://mochi.readthedocs.io/en/latest/thallium.html) for RPC/RDMA networking and [ABT-IO](https://mochi.readthedocs.io/en/latest/abt-io.html) for non-blocking persistent I/O.

## Design Overview

### Architecture

mochi-raft replaces c-raft's default libuv-based I/O layer with Mochi components:

```
                    +-------------------+
                    | MochiRaftServer   |
                    |   (public API)    |
                    +--------+----------+
                             |
          +------------------+------------------+
          |                  |                  |
    +-----+------+     +-----+-----+     +------+------+
    |  Network   |     |  Storage  |     | EventQueue  |
    | (Thallium) |     | (ABT-IO)  |     | (ABT sync)  |
    +-----+------+     +-----+-----+     +------+------+
          |                  |                  |
    Thallium RPCs      Segment files      ABT_mutex/cond
    (na+sm, tcp,       (metadata +        (thread-safe
     verbs, etc.)       log segments)      event delivery)
```

### Step-Based API

This library uses c-raft's **step-based API** (`raft_step()`) rather than the deprecated `raft_io` callback vtable. The step-based API works as follows:

1. An **event** occurs (timeout, message received, entries persisted, client submit)
2. The application calls `raft_step(raft, &event, &update)`
3. c-raft returns an **update** describing actions the application must perform:
   - Persist term/vote changes (synchronous, before sending messages)
   - Persist new log entries
   - Send messages to peers
   - Apply committed entries to the state machine

This design gives the application full control over I/O scheduling and allows integration with any networking and storage stack.

### Components

- **MochiRaftServer** (`include/mochi-raft.hpp`): Public API. Manages the raft instance lifecycle, event loop, and coordinates all components.

- **Storage** (`src/storage.hpp`): Persistent storage using ABT-IO. Stores metadata (term, voted_for) in double-buffered files and log entries in segment files with automatic rotation. Maintains an in-memory entry cache for fast AppendEntries message assembly.

- **Network** (`src/network.hpp`): Thallium-based RPC layer. Inherits `tl::provider<Network>` for provider-id routing, enabling multiple raft groups on the same engine. Uses one-way RPCs for fire-and-forget message delivery. Caches endpoint lookups.

- **EventQueue** (`src/event_queue.hpp`): Thread-safe event queue using ABT_mutex/ABT_cond. Supports timed blocking pops for election/heartbeat timeout handling. Uses `OwnedEvent` wrappers to manage heap-allocated entry data across thread boundaries.

- **Serialization** (`src/serialization.hpp`): Cereal-compatible wrapper types for all raft message types (RequestVote, AppendEntries, InstallSnapshot, TimeoutNow, and their results). Handles conversion between c-raft C structs and serializable C++ types.

### Event Loop

The event loop runs as an Argobots ULT on the engine's handler pool:

```
while (running) {
    event = queue.pop(timeout_ms)
    if (event == nullptr && raft_timeout_has_elapsed)
        event = RAFT_TIMEOUT
    raft_step(raft, &event, &update)
    handle_update(update):
        1. Persist term/vote (synchronous — must complete before sending)
        2. Persist entries → immediately fire RAFT_PERSISTED_ENTRIES
        3. Send messages to peers via Network
        4. Apply committed entries to FSM
}
```

### Storage Format

- **Metadata**: Two files (`metadata1`, `metadata2`) with version-based double buffering. Format: `[format:8][version:8][term:8][voted_for:8]` (32 bytes, little-endian).
- **Log segments**: Open segments (`open-N`) are renamed to closed segments (`XXXXXXXXXXXXXXXX-XXXXXXXXXXXXXXXX`) on rotation. Entry format: `[term:8][type:1][padding:7][data_len:8][data:N][padding to 8-byte align]`. Segments rotate at 8MB.

## Building

### Prerequisites

- CMake >= 3.20
- C++17 compiler
- Spack environment with:
  - `thallium`
  - `abt-io`
  - `argobots`

c-raft is automatically fetched via CMake FetchContent.

### Build Steps

```bash
# Activate your spack environment
spack env activate <your-env>

# Build
mkdir build && cd build
cmake ..
make -j$(nproc)

# Run tests
ctest --output-on-failure
```

## Usage

### Implementing the FSM

```cpp
#include <mochi-raft.hpp>

struct MyFsm : mraft::Fsm {
    int apply(const struct raft_buffer& buf) override {
        // Apply committed entry to your state machine.
        // buf.base contains the data, buf.len its size.
        // Called in log order, exactly once per committed RAFT_COMMAND entry.
        return 0;
    }
};
```

### Creating a Server

```cpp
#include <thallium.hpp>
#include <abt-io.h>
#include <mochi-raft.hpp>

namespace tl = thallium;

int main() {
    // Initialize Thallium engine
    tl::engine engine("na+sm", THALLIUM_SERVER_MODE, true, 1);
    std::string address = static_cast<std::string>(engine.self());

    // Initialize ABT-IO
    abt_io_instance_id abt_io = abt_io_init(1);

    MyFsm fsm;

    // Create server (id=1, provider_id=0)
    mraft::MochiRaftServer server(
        engine, abt_io, 1, address, "/tmp/raft-data", fsm);

    // Bootstrap a 3-node cluster (call on ALL nodes before start)
    server.bootstrap({
        {1, "na+sm://addr1"},
        {2, "na+sm://addr2"},
        {3, "na+sm://addr3"}
    });

    // Start the server
    server.start();

    // Submit entries (leader only, returns RAFT_NOTLEADER otherwise)
    const char* data = "my-command";
    int rv = server.submit(data, strlen(data));

    // Query state
    unsigned short state = server.state(); // RAFT_LEADER, RAFT_FOLLOWER, etc.
    raft_index commit = server.commit_index();

    // Shutdown
    server.shutdown();
    engine.finalize();
    abt_io_finalize(abt_io);
}
```

### API Reference

#### `mraft::Fsm`

| Method | Description |
|--------|-------------|
| `virtual int apply(const raft_buffer& buf)` | Apply a committed entry. Called in order for each `RAFT_COMMAND` entry. Return 0 on success. |

#### `mraft::MochiRaftServer`

| Method | Description |
|--------|-------------|
| `MochiRaftServer(engine, abt_io, id, address, data_dir, fsm, provider_id=0)` | Construct a server. `provider_id` allows multiple raft groups on one engine. |
| `int bootstrap(servers)` | Bootstrap cluster configuration. Call on all nodes before `start()`. |
| `int start()` | Load persisted state and start the event loop. |
| `int submit(data, len)` | Submit an entry for replication. Returns `RAFT_NOTLEADER` if not leader. |
| `raft_id id() const` | Get this server's raft ID. |
| `unsigned short state() const` | Get current state (`RAFT_FOLLOWER`, `RAFT_CANDIDATE`, `RAFT_LEADER`). |
| `raft_term current_term() const` | Get current term. |
| `raft_index commit_index() const` | Get current commit index. |
| `void shutdown()` | Stop the event loop and clean up. |
| `void set_isolation(IsolationMode mode)` | Set network isolation: `NONE`, `INBOUND`, `OUTBOUND`, `BOTH`. |
| `int transfer(raft_id target_id)` | Initiate leadership transfer to target node. |

#### `mraft::IsolationMode`

| Value | Description |
|-------|-------------|
| `NONE` | No isolation (default) |
| `INBOUND` | Drop incoming messages |
| `OUTBOUND` | Drop outgoing messages |
| `BOTH` | Drop all messages (full partition) |

## CLI Testing Tool

An interactive CLI (`mochi-raft-ctl`) allows testing cluster behavior without writing code. It spawns child worker processes that each run a `MochiRaftServer`, communicating via pipes (no Mochi in the master process).

### Running

```bash
cd build
./tools/mochi-raft-ctl
```

### Commands

| Command | Description |
|---------|-------------|
| `spawn <id1> [id2 ...]` | Spawn, bootstrap, and start raft nodes |
| `shutdown <id>` | Cleanly shut down a node |
| `kill <id>` | Force-kill a node (SIGKILL) |
| `isolate <id> [inbound\|outbound\|both]` | Simulate network partition |
| `deisolate <id>` | Remove network isolation |
| `list` | Show all nodes with state, term, commit index |
| `transfer <id>` | Transfer leadership to a specific node |
| `put <key> <value>` | Store a key-value pair (via raft consensus) |
| `get <key> [id]` | Fetch a value (from leader, or specific node) |
| `help` | Show available commands |
| `quit` | Shutdown all nodes and exit |

### Example Session

```
> spawn 1 2 3
Spawning 3 worker(s)...
  Node 1 initialized at na+sm://...
  Node 2 initialized at na+sm://...
  Node 3 initialized at na+sm://...
Bootstrapping...
Starting...
Leader elected: node 1

> list
ID  STATE       TERM  COMMIT  ADDRESS
1   leader      2     1       na+sm://...
2   follower    2     1       na+sm://...
3   follower    2     1       na+sm://...

> put foo bar
OK

> get foo
bar

> isolate 1 both
Node 1 isolated (both): OK

> list
ID  STATE       TERM  COMMIT  ADDRESS
1   candidate   3     2       na+sm://...
2   leader      4     2       na+sm://...
3   follower    4     2       na+sm://...

> deisolate 1

> transfer 3
Transferring leadership to node 3...
Node 3 is now leader

> kill 2
Node 2 killed

> list
ID  STATE       TERM  COMMIT  ADDRESS
1   follower    5     3       na+sm://...
2   dead        -     -       na+sm://...
3   leader      5     3       na+sm://...

> quit
```

## Tests

| Test | Description |
|------|-------------|
| `test_raft_init` | c-raft initialization and configuration |
| `test_storage` | Metadata persistence, log segment I/O, bootstrap |
| `test_raft_step` | Single-node raft_step lifecycle (start, timeout, submit, persist) |
| `test_serialization` | Round-trip cereal serialization of all message types |
| `test_network` | Thallium RPC message delivery between two engines |
| `test_event_queue` | Thread-safe event queue push/pop with timeouts |
| `test_single_node` | Single-node MochiRaftServer: leader election, entry submission + FSM application |
| `test_cluster` | 3-node cluster: leader election, replication, network isolation, leadership transfer |

## License

See [COPYRIGHT](COPYRIGHT).
