#ifndef MRAFT_TEST_WORKER_HPP
#define MRAFT_TEST_WORKER_HPP

#include <thallium.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <mochi-raft.hpp>
#include <mochi-raft-test.h>
#include <chrono>
#include <unistd.h>

namespace tl = thallium;

struct WorkerOptions {
    std::string       protocol;
    tl::logger::level logLevel;
    std::string       logPath;
    std::string       logType;
    std::string       tracePath;
};

struct FSM : public mraft::StateMachine {

    void apply(const struct raft_buffer* buf, void** result) override {
        std::unique_lock<tl::mutex> lock{content_mtx};
        content.append(std::string_view{(const char*)buf->base, buf->len});
    }

    void snapshot(struct raft_buffer* bufs[], unsigned* n_bufs) override {
        std::unique_lock<tl::mutex> lock{content_mtx};
        *bufs   = static_cast<raft_buffer*>(raft_malloc(sizeof(**bufs)));
        *n_bufs = 1;
        (*bufs)[0].base = strdup(content.c_str());
        (*bufs)[0].len  = content.size();
    }

    void restore(struct raft_buffer* buf) override {
        std::unique_lock<tl::mutex> lock{content_mtx};
        content.assign(std::string_view{(const char*)buf->base, buf->len});
    }

    std::string       content;
    mutable tl::mutex content_mtx;

};

struct Worker : public tl::provider<Worker> {

    Worker(tl::engine engine, raft_id raftID, std::unique_ptr<mraft::Log> theLog)
    : tl::provider<Worker>(engine, 0)
    , fsm{}
    , log{std::move(theLog)}
    , raft{engine.get_margo_instance(), raftID, fsm, *log}
    , id{raftID} {
        #define DEFINE_WORKER_RPC(__rpc__) define("mraft_test_" #__rpc__, &Worker::__rpc__)
        DEFINE_WORKER_RPC(bootstrap);
        DEFINE_WORKER_RPC(start);
        DEFINE_WORKER_RPC(add);
        DEFINE_WORKER_RPC(assign);
        DEFINE_WORKER_RPC(remove);
        DEFINE_WORKER_RPC(apply);
        DEFINE_WORKER_RPC(get_leader);
        DEFINE_WORKER_RPC(transfer);
        DEFINE_WORKER_RPC(suspend);
        DEFINE_WORKER_RPC(barrier);
        DEFINE_WORKER_RPC(isolate);
        DEFINE_WORKER_RPC(get_fsm_content);
        #undef DEFINE_WORKER_RPC
        raft.enable_tracer(true);
        raft.set_tracer(
            [mid=engine.get_margo_instance(), raftID](const char* file, int line, const char* msg) {
                margo_trace(mid, "[worker:%d] [%s:%d] %s", (unsigned)raftID, file, line, msg);
            });
    }

    #define WRAP_CALL(func, ...)                          \
    do {                                                  \
        try {                                             \
            raft.func(__VA_ARGS__);                       \
        } catch (const mraft::RaftException& ex) {        \
            margo_error(get_engine().get_margo_instance(),\
                "[worker:%lu] %s failed: %s",             \
                id, #func, raft_strerror(ex.code()));     \
            return ex.code();                             \
        }                                                 \
        return 0;                                         \
    } while (0)

    int bootstrap(const std::vector<mraft::ServerInfo>& servers) {
        WRAP_CALL(bootstrap, servers);
    }

    int start() {
        WRAP_CALL(start);
    }

    int add(const raft_id& raftId, const std::string& addr) {
        WRAP_CALL(add, raftId, addr.c_str());
    }

    int assign(const raft_id& raftId, const mraft::Role& role) {
        WRAP_CALL(assign, raftId, role);
    }

    int remove(const raft_id& raftId) {
        WRAP_CALL(remove, raftId);
    }

    int apply(const std::string& buffer) {
        struct raft_buffer bufs = {
            .base = (void*)buffer.c_str(),
            .len  = buffer.size()
        };
        WRAP_CALL(apply, &bufs, 1U);
    }

    int barrier() {
        WRAP_CALL(barrier);
    }

    int isolate(bool flag) {
        auto raft_io = raft.get_raft_io();
        return mraft_io_simulate_dead(raft_io, flag);
    }

    mraft::ServerInfo get_leader() const {
        mraft::ServerInfo leaderInfo{0, ""};
        try {
            leaderInfo = raft.get_leader();
        } catch (mraft::RaftException& ex) {}
        return leaderInfo;
    }

    int transfer(raft_id transferToId) {
        WRAP_CALL(transfer, transferToId);
    }

    void suspend(const tl::request& req, unsigned msec) const {
        req.respond(0);
        usleep(msec*1000);
    }

    std::string get_fsm_content() const {
        std::unique_lock<tl::mutex> lock{fsm.content_mtx};
        return fsm.content;
    }

    #undef WRAP_CALL

    raft_id                     id;
    FSM                         fsm;
    std::unique_ptr<mraft::Log> log;
    mraft::Raft                 raft;
};

static inline int runWorker(int fdToMaster, raft_id raftID, const WorkerOptions& options) {

    if(!options.tracePath.empty()) {
        auto stdoutFilename = options.tracePath + "/" + std::to_string(raftID) + ".out";
        auto stderrFilename = options.tracePath + "/" + std::to_string(raftID) + ".err";
        FILE* _ = freopen(stdoutFilename.c_str(), "a+", stdout);
        _ = freopen(stderrFilename.c_str(), "a+", stderr);
    }

    auto engine = tl::engine(options.protocol, MARGO_SERVER_MODE);
    engine.set_log_level(options.logLevel);
    engine.enable_remote_shutdown();

    std::unique_ptr<mraft::Log> log;
    if(options.logType == "abt-io") {
        auto config = std::string{"{\"path\":\""} + options.logPath + "\"}";
        log = std::make_unique<mraft::AbtIoLog>(
                raftID, ABT_IO_INSTANCE_NULL, config.c_str(), engine.get_margo_instance());
    } else {
        if(options.logPath != ".")
            margo_warning(engine.get_margo_instance(),
                    "--log-path set to \"%s\" but will be ignored with the memory log",
                    options.logPath.c_str());
        log = std::make_unique<mraft::MemoryLog>();
    }

    auto worker = new Worker{engine, raftID, std::move(log)};
    engine.push_finalize_callback([worker](){ delete worker; });

    auto address = static_cast<std::string>(engine.self());
    address.resize(1024);
    ssize_t _ = write(fdToMaster, address.data(), 1024);
    close(fdToMaster);

    engine.wait_for_finalize();
    return 0;
}

#endif
