#ifndef MRAFT_TEST_MASTER_HPP
#define MRAFT_TEST_MASTER_HPP

#include "worker.hpp"
#include <optional>
#include <memory>
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/wait.h>
#include <string_view>
#include <map>
#include <thallium.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <mochi-raft.hpp>
#include <chrono>

namespace tl = thallium;
using namespace std::literals::chrono_literals;

struct MasterContext;

struct MasterOptions {
    std::string       protocol;
    tl::logger::level logLevel;
};

static inline auto& operator<<(std::ostream& stream, const mraft::Role& role) {
    switch(role) {
    case mraft::Role::SPARE:
        stream << "SPARE"; break;
    case mraft::Role::VOTER:
        stream << "VOTER"; break;
    case mraft::Role::STANDBY:
        stream << "STANDBY"; break;
    default:
        stream << "???";
    }
    return stream;
}

struct WorkerHandle {

    raft_id     raftID  = 0;
    pid_t       pID     = -1;
    std::string address;
    mraft::Role role    = mraft::Role::SPARE;
    bool        running = false;
    std::weak_ptr<MasterContext> master;

    WorkerHandle() = default;

    WorkerHandle(std::shared_ptr<MasterContext> m, raft_id id, pid_t pid, std::string addr)
    : raftID(id)
    , pID(pid)
    , address(std::move(addr))
    , master(m) {}

    auto to_string() const {
        std::stringstream stream;
        auto addr = (pID == -1) ? std::string{"?"} : static_cast<std::string>(address);
        stream << "WorkerHandle"
               << " {raftID=" << raftID
               << ", pID=" << pID
               << ", address=" << addr
               << ", role=" << role
               << ", started=" << std::boolalpha << running
               << "}";
        return stream.str();
    }

    bool operator==(const WorkerHandle& other) const {
        return raftID == other.raftID
            && pID == other.pID
            && address == other.address;
    }
};

struct ClusterHandle {
    std::map<raft_id, std::shared_ptr<WorkerHandle>> workers;
    raft_id                                          nextRaftID = 1;

    auto to_string() const {
        std::stringstream stream;
        for(const auto& [raftID, worker] : workers) {
            stream << "- " << worker->to_string() << "\n";
        }
        auto result = stream.str();
        if(!result.empty()) result.resize(result.size()-1);
        return result;
    }

    auto size() const { return workers.size(); }
    auto begin() const { return workers.begin(); }
    auto end() const { return workers.end(); }
    auto begin() { return workers.begin(); }
    auto end() { return workers.end(); }
};

struct Client {

    tl::engine engine;

    tl::remote_procedure bootstrap;
    tl::remote_procedure start;
    tl::remote_procedure apply;
    tl::remote_procedure get_leader;
    tl::remote_procedure get_fsm_content;
    tl::remote_procedure barrier;
    tl::remote_procedure assign;
    tl::remote_procedure transfer;
    tl::remote_procedure add;
    tl::remote_procedure remove;
    tl::remote_procedure suspend;
    tl::remote_procedure isolate;

    Client(const char* protocol, tl::logger::level logLevel)
    : engine{protocol, THALLIUM_CLIENT_MODE}
    , bootstrap(engine.define("mraft_test_bootstrap"))
    , start(engine.define("mraft_test_start"))
    , apply(engine.define("mraft_test_apply"))
    , get_leader(engine.define("mraft_test_get_leader"))
    , get_fsm_content(engine.define("mraft_test_get_fsm_content"))
    , barrier(engine.define("mraft_test_barrier"))
    , assign(engine.define("mraft_test_assign"))
    , transfer(engine.define("mraft_test_transfer"))
    , add(engine.define("mraft_test_add"))
    , remove(engine.define("mraft_test_remove"))
    , suspend(engine.define("mraft_test_suspend"))
    , isolate(engine.define("mraft_test_isolate")) {
        engine.set_log_level(logLevel);
    }
};

struct MasterContext : public std::enable_shared_from_this<MasterContext> {

    MasterOptions                  masterOptions;
    WorkerOptions                  workerOptions;
    std::shared_ptr<ClusterHandle> cluster;
    std::shared_ptr<Client>        client;

    MasterContext(const MasterOptions& mOptions, const WorkerOptions& wOptions)
    : masterOptions{mOptions}
    , workerOptions{wOptions}
    , cluster{std::make_shared<ClusterHandle>()} {
        reinitializeClient();
    }

    void finalizeClient() { client.reset(); }

    void reinitializeClient() {
        client = std::make_shared<Client>(masterOptions.protocol.c_str(), masterOptions.logLevel);
    }

    void bootstrap() {
        std::vector<mraft::ServerInfo> servers;
        servers.reserve(cluster->size());
        for(const auto& [id, worker] : *cluster)
            servers.push_back({id, worker->address, mraft::Role::VOTER});
        std::vector<tl::async_response> responses;
        responses.reserve(cluster->size());
        for(const auto& [id, worker] : *cluster) {
            tl::provider_handle ph = client->engine.lookup(worker->address);
            responses.push_back(client->bootstrap.on(ph).async(servers));
        }
        for(auto& response : responses)
            response.wait();
        for(auto& [id, worker] : *cluster)
            worker->role = mraft::Role::VOTER;
    }

    void start() {
        std::vector<tl::async_response> responses;
        responses.reserve(cluster->size());
        for(auto& [id, worker] : *cluster) {
            if(worker->running) continue;
            tl::provider_handle ph = client->engine.lookup(worker->address);
            responses.push_back(client->start.on(ph).async());
        }
        for(auto& response : responses)
            response.wait();
        for(auto& [id, worker] : *cluster)
            worker->running = true;
    }

    void waitForLeader() {
        for(const auto& [id, worker] : *cluster) {
            mraft::ServerInfo info;
            while(true) {
                tl::provider_handle ph = client->engine.lookup(worker->address);
                info = client->get_leader.on(ph)();
                if(info.id != 0) break;
                tl::thread::sleep(client->engine, 500);
            }
        }
    }

    struct CatchMeIfYouCan {};

    std::shared_ptr<WorkerHandle> spawnWorker(raft_id id = 0) {

        int pipefd[2];
        if (pipe(pipefd) == -1) {
            perror("Pipe creation failed");
            return nullptr;
        }
        auto raftID = id != 0 ? id : cluster->nextRaftID++;

        // we need to temporarily destroy everything thallium-related
        // so that the child process does not inherit thems
        finalizeClient();

        pid_t pid = fork();
        if (pid < 0) {
            perror("Fork failed");
            if(id == 0) cluster->nextRaftID--;
            reinitializeClient();
            return nullptr;

        } else if (pid == 0) {
            // Child process
            close(pipefd[0]);
            runWorker(pipefd[1], raftID, workerOptions);
            // force a proper stack unwinding all the way back to main
            throw CatchMeIfYouCan{};

        } else {
            // Parent process
            close(pipefd[1]);
            char address[1024];
            ssize_t bytes_read = read(pipefd[0], address, 1024);
            (void)bytes_read;
            close(pipefd[0]);

            reinitializeClient();

            // add the new worker
            cluster->workers[raftID] = std::make_shared<WorkerHandle>(
                shared_from_this(), raftID, pid, address);

            return cluster->workers[raftID];
        }
    }

    #define ENSURE_WORKER_HANDLE_RUNNING(w, ret) do {       \
    if(!w.running) {                                        \
        margo_error(mid, "[master] worker is not running"); \
        return ret;                                         \
    }                                                       \
    } while(0)

    #define CALL_RPC(rpc, w, ...) do {                             \
        tl::provider_handle ph = client->engine.lookup(w.address); \
        int ret = client->rpc.on(ph).timed(1s, ##__VA_ARGS__);       \
        if(ret != 0) throw std::runtime_error(                     \
            std::string{"RPC returned "} + std::to_string(ret));   \
    } while(0)

    #define CATCH_ERRORS(rpc, ret)                                       \
        catch(tl::timeout& ex) {                                         \
            margo_error(mid, "[master] " #rpc " timed out");             \
            return ret;                                                  \
        } catch(const std::exception& ex) {                              \
            margo_error(mid, "[master] " #rpc " failed: %s", ex.what()); \
            return ret;                                                  \
        }

    bool apply(const WorkerHandle& w, const std::string& command) const {
        auto mid = client->engine.get_margo_instance();
        ENSURE_WORKER_HANDLE_RUNNING(w, false);
        try {
            CALL_RPC(apply, w, command);
        } CATCH_ERRORS(apply, false);
        return true;
    }

    bool assign(const WorkerHandle& w, WorkerHandle& target, mraft::Role role) const {
        auto mid = client->engine.get_margo_instance();
        ENSURE_WORKER_HANDLE_RUNNING(w, false);
        try {
            CALL_RPC(assign, w, target.raftID, role);
            target.role = role;
        } CATCH_ERRORS(assign, false);
        return true;
    }

    bool add(const WorkerHandle& w, WorkerHandle& target) const {
        auto mid = client->engine.get_margo_instance();
        ENSURE_WORKER_HANDLE_RUNNING(w, false);
        try {
            CALL_RPC(add, w, target.raftID, target.address);
        } CATCH_ERRORS(add, false);
        return true;
    }

    bool remove(const WorkerHandle& w, WorkerHandle& target) const {
        return remove(w, target.raftID);
    }

    bool remove(const WorkerHandle& w, raft_id target) const {
        auto mid = client->engine.get_margo_instance();
        ENSURE_WORKER_HANDLE_RUNNING(w, false);
        try {
            CALL_RPC(remove, w, target);
        } CATCH_ERRORS(remove, false);
        return true;
    }

    bool start(WorkerHandle& w) const {
        auto mid = client->engine.get_margo_instance();
        if(w.running) {
            margo_error(mid, "[master] worker is alread running");
                return false;
        }
        try {
            CALL_RPC(start, w);
            w.running = true;
        } CATCH_ERRORS(start, false);
        return true;
    }

    bool transfer(const WorkerHandle& w, WorkerHandle& target) const {
        auto mid = client->engine.get_margo_instance();
        ENSURE_WORKER_HANDLE_RUNNING(w, false);
        ENSURE_WORKER_HANDLE_RUNNING(target, false);
        try {
            CALL_RPC(transfer, w, target.raftID);
        } CATCH_ERRORS(transfer, false);
        return true;
    }

    bool barrier(const WorkerHandle& w) const {
        auto mid = client->engine.get_margo_instance();
        ENSURE_WORKER_HANDLE_RUNNING(w, false);
        try {
            CALL_RPC(barrier, w);
        } CATCH_ERRORS(barrier, false);
        return true;
    }

    bool suspend(const WorkerHandle& w, unsigned msec) const {
        auto mid = client->engine.get_margo_instance();
        ENSURE_WORKER_HANDLE_RUNNING(w, false);
        try {
            CALL_RPC(suspend, w, msec);
        } CATCH_ERRORS(suspend, false);
        return true;
    }

    bool isolate(const WorkerHandle& w, bool flag) const {
        auto mid = client->engine.get_margo_instance();
        ENSURE_WORKER_HANDLE_RUNNING(w, false);
        try {
            CALL_RPC(isolate, w, flag);
        } CATCH_ERRORS(isolate, false);
        return true;
    }

    std::optional<std::string> get_fsm_content(const WorkerHandle& w) const {
        auto mid = client->engine.get_margo_instance();
        ENSURE_WORKER_HANDLE_RUNNING(w, std::nullopt);
        try {
            tl::provider_handle ph = client->engine.lookup(w.address);
            std::string content = client->get_fsm_content.on(ph).timed(1s);
            return content;
        } CATCH_ERRORS(get_fsm_content, std::nullopt);
    }

    std::shared_ptr<WorkerHandle> get_leader(const WorkerHandle& w) const {
        auto mid = client->engine.get_margo_instance();
        ENSURE_WORKER_HANDLE_RUNNING(w, nullptr);
        try {
            tl::provider_handle ph = client->engine.lookup(w.address);
            raft_id leaderId = client->get_leader.on(ph).timed(1s);
            if(leaderId == 0 || cluster->workers.count(leaderId) == 0)
                return nullptr;
            else
                return cluster->workers[leaderId];
        } CATCH_ERRORS(get_leader, nullptr);
    }

    bool shutdown(WorkerHandle& w) const {
        static const WorkerHandle none;
        auto mid = client->engine.get_margo_instance();
        auto it = cluster->workers.find(w.raftID);
        if(it == cluster->workers.end()) {
            margo_error(mid, "[master] worker not found in the cluster");
            return false;
        }
        try {
            tl::endpoint addr = client->engine.lookup(w.address);
            client->engine.shutdown_remote_engine(addr);
        } catch(const std::exception& ex) {
            margo_error(mid, "[master] shutdown failed: %s", ex.what());
            return false;
        }
        while(waitpid(w.pID, NULL, WNOHANG) != w.pID) {
            usleep(100);
        }
        cluster->workers.erase(it);
        w = none;
        return true;
    }

    bool kill(WorkerHandle& w) const {
        static const WorkerHandle none;
        auto mid = client->engine.get_margo_instance();
        auto it = cluster->workers.find(w.raftID);
        if(it == cluster->workers.end()) {
            margo_error(mid, "[master] worker not found in the cluster");
            return false;
        }
        ::kill(w.pID, SIGKILL);
        while(waitpid(w.pID, NULL, WNOHANG) != w.pID) {
            usleep(100);
        }
        cluster->workers.erase(it);
        w = none;
        return true;
    }
};

#endif
