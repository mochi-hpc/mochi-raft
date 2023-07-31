/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOCHI_RAFT_HPP
#define MOCHI_RAFT_HPP

#include <mochi-raft.h>
#include <mochi-raft-memory-log.h>
#include <mochi-raft-abt-io-log.h>
#include <functional>
#include <stdexcept>
#include <string>

namespace mraft {

class RaftException : public std::runtime_error {

    int m_code;

  public:
    template <typename... Args>
    explicit RaftException(int code, Args&&... args)
        : std::runtime_error(std::forward<Args>(args)...), m_code(code)
    {}

    int code() const { return m_code; }
};

class Log {

  public:
    virtual ~Log() = default;

    virtual void load(raft_term*             term,
                      raft_id*               id,
                      struct raft_snapshot** snap,
                      raft_index*            start_index,
                      struct raft_entry*     entries[],
                      size_t*                n_entries)
        = 0;

    virtual void bootstrap(const struct raft_configuration* config) = 0;

    virtual void recover(const struct raft_configuration* config) = 0;

    virtual void set_term(raft_term term) = 0;

    virtual void set_vote(raft_id id) = 0;

    virtual void append(const struct raft_entry entries[], unsigned n_entries)
        = 0;

    virtual void truncate(raft_index index) = 0;

    virtual void snapshot_put(unsigned                    trailing,
                              const struct raft_snapshot* snap)
        = 0;

    virtual void snapshot_get(struct raft_io_snapshot_get* req,
                              raft_io_snapshot_get_cb      cb)
        = 0;
};

class StateMachine {

  public:
    virtual ~StateMachine() = default;

    virtual void apply(const struct raft_buffer* buf, void** result) = 0;

    virtual void snapshot(struct raft_buffer* bufs[], unsigned* n_bufs) = 0;

    virtual void restore(struct raft_buffer* buf) = 0;
};

enum class Role : int {
    STANDBY = RAFT_STANDBY,
    VOTER   = RAFT_VOTER,
    SPARE   = RAFT_SPARE
};

struct ServerInfo {
    raft_id     id;
    std::string address;
    Role        role;

    template <class Archive> void serialize(Archive& ar)
    {
        ar& id;
        ar& address;
        ar& role;
    }
};

#define MRAFT_CHECK_RET_AND_RAISE(ret, func)                                   \
    do {                                                                       \
        if (ret != 0) {                                                        \
            throw RaftException((int)ret,                                      \
                                std::string{#func " failed  with error code "} \
                                    + std::to_string(ret));                    \
        }                                                                      \
    } while (0)

class Raft {

  public:
    using tracer_fn = std::function<void(const char*, int, const char*)>;

  private:
    raft_fsm    m_raft_fsm;
    mraft_log   m_raft_log;
    raft_io     m_raft_io;
    raft_tracer m_raft_tracer;
    tracer_fn   m_tracer_fn;
    raft        m_raft;

  public:
    Raft(margo_instance_id mid,
         raft_id           id,
         StateMachine&     fsm,
         Log&              log,
         uint16_t          provider_id = 0,
         ABT_pool          pool        = ABT_POOL_NULL)
    {
        int         ret;
        hg_return_t hret;

        hg_addr_t self_addr = HG_ADDR_NULL;
        hret                = margo_addr_self(mid, &self_addr);
        MRAFT_CHECK_RET_AND_RAISE(hret, margo_addr_self);
        char      self_addr_str[256];
        hg_size_t self_addr_str_size = 256;
        hret = margo_addr_to_string(mid, self_addr_str, &self_addr_str_size,
                                    self_addr);
        MRAFT_CHECK_RET_AND_RAISE(hret, margo_addr_to_string);
        hret = margo_addr_free(mid, self_addr);
        MRAFT_CHECK_RET_AND_RAISE(hret, margo_addr_free);

        m_raft_fsm.data     = static_cast<void*>(&fsm);
        m_raft_fsm.version  = 1;
        m_raft_fsm.apply    = _fsm_apply;
        m_raft_fsm.snapshot = _fsm_snapshot;
        m_raft_fsm.restore  = _fsm_restore;

        m_raft_log.data         = static_cast<void*>(&log);
        m_raft_log.load         = _log_load;
        m_raft_log.bootstrap    = _log_bootstrap;
        m_raft_log.recover      = _log_recover;
        m_raft_log.set_term     = _log_set_term;
        m_raft_log.set_vote     = _log_set_vote;
        m_raft_log.append       = _log_append;
        m_raft_log.truncate     = _log_truncate;
        m_raft_log.snapshot_put = _log_snapshot_put;
        m_raft_log.snapshot_get = _log_snapshot_get;

        m_raft_tracer.impl    = static_cast<void*>(&m_tracer_fn);
        m_raft_tracer.enabled = false;
        m_raft_tracer.emit    = _tracer_emit;

        mraft_io_init_args io_init_args = {mid, pool, provider_id, &m_raft_log};
        ret = mraft_io_init(&io_init_args, &m_raft_io);
        MRAFT_CHECK_RET_AND_RAISE(ret, mraft_io_init);
        ret = mraft_init(&m_raft, &m_raft_io, &m_raft_fsm, id, self_addr_str);
        MRAFT_CHECK_RET_AND_RAISE(ret, mraft_init);

        raft_set_snapshot_threshold(&m_raft, 5U);
        // raft_set_snapshot_trailing(&m_raft, 1U);
        m_raft.tracer = &m_raft_tracer;
    }

    ~Raft()
    {
        mraft_close(&m_raft);
        mraft_io_finalize(&m_raft_io);
    }

    Raft(const Raft&)            = delete;
    Raft(Raft&&)                 = delete;
    Raft& operator=(const Raft&) = delete;
    Raft& operator=(Raft&&)      = delete;

    template <typename ServerInfoContainer>
    void bootstrap(const ServerInfoContainer& serverList)
    {
        raft_configuration config;
        raft_configuration_init(&config);
        int ret;
        for (const auto& serverInfo : serverList) {
            ret = raft_configuration_add(&config, serverInfo.id,
                                         serverInfo.address.c_str(),
                                         static_cast<int>(serverInfo.role));
            MRAFT_CHECK_RET_AND_RAISE(ret, raft_configuration_add);
        }
        ret = mraft_bootstrap(&m_raft, &config);
        MRAFT_CHECK_RET_AND_RAISE(ret, mraft_bootstrap);
        raft_configuration_close(&config);
    }

    template <typename ServerInfoContainer>
    void recover(const ServerInfoContainer& serverList)
    {
        raft_configuration config;
        raft_configuration_init(&config);
        int ret;
        for (const auto& serverInfo : serverList) {
            ret = raft_configuration_add(&config, serverInfo.id,
                                         serverInfo.address.c_str(),
                                         static_cast<int>(serverInfo.role));
            MRAFT_CHECK_RET_AND_RAISE(ret, raft_configuration_add);
        }
        ret = mraft_recover(&m_raft, &config);
        MRAFT_CHECK_RET_AND_RAISE(ret, mraft_recover);
        raft_configuration_close(&config);
    }

    void start()
    {
        int ret = mraft_start(&m_raft);
        MRAFT_CHECK_RET_AND_RAISE(ret, mraft_start);
    }

    void apply(raft_buffer bufs[], const unsigned n)
    {
        int ret = mraft_apply(&m_raft, bufs, n);
        MRAFT_CHECK_RET_AND_RAISE(ret, mraft_apply);
    }

    void barrier()
    {
        int ret = mraft_barrier(&m_raft);
        MRAFT_CHECK_RET_AND_RAISE(ret, mraft_barrier);
    }

    void add(raft_id id, const char* address)
    {
        int ret = mraft_add(&m_raft, id, address);
        MRAFT_CHECK_RET_AND_RAISE(ret, mraft_add);
    }

    void assign(raft_id id, Role role)
    {
        int ret = mraft_assign(&m_raft, id, (int)role);
        MRAFT_CHECK_RET_AND_RAISE(ret, mraft_assign);
    }

    void remove(raft_id id)
    {
        int ret = mraft_remove(&m_raft, id);
        MRAFT_CHECK_RET_AND_RAISE(ret, mraft_remove);
    }

    void transfer(raft_id id)
    {
        int ret = mraft_transfer(&m_raft, id);
        MRAFT_CHECK_RET_AND_RAISE(ret, mraft_transfer);
    }

    raft_id get_raft_id(const char* address) const
    {
        raft_id id = 0;
        int ret = mraft_get_raft_id(const_cast<raft*>(&m_raft), address, &id);
        MRAFT_CHECK_RET_AND_RAISE(ret, mraft_get_raft_id);
        return id;
    }

    ServerInfo get_leader() const
    {
        raft_id     leader_id;
        const char* leader_addr;
        raft_leader(const_cast<raft*>(&m_raft), &leader_id, &leader_addr);
        std::string leader_addr_str((!leader_addr) ? "" : leader_addr);
        leader_addr_str.resize(256, '\0');
        ServerInfo leader = {.id = leader_id, .address = leader_addr_str};
        return leader;
    }

    void enable_tracer(bool enable) { m_raft_tracer.enabled = enable; }

    void set_tracer(tracer_fn f) { m_tracer_fn = std::move(f); }

    void set_rpc_timeout(double timeout_ms)
    {
        mraft_io_set_rpc_timeout(&m_raft_io, timeout_ms);
    }

  private:
#define MRAFT_WRAP_CPP_LOG_CALL(func, ...)          \
    do {                                            \
        auto theLog = static_cast<Log*>(log->data); \
        try {                                       \
            theLog->func(__VA_ARGS__);              \
        } catch (const RaftException& ex) {         \
            return ex.code();                       \
        }                                           \
        return 0;                                   \
    } while (0)

    static int _log_load(struct mraft_log*      log,
                         raft_term*             term,
                         raft_id*               id,
                         struct raft_snapshot** snap,
                         raft_index*            start_index,
                         struct raft_entry**    entries,
                         size_t*                n_entries)
    {
        MRAFT_WRAP_CPP_LOG_CALL(load, term, id, snap, start_index, entries,
                                n_entries);
    }

    static int _log_bootstrap(struct mraft_log*                log,
                              const struct raft_configuration* config)
    {
        MRAFT_WRAP_CPP_LOG_CALL(bootstrap, config);
    }

    static int _log_recover(struct mraft_log*                log,
                            const struct raft_configuration* config)
    {
        MRAFT_WRAP_CPP_LOG_CALL(recover, config);
    }

    static int _log_set_term(struct mraft_log* log, raft_term term)
    {
        MRAFT_WRAP_CPP_LOG_CALL(set_term, term);
    }

    static int _log_set_vote(struct mraft_log* log, raft_id id)
    {
        MRAFT_WRAP_CPP_LOG_CALL(set_vote, id);
    }

    static int _log_append(struct mraft_log*       log,
                           const struct raft_entry entries[],
                           unsigned                n_entries)
    {
        MRAFT_WRAP_CPP_LOG_CALL(append, entries, n_entries);
    }

    static int _log_truncate(struct mraft_log* log, raft_index index)
    {
        MRAFT_WRAP_CPP_LOG_CALL(truncate, index);
    }

    static int _log_snapshot_put(struct mraft_log*           log,
                                 unsigned                    trailing,
                                 const struct raft_snapshot* snap)
    {
        MRAFT_WRAP_CPP_LOG_CALL(snapshot_put, trailing, snap);
    }

    static int _log_snapshot_get(struct mraft_log*            log,
                                 struct raft_io_snapshot_get* req,
                                 raft_io_snapshot_get_cb      cb)
    {
        MRAFT_WRAP_CPP_LOG_CALL(snapshot_get, req, cb);
    }

#define MRAFT_WRAP_CPP_FSM_CALL(func, ...)                   \
    do {                                                     \
        auto theFSM = static_cast<StateMachine*>(fsm->data); \
        try {                                                \
            theFSM->func(__VA_ARGS__);                       \
        } catch (const RaftException& ex) {                  \
            return ex.code();                                \
        }                                                    \
        return 0;                                            \
    } while (0)

    static int _fsm_apply(struct raft_fsm*          fsm,
                          const struct raft_buffer* buf,
                          void**                    result)
    {
        MRAFT_WRAP_CPP_FSM_CALL(apply, buf, result);
    }

    static int _fsm_snapshot(struct raft_fsm*    fsm,
                             struct raft_buffer* bufs[],
                             unsigned*           n_bufs)
    {
        MRAFT_WRAP_CPP_FSM_CALL(snapshot, bufs, n_bufs);
    }

    static int _fsm_restore(struct raft_fsm* fsm, struct raft_buffer* buf)
    {
        MRAFT_WRAP_CPP_FSM_CALL(restore, buf);
    }

    static void _tracer_emit(struct raft_tracer* tracer,
                             const char*         file,
                             int                 line,
                             const char*         message)
    {
        auto emit = static_cast<tracer_fn*>(tracer->impl);
        if (emit && *emit) (*emit)(file, line, message);
    }
};

class MemoryLog : public Log {

  private:
    mraft_log m_log;

#define MRAFT_WRAP_C_LOG_CALL(func, ...)                                       \
    do {                                                                       \
        int ret = (m_log.func)(&m_log, __VA_ARGS__);                           \
        if (ret != 0) throw RaftException(ret, "MemoryLog::" #func " failed"); \
    } while (0)

  public:
    MemoryLog() { mraft_memory_log_init(&m_log); }

    ~MemoryLog() { mraft_memory_log_finalize(&m_log); }

    void load(raft_term*             term,
              raft_id*               id,
              struct raft_snapshot** snap,
              raft_index*            start_index,
              struct raft_entry*     entries[],
              size_t*                n_entries) override
    {
        MRAFT_WRAP_C_LOG_CALL(load, term, id, snap, start_index, entries,
                              n_entries);
    }

    void bootstrap(const struct raft_configuration* config) override
    {
        MRAFT_WRAP_C_LOG_CALL(bootstrap, config);
    }

    void recover(const struct raft_configuration* config) override
    {
        MRAFT_WRAP_C_LOG_CALL(recover, config);
    }

    void set_term(raft_term term) override
    {
        MRAFT_WRAP_C_LOG_CALL(set_term, term);
    }

    void set_vote(raft_id id) override { MRAFT_WRAP_C_LOG_CALL(set_vote, id); }

    void append(const struct raft_entry entries[], unsigned n_entries) override
    {
        MRAFT_WRAP_C_LOG_CALL(append, entries, n_entries);
    }

    void truncate(raft_index index) override
    {
        MRAFT_WRAP_C_LOG_CALL(truncate, index);
    }

    void snapshot_put(unsigned                    trailing,
                      const struct raft_snapshot* snap) override
    {
        MRAFT_WRAP_C_LOG_CALL(snapshot_put, trailing, snap);
    }

    void snapshot_get(struct raft_io_snapshot_get* req,
                      raft_io_snapshot_get_cb      cb) override
    {
        MRAFT_WRAP_C_LOG_CALL(snapshot_get, req, cb);
    }
};

class AbtIoLog : public Log {

  private:
    mraft_log m_log;

  public:
    AbtIoLog(raft_id raftId) { mraft_abt_io_log_init(&m_log, raftId); }

    ~AbtIoLog() { mraft_abt_io_log_finalize(&m_log); }

    void load(raft_term*             term,
              raft_id*               id,
              struct raft_snapshot** snap,
              raft_index*            start_index,
              struct raft_entry*     entries[],
              size_t*                n_entries) override
    {
        MRAFT_WRAP_C_LOG_CALL(load, term, id, snap, start_index, entries,
                              n_entries);
    }

    void bootstrap(const struct raft_configuration* config) override
    {
        MRAFT_WRAP_C_LOG_CALL(bootstrap, config);
    }

    void recover(const struct raft_configuration* config) override
    {
        MRAFT_WRAP_C_LOG_CALL(recover, config);
    }

    void set_term(raft_term term) override
    {
        MRAFT_WRAP_C_LOG_CALL(set_term, term);
    }

    void set_vote(raft_id id) override { MRAFT_WRAP_C_LOG_CALL(set_vote, id); }

    void append(const struct raft_entry entries[], unsigned n_entries) override
    {
        MRAFT_WRAP_C_LOG_CALL(append, entries, n_entries);
    }

    void truncate(raft_index index) override
    {
        MRAFT_WRAP_C_LOG_CALL(truncate, index);
    }

    void snapshot_put(unsigned                    trailing,
                      const struct raft_snapshot* snap) override
    {
        MRAFT_WRAP_C_LOG_CALL(snapshot_put, trailing, snap);
    }

    void snapshot_get(struct raft_io_snapshot_get* req,
                      raft_io_snapshot_get_cb      cb) override
    {
        MRAFT_WRAP_C_LOG_CALL(snapshot_get, req, cb);
    }
};
} // namespace mraft

#undef MRAFT_WRAP_CPP_LOG_CALL
#undef MRAFT_WRAP_C_LOG_CALL
#undef MRAFT_CHECK_RET_AND_RAISE

#endif