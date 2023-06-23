/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mochi-raft.h"
#include "mraft-io.h"
#include "mraft-rpc.h"
#include <stdlib.h>
#include <margo.h>

static DECLARE_MARGO_RPC_HANDLER(mraft_craft_rpc_ult)
static void mraft_craft_rpc_ult(hg_handle_t h);

static DECLARE_MARGO_RPC_HANDLER(mraft_apply_rpc_ult)
static void mraft_apply_rpc_ult(hg_handle_t h);

static DECLARE_MARGO_RPC_HANDLER(mraft_barrier_rpc_ult)
static void mraft_barrier_rpc_ult(hg_handle_t h);

static DECLARE_MARGO_RPC_HANDLER(mraft_add_rpc_ult)
static void mraft_add_rpc_ult(hg_handle_t h);

static DECLARE_MARGO_RPC_HANDLER(mraft_assign_rpc_ult)
static void mraft_assign_rpc_ult(hg_handle_t h);

static DECLARE_MARGO_RPC_HANDLER(mraft_remove_rpc_ult)
static void mraft_remove_rpc_ult(hg_handle_t h);

static DECLARE_MARGO_RPC_HANDLER(mraft_transfer_rpc_ult)
static void mraft_transfer_rpc_ult(hg_handle_t h);

static DECLARE_MARGO_RPC_HANDLER(mraft_get_raft_id_rpc_ult)
static void mraft_get_raft_id_rpc_ult(hg_handle_t h);

static inline void free_server_list(struct raft_io *io)
{
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;
    for(unsigned i=0; i < impl->servers.count; i++) {
        free(impl->servers.str_addr[i]);
        margo_addr_free(impl->mid, impl->servers.hg_addr[i]);
    }
    free(impl->servers.ids);
    free(impl->servers.str_addr);
    free(impl->servers.hg_addr);
    impl->servers.count    = 0;
    impl->servers.ids      = NULL;
    impl->servers.str_addr = NULL;
    impl->servers.hg_addr  = NULL;
}

static inline int populate_server_list(struct raft_io *io, const struct raft_configuration *conf)
{
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;

    impl->servers.count    = conf->n;
    impl->servers.ids      = calloc(conf->n, sizeof(*impl->servers.ids));
    impl->servers.str_addr = calloc(conf->n, sizeof(*impl->servers.str_addr));
    impl->servers.hg_addr  = calloc(conf->n, sizeof(*impl->servers.hg_addr));

    for(unsigned i=0; i < conf->n; i++) {
        impl->servers.ids[i]      = conf->servers[i].id;
        impl->servers.str_addr[i] = strdup(conf->servers[i].address);
        hg_return_t ret = margo_addr_lookup(impl->mid, conf->servers[i].address, &impl->servers.hg_addr[i]);
        if(ret != HG_SUCCESS) goto error;
    }

    return MRAFT_SUCCESS;

error:
    free_server_list(io);
    return RAFT_CANTBOOTSTRAP;
}

int mraft_io_impl_init(struct raft_io *io, raft_id id, const char *address)
{
    (void)id;
    (void)address;
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;

    margo_trace(impl->mid, "[mraft] Initializing mraft instance with id %lu", id);

    hg_bool_t flag;
    hg_id_t rpc_id;
    margo_provider_registered_name(impl->mid, "mraft_craft", impl->provider_id, &rpc_id, &flag);
    if(flag == HG_TRUE) {
        margo_error(impl->mid,
            "[mraft] An instance of mraft is already registered with provider id %u",
            impl->provider_id);
        return MRAFT_ERR_ID_USED;
    }

    id = MARGO_REGISTER_PROVIDER(impl->mid, "mraft_craft",
            mraft_craft_in_t, void, mraft_craft_rpc_ult, impl->provider_id, impl->pool);
    margo_registered_disable_response(impl->mid, id, HG_TRUE);
    margo_register_data(impl->mid, id, (void*)io, NULL);
    impl->craft_rpc_id = id;

    id = MARGO_REGISTER_PROVIDER(impl->mid, "mraft_apply",
            mraft_apply_in_t, mraft_apply_out_t, mraft_apply_rpc_ult, impl->provider_id, impl->pool);
    margo_register_data(impl->mid, id, (void*)io, NULL);
    impl->forward.apply_rpc_id = id;

    id = MARGO_REGISTER_PROVIDER(impl->mid, "mraft_barrier",
            void, mraft_barrier_out_t, mraft_barrier_rpc_ult, impl->provider_id, impl->pool);
    margo_register_data(impl->mid, id, (void*)io, NULL);
    impl->forward.barrier_rpc_id = id;

    id = MARGO_REGISTER_PROVIDER(impl->mid, "mraft_add",
            mraft_add_in_t, mraft_add_out_t, mraft_add_rpc_ult, impl->provider_id, impl->pool);
    margo_register_data(impl->mid, id, (void*)io, NULL);
    impl->forward.add_rpc_id = id;

    id = MARGO_REGISTER_PROVIDER(impl->mid, "mraft_assign",
            mraft_assign_in_t, mraft_assign_out_t, mraft_assign_rpc_ult, impl->provider_id, impl->pool);
    margo_register_data(impl->mid, id, (void*)io, NULL);
    impl->forward.assign_rpc_id = id;

    id = MARGO_REGISTER_PROVIDER(impl->mid, "mraft_remove",
            mraft_remove_in_t, mraft_remove_out_t, mraft_remove_rpc_ult, impl->provider_id, impl->pool);
    margo_register_data(impl->mid, id, (void*)io, NULL);
    impl->forward.remove_rpc_id = id;

    id = MARGO_REGISTER_PROVIDER(impl->mid, "mraft_transfer",
            mraft_transfer_in_t, mraft_transfer_out_t, mraft_transfer_rpc_ult, impl->provider_id, impl->pool);
    margo_register_data(impl->mid, id, (void*)io, NULL);
    impl->forward.transfer_rpc_id = id;

    id = MARGO_REGISTER_PROVIDER(impl->mid, "mraft_get_raft_id",
        void, mraft_get_raft_id_out_t, mraft_get_raft_id_rpc_ult, impl->provider_id, impl->pool);
    margo_register_data(impl->mid, id, (void*)io, NULL);
    impl->forward.get_raft_id_rpc_id = id;

    return MRAFT_SUCCESS;
}

void mraft_io_impl_close(struct raft_io *io, raft_io_close_cb cb)
{
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;

    margo_trace(impl->mid, "[mraft] Closing mraft instance");

    impl->recv_cb = NULL;
    impl->tick_cb = NULL;
    if(impl->tick_ult && impl->tick_ult != ABT_THREAD_NULL) {
        ABT_thread_join(impl->tick_ult);
        ABT_thread_free(&impl->tick_ult);
        impl->tick_ult = ABT_THREAD_NULL;
    }
    margo_deregister(impl->mid, impl->craft_rpc_id);
    margo_deregister(impl->mid, impl->forward.apply_rpc_id);
    free_server_list(io);
    if(cb) cb(io);
}

int mraft_io_impl_load(struct raft_io *io,
                       raft_term *term,
                       raft_id *voted_for,
                       struct raft_snapshot **snapshot,
                       raft_index *start_index,
                       struct raft_entry *entries[],
                       size_t *n_entries)
{
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;
    margo_trace(impl->mid, "[mraft] Loading state from storage");
    if(!impl->log->load) {
        margo_error(impl->mid, "[mraft] load function in mraft_log structure not implemented");
        return RAFT_NOTFOUND;
    }
    return (impl->log->load)(impl->log, term, voted_for, snapshot, start_index, entries, n_entries);
}

static void ticker_ult(void* args)
{
    struct raft_io* io = (struct raft_io*)args;
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;
    margo_trace(impl->mid, "[mraft] Starting ticker ULT");
    raft_io_tick_cb tick = impl->tick_cb;
    while(tick) {
#ifdef MRAFT_ENABLE_TESTS
        if(!impl->simulate_dead)
#endif
        tick(io);
        margo_thread_sleep(impl->mid, impl->tick_msec);
        tick = impl->tick_cb;
    }
}

int mraft_io_impl_start(struct raft_io *io,
                        unsigned msecs,
                        raft_io_tick_cb tick,
                        raft_io_recv_cb recv)
{
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;
    margo_trace(impl->mid, "[mraft] Starting raft");
    impl->recv_cb           = recv;
    impl->tick_cb           = tick;
    impl->tick_msec         = msecs;
    int ret = ABT_thread_create(impl->pool, ticker_ult, io, ABT_THREAD_ATTR_NULL, &impl->tick_ult);
    if(ret != ABT_SUCCESS) return MRAFT_ERR_FROM_ARGOBOTS;
    return MRAFT_SUCCESS;
}

int mraft_io_impl_bootstrap(struct raft_io *io, const struct raft_configuration *conf)
{
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;
    margo_trace(impl->mid, "[mraft] Boostrapping raft cluster");
    if(!impl->log->bootstrap) {
        margo_error(impl->mid, "[mraft] bootstrap function in mraft_log structure not implemented");
        return RAFT_NOTFOUND;
    }
    if(impl->servers.count != 0) return RAFT_CANTBOOTSTRAP;
    int ret = populate_server_list(io, conf);
    if(ret != 0) {
        margo_error(impl->mid, "[mraft] Could not populate server list from configuration");
        return ret;
    }
    return (impl->log->bootstrap)(impl->log, conf);
}

int mraft_io_impl_recover(struct raft_io *io, const struct raft_configuration *conf)
{
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;
    margo_trace(impl->mid, "[mraft] Recovering raft cluster");
    if(!impl->log->recover) {
        margo_error(impl->mid, "[mraft] recover function in mraft_log structure not implemented");
        return RAFT_NOTFOUND;
    }
    free_server_list(io);
    int ret = populate_server_list(io, conf);
    if(ret != 0) {
        margo_error(impl->mid, "[mraft] Could not populate server list from configuration");
        return ret;
    }
    return (impl->log->recover)(impl->log, conf);
}

int mraft_io_impl_set_term(struct raft_io *io, raft_term term)
{
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;
    margo_trace(impl->mid, "[mraft] Setting term to %lu", term);
    if(!impl->log->set_term) {
        margo_error(impl->mid, "[mraft] set_term function in mraft_log structure not implemented");
        return RAFT_NOTFOUND;
    }
    return (impl->log->set_term)(impl->log, term);
}

int mraft_io_impl_set_vote(struct raft_io *io, raft_id server_id)
{
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;
    margo_trace(impl->mid, "[mraft] Setting vote to %lu", server_id);
    if(!impl->log->set_vote) {
        margo_error(impl->mid, "[mraft] set_vote function in mraft_log structure not implemented");
        return RAFT_NOTFOUND;
    }
    return (impl->log->set_vote)(impl->log, server_id);
}

int mraft_io_impl_send(struct raft_io *io,
                       struct raft_io_send *req,
                       const struct raft_message *message,
                       raft_io_send_cb cb)
{
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;
    hg_handle_t     h;
    hg_return_t     hret;
    hg_addr_t       addr = HG_ADDR_NULL;

#ifdef MRAFT_ENABLE_TESTS
    if(impl->simulate_dead)
        return MRAFT_SUCCESS;
#endif

    margo_trace(impl->mid,
        "[mraft] Sending message of type %d to server id %lu (%s)",
        message->type, message->server_id, message->server_address);

    req->cb = cb;

    hret = margo_addr_lookup(impl->mid, message->server_address, &addr);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could not resolve address %s: margo_addr_lookup returned %d",
            message->server_address, hret);
        if(cb) cb(req, RAFT_CANCELED);
        return MRAFT_ERR_FROM_MERCURY;
    }

    hret = margo_create(impl->mid, addr, impl->craft_rpc_id, &h);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could not create handle: margo_create returned %d", hret);
        if(cb) cb(req, RAFT_CANCELED);
        return MRAFT_ERR_FROM_MERCURY;
    }

    hret = margo_provider_forward(impl->provider_id, h, (void*)message);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could forward handle: margo_provider_forward returned %d", hret);
        if(cb) cb(req, RAFT_CANCELED);
        return MRAFT_ERR_FROM_MERCURY;
    }

    margo_destroy(h);

    if(cb) cb(req, 0);

    return MRAFT_SUCCESS;
}

struct append_args {
    struct raft_io *io;
    struct raft_io_append *req;
    const struct raft_entry* entries;
    unsigned n;
};

static void append_ult(void* x)
{
    struct append_args* args = (struct append_args*)x;
    struct mraft_io_impl* impl = (struct mraft_io_impl*)args->io->impl;
    int status = impl->log->append ?
        (impl->log->append)(impl->log, args->entries, args->n) : RAFT_IOERR;
    if(args->req->cb) (args->req->cb)(args->req, status);
    free(args);
}

int mraft_io_impl_append(struct raft_io *io,
                      struct raft_io_append *req,
                      const struct raft_entry entries[],
                      unsigned n,
                      raft_io_append_cb cb)
{
    struct mraft_io_impl* impl  = (struct mraft_io_impl*)io->impl;
    margo_trace(impl->mid, "[mraft] Appending %u entries to the log", n);
    struct append_args* args = (struct append_args*)calloc(1, sizeof(*args));
    args->io                 = io;
    args->req                = req;
    args->entries            = entries;
    args->n                  = n;
    req->cb                  = cb;
    return ABT_thread_create(impl->pool, append_ult, args, ABT_THREAD_ATTR_NULL, NULL);
}

struct truncate_args {
    struct raft_io *io;
    raft_index index;
};

static void truncate_ult(void* x)
{
    struct truncate_args* args = (struct truncate_args*)x;
    struct mraft_io_impl* impl = (struct mraft_io_impl*)args->io->impl;
    if(impl->log->truncate)
        (impl->log->truncate)(impl->log, args->index);
    free(args);
}

int mraft_io_impl_truncate(struct raft_io *io, raft_index index)
{
    struct mraft_io_impl* impl    = (struct mraft_io_impl*)io->impl;
    margo_trace(impl->mid, "[mraft] Truncating the log at index %lu", index);
    struct truncate_args* args = (struct truncate_args*)calloc(1, sizeof(*args));
    args->io                   = io;
    args->index                = index;
    return ABT_thread_create(impl->pool, truncate_ult, args, ABT_THREAD_ATTR_NULL, NULL);
}

struct snapshot_put_args {
    struct raft_io*              io;
    unsigned                     trailing;
    struct raft_io_snapshot_put* req;
    const struct raft_snapshot*  snapshot;
};

static void snapshot_put_ult(void* x)
{
    struct snapshot_put_args* args = (struct snapshot_put_args*)x;
    struct mraft_io_impl* impl = (struct mraft_io_impl*)args->io->impl;
    int status = impl->log->snapshot_put ?
        (impl->log->snapshot_put)(impl->log, args->trailing, args->snapshot) : RAFT_IOERR;
    if(args->req->cb) (args->req->cb)(args->req, status);
    free(args);
}

int mraft_io_impl_snapshot_put(struct raft_io *io,
                            unsigned trailing,
                            struct raft_io_snapshot_put *req,
                            const struct raft_snapshot *snapshot,
                            raft_io_snapshot_put_cb cb)
{
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;
    margo_trace(impl->mid, "[mraft] Creating a snapshot");
    req->cb = cb;
    struct snapshot_put_args* args = (struct snapshot_put_args*)calloc(1, sizeof(*args));
    args->io       = io;
    args->trailing = trailing;
    args->req      = req;
    args->snapshot = snapshot;
    return ABT_thread_create(impl->pool, snapshot_put_ult, args, ABT_THREAD_ATTR_NULL, NULL);
}

struct snapshot_get_args {
    struct raft_io*              io;
    struct raft_io_snapshot_get* req;
};

static void snapshot_get_ult(void* x)
{
    struct snapshot_get_args* args = (struct snapshot_get_args*)x;
    struct mraft_io_impl* impl = (struct mraft_io_impl*)args->io->impl;
    if(impl->log->snapshot_get)
        (impl->log->snapshot_get)(impl->log, args->req, args->req->cb);
    free(args);
}

int mraft_io_impl_snapshot_get(struct raft_io *io,
                            struct raft_io_snapshot_get *req,
                            raft_io_snapshot_get_cb cb)
{
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;
    margo_trace(impl->mid, "[mraft] Retrieving a snapshot");
    req->cb = cb;
    struct snapshot_get_args* args = (struct snapshot_get_args*)calloc(1, sizeof(*args));
    args->io  = io;
    args->req = req;
    return ABT_thread_create(impl->pool, snapshot_get_ult, args, ABT_THREAD_ATTR_NULL, NULL);
}

raft_time mraft_io_impl_time(struct raft_io *io)
{
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;
    raft_time t = 1000*ABT_get_wtime();
    return t;
}

int mraft_io_impl_random(struct raft_io *io, int min, int max)
{
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;
    int r = min + pcg32_boundedrand_r(&impl->rng_state, max-min);
    return r;
}

static void async_work_ult(void* args)
{
    struct raft_io_async_work *req = (struct raft_io_async_work*)args;
    int status = req->work(req);
    if(req->cb) req->cb(req, status);
}

int mraft_io_impl_async_work(struct raft_io *io,
                          struct raft_io_async_work *req,
                          raft_io_async_work_cb cb)
{
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;
    req->cb = cb;
    return ABT_thread_create(impl->pool, async_work_ult, req, ABT_THREAD_ATTR_NULL, NULL);
}

static DEFINE_MARGO_RPC_HANDLER(mraft_craft_rpc_ult)
static void mraft_craft_rpc_ult(hg_handle_t h)
{
    hg_return_t hret;
    struct raft_message msg = {0};

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    const struct hg_info* info = margo_get_info(h);
    struct raft_io* io = (struct raft_io*)margo_registered_data(mid, info->id);
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;

    hret = margo_get_input(h, &msg);
    if(hret != HG_SUCCESS) {
        margo_error(mid, "[mraft] Could not deserialize output (mercury error %d)", hret);
        margo_destroy(h);
        return;
    }

    for(unsigned i=0; i < impl->servers.count; i++) {
        if(margo_addr_cmp(mid, info->addr, impl->servers.hg_addr[i])) {
            msg.server_address = impl->servers.str_addr[i];
            msg.server_id      = impl->servers.ids[i];
            break;
        }
    }
    if(!msg.server_address) {
        margo_warning(mid, "[mraft] Ignoring RPC received from an unknown server");
        goto finish;
    }

    margo_trace(mid, "[mraft] Received message of type %d from server id %lu (%s)",
                msg.type, msg.server_id, msg.server_address);
    raft_io_recv_cb recv_cb = impl->recv_cb;
#ifdef MRAFT_ENABLE_TESTS
    if(!impl->simulate_dead)
#endif
    if(recv_cb) {
        recv_cb(io, &msg);
        // this change in the message is necessary because the callback
        // has already freed some fields but did not reset them
        switch(msg.type) {
        case RAFT_IO_APPEND_ENTRIES:
            msg.append_entries.entries = NULL;
            msg.append_entries.n_entries = 0;
            break;
        case RAFT_IO_APPEND_ENTRIES_RESULT:
            break;
        case RAFT_IO_REQUEST_VOTE:
            break;
        case RAFT_IO_REQUEST_VOTE_RESULT:
            break;
        case RAFT_IO_INSTALL_SNAPSHOT:
            msg.install_snapshot.conf.n = 0;
            msg.install_snapshot.conf.servers = NULL;
            msg.install_snapshot.data.base = NULL;
            msg.install_snapshot.data.len = 0;
            break;
        case RAFT_IO_TIMEOUT_NOW:
            break;
      }
    }

finish:
    margo_free_input(h, &msg);
    margo_destroy(h);
}

static DEFINE_MARGO_RPC_HANDLER(mraft_apply_rpc_ult)
static void mraft_apply_rpc_ult(hg_handle_t h)
{
    hg_return_t hret;

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    const struct hg_info* info = margo_get_info(h);
    struct raft_io* io = (struct raft_io*)margo_registered_data(mid, info->id);
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;

    struct apply_in in = {0};
    mraft_apply_out_t out = {0};

    hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(mid, "[mraft] Could not deserialize output (mercury error %d)", hret);
        out.ret = RAFT_INVALID;
        goto finish;
    }

    // from looking at the craft code, it sets io->data to the parent raft
    struct raft* raft = (struct raft*)io->data;

    margo_trace(impl->mid, "[mraft] Received forwarded apply request");

    out.ret = mraft_apply(raft, in.bufs, in.n_bufs);

finish:
    margo_respond(h, &out);
    margo_free_input(h, &in);
    margo_destroy(h);
}

static DEFINE_MARGO_RPC_HANDLER(mraft_barrier_rpc_ult)
static void mraft_barrier_rpc_ult(hg_handle_t h)
{
    hg_return_t hret;

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    const struct hg_info* info = margo_get_info(h);
    struct raft_io* io = (struct raft_io*)margo_registered_data(mid, info->id);
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;

    mraft_barrier_out_t out = {0};

    struct raft* raft = (struct raft*)io->data;

    margo_trace(impl->mid, "[mraft] Received forwarded barrier request");

    out.ret = mraft_barrier(raft);

finish:
    margo_respond(h, &out);
    margo_destroy(h);
}

static DEFINE_MARGO_RPC_HANDLER(mraft_add_rpc_ult)
static void mraft_add_rpc_ult(hg_handle_t h)
{
    hg_return_t hret;

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    const struct hg_info* info = margo_get_info(h);
    struct raft_io* io = (struct raft_io*)margo_registered_data(mid, info->id);
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;

    mraft_add_in_t in = {0};
    mraft_add_out_t out = {0};

    hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(mid, "[mraft] Could not deserialize output (mercury error %d)", hret);
        out.ret = RAFT_INVALID;
        goto finish;
    }

    struct raft* raft = (struct raft*)io->data;

    margo_trace(impl->mid, "[mraft] Received forwarded add request");

    out.ret = mraft_add(raft, in.id, in.address);

finish:
    margo_respond(h, &out);
    margo_free_input(h, &in);
    margo_destroy(h);
}

static DEFINE_MARGO_RPC_HANDLER(mraft_assign_rpc_ult)
static void mraft_assign_rpc_ult(hg_handle_t h)
{
    hg_return_t hret;

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    const struct hg_info* info = margo_get_info(h);
    struct raft_io* io = (struct raft_io*)margo_registered_data(mid, info->id);
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;

    mraft_assign_in_t in = {0};
    mraft_assign_out_t out = {0};

    hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(mid, "[mraft] Could not deserialize output (mercury error %d)", hret);
        out.ret = RAFT_INVALID;
        goto finish;
    }

    struct raft* raft = (struct raft*)io->data;

    margo_trace(impl->mid, "[mraft] Received forwarded assign request");

    out.ret = mraft_assign(raft, in.id, in.role);

finish:
    margo_respond(h, &out);
    margo_free_input(h, &in);
    margo_destroy(h);
}

static DEFINE_MARGO_RPC_HANDLER(mraft_remove_rpc_ult)
static void mraft_remove_rpc_ult(hg_handle_t h)
{
    hg_return_t hret;

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    const struct hg_info* info = margo_get_info(h);
    struct raft_io* io = (struct raft_io*)margo_registered_data(mid, info->id);
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;

    mraft_remove_in_t in = {0};
    mraft_remove_out_t out = {0};

    hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(mid, "[mraft] Could not deserialize output (mercury error %d)", hret);
        out.ret = RAFT_INVALID;
        goto finish;
    }

    struct raft* raft = (struct raft*)io->data;

    margo_trace(impl->mid, "[mraft] Received forwarded remove request");

    out.ret = mraft_remove(raft, in.id);

finish:
    margo_respond(h, &out);
    margo_free_input(h, &in);
    margo_destroy(h);
}

static DEFINE_MARGO_RPC_HANDLER(mraft_transfer_rpc_ult)
static void mraft_transfer_rpc_ult(hg_handle_t h)
{
    hg_return_t hret;

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    const struct hg_info* info = margo_get_info(h);
    struct raft_io* io = (struct raft_io*)margo_registered_data(mid, info->id);
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;

    mraft_transfer_in_t in = {0};
    mraft_transfer_out_t out = {0};

    hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(mid, "[mraft] Could not deserialize output (mercury error %d)", hret);
        out.ret = RAFT_INVALID;
        goto finish;
    }

    struct raft* raft = (struct raft*)io->data;

    margo_trace(impl->mid, "[mraft] Received forwarded transfer request");

    out.ret = mraft_transfer(raft, in.id);

finish:
    margo_respond(h, &out);
    margo_free_input(h, &in);
    margo_destroy(h);
}

static DEFINE_MARGO_RPC_HANDLER(mraft_get_raft_id_rpc_ult)
static void mraft_get_raft_id_rpc_ult(hg_handle_t h)
{
    hg_return_t hret;

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    const struct hg_info* info = margo_get_info(h);
    struct raft_io* io = (struct raft_io*)margo_registered_data(mid, info->id);
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;

    mraft_get_raft_id_out_t out = {0};

    struct raft* raft = (struct raft*)io->data;

    margo_trace(impl->mid, "[mraft] Received forwarded get_raft_id request");

    out.id = raft->id;

    margo_respond(h, &out);
    margo_destroy(h);
}

int mraft_io_simulate_dead(struct raft_io* io, bool dead)
{
    struct mraft_io_impl* impl = (struct mraft_io_impl*)io->impl;
#ifdef MRAFT_ENABLE_TESTS
    impl->simulate_dead = dead;
    return MRAFT_SUCCESS;
#else
    return RAFT_NOTFOUND;
#endif
}
