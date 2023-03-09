/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mochi-raft.h"
#include "mraft-impl.h"
#include "mraft-rpc.h"
#include <stdlib.h>
#include <margo.h>

static DECLARE_MARGO_RPC_HANDLER(mraft_rpc_ult)
static void mraft_rpc_ult(hg_handle_t h);

static inline void free_server_list(struct raft_io *io)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->impl;
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
    struct mraft_impl* impl = (struct mraft_impl*)io->impl;

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

/* Initialize the backend with operational parameters such as server ID and address. */
int mraft_impl_init(struct raft_io *io, raft_id id, const char *address)
{
    (void)id;
    (void)address;
    struct mraft_impl* impl = (struct mraft_impl*)io->impl;

    margo_trace(impl->mid, "[raft] Initializing mraft instance with id %lu", id);

    hg_bool_t flag;
    hg_id_t rpc_id;
    margo_provider_registered_name(impl->mid, "mraft_send", impl->provider_id, &rpc_id, &flag);
    if(flag == HG_TRUE) {
        margo_error(impl->mid,
            "[raft] An instance of mraft is already registered with provider id %u",
            impl->provider_id);
        return MRAFT_ERR_ID_USED;
    }
    impl->send_rpc_id = MARGO_REGISTER_PROVIDER(impl->mid, "mraft_send",
            mraft_send_in_t, void, mraft_rpc_ult, impl->provider_id, impl->pool);
    margo_registered_disable_response(impl->mid, impl->send_rpc_id, HG_TRUE);
    margo_register_data(impl->mid, impl->send_rpc_id, (void*)io, NULL);

    return MRAFT_SUCCESS;
}

/* Release all resources used by the backend.
 *
 * The tick and recv callbacks must not be invoked anymore, and pending asynchronous
 * requests be completed or canceled as soon as possible. Invoke the close callback
 * once the raft_io instance can be freed.
 */
void mraft_impl_close(struct raft_io *io, raft_io_close_cb cb)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->impl;

    margo_trace(impl->mid, "[raft] Closing mraft instance");

    impl->recv_cb = NULL;
    impl->tick_cb = NULL;
    if(impl->tick_ult && impl->tick_ult != ABT_THREAD_NULL) {
        ABT_thread_join(impl->tick_ult);
        ABT_thread_free(&impl->tick_ult);
        impl->tick_ult = ABT_THREAD_NULL;
    }
    margo_deregister(impl->mid, impl->send_rpc_id);
    free_server_list(io);
    if(cb) cb(io);
}

/* Load persisted state from storage.
 *
 * The implementation must synchronously load the current state from its storage
 * backend and return information about it through the given pointers.
 *
 * The implementation can safely assume that this method will be invoked exactly
 * one time, before any call to raft_io.append() or c:func:raft_io.truncate(), and
 * then won’t be invoked again.
 *
 * The snapshot object and entries array must be allocated and populated using
 * raft_malloc(). If this function completes successfully, ownership of such memory
 * is transfered to the caller.
 */
int mraft_impl_load(struct raft_io *io,
                    raft_term *term,
                    raft_id *voted_for,
                    struct raft_snapshot **snapshot,
                    raft_index *start_index,
                    struct raft_entry *entries[],
                    size_t *n_entries)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->impl;
    margo_trace(impl->mid, "[raft] Loading state from storage");
    if(!impl->log->load) {
        margo_error(impl->mid, "[raft] load function in mraft_log structure not implemented");
        return RAFT_NOTFOUND;
    }
    return (impl->log->load)(impl->log, term, voted_for, snapshot, start_index, entries, n_entries);
}

static void ticker_ult(void* args)
{
    struct raft_io* io = (struct raft_io*)args;
    struct mraft_impl* impl = (struct mraft_impl*)io->impl;
    margo_trace(impl->mid, "[raft] Starting ticker ULT");
    raft_io_tick_cb tick = impl->tick_cb;
    while(tick) {
        tick(io);
        margo_thread_sleep(impl->mid, impl->tick_msec);
        tick = impl->tick_cb;
    }
}

/* Start the backend.
 *
 * From now on the implementation must start accepting RPC requests and must invoke
 * the tick callback every msecs milliseconds. The recv callback must be invoked
 * when receiving a message.
 */
int mraft_impl_start(struct raft_io *io,
                     unsigned msecs,
                     raft_io_tick_cb tick,
                     raft_io_recv_cb recv)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->impl;
    margo_trace(impl->mid, "[raft] Starting raft");
    impl->recv_cb           = recv;
    impl->tick_cb           = tick;
    impl->tick_msec         = msecs;
    int ret = ABT_thread_create(impl->pool, ticker_ult, io, ABT_THREAD_ATTR_NULL, &impl->tick_ult);
    if(ret != ABT_SUCCESS) return MRAFT_ERR_FROM_ARGOBOTS;
    return MRAFT_SUCCESS;
}

/* Bootstrap a server belonging to a new cluster.
 *
 * The implementation must synchronously persist the given configuration as the
 * first entry of the log. The current persisted term must be set to 1 and the vote to nil.
 *
 * If an attempt is made to bootstrap a server that has already some state,
 * then RAFT_CANTBOOTSTRAP must be returned.
 */
int mraft_impl_bootstrap(struct raft_io *io, const struct raft_configuration *conf)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->impl;
    margo_trace(impl->mid, "[raft] Boostrapping raft cluster");
    if(!impl->log->bootstrap) {
        margo_error(impl->mid, "[raft] bootstrap function in mraft_log structure not implemented");
        return RAFT_NOTFOUND;
    }
    if(impl->servers.count != 0) return RAFT_CANTBOOTSTRAP;
    int ret = populate_server_list(io, conf);
    if(ret != 0) {
        margo_error(impl->mid, "[raft] Could not populate server list from configuration");
        return ret;
    }
    return (impl->log->bootstrap)(impl->log, conf);
}

/* Force appending a new configuration as last entry of the log. */
int mraft_impl_recover(struct raft_io *io, const struct raft_configuration *conf)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->impl;
    margo_trace(impl->mid, "[raft] Recovering raft cluster");
    if(!impl->log->recover) {
        margo_error(impl->mid, "[raft] recover function in mraft_log structure not implemented");
        return RAFT_NOTFOUND;
    }
    free_server_list(io);
    int ret = populate_server_list(io, conf);
    if(ret != 0) {
        margo_error(impl->mid, "[raft] Could not populate server list from configuration");
        return ret;
    }
    return (impl->log->recover)(impl->log, conf);
}

/* Synchronously persist current term (and nil vote).
 *
 * The implementation MUST ensure that the change is durable before returning
 * (e.g. using fdatasync() or O_DSYNC).
 */
int mraft_impl_set_term(struct raft_io *io, raft_term term)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->impl;
    margo_trace(impl->mid, "[raft] Setting term to %lu", term);
    if(!impl->log->set_term) {
        margo_error(impl->mid, "[raft] set_term function in mraft_log structure not implemented");
        return RAFT_NOTFOUND;
    }
    return (impl->log->set_term)(impl->log, term);
}

/* Synchronously persist who we voted for.
 * The implementation MUST ensure that the change is durable before returning
 * (e.g. using fdatasync() or O_DSYNC).
 */
int mraft_impl_set_vote(struct raft_io *io, raft_id server_id)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->impl;
    margo_trace(impl->mid, "[raft] Setting vote to %lu", server_id);
    if(!impl->log->set_vote) {
        margo_error(impl->mid, "[raft] set_vote function in mraft_log structure not implemented");
        return RAFT_NOTFOUND;
    }
    return (impl->log->set_vote)(impl->log, server_id);
}

/* Asynchronously send an RPC message.
 *
 * The implementation is guaranteed that the memory referenced in the given message
 * will not be released until the cb callback is invoked.
 */
int mraft_impl_send(struct raft_io *io,
                    struct raft_io_send *req,
                    const struct raft_message *message,
                    raft_io_send_cb cb)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->impl;
    hg_handle_t     h;
    hg_return_t     hret;
    hg_addr_t       addr = HG_ADDR_NULL;

    margo_trace(impl->mid,
        "[raft] Sending message of type %d to server id %lu (%s)",
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

    hret = margo_create(impl->mid, addr, impl->send_rpc_id, &h);
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
    struct mraft_impl* impl = (struct mraft_impl*)args->io->impl;
    int status = impl->log->append ?
        (impl->log->append)(impl->log, args->entries, args->n) : RAFT_IOERR;
    if(args->req->cb) (args->req->cb)(args->req, status);
    free(args);
}

/* Asynchronously append the given entries to the log.
 *
 * The implementation is guaranteed that the memory holding the given entries will
 * not be released until the cb callback is invoked.
 */
int mraft_impl_append(struct raft_io *io,
                      struct raft_io_append *req,
                      const struct raft_entry entries[],
                      unsigned n,
                      raft_io_append_cb cb)
{
    struct mraft_impl* impl  = (struct mraft_impl*)io->impl;
    margo_trace(impl->mid, "[raft] Appending %u entries to the log", n);
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
    struct mraft_impl* impl = (struct mraft_impl*)args->io->impl;
    if(impl->log->truncate)
        (impl->log->truncate)(impl->log, args->index);
    free(args);
}

/* Asynchronously truncate all log entries from the given index onwards. */
int mraft_impl_truncate(struct raft_io *io, raft_index index)
{
    struct mraft_impl* impl    = (struct mraft_impl*)io->impl;
    margo_trace(impl->mid, "[raft] Truncating the log at index %lu", index);
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
    struct mraft_impl* impl = (struct mraft_impl*)args->io->impl;
    int status = impl->log->snapshot_put ?
        (impl->log->snapshot_put)(impl->log, args->trailing, args->snapshot) : RAFT_IOERR;
    if(args->req->cb) (args->req->cb)(args->req, status);
    free(args);
}

/* Asynchronously persist a new snapshot. If the trailing parameter is greater
 * than zero, then all entries older that snapshot->index - trailing must be deleted.
 * If the trailing parameter is 0, then the snapshot completely replaces all existing
 * entries, which should all be deleted. Subsequent calls to append() should append
 * entries starting at index snapshot->index + 1.
 *
 * If a request is submitted, the raft engine won’t submit any other request until
 * the original one has completed.
 */
int mraft_impl_snapshot_put(struct raft_io *io,
                            unsigned trailing,
                            struct raft_io_snapshot_put *req,
                            const struct raft_snapshot *snapshot,
                            raft_io_snapshot_put_cb cb)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->impl;
    margo_trace(impl->mid, "[raft] Creating a snapshot");
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
    struct mraft_impl* impl = (struct mraft_impl*)args->io->impl;
    if(impl->log->snapshot_get)
        (impl->log->snapshot_get)(impl->log, args->req, args->req->cb);
    free(args);
}

/* Asynchronously load the last snapshot. */
int mraft_impl_snapshot_get(struct raft_io *io,
                            struct raft_io_snapshot_get *req,
                            raft_io_snapshot_get_cb cb)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->impl;
    margo_trace(impl->mid, "[raft] Retrieving a snapshot");
    req->cb = cb;
    struct snapshot_get_args* args = (struct snapshot_get_args*)calloc(1, sizeof(*args));
    args->io  = io;
    args->req = req;
    return ABT_thread_create(impl->pool, snapshot_get_ult, args, ABT_THREAD_ATTR_NULL, NULL);
}

/* Return the current time, expressed in milliseconds. */
raft_time mraft_impl_time(struct raft_io *io)
{
    return 1000*ABT_get_wtime();
}

/* Generate a random integer between min and max. */
int mraft_impl_random(struct raft_io *io, int min, int max)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->impl;
    return min + pcg32_boundedrand_r(&impl->rng_state, max-min);
}

static void async_work_ult(void* args)
{
    struct raft_io_async_work *req = (struct raft_io_async_work*)args;
    int status = req->work(req);
    if(req->cb) req->cb(req, status);
}

/* Submit work to be completed asynchronously */
int mraft_impl_async_work(struct raft_io *io,
                          struct raft_io_async_work *req,
                          raft_io_async_work_cb cb)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->impl;
    req->cb = cb;
    return ABT_thread_create(impl->pool, async_work_ult, req, ABT_THREAD_ATTR_NULL, NULL);
}

static DEFINE_MARGO_RPC_HANDLER(mraft_rpc_ult)
static void mraft_rpc_ult(hg_handle_t h)
{
    hg_return_t hret;
    struct raft_message msg = {0};

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    const struct hg_info* info = margo_get_info(h);
    struct raft_io* io = (struct raft_io*)margo_registered_data(mid, info->id);
    struct mraft_impl* impl = (struct mraft_impl*)io->impl;

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

    margo_trace(mid, "[raft] Received message of type %d from server id %lu (%s)",
                msg.type, msg.server_id, msg.server_address);
    raft_io_recv_cb recv_cb = impl->recv_cb;
    if(recv_cb) recv_cb(io, &msg);

finish:
    margo_free_input(h, &msg);
    margo_destroy(h);
}
