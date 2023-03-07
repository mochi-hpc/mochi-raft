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

/* Initialize the backend with operational parameters such as server ID and address. */
int mraft_impl_init(struct raft_io *io, raft_id id, const char *address)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->data;

    hg_bool_t flag;
    hg_id_t rpc_id;
    margo_registered_name(impl->mid, "mraft_send", &rpc_id, &flag);
    if(flag == HG_TRUE) {
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
    struct mraft_impl* impl = (struct mraft_impl*)io->data;
    // TODO
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
    struct mraft_impl* impl = (struct mraft_impl*)io->data;
    // TODO
}

static void ticker_ult(void* args)
{
    struct raft_io* io = (struct raft_io*)args;
    struct mraft_impl* impl = (struct mraft_impl*)io->data;
    raft_io_tick_cb tick = impl->tick_cb;
    do {
        tick(io);
        margo_thread_sleep(impl->mid, impl->tick_msec);
        raft_io_tick_cb tick = impl->tick_cb;
    } while(tick);
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
    struct mraft_impl* impl = (struct mraft_impl*)io->data;
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
    struct mraft_impl* impl = (struct mraft_impl*)io->data;
    // TODO
}

/* Force appending a new configuration as last entry of the log. */
int mraft_impl_recover(struct raft_io *io, const struct raft_configuration *conf)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->data;
    // TODO
}

/* Synchronously persist current term (and nil vote).
 *
 * The implementation MUST ensure that the change is durable before returning
 * (e.g. using fdatasync() or O_DSYNC).
 */
int mraft_impl_set_term(struct raft_io *io, raft_term term)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->data;
    // TODO
}

/* Synchronously persist who we voted for.
 * The implementation MUST ensure that the change is durable before returning
 * (e.g. using fdatasync() or O_DSYNC).
 */
int mraft_impl_set_vote(struct raft_io *io, raft_id server_id)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->data;
    // TODO
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
    struct mraft_impl* impl = (struct mraft_impl*)io->data;
    // TODO
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
    struct mraft_impl* impl = (struct mraft_impl*)io->data;
    // TODO
}

/* Asynchronously truncate all log entries from the given index onwards. */
int mraft_impl_truncate(struct raft_io *io, raft_index index)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->data;
    // TODO
}

struct snapshot_put_args {
    struct raft_io*              io;
    struct raft_io_snapshot_put* req;
};

static void snapshot_put_ult(void* args)
{
    // TODO
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
    struct mraft_impl* impl = (struct mraft_impl*)io->data;
    req->cb = cb;
    struct snapshot_put_args* args = (struct snapshot_put_args*)calloc(1, sizeof(*args));
    args->io  = io;
    args->req = req;
    return ABT_thread_create(impl->pool, snapshot_put_ult, args, ABT_THREAD_ATTR_NULL, NULL);
}

struct snapshot_get_args {
    struct raft_io*              io;
    struct raft_io_snapshot_get* req;
};

static void snapshot_get_ult(void* args)
{
    // TODO
    free(args);
}

/* Asynchronously load the last snapshot. */
int mraft_impl_snapshot_get(struct raft_io *io,
                            struct raft_io_snapshot_get *req,
                            raft_io_snapshot_get_cb cb)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->data;
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
    struct mraft_impl* impl = (struct mraft_impl*)io->data;
    return min + pcg32_boundedrand_r(&impl->rng_state, max-min);
}

static void async_work_ult(void* args)
{
    struct raft_io_async_work *req = (struct raft_io_async_work*)args;
    int status = req->work(req);
    req->cb(req, status);
}

/* Submit work to be completed asynchronously */
int mraft_impl_async_work(struct raft_io *io,
                          struct raft_io_async_work *req,
                          raft_io_async_work_cb cb)
{
    struct mraft_impl* impl = (struct mraft_impl*)io->data;
    req->cb = cb;
    return ABT_thread_create(impl->pool, async_work_ult, req, ABT_THREAD_ATTR_NULL, NULL);
}

static DEFINE_MARGO_RPC_HANDLER(mraft_rpc_ult)
static void mraft_rpc_ult(hg_handle_t h)
{
    hg_return_t hret;
    mraft_send_in_t in = {0};

    margo_instance_id mid = margo_hg_handle_get_instance(h);
    const struct hg_info* info = margo_get_info(h);
    struct raft_io* io = (struct raft_io*)margo_registered_data(mid, info->id);

    hret = margo_get_input(h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(mid, "Could not deserialize output (mercury error %d)", hret);
        margo_destroy(h);
        return;
    }

    // FIXME: set the id and address of the message

    struct mraft_impl* impl = (struct mraft_impl*)io->data;
    raft_io_recv_cb recv_cb = impl->recv_cb;
    if(recv_cb) recv_cb(io, &in.message);

    margo_free_input(h, &in);
    margo_destroy(h);
}
