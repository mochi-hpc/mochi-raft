/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MRAFT_IMPL_H
#define MRAFT_IMPL_H

#include "pcg_basic.h"
#include "mochi-raft.h"

struct mraft_io_impl {
    margo_instance_id       mid;
    ABT_pool                pool;
    uint16_t                provider_id;
    pcg32_random_t          rng_state;
    raft_id                 self_id;
    char*                   self_address;
    struct mraft_log*       log;
    hg_id_t                 craft_rpc_id;
    hg_id_t                 get_raft_id_rpc_id;
    struct {
        hg_id_t apply_rpc_id;
        hg_id_t barrier_rpc_id;
        hg_id_t add_rpc_id;
        hg_id_t assign_rpc_id;
        hg_id_t remove_rpc_id;
        hg_id_t transfer_rpc_id;
        hg_id_t get_raft_id_rpc_id;
    } forward;
    unsigned                tick_msec;
    ABT_thread              tick_ult;
    _Atomic raft_io_tick_cb tick_cb;
    _Atomic raft_io_recv_cb recv_cb;
#ifdef MRAFT_ENABLE_TESTS
    _Atomic bool simulate_dead;
#endif
};

int mraft_io_impl_init(struct raft_io *io, raft_id id, const char *address);

void mraft_io_impl_close(struct raft_io *io, raft_io_close_cb cb);

int mraft_io_impl_load(struct raft_io *io,
                       raft_term *term,
                       raft_id *voted_for,
                       struct raft_snapshot **snapshot,
                       raft_index *start_index,
                       struct raft_entry *entries[],
                       size_t *n_entries);

int mraft_io_impl_start(struct raft_io *io,
                        unsigned msecs,
                        raft_io_tick_cb tick,
                        raft_io_recv_cb recv);

int mraft_io_impl_bootstrap(struct raft_io *io, const struct raft_configuration *conf);

int mraft_io_impl_recover(struct raft_io *io, const struct raft_configuration *conf);

int mraft_io_impl_set_term(struct raft_io *io, raft_term term);

int mraft_io_impl_set_vote(struct raft_io *io, raft_id server_id);

int mraft_io_impl_send(struct raft_io *io,
                       struct raft_io_send *req,
                       const struct raft_message *message,
                       raft_io_send_cb cb);

int mraft_io_impl_append(struct raft_io *io,
                         struct raft_io_append *req,
                         const struct raft_entry entries[],
                         unsigned n,
                         raft_io_append_cb cb);

int mraft_io_impl_truncate(struct raft_io *io, raft_index index);

int mraft_io_impl_snapshot_put(struct raft_io *io,
                               unsigned trailing,
                               struct raft_io_snapshot_put *req,
                               const struct raft_snapshot *snapshot,
                               raft_io_snapshot_put_cb cb);

int mraft_io_impl_snapshot_get(struct raft_io *io,
                               struct raft_io_snapshot_get *req,
                               raft_io_snapshot_get_cb cb);

raft_time mraft_io_impl_time(struct raft_io *io);

int mraft_io_impl_random(struct raft_io *io, int min, int max);

int mraft_io_impl_async_work(struct raft_io *io,
                             struct raft_io_async_work *req,
                             raft_io_async_work_cb cb);

#endif
