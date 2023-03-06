/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mochi-raft.h"
#include "mraft-impl.h"
#include <stdlib.h>

int mraft_init(const struct mraft_init_args* args, struct raft_io* raft_io)
{
    if (!args) return -1;
    if (args->mid == MARGO_INSTANCE_NULL) return -1;

    memset(raft_io, 0, sizeof(*raft_io));
    raft_io->version = 2;
#define MRAFT_SET(__name__) raft_io->__name__ = mraft_impl_##__name__
    MRAFT_SET(init);
    MRAFT_SET(close);
    MRAFT_SET(load);
    MRAFT_SET(start);
    MRAFT_SET(bootstrap);
    MRAFT_SET(recover);
    MRAFT_SET(set_term);
    MRAFT_SET(set_vote);
    MRAFT_SET(send);
    MRAFT_SET(append);
    MRAFT_SET(truncate);
    MRAFT_SET(snapshot_put);
    MRAFT_SET(snapshot_get);
    MRAFT_SET(time);
    MRAFT_SET(random);
    MRAFT_SET(async_work);
#undef MRAFT_SET
    struct mraft_impl* impl = (struct mraft_impl*)calloc(1, sizeof(struct mraft_impl));
    impl->mid  = args->mid;
    impl->pool = args->pool;
    if (!impl->pool || impl->pool == ABT_POOL_NULL)
        margo_get_handler_pool(args->mid, &impl->pool);

    pcg32_srandom_r(&impl->rng_state, 0x853c49e6748fea9bULL, 0xda3e39cb94b95bdbULL);

    raft_io->data = impl;
    return MRAFT_SUCCESS;
}

int mraft_finalize(struct raft_io* raft_io)
{
    free(raft_io->data);
    return MRAFT_SUCCESS;
}
