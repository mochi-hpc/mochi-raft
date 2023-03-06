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
    raft_io->data = calloc(1, sizeof(struct mraft_impl));
    return MRAFT_SUCCESS;
}

int mraft_finalize(struct raft_io* raft_io)
{

    return MRAFT_SUCCESS;
}
