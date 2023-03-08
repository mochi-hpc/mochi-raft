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
    if (!args) return RAFT_INVALID;
    if (args->mid == MARGO_INSTANCE_NULL) return RAFT_INVALID;
    if (!args->log) return RAFT_INVALID;

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
    impl->log  = args->log;
    if (!impl->pool || impl->pool == ABT_POOL_NULL)
        margo_get_handler_pool(args->mid, &impl->pool);

    unsigned long long seed = 0x853c49e6748fea9bULL;
    char addr_str[256];
    hg_size_t addr_size = 256;
    hg_addr_t self_addr = HG_ADDR_NULL;
    margo_addr_self(impl->mid, &self_addr);
    margo_addr_to_string(impl->mid, addr_str, &addr_size, self_addr);
    margo_addr_free(impl->mid, self_addr);
    const unsigned char* p = (const unsigned char*)addr_str;
    while (*p != '\0') {
        seed = (seed << 5) + seed + *p;
        ++p;
    }
    seed |= 1;
    pcg32_srandom_r(&impl->rng_state, seed, 0xda3e39cb94b95bdbULL);

    raft_io->impl = impl;
    return MRAFT_SUCCESS;
}

int mraft_finalize(struct raft_io* raft_io)
{
    free(raft_io->impl);
    return MRAFT_SUCCESS;
}
