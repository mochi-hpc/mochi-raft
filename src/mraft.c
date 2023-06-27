/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include "mochi-raft.h"
#include "mraft-io.h"
#include "mraft-rpc.h"
#include <margo.h>
#include <stdlib.h>
#include "config.h"
#ifdef ENABLE_SSG
#include <ssg.h>
#endif

int mraft_io_init(const struct mraft_io_init_args* args, struct raft_io* raft_io)
{
    if (!args) return RAFT_INVALID;
    if (args->mid == MARGO_INSTANCE_NULL) return RAFT_INVALID;
    if (!args->log) return RAFT_INVALID;

    memset(raft_io, 0, sizeof(*raft_io));
    raft_io->version = 2;
#define MRAFT_SET(__name__) raft_io->__name__ = mraft_io_impl_##__name__
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
    struct mraft_io_impl* impl = (struct mraft_io_impl*)calloc(1, sizeof(struct mraft_io_impl));
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

int mraft_io_finalize(struct raft_io* raft_io)
{
    free(raft_io->impl);
    return MRAFT_SUCCESS;
}

void mraft_close(struct raft* r)
{
    raft_close(r, NULL);
}

#ifdef ENABLE_SSG
int mraft_bootstrap_from_ssg(struct raft* r,
                             ssg_group_id_t gid)
{
    int ret;
    struct mraft_io_impl* impl = (struct mraft_io_impl*)r->io->impl;
    struct raft_configuration conf = {0};
    raft_configuration_init(&conf);
    int group_size;
    ssg_get_group_size(gid, &group_size);
    for(unsigned i = 0; i < group_size; i++) {
        ssg_member_id_t member_id = 0;
        char* address = NULL;
        ret = ssg_get_group_member_id_from_rank(gid, i, &member_id);
        if(ret != SSG_SUCCESS) {
            margo_error(impl->mid, "[mraft] In mraft_bootstrap_from_ssg:"
                " ssg_get_group_member_id_from_rank returned %d", ret);
            goto error;
        }
        ret = ssg_get_group_member_addr_str(gid, member_id, &address);
        if(ret != SSG_SUCCESS) {
            margo_error(impl->mid, "[mraft] In mraft_bootstrap_from_ssg:"
                " ssg_get_group_member_add_str returned %d", ret);
            goto error;
        }
        raft_configuration_add(&conf, member_id, address, RAFT_VOTER);
    }
    ret = raft_bootstrap(r, &conf);
    raft_configuration_close(&conf);
    return ret;
error:
    raft_configuration_close(&conf);
    return RAFT_INVALID;
}
#else
int mraft_bootstrap_from_ssg(struct raft* r,
                             ssg_group_id_t gid)
{
    (void)gid;
    struct mraft_io_impl* impl = (struct mraft_io_impl*)r->io->impl;
    margo_error(impl->mid, "[mraft] Mraft was not compiled with SSG support");
    return RAFT_UNAUTHORIZED;
}
#endif

static void mraft_apply_cb(struct raft_apply *req, int status, void *result)
{
    (void)result;
    ABT_eventual ev = (ABT_eventual)req->data;
    ABT_eventual_set(ev, &status, sizeof(status));
}

int mraft_apply(struct raft *r,
                const struct raft_buffer bufs[],
                const unsigned n)
{
    int ret;
    if(raft_state(r) == RAFT_LEADER) {
        struct raft_apply req = {0};
        ABT_eventual ev = ABT_EVENTUAL_NULL;
        ABT_eventual_create(sizeof(int), &ev);
        req.data = (void*)ev;

        // make copies of the buffers because raft wants to
        // take responsibility for them.
        struct raft_buffer* bufs_cpy = alloca(n*sizeof(*bufs_cpy));
        for(unsigned i = 0; i < n; i++) {
            bufs_cpy[i].len = bufs[i].len;
            bufs_cpy[i].base = raft_malloc(bufs_cpy[i].len);
            memcpy(bufs_cpy[i].base, bufs[i].base, bufs_cpy[i].len);
        }

        ret = raft_apply(r, &req, bufs_cpy, n, mraft_apply_cb);
        if(ret != 0) {
            ABT_eventual_free(&ev);
            for(unsigned i=0; i < n; i++) {
                raft_free(bufs_cpy[i].base);
            }
            return ret;
        }
        int* status = NULL;
        ABT_eventual_wait(ev, (void**)&status);
        ret = *status;
        ABT_eventual_free(&ev);
        return 0;
    }

    raft_id     leader_id = 0;
    const char* leader_address = NULL;
    raft_leader(r, &leader_id, &leader_address);

    if(leader_id == 0) return RAFT_LEADERSHIPLOST;

    struct mraft_io_impl* impl = (struct mraft_io_impl*)r->io->impl;

    hg_handle_t     h = HG_HANDLE_NULL;
    hg_return_t     hret;
    hg_addr_t       addr = HG_ADDR_NULL;

    hret = margo_addr_lookup(impl->mid, leader_address, &addr);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could not resolve address %s: margo_addr_lookup returned %d",
            leader_address, hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    hret = margo_create(impl->mid, addr, impl->forward.apply_rpc_id, &h);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could not create handle: margo_create returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    struct apply_in in = {
        .n_bufs = n,
        .bufs = (struct raft_buffer*)bufs
    };

    hret = margo_provider_forward(impl->provider_id, h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could forward handle: margo_provider_forward returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    mraft_apply_out_t out = {0};

    hret = margo_get_output(h, &out);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could get output: margo_get_output returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    ret = out.ret;

finish:
    margo_free_output(h, &out);
    margo_destroy(h);
    return ret;
}

static void mraft_barrier_cb(struct raft_barrier *req, int status)
{
    ABT_eventual ev = (ABT_eventual)req->data;
    ABT_eventual_set(ev, &status, sizeof(status));
}

int mraft_barrier(struct raft *r)
{
    int ret;
    if(raft_state(r) == RAFT_LEADER) {
        struct raft_barrier req = {0};
        ABT_eventual ev = ABT_EVENTUAL_NULL;
        ABT_eventual_create(sizeof(int), &ev);
        req.data = (void*)ev;
        ret = raft_barrier(r, &req, mraft_barrier_cb);
        if(ret != 0) {
            ABT_eventual_free(&ev);
            return ret;
        }
        int* status = NULL;
        ABT_eventual_wait(ev, (void**)&status);
        ret = *status;
        ABT_eventual_free(&ev);
        return 0;
    }

    raft_id     leader_id = 0;
    const char* leader_address = NULL;
    raft_leader(r, &leader_id, &leader_address);

    if(leader_id == 0) return RAFT_LEADERSHIPLOST;

    struct mraft_io_impl* impl = (struct mraft_io_impl*)r->io->impl;

    hg_handle_t     h = HG_HANDLE_NULL;
    hg_return_t     hret;
    hg_addr_t       addr = HG_ADDR_NULL;

    hret = margo_addr_lookup(impl->mid, leader_address, &addr);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could not resolve address %s: margo_addr_lookup returned %d",
            leader_address, hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    hret = margo_create(impl->mid, addr, impl->forward.barrier_rpc_id, &h);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could not create handle: margo_create returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    hret = margo_provider_forward(impl->provider_id, h, NULL);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could forward handle: margo_provider_forward returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    mraft_barrier_out_t out = {0};

    hret = margo_get_output(h, &out);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could get output: margo_get_output returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    ret = out.ret;

finish:
    margo_free_output(h, &out);
    margo_destroy(h);
    return ret;
}

static void mraft_change_cb(struct raft_change *req, int status)
{
    ABT_eventual ev = (ABT_eventual)req->data;
    ABT_eventual_set(ev, &status, sizeof(status));
}

int mraft_add(struct raft *r,
              raft_id id,
              const char *address)
{
    int ret;
    if(raft_state(r) == RAFT_LEADER) {
        struct raft_change req = {0};
        ABT_eventual ev = ABT_EVENTUAL_NULL;
        ABT_eventual_create(sizeof(int), &ev);
        req.data = (void*)ev;
        ret = raft_add(r, &req, id, address, mraft_change_cb);
        if(ret != 0) {
            ABT_eventual_free(&ev);
            return ret;
        }
        int* status = NULL;
        ABT_eventual_wait(ev, (void**)&status);
        ret = *status;
        ABT_eventual_free(&ev);
        return 0;
    }

    raft_id     leader_id = 0;
    const char* leader_address = NULL;
    raft_leader(r, &leader_id, &leader_address);

    if(leader_id == 0) return RAFT_LEADERSHIPLOST;

    struct mraft_io_impl* impl = (struct mraft_io_impl*)r->io->impl;

    hg_handle_t     h = HG_HANDLE_NULL;
    hg_return_t     hret;
    hg_addr_t       addr = HG_ADDR_NULL;

    hret = margo_addr_lookup(impl->mid, leader_address, &addr);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could not resolve address %s: margo_addr_lookup returned %d",
            leader_address, hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    hret = margo_create(impl->mid, addr, impl->forward.add_rpc_id, &h);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could not create handle: margo_create returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    mraft_add_in_t in = {
        .id = id,
        .address = address
    };

    hret = margo_provider_forward(impl->provider_id, h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could forward handle: margo_provider_forward returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    mraft_add_out_t out = {0};

    hret = margo_get_output(h, &out);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could get output: margo_get_output returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    ret = out.ret;

finish:
    margo_free_output(h, &out);
    margo_destroy(h);
    return ret;
}

int mraft_assign(struct raft *r,
                 raft_id id,
                 int role)
{
    int ret;
    if(raft_state(r) == RAFT_LEADER) {
        struct raft_change req = {0};
        ABT_eventual ev = ABT_EVENTUAL_NULL;
        ABT_eventual_create(sizeof(int), &ev);
        req.data = (void*)ev;
        ret = raft_assign(r, &req, id, role, mraft_change_cb);
        if(ret != 0) {
            ABT_eventual_free(&ev);
            return ret;
        }
        int* status = NULL;
        ABT_eventual_wait(ev, (void**)&status);
        ret = *status;
        ABT_eventual_free(&ev);
        return 0;
    }

    raft_id     leader_id = 0;
    const char* leader_address = NULL;
    raft_leader(r, &leader_id, &leader_address);

    if(leader_id == 0) return RAFT_LEADERSHIPLOST;

    struct mraft_io_impl* impl = (struct mraft_io_impl*)r->io->impl;

    hg_handle_t     h = HG_HANDLE_NULL;
    hg_return_t     hret;
    hg_addr_t       addr = HG_ADDR_NULL;

    hret = margo_addr_lookup(impl->mid, leader_address, &addr);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could not resolve address %s: margo_addr_lookup returned %d",
            leader_address, hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    hret = margo_create(impl->mid, addr, impl->forward.assign_rpc_id, &h);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could not create handle: margo_create returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    mraft_assign_in_t in = {
        .id = id,
        .role = role
    };

    hret = margo_provider_forward(impl->provider_id, h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could forward handle: margo_provider_forward returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    mraft_assign_out_t out = {0};

    hret = margo_get_output(h, &out);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could get output: margo_get_output returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    ret = out.ret;

finish:
    margo_free_output(h, &out);
    margo_destroy(h);
    return ret;
}

int mraft_remove(struct raft *r,
                 raft_id id)
{
    int ret;
    if(raft_state(r) == RAFT_LEADER) {
        struct raft_change req = {0};
        ABT_eventual ev = ABT_EVENTUAL_NULL;
        ABT_eventual_create(sizeof(int), &ev);
        req.data = (void*)ev;
        ret = raft_remove(r, &req, id, mraft_change_cb);
        if(ret != 0) {
            ABT_eventual_free(&ev);
            return ret;
        }
        int* status = NULL;
        ABT_eventual_wait(ev, (void**)&status);
        ret = *status;
        ABT_eventual_free(&ev);
        return 0;
    }

    raft_id     leader_id = 0;
    const char* leader_address = NULL;
    raft_leader(r, &leader_id, &leader_address);

    if(leader_id == 0) return RAFT_LEADERSHIPLOST;

    struct mraft_io_impl* impl = (struct mraft_io_impl*)r->io->impl;

    hg_handle_t     h = HG_HANDLE_NULL;
    hg_return_t     hret;
    hg_addr_t       addr = HG_ADDR_NULL;

    hret = margo_addr_lookup(impl->mid, leader_address, &addr);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could not resolve address %s: margo_addr_lookup returned %d",
            leader_address, hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    hret = margo_create(impl->mid, addr, impl->forward.remove_rpc_id, &h);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could not create handle: margo_create returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    mraft_remove_in_t in = {
        .id = id,
    };

    hret = margo_provider_forward(impl->provider_id, h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could forward handle: margo_provider_forward returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    mraft_remove_out_t out = {0};

    hret = margo_get_output(h, &out);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could get output: margo_get_output returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    ret = out.ret;

finish:
    margo_free_output(h, &out);
    margo_destroy(h);
    return ret;
}

static void mraft_transfer_cb(struct raft_transfer *req)
{
    ABT_eventual ev = (ABT_eventual)req->data;
    ABT_eventual_set(ev, NULL, 0);
}

int mraft_transfer(struct raft *r,
                   raft_id id)
{
    int ret;
    if(raft_state(r) == RAFT_LEADER) {
        struct raft_transfer req = {0};
        ABT_eventual ev = ABT_EVENTUAL_NULL;
        ABT_eventual_create(0, &ev);
        req.data = (void*)ev;
        ret = raft_transfer(r, &req, id, mraft_transfer_cb);
        if(ret != 0) {
            ABT_eventual_free(&ev);
            return ret;
        }
        ABT_eventual_wait(ev, NULL);
        ABT_eventual_free(&ev);
        return 0;
    }

    raft_id     leader_id = 0;
    const char* leader_address = NULL;
    raft_leader(r, &leader_id, &leader_address);

    if(leader_id == 0) return RAFT_LEADERSHIPLOST;

    struct mraft_io_impl* impl = (struct mraft_io_impl*)r->io->impl;

    hg_handle_t     h = HG_HANDLE_NULL;
    hg_return_t     hret;
    hg_addr_t       addr = HG_ADDR_NULL;

    hret = margo_addr_lookup(impl->mid, leader_address, &addr);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could not resolve address %s: margo_addr_lookup returned %d",
            leader_address, hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    hret = margo_create(impl->mid, addr, impl->forward.transfer_rpc_id, &h);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could not create handle: margo_create returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    mraft_transfer_in_t in = {
        .id = id,
    };

    hret = margo_provider_forward(impl->provider_id, h, &in);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could forward handle: margo_provider_forward returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    mraft_transfer_out_t out = {0};

    hret = margo_get_output(h, &out);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could get output: margo_get_output returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    ret = out.ret;

finish:
    margo_free_output(h, &out);
    margo_destroy(h);
    return ret;
}

int mraft_get_raft_id(struct raft *r,
                      const char* address,
                      raft_id* id)
{
    int ret = 0;

    struct mraft_io_impl* impl = (struct mraft_io_impl*)r->io->impl;

    hg_handle_t     h = HG_HANDLE_NULL;
    hg_return_t     hret;
    hg_addr_t       addr = HG_ADDR_NULL;

    hret = margo_addr_lookup(impl->mid, address, &addr);

    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could not resolve address %s: margo_addr_lookup returned %d",
            address, hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    hret = margo_create(impl->mid, addr, impl->get_raft_id_rpc_id, &h);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could not create handle: margo_create returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    hret = margo_provider_forward(impl->provider_id, h, NULL);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could forward handle: margo_provider_forward returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    mraft_get_raft_id_out_t out = {0};

    hret = margo_get_output(h, &out);
    if(hret != HG_SUCCESS) {
        margo_error(impl->mid,
            "[mraft] Could get output: margo_get_output returned %d", hret);
        ret = MRAFT_ERR_FROM_MERCURY;
        goto finish;
    }

    *id = out.id;

finish:
    margo_free_output(h, &out);
    margo_destroy(h);
    return ret;
}
