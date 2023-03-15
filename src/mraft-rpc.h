/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MRAFT_RPC_H
#define MRAFT_RPC_H

#include "mochi-raft.h"
#include <mercury.h>
#include <mercury_macros.h>
#include <mercury_proc.h>
#include <mercury_proc_string.h>
#include <stdlib.h>

#define hg_proc_raft_term hg_proc_uint64_t
#define hg_proc_raft_id hg_proc_uint64_t
#define hg_proc_raft_index hg_proc_uint64_t

#define MRAFT_CHECK(__op__) do { \
    ret = __op__;                \
    if(ret != HG_SUCCESS) {      \
        return ret;              \
    }                            \
} while(0)

static inline hg_return_t hg_proc_raft_buffer(hg_proc_t proc, struct raft_buffer* buf)
{
    hg_return_t ret = HG_SUCCESS;
    MRAFT_CHECK(hg_proc_hg_size_t(proc, &buf->len));
    switch(hg_proc_get_op(proc)) {
    case HG_DECODE:
        buf->base = raft_calloc(1, buf->len);
        /* fall through */
    case HG_ENCODE:
        MRAFT_CHECK(hg_proc_memcpy(proc, buf->base, buf->len));
        break;
    case HG_FREE:
        raft_free(buf->base);
        buf->len = 0;
        buf->base = NULL;
        break;
    default:
        break;
    }
    return HG_SUCCESS;
}

static inline hg_return_t hg_proc_raft_entry(hg_proc_t proc, struct raft_entry* entry)
{
    hg_return_t ret = HG_SUCCESS;
    MRAFT_CHECK(hg_proc_raft_term(proc, &entry->term));
    MRAFT_CHECK(hg_proc_uint16_t(proc, &entry->type));
    MRAFT_CHECK(hg_proc_raft_buffer(proc, &entry->buf));
    return ret;
}

static inline hg_return_t hg_proc_raft_request_vote(hg_proc_t proc, struct raft_request_vote* req)
{
    hg_return_t ret = HG_SUCCESS;
    MRAFT_CHECK(hg_proc_int32_t(proc, &req->version));
    MRAFT_CHECK(hg_proc_raft_term(proc, &req->term));
    MRAFT_CHECK(hg_proc_raft_id(proc, &req->candidate_id));
    MRAFT_CHECK(hg_proc_raft_index(proc, &req->last_log_index));
    MRAFT_CHECK(hg_proc_raft_index(proc, &req->last_log_term));
    MRAFT_CHECK(hg_proc_hg_bool_t(proc, &req->disrupt_leader));
    MRAFT_CHECK(hg_proc_hg_bool_t(proc, &req->pre_vote));
    return ret;
}

static inline hg_return_t hg_proc_raft_request_vote_result(hg_proc_t proc, struct raft_request_vote_result* req)
{
    hg_return_t ret = HG_SUCCESS;
    MRAFT_CHECK(hg_proc_int32_t(proc, &req->version));
    MRAFT_CHECK(hg_proc_raft_term(proc, &req->term));
    MRAFT_CHECK(hg_proc_hg_bool_t(proc, &req->vote_granted));
    MRAFT_CHECK(hg_proc_hg_bool_t(proc, &req->pre_vote));
    return ret;
}

static inline hg_return_t hg_proc_raft_append_entries(hg_proc_t proc, struct raft_append_entries* arg)
{
    hg_return_t ret = HG_SUCCESS;
    MRAFT_CHECK(hg_proc_int32_t(proc, &arg->version));
    MRAFT_CHECK(hg_proc_raft_term(proc, &arg->term));
    MRAFT_CHECK(hg_proc_raft_index(proc, &arg->prev_log_index));
    MRAFT_CHECK(hg_proc_raft_term(proc, &arg->prev_log_term));
    MRAFT_CHECK(hg_proc_raft_index(proc, &arg->leader_commit));
    MRAFT_CHECK(hg_proc_uint32_t(proc, &arg->n_entries));
    switch(hg_proc_get_op(proc)) {
    case HG_DECODE:
        arg->entries = raft_calloc(arg->n_entries, sizeof(*(arg->entries)));
        /* fall through */
    case HG_ENCODE:
        for(unsigned i=0; i < arg->n_entries; i++) {
            MRAFT_CHECK(hg_proc_raft_entry(proc, arg->entries + i));
        }
        break;
    case HG_FREE:
        break;
    default:
        break;
    }
    return HG_SUCCESS;
}

static inline hg_return_t hg_proc_raft_append_entries_result(hg_proc_t proc, struct raft_append_entries_result* req)
{
    hg_return_t ret = HG_SUCCESS;
    MRAFT_CHECK(hg_proc_int32_t(proc, &req->version));
    MRAFT_CHECK(hg_proc_raft_term(proc, &req->term));
    MRAFT_CHECK(hg_proc_raft_index(proc, &req->rejected));
    MRAFT_CHECK(hg_proc_raft_index(proc, &req->last_log_index));
    return ret;
}

static inline hg_return_t hg_proc_raft_server(hg_proc_t proc, struct raft_server* server)
{
    hg_return_t ret = HG_SUCCESS;
    MRAFT_CHECK(hg_proc_raft_id(proc, &server->id));
    MRAFT_CHECK(hg_proc_hg_string_t(proc, &server->address));
    MRAFT_CHECK(hg_proc_int32_t(proc, &server->role));
    return ret;
}

static inline hg_return_t hg_proc_raft_configuration(hg_proc_t proc, struct raft_configuration* conf)
{
    hg_return_t ret = HG_SUCCESS;
    MRAFT_CHECK(hg_proc_uint32_t(proc, &conf->n));
    switch(hg_proc_get_op(proc)) {
    case HG_DECODE:
        conf->servers = raft_calloc(conf->n, sizeof(*(conf->servers)));
        /* fall through */
    case HG_ENCODE:
        for(unsigned i=0; i < conf->n; i++) {
            MRAFT_CHECK(hg_proc_raft_server(proc, conf->servers + i));
        }
        break;
    case HG_FREE:
        raft_configuration_close(conf);
        break;
    }
    return HG_SUCCESS;
}

static inline hg_return_t hg_proc_raft_install_snapshot(hg_proc_t proc, struct raft_install_snapshot* req)
{
    hg_return_t ret = HG_SUCCESS;
    MRAFT_CHECK(hg_proc_int32_t(proc, &req->version));
    MRAFT_CHECK(hg_proc_raft_term(proc, &req->term));
    MRAFT_CHECK(hg_proc_raft_index(proc, &req->last_index));
    MRAFT_CHECK(hg_proc_raft_term(proc, &req->last_term));
    MRAFT_CHECK(hg_proc_raft_configuration(proc, &req->conf));
    MRAFT_CHECK(hg_proc_raft_index(proc, &req->conf_index));
    MRAFT_CHECK(hg_proc_raft_buffer(proc, &req->data));
    return ret;
}

static inline hg_return_t hg_proc_raft_timeout_now(hg_proc_t proc, struct raft_timeout_now* timeout)
{
    hg_return_t ret = HG_SUCCESS;
    MRAFT_CHECK(hg_proc_int32_t(proc, &timeout->version));
    MRAFT_CHECK(hg_proc_raft_term(proc, &timeout->term));
    MRAFT_CHECK(hg_proc_raft_index(proc, &timeout->last_log_index));
    MRAFT_CHECK(hg_proc_raft_index(proc, &timeout->last_log_term));
    return ret;
}

static inline hg_return_t hg_proc_raft_message(hg_proc_t proc, struct raft_message* msg)
{
    hg_return_t ret = HG_SUCCESS;
    MRAFT_CHECK(hg_proc_uint16_t(proc, &msg->type));
    switch(msg->type) {
    case RAFT_IO_APPEND_ENTRIES:
        return hg_proc_raft_append_entries(proc, &msg->append_entries);
    case RAFT_IO_APPEND_ENTRIES_RESULT:
        return hg_proc_raft_append_entries_result(proc, &msg->append_entries_result);
    case RAFT_IO_REQUEST_VOTE:
        return hg_proc_raft_request_vote(proc, &msg->request_vote);
    case RAFT_IO_REQUEST_VOTE_RESULT:
        return hg_proc_raft_request_vote_result(proc, &msg->request_vote_result);
    case RAFT_IO_INSTALL_SNAPSHOT:
        return hg_proc_raft_install_snapshot(proc, &msg->install_snapshot);
    case RAFT_IO_TIMEOUT_NOW:
        return hg_proc_raft_timeout_now(proc, &msg->timeout_now);
    }
    return HG_SUCCESS;
}

static inline hg_return_t hg_proc_mraft_craft_in_t(hg_proc_t proc, void* arg)
{
    return hg_proc_raft_message(proc, (struct raft_message*)arg);
}

struct apply_in {
    uint32_t            n_bufs;
    struct raft_buffer* bufs;
};

static inline hg_return_t hg_proc_mraft_apply_in_t(hg_proc_t proc, void* arg)
{
    hg_return_t ret;
    struct apply_in* in = (struct apply_in*)arg;
    MRAFT_CHECK(hg_proc_uint32_t(proc, &in->n_bufs));
    switch(hg_proc_get_op(proc)) {
    case HG_DECODE:
        in->bufs = (struct raft_buffer*)calloc(in->n_bufs, sizeof(*in->bufs));
        break;
    case HG_FREE:
        break;
    default:
        break;
    }
    for(unsigned i=0; i < in->n_bufs; i++) {
        MRAFT_CHECK(hg_proc_raft_buffer(proc, &in->bufs[i]));
    }
    return HG_SUCCESS;
}

MERCURY_GEN_PROC(mraft_apply_out_t,
    ((uint32_t)(ret)))

MERCURY_GEN_PROC(mraft_barrier_out_t,
    ((uint32_t)(ret)))

MERCURY_GEN_PROC(mraft_add_in_t,
    ((raft_id)(id))
    ((hg_const_string_t)(address)))

MERCURY_GEN_PROC(mraft_add_out_t,
    ((uint32_t)(ret)))

MERCURY_GEN_PROC(mraft_assign_in_t,
    ((raft_id)(id))
    ((uint32_t)(role)))

MERCURY_GEN_PROC(mraft_assign_out_t,
    ((uint32_t)(ret)))

MERCURY_GEN_PROC(mraft_remove_in_t,
    ((raft_id)(id)))

MERCURY_GEN_PROC(mraft_remove_out_t,
    ((uint32_t)(ret)))

MERCURY_GEN_PROC(mraft_transfer_in_t,
    ((raft_id)(id)))

MERCURY_GEN_PROC(mraft_transfer_out_t,
    ((uint32_t)(ret)))

#endif
