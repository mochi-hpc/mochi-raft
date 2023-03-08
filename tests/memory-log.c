/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MEMORY_LOG_H
#define MEMORY_LOG_H

#include <mochi-raft.h>
#include <stdlib.h>

struct memory_log {
    raft_term term;
    raft_id   voted_for;

    struct {
        struct raft_entry* array;
        unsigned           count;
        unsigned           capacity;
    } entries;

    struct raft_snapshot last_snapshot;
};

static int memory_log_load(
    struct mraft_log* log, raft_term* term,
    raft_id* voted_for, struct raft_snapshot** snapshot,
    raft_index* start_index, struct raft_entry* entries[],
    size_t* n_entries)
{
    (void)log;
    (void)term;
    (void)voted_for;
    (void)snapshot;
    (void)start_index;
    (void)entries;
    (void)n_entries;
    return RAFT_NOTFOUND;
}

static int memory_log_bootstrap(
    struct mraft_log* log, const struct raft_configuration* conf)
{
    struct memory_log* mlog = (struct memory_log*)log->data;
    mlog->term      = 1;
    mlog->voted_for = 0;
    mlog->entries.array    = calloc(1, sizeof(*mlog->entries.array));
    mlog->entries.capacity = 1;
    mlog->entries.count    = 1;
    struct raft_entry* last = mlog->entries.array;
    raft_configuration_encode(conf, &last->buf);
    return 0;
}

static int memory_log_recover(
    struct mraft_log* log, const struct raft_configuration* conf)
{
    struct memory_log* mlog = (struct memory_log*)log->data;
    if(mlog->entries.capacity == mlog->entries.count)
        mlog->entries.capacity = (1+mlog->entries.capacity)*2;
    mlog->entries.array = realloc(mlog->entries.array,
            mlog->entries.capacity*sizeof(*mlog->entries.array));
    struct raft_entry* last = &mlog->entries.array[mlog->entries.count];
    raft_configuration_encode(conf, &last->buf);
    mlog->entries.count += 1;
    return RAFT_NOTFOUND;
}

static int memory_log_set_term(
    struct mraft_log* log, raft_term term)
{
    struct memory_log* mlog = (struct memory_log*)log->data;
    mlog->term = term;
    return 0;
}

static int memory_log_set_vote(
    struct mraft_log* log, raft_id server_id)
{
    struct memory_log* mlog = (struct memory_log*)log->data;
    mlog->voted_for = server_id;
    return 0;
}

static int memory_log_append(
    struct mraft_log* log, const struct raft_entry entries[], unsigned n)
{
    struct memory_log* mlog = (struct memory_log*)log->data;
    unsigned capacity = mlog->entries.capacity;
    while(capacity - mlog->entries.count < n)
        capacity = (1 + capacity)*2;
    if(capacity > mlog->entries.capacity) {
        mlog->entries.array =
            (struct raft_entry*)realloc(
                    mlog->entries.array, capacity*sizeof(*mlog->entries.array));
        mlog->entries.capacity = capacity;
    }
    for(unsigned i = 0; i < n; i++, mlog->entries.count++) {
        struct raft_entry* last = &mlog->entries.array[mlog->entries.count];
        last->term     = entries[i].term;
        last->type     = entries[i].type;
        last->buf.len  = entries[i].buf.len;
        last->buf.base = malloc(entries[i].buf.len);
        memcpy(last->buf.base, entries[i].buf.base, entries[i].buf.len);
    }
    return 0;
}

static int memory_log_truncate(
    struct mraft_log* log, raft_index index)
{
    struct memory_log* mlog = (struct memory_log*)log->data;
    while(mlog->entries.count > index) {
        struct raft_entry* last = &mlog->entries.array[mlog->entries.count];
        free(last->buf.base);
        memset(last, 0, sizeof(*last));
        mlog->entries.count -= 1;
    }
    return 0;
}

static int memory_log_snapshot_put(
    struct mraft_log* log, unsigned trailing,
    const struct raft_snapshot* snapshot)
{
    (void)trailing; /* ignore for now */
    struct memory_log* mlog = (struct memory_log*)log->data;
    mlog->last_snapshot.index = snapshot->index;
    mlog->last_snapshot.term  = snapshot->term;
    mlog->last_snapshot.configuration_index = snapshot->configuration_index;
    mlog->last_snapshot.configuration.n = snapshot->configuration.n;
    mlog->last_snapshot.configuration.servers =
        calloc(snapshot->configuration.n, sizeof(*snapshot->configuration.servers));
    for(unsigned i=0; i < snapshot->configuration.n; i++) {
        mlog->last_snapshot.configuration.servers[i].address =
            strdup(snapshot->configuration.servers[i].address);
        mlog->last_snapshot.configuration.servers[i].id =
            snapshot->configuration.servers[i].id;
        mlog->last_snapshot.configuration.servers[i].role =
            snapshot->configuration.servers[i].role;
    }
    mlog->last_snapshot.n_bufs = snapshot->n_bufs;
    mlog->last_snapshot.bufs = calloc(mlog->last_snapshot.n_bufs, sizeof(*mlog->last_snapshot.bufs));
    for(unsigned i=0; i < mlog->last_snapshot.n_bufs; i++) {
        mlog->last_snapshot.bufs[i].len = snapshot->bufs[i].len;
        mlog->last_snapshot.bufs[i].base = raft_malloc(snapshot->bufs[i].len);
        memcpy(mlog->last_snapshot.bufs[i].base,
               snapshot->bufs[i].base,
               snapshot->bufs[i].len);
    }
    return 0;
}

static int memory_log_snapshot_get(
        struct mraft_log* log,
        struct raft_io_snapshot_get* req, raft_io_snapshot_get_cb cb)
{
    struct memory_log* mlog = (struct memory_log*)log->data;
    int status = 0;
    if(mlog->last_snapshot.index == 0) status = RAFT_NOTFOUND;
    cb(req, &mlog->last_snapshot, status);
    return 0;
}

struct mraft_log* memory_log_create()
{
    struct mraft_log* log = (struct mraft_log*)calloc(1, sizeof(*log));
#define SET_FUNCTION(__name__) log->__name__ = memory_log_##__name__
    SET_FUNCTION(load);
    SET_FUNCTION(bootstrap);
    SET_FUNCTION(recover);
    SET_FUNCTION(set_term);
    SET_FUNCTION(set_vote);
    SET_FUNCTION(append);
    SET_FUNCTION(truncate);
    SET_FUNCTION(snapshot_put);
    SET_FUNCTION(snapshot_get);
#undef SET_FUNCTION
    log->data = calloc(1, sizeof(struct memory_log));
    return log;
}

void memory_log_free(struct mraft_log* log)
{
    struct memory_log* mlog = (struct memory_log*)log->data;
    for(unsigned i=0; i < mlog->entries.count; i++) {
        free(mlog->entries.array[i].buf.base);
    }
    free(mlog->entries.array);
    for(unsigned i=0; i < mlog->last_snapshot.n_bufs; i++) {
        free(mlog->last_snapshot.bufs[i].base);
    }
    free(mlog->last_snapshot.bufs);
    for(unsigned i=0; i < mlog->last_snapshot.configuration.n; i++) {
        free(mlog->last_snapshot.configuration.servers->address);
    }
    free(mlog->last_snapshot.configuration.servers);
    free(mlog);
    free(log);
}

#ifdef __cplusplus
}
#endif

#endif
