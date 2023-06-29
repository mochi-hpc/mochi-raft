/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <mochi-raft-memory-log.h>
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

static int memory_log_load(struct mraft_log*      log,
                           raft_term*             term,
                           raft_id*               voted_for,
                           struct raft_snapshot** snapshot,
                           raft_index*            start_index,
                           struct raft_entry*     entries[],
                           size_t*                n_entries)
{
    struct memory_log* mlog = (struct memory_log*)log->data;
    *term                   = mlog->term;
    *start_index            = 1;
    *voted_for              = mlog->voted_for;
    *n_entries              = mlog->entries.count;
    *entries                = raft_malloc((*n_entries) * sizeof(**entries));
    *snapshot               = NULL;
    for (unsigned i = 0; i < *n_entries; i++) {
        (*entries)[i].batch    = NULL;
        (*entries)[i].term     = mlog->entries.array[i].term;
        (*entries)[i].type     = mlog->entries.array[i].type;
        (*entries)[i].buf.len  = mlog->entries.array[i].buf.len;
        (*entries)[i].buf.base = raft_malloc((*entries)[i].buf.len);
        memcpy((*entries)[i].buf.base, mlog->entries.array[i].buf.base,
               (*entries)[i].buf.len);
    }
    // Note: a snapshot is redundant in this memory log since we can
    // load all the entries directly from memory. In a real log, the
    // snapshot would contain whatever came before all the entries we
    // are loading.
#if 0
    struct raft_snapshot* snap
        = (struct raft_snapshot*)raft_calloc(1, sizeof(*snap));
    snap->index               = mlog->last_snapshot.index;
    snap->term                = mlog->last_snapshot.term;
    snap->configuration_index = mlog->last_snapshot.configuration_index;
    snap->n_bufs              = mlog->last_snapshot.n_bufs;
    snap->bufs
        = snap->n_bufs ? raft_calloc(snap->n_bufs, sizeof(*snap->bufs)) : NULL;
    for (unsigned i = 0; i < snap->n_bufs; i++) {
        snap->bufs[i].len  = mlog->last_snapshot.bufs[i].len;
        snap->bufs[i].base = raft_malloc(snap->bufs[i].len);
        memcpy(snap->bufs[i].base, mlog->last_snapshot.bufs[i].base,
               snap->bufs[i].len);
    }
    snap->configuration.n = mlog->last_snapshot.configuration.n;
    snap->configuration.servers
        = mlog->last_snapshot.configuration.n ? raft_calloc(
              snap->configuration.n, sizeof(*snap->configuration.servers))
                                              : NULL;
    for (unsigned i = 0; i < snap->configuration.n; i++) {
        snap->configuration.servers[i].id
            = mlog->last_snapshot.configuration.servers[i].id;
        snap->configuration.servers[i].role
            = mlog->last_snapshot.configuration.servers[i].role;
        snap->configuration.servers[i].address = raft_calloc(
            strlen(mlog->last_snapshot.configuration.servers[i].address) + 1,
            1);
        strcpy(snap->configuration.servers[i].address,
               mlog->last_snapshot.configuration.servers[i].address);
    }
    *snapshot = snap;
#endif
    return 0;
}

static int memory_log_bootstrap(struct mraft_log*                log,
                                const struct raft_configuration* conf)
{
    struct memory_log* mlog = (struct memory_log*)log->data;
    mlog->term              = 1;
    mlog->voted_for         = 0;
    mlog->entries.array     = calloc(1, sizeof(*mlog->entries.array));
    mlog->entries.capacity  = 1;
    mlog->entries.count     = 1;
    struct raft_entry* last = mlog->entries.array;
    last->term              = 1;
    last->type              = RAFT_CHANGE;
    raft_configuration_encode(conf, &last->buf);
    mlog->last_snapshot.index               = 0;
    mlog->last_snapshot.term                = 1;
    mlog->last_snapshot.configuration_index = 0;
    mlog->last_snapshot.n_bufs              = 1;
    mlog->last_snapshot.bufs
        = raft_calloc(1, sizeof(*mlog->last_snapshot.bufs));
    mlog->last_snapshot.bufs[0].len = 0;
    raft_configuration_init(&mlog->last_snapshot.configuration);
    for (unsigned i = 0; i < conf->n; i++) {
        raft_configuration_add(&mlog->last_snapshot.configuration,
                               conf->servers[i].id, conf->servers[i].address,
                               conf->servers[i].role);
    }
    return 0;
}

static int memory_log_recover(struct mraft_log*                log,
                              const struct raft_configuration* conf)
{
    struct memory_log* mlog = (struct memory_log*)log->data;
    if (mlog->entries.capacity == mlog->entries.count)
        mlog->entries.capacity = (1 + mlog->entries.capacity) * 2;
    mlog->entries.array
        = realloc(mlog->entries.array,
                  mlog->entries.capacity * sizeof(*mlog->entries.array));
    struct raft_entry* last = &mlog->entries.array[mlog->entries.count];
    raft_configuration_encode(conf, &last->buf);
    mlog->entries.count += 1;
    return RAFT_NOTFOUND;
}

static int memory_log_set_term(struct mraft_log* log, raft_term term)
{
    struct memory_log* mlog = (struct memory_log*)log->data;
    mlog->term              = term;
    mlog->voted_for         = 0;
    return 0;
}

static int memory_log_set_vote(struct mraft_log* log, raft_id server_id)
{
    struct memory_log* mlog = (struct memory_log*)log->data;
    mlog->voted_for         = server_id;
    return 0;
}

static int memory_log_append(struct mraft_log*       log,
                             const struct raft_entry entries[],
                             unsigned                n)
{
    struct memory_log* mlog     = (struct memory_log*)log->data;
    unsigned           capacity = mlog->entries.capacity;
    while (capacity - mlog->entries.count < n) capacity = (1 + capacity) * 2;
    if (capacity > mlog->entries.capacity) {
        mlog->entries.array = (struct raft_entry*)realloc(
            mlog->entries.array, capacity * sizeof(*mlog->entries.array));
        mlog->entries.capacity = capacity;
    }
    for (unsigned i = 0; i < n; i++, mlog->entries.count++) {
        struct raft_entry* last = &mlog->entries.array[mlog->entries.count];
        last->term              = entries[i].term;
        last->type              = entries[i].type;
        last->buf.len           = entries[i].buf.len;
        last->buf.base          = malloc(entries[i].buf.len);
        memcpy(last->buf.base, entries[i].buf.base, entries[i].buf.len);
    }
    return 0;
}

static int memory_log_truncate(struct mraft_log* log, raft_index index)
{
    struct memory_log* mlog = (struct memory_log*)log->data;
    while (mlog->entries.count > index) {
        struct raft_entry* last = &mlog->entries.array[mlog->entries.count];
        free(last->buf.base);
        memset(last, 0, sizeof(*last));
        mlog->entries.count -= 1;
    }
    return 0;
}

static int memory_log_snapshot_put(struct mraft_log*           log,
                                   unsigned                    trailing,
                                   const struct raft_snapshot* snapshot)
{
    (void)trailing; /* ignore for now */
    struct memory_log* mlog                   = (struct memory_log*)log->data;
    mlog->last_snapshot.index                 = snapshot->index;
    mlog->last_snapshot.term                  = snapshot->term;
    mlog->last_snapshot.configuration_index   = snapshot->configuration_index;
    mlog->last_snapshot.configuration.n       = snapshot->configuration.n;
    mlog->last_snapshot.configuration.servers = calloc(
        snapshot->configuration.n, sizeof(*snapshot->configuration.servers));
    for (unsigned i = 0; i < snapshot->configuration.n; i++) {
        mlog->last_snapshot.configuration.servers[i].address
            = strdup(snapshot->configuration.servers[i].address);
        mlog->last_snapshot.configuration.servers[i].id
            = snapshot->configuration.servers[i].id;
        mlog->last_snapshot.configuration.servers[i].role
            = snapshot->configuration.servers[i].role;
    }
    mlog->last_snapshot.n_bufs = snapshot->n_bufs ? 1 : 0;
    if (!mlog->last_snapshot.n_bufs) return 0;
    size_t buf_size = 0;
    for (unsigned i = 0; i < snapshot->n_bufs; i++) {
        buf_size += snapshot->bufs[i].len;
    }
    mlog->last_snapshot.bufs
        = raft_calloc(1, sizeof(*mlog->last_snapshot.bufs));
    mlog->last_snapshot.bufs[0].base = raft_calloc(1, buf_size);
    mlog->last_snapshot.bufs[0].len  = buf_size;
    size_t offset                    = 0;
    for (unsigned i = 0; i < snapshot->n_bufs; i++) {
        memcpy((char*)mlog->last_snapshot.bufs[0].base + offset,
               snapshot->bufs[i].base, snapshot->bufs[i].len);
        offset += snapshot->bufs[i].len;
    }
    return 0;
}

static int memory_log_snapshot_get(struct mraft_log*            log,
                                   struct raft_io_snapshot_get* req,
                                   raft_io_snapshot_get_cb      cb)
{
    struct memory_log*    mlog = (struct memory_log*)log->data;
    struct raft_snapshot* snap
        = (struct raft_snapshot*)raft_calloc(1, sizeof(*snap));
    snap->index               = mlog->last_snapshot.index;
    snap->term                = mlog->last_snapshot.term;
    snap->configuration_index = mlog->last_snapshot.configuration_index;
    snap->n_bufs              = mlog->last_snapshot.n_bufs;
    snap->bufs
        = snap->n_bufs ? raft_calloc(snap->n_bufs, sizeof(*snap->bufs)) : NULL;
    // note: technically n_bufs is always 0 or 1 because of the way
    // memory_log_snapshot_put works
    for (unsigned i = 0; i < snap->n_bufs; i++) {
        snap->bufs[i].len  = mlog->last_snapshot.bufs[i].len;
        snap->bufs[i].base = raft_malloc(snap->bufs[i].len);
        memcpy(snap->bufs[i].base, mlog->last_snapshot.bufs[i].base,
               snap->bufs[i].len);
    }
    snap->configuration.n = mlog->last_snapshot.configuration.n;
    snap->configuration.servers
        = mlog->last_snapshot.configuration.n ? raft_calloc(
              snap->configuration.n, sizeof(*snap->configuration.servers))
                                              : NULL;
    for (unsigned i = 0; i < snap->configuration.n; i++) {
        snap->configuration.servers[i].id
            = mlog->last_snapshot.configuration.servers[i].id;
        snap->configuration.servers[i].role
            = mlog->last_snapshot.configuration.servers[i].role;
        snap->configuration.servers[i].address = raft_calloc(
            strlen(mlog->last_snapshot.configuration.servers[i].address) + 1,
            1);
        strcpy(snap->configuration.servers[i].address,
               mlog->last_snapshot.configuration.servers[i].address);
    }
    cb(req, snap, 0);
    return 0;
}

void mraft_memory_log_init(struct mraft_log* log)
{
    log->data = calloc(1, sizeof(struct memory_log));
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
}

void mraft_memory_log_finalize(struct mraft_log* log)
{
    struct memory_log* mlog = (struct memory_log*)log->data;
    for (unsigned i = 0; i < mlog->entries.count; i++) {
        free(mlog->entries.array[i].buf.base);
    }
    free(mlog->entries.array);
    for (unsigned i = 0; i < mlog->last_snapshot.n_bufs; i++) {
        free(mlog->last_snapshot.bufs[i].base);
    }
    free(mlog->last_snapshot.bufs);
    raft_configuration_close(&mlog->last_snapshot.configuration);
    free(mlog);
    memset(log, 0, sizeof(*log));
}

int mraft_memory_log_get_entries(struct mraft_log*   log,
                                 struct raft_entry** entries,
                                 unsigned*           n_entries)
{
    struct memory_log* mlog = (struct memory_log*)log->data;
    *entries                = mlog->entries.array;
    *n_entries              = mlog->entries.count;
#ifdef MRAFT_ENABLE_TESTS
    return MRAFT_SUCCESS;
#else
    return RAFT_NOTFOUND;
#endif
}