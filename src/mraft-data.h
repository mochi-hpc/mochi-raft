/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MRAFT_DATA_H
#define MRAFT_DATA_H

#include "mochi-raft.h"

struct mraft_data {
    ABT_mutex mutex;
};

static inline void raft_lock(struct raft* r) {
    struct mraft_data* __data = (struct mraft_data*)((r)->data);
    ABT_mutex_lock(__data->mutex);
}

static inline void raft_unlock(struct raft* r) {
    struct mraft_data* __data = (struct mraft_data*)((r)->data);
    ABT_mutex_unlock(__data->mutex);
}

#endif
