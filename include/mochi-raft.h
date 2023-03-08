/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOCHI_RAFT_H
#define MOCHI_RAFT_H

#include <raft.h>
#include <margo.h>

#ifdef __cplusplus
extern "C" {
#endif

#define MRAFT_SUCCESS           0
#define MRAFT_ERR_INVALID_ARGS  1
#define MRAFT_ERR_FROM_MERCURY  2
#define MRAFT_ERR_FROM_ARGOBOTS 3
#define MRAFT_ERR_ID_USED       4


/**
 * @brief The mraft_log represents a custom implementation of a persistent log.
 * The function pointers have the same meaning as in the raft_io structure,
 * but their prototype may differ: asynchronous ones are made synchronous and
 * thus do not have a callback and request. Asynchrony is handled by Argobots
 * using ULTs posted to the pool provided to mraft_init.
 * (The exception is snapshot_get, which still takes a callback as arguments
 * so that the function can provide it with the snapshot).
 */
struct mraft_log {
    void* data;
    int (*load)(struct mraft_log*, raft_term*, raft_id*, struct raft_snapshot**, raft_index*, struct raft_entry*[], size_t*);
    int (*bootstrap)(struct mraft_log*, const struct raft_configuration*);
    int (*recover)(struct mraft_log*, const struct raft_configuration*);
    int (*set_term)(struct mraft_log*, raft_term);
    int (*set_vote)(struct mraft_log*, raft_id);
    int (*append)(struct mraft_log*, const struct raft_entry[], unsigned);
    int (*truncate)(struct mraft_log*, raft_index);
    int (*snapshot_put)(struct mraft_log*, unsigned, const struct raft_snapshot*);
    int (*snapshot_get)(struct mraft_log*, struct raft_io_snapshot_get* req, raft_io_snapshot_get_cb cb);
};

/**
 * @brief Structure containing all the arguments for initializing
 * a raft_io instance with a Mochi backend.
 */
struct mraft_init_args {
    margo_instance_id mid;  /* Margo instance */
    ABT_pool          pool; /* Pool in which to execute RPCs */
    uint16_t          id;   /* Provider id */
    struct mraft_log* log;  /* Log implementation */
};

/**
 * @brief Initialize a raft_io structure with a Mochi-based backend.
 *
 * @param [in] args Arguments.
 * @param [out] raft_io Instance to initialize.
 *
 * @return MRAFT_SUCCESS or other error code.
 */
int mraft_init(
    const struct mraft_init_args* args,
    struct raft_io* raft_io);

/**
 * @brief Finalize the raft_io structure. This function may be called
 * at any moment to remove the current process from the RAFT group
 * (the process will be considered dead by other processes). It will
 * be automatically called by Margo when Margo finalizes, if it hasn't
 * been called before.
 *
 * @param raft_io Instance to finalize.
 *
 * @return MRAFT_SUCCESS or other error code.
 */
int mraft_finalize(struct raft_io* raft_io);

#ifdef __cplusplus
}
#endif

#endif
