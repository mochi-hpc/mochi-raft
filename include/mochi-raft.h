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

/*
 * Important: the mochi-raft API (mraft_* functions) complement
 * the raft API but does not replace it. The user will need to be
 * familiar with the raft API, which can be found here:
 * https://github.com/canonical/raft/blob/master/include/raft.h
 */

#define MRAFT_SUCCESS           0
#define MRAFT_ERR_INVALID_ARGS  RAFT_INVALID
#define MRAFT_ERR_ID_USED       RAFT_DUPLICATEID
#define MRAFT_ERR_FROM_MERCURY  RAFT_IOERR
#define MRAFT_ERR_FROM_ARGOBOTS RAFT_IOERR

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
    int (*snapshot_get)(struct mraft_log*, struct raft_io_snapshot_get*, raft_io_snapshot_get_cb);
};

/**
 * @brief Structure containing all the arguments for initializing
 * a raft_io instance with a Mochi backend.
 */
struct mraft_io_init_args {
    margo_instance_id mid;  /* Margo instance */
    ABT_pool          pool; /* Pool in which to execute RPCs */
    uint16_t          id;   /* Provider id */
    struct mraft_log* log;  /* Log implementation */
};

/**
 * @brief Initialize a raft_io structure with a Mochi-based backend.
 *
 * Note: this function should be used when initializing a raft structure
 * using raft_init, which will expect a pointer to a struct raft_io instance.
 * The mraft_init function, on the other hand, will automatically call
 * mraft_io_init and therefore does not take a raft_io instance as argument.
 *
 * @param [in] args Arguments.
 * @param [out] raft_io Instance to initialize.
 *
 * @return MRAFT_SUCCESS or other error code.
 */
int mraft_io_init(
    const struct mraft_io_init_args* args,
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
int mraft_io_finalize(struct raft_io* raft_io);

/**
 * @see raft_init
 */
static inline int mraft_init(struct raft* r,
                             struct raft_io* io,
                             struct raft_fsm* fsm,
                             raft_id id,
                             const char* address)
{
    return raft_init(r, io, fsm, id, address);
}

/**
 * @brief Close the raft instance.
 *
 * Contrary to raft_close, which is non-blocking and takes
 * a callback that is called when the raft instance is closed,
 * mraft_close is blocking and will return only when the raft
 * instance has been closed.
 *
 * @param r Raft instance to close.
 */
void mraft_close(struct raft* r);

/**
 * @see raft_bootstrap.
 */
static inline int mraft_bootstrap(struct raft *r,
                                  const struct raft_configuration *conf)
{
    return raft_bootstrap(r, conf);
}


/**
 * @brief Declaration to avoid including ssg.h if mochi-raft has not been
 * build with ssg support.
 */
typedef uint64_t ssg_group_id_t;

/**
 * @brief Same as mraft_bootstrap but will take the addresses and ids
 * from the SSG group. If using this function, it is expected that
 * the address provided to mraft_init is the result of margo_self_addr,
 * and the ID is the result of ssg_get_self_id.
 *
 * @param r Raft instance to bootstrap.
 * @param gid Group ID.
 *
 * @return 0 or other MRAFT error codes.
 */
int mraft_bootstrap_from_ssg(struct raft* r,
                             ssg_group_id_t gid);

/**
 * @see raft_recover.
 */
static inline int mraft_recover(struct raft *r,
                                const struct raft_configuration *conf)
{
    return raft_recover(r, conf);
}

/**
 * @see raft_start.
 */
static inline int mraft_start(struct raft *r)
{
    return raft_start(r);
}

/**
 * @brief Apply a command (series of buffers) to the state machine.
 *
 * Contrary to raft_apply, this function (1) is blocking until the
 * command has been applied, and (2) will attempt to forward the
 * command to the leader if this process is not the leader.
 *
 * This function can still return RAFT_LEADERSHIPLOST if the calling
 * process does not know or could not reach the current leader (e.g.
 * if the system is in the middle of an election). In this case, the
 * caller must retry.
 *
 * @param r Raft instance.
 * @param bufs[] Buffers to send to the state machine.
 * @param n Number of buffers.
 *
 * @return 0 or other MRAFT error codes.
 */
int mraft_apply(struct raft *r,
                const struct raft_buffer bufs[],
                const unsigned n);

/**
 * @brief Propose to append a log entry of type RAFT_BARRIER.
 * This can be used to ensure that there are no unapplied commands.
 *
 * Contrary to raft_barrier, this function can be called by any
 * process, not just the leader, and it will block until the barrier
 * has been applied.
 *
 * This function can still return RAFT_LEADERSHIPLOST if the calling
 * process does not know or could not reach the current leader (e.g.
 * if the system is in the middle of an election). In this case, the
 * caller must retry.
 *
 * @param r Raft instance.
 *
 * @return 0 or other MRAFT error codes.
 */
int mraft_barrier(struct raft *r);

/**
 * @brief Add a new server to the cluster configuration.
 * Its initial role will be RAFT_SPARE.
 *
 * Similarly to mraft_apply, this function is blocking and can be
 * called by processes other than the leader.
 *
 * @param r Raft instance.
 * @param id Id of the new process.
 * @param address Address of the new process.
 *
 * @return 0 or other MRAFT error codes.
 */
int mraft_add(struct raft *r,
              raft_id id,
              const char *address);

/**
 * @brief Assign a new role to the given server.
 *
 * If the server has already the given role, or if the given role is unknown,
 * RAFT_BADROLE is returned.
 *
 * Similarly to mraft_apply, this function is blocking and can be
 * called by processes other than the leader.
 *
 * @param r Raft instance.
 * @param id Id of the process to change role.
 * @param role New role.
 *
 * @return 0 or other MRAFT error codes.
 */
int mraft_assign(struct raft *r,
                 raft_id id,
                 int role);

/**
 * @brief Remove a a process from the cluster.
 *
 * Similarly to mraft_apply, this function is blocking and can be
 * called by processes other than the leader.
 *
 * @param r Raft instance.
 * @param id Id of the process to remove.
 *
 * @return 0 or other MRAFT error codes.
 */
int mraft_remove(struct raft *r,
                 raft_id id);

/**
 * @brief Transfer leadership to the server with the given ID.
 * @see raft_transfer for more details.
 *
 * Similarly to mraft_apply, this function is blocking and can be
 * called by processes other than the leader.
 *
 * @param r Raft instance.
 * @param id Id of the process to remove.
 *
 * @return 0 or other MRAFT error codes.
 */
int mraft_transfer(struct raft *r,
                   raft_id id);


#ifdef __cplusplus
}
#endif

#endif
