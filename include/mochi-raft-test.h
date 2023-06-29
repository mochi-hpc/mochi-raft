/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MOCHI_RAFT_TEST_H
#define MOCHI_RAFT_TEST_H

#include <mochi-raft.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief This function is used for testing. Calling it with
 * dead=true will make the process stop answering RPCs, acting
 * as if the process was dead.
 *
 * @param raft_io Raft-IO structure
 * @param dead Whether to play dead or not
 *
 * @return 0 or error code.
 */
int mraft_io_simulate_dead(struct raft_io* raft_io, bool dead);

/**
 * @brief This function is used for testing. Reads the log entries from `log`
 * into the given parameters
 *
 * @param [in] mraft log that contains the entries we want to read from
 * @param [out] entries Pointer to store the log entries from `log`
 * @param [out] n_entries Pointer to store the number of in `log`
 *
 * @return 0 or error code.
 */
int mraft_memory_log_get_entries(struct mraft_log*   log,
                                 struct raft_entry** entries,
                                 unsigned*           n_entries);

#ifdef __cplusplus
}
#endif

#endif
