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

#ifdef __cplusplus
}
#endif

#endif
