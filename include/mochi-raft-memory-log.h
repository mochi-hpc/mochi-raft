/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MRAFT_MEMORY_LOG_H
#define MRAFT_MEMORY_LOG_H

#include <mochi-raft.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Allocate the resources for a memory-based log
 * and set the log argument.
 *
 * @param [out] log implementation.
 */
void mraft_memory_log_init(struct mraft_log* log);

/**
 * @brief Free the resources of a memory-based log.
 *
 * @param [in] log Log to free.
 */
void mraft_memory_log_finalize(struct mraft_log* log);

#ifdef __cplusplus
}
#endif

#endif
