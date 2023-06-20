/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MEMORY_LOG_H
#define MEMORY_LOG_H

#include <mochi-raft.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Allocate a memory-based log.
 *
 * @return mraft_log implementation.
 */
struct mraft_log* memory_log_create();

/**
 * @brief Free a memory-based log.
 *
 * @param log Log to free.
 */
void memory_log_free(struct mraft_log* log);

/**
 * @brief Function used for testing only.
 * Reads the log entries from `log` into the given parameters
 */
void memory_log_get_entries(struct mraft_log*   log,
                            struct raft_entry** entries,
                            unsigned*           n_entries);

#ifdef __cplusplus
}
#endif

#endif
