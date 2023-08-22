/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#ifndef MRAFT_ABT_IO_LOG_H
#define MRAFT_ABT_IO_LOG_H

#include <mochi-raft.h>
#include <abt-io.h>
#include <margo.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    const char*        config;  /* JSON-formatted configuration string */
    abt_io_instance_id abtio;   /* ABT-IO instance */
    margo_instance_id  mid;     /* Margo instance */
} mraft_abt_io_log_args;

#define MRAFT_ABT_IO_LOG_ARGS_INIT {NULL, ABT_IO_INSTANCE_NULL, MARGO_INSTANCE_NULL}

/**
 * @brief Allocate the resources for a memory-based log
 * and set the log argument.
 *
 * If args is NULL or if args->abtio is set to ABT_IO_INSTANCE_NULL,
 * the log will create its own instance of ABT-IO, backed by one
 * execution stream.
 *
 * args->config may be NULL or contain a valid JSON object.
 * This object may have the following properties:
 * - "path": (string) directory that will contain the log.
 * - "max_entry_file_size": (int) maximum file size for entry files
 *   (may be exceeded if the size of an entry exceeds this size).
 * - "abt_io_thread_count": (int) number of ABT-IO execution streams
 *   (default to 1, ignored if providing an external ABT-IO instance).
 *
 * args->mid may be MARGO_INSTANCE_NULL or a valid margo instance.
 * At present, this margo instance is only used for tracing functions.
 *
 * @param [out] log Implementation.
 * @param [in] id Instance ID.
 * @param [in] args Optional configuration.
 */
int mraft_abt_io_log_init(struct mraft_log* log, raft_id id,
                           mraft_abt_io_log_args* args);

/**
 * @brief Free the resources of a memory-based log.
 *
 * @param [in] log Log to free.
 */
int mraft_abt_io_log_finalize(struct mraft_log* log);

#ifdef __cplusplus
}
#endif

#endif /* MRAFT_ABT_IO_LOG_H */
