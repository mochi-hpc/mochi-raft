/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#ifndef ABT_IO_LOG_HELPERS_H
#define ABT_IO_LOG_HELPERS_H

#ifdef __cplusplus
extern "C" {
#endif

#include <mochi-raft.h>
#include <abt-io.h>

/**
 * @brief Global temporary error handler, prints an error message and returns
 * the error code
 */
int _abt_io_error_handler(abt_io_instance_id abtio, int fd, int error_code);

/**
 * @brief Read from the metadata file identified by `fd`.
 * @param abtio ABT IO instance
 * @param fd The file descriptor of the metadata file
 * @param term If not NULL, Read the current term from the metadata file into
 * `term`
 * @param voted_for If not NULL, Read the current voted for from the metadata
 * file into `voted_for`
 * @param n_entries If not NULL, Read the number of entries from the metadata
 * file into `n_entries`
 * @param n_entry_files If not NULL, Read the number of entry files from the
 * metadata file into `n_entry_files`
 * @returns 0 on success, ABT_IO_PREAD_ERROR on failure
 */
int _read_metadata(abt_io_instance_id abtio,
                   int                fd,
                   raft_term*         term,
                   raft_id*           voted_for,
                   size_t*            n_entries,
                   size_t*            n_entry_files);

/**
 * @brief Write to the metadata file identified by `fd`.
 * @param abtio ABT IO instance
 * @param fd The file descriptor of the metadata file.
 * @param term If not NULL, write the current term to the metadata file.
 * @param voted_for If not NULL, write the current voted for to the metadata
 * file.
 * @param n_entries If not NULL, write the number of entries to the metadata
 * file.
 * @param n_entry_files If not NULL, write the number of entry files to the
 * metadata file.
 * @returns 0 on success, ABT_IO_PWRITE_ERROR on failure
 */
int _write_metadata(abt_io_instance_id abtio,
                    int                fd,
                    raft_term*         term,
                    raft_id*           voted_for,
                    size_t*            n_entries,
                    size_t*            n_entry_files);

/**
 * @brief Read the entry in the file identified by `fd` into `entry`
 * @param abtio ABT_IO instance ID
 * @param fd The file descriptor of the entry file
 * @param offset The offset of the entry in the file
 * @param entry The entry to read into
 * @returns The number of bytes read on success, ABT_IO_PREAD_ERROR on error
 */
int _read_entry(abt_io_instance_id abtio,
                int                fd,
                off_t              offset,
                struct raft_entry* entry);

/**
 * @brief Write the `entry` in the file identified by `fd`
 * @param abtio ABT_IO instance ID
 * @param fd The file descriptor of the entry file
 * @param offset The offset of the entry in the file
 * @param entry The entry to read into
 * @returns The number of bytes written on success, ABT_IO_PWRITE_ERROR on error
 */
int _write_entry(abt_io_instance_id abtio,
                 int                fd,
                 off_t              offset,
                 struct raft_entry* entry);

/**
 * @brief Read the snapshot in the file identified by `fd` into `snapshot`
 * @param abtio ABT_IO instance ID
 * @param fd The file descriptor of the snapshot file
 * @param snap The snapshot to read into
 * @returns 0 on success, ABT_IO_PREAD_ERROR on failure.
 */
int _read_snapshot(abt_io_instance_id               abtio,
                   int                              fd,
                   struct raft_snapshot*            snap);

/**
 * @brief Write `snapshot` in the file identified by `fd`
 * @param abtio ABT_IO instance ID
 * @param fd The file descriptor of the snapshot file
 * @param snap The snapshot to write to the file
 * @returns 0 on success, ABT_IO_PREAD_ERROR on failure.
 */
int _write_snapshot(abt_io_instance_id               abtio,
                    int                              fd,
                    struct raft_snapshot*            snap);

/**
 * @brief  Read the entry mapping at `offset`
 * @param abtio ABT_IO instance ID
 * @param fd The file descriptor of the mapping file
 * @param offset The offset of the entry we are reading in the file
 * @param entry_file Read the entry file into `entry_file`
 * @param entry_offset Read the entry offset into `entry_offset`
 * @returns 0 on success, ABT_IO_PREAD_ERROR on failure.
 */
int _read_mapping(abt_io_instance_id abtio,
                  int                fd,
                  off_t              offset,
                  size_t*            entry_file,
                  off_t*             entry_offset);

/**
 * @brief  Write the entry mapping at `offset`
 * @param abtio ABT_IO instance ID
 * @param fd The file descriptor of the mapping file
 * @param offset The offset of the entry we are writing in the file
 * @param entry_file The entry file we map the entry to
 * @param entry_offset The entry offset we map the entry to
 * @returns 0 on success, ABT_IO_PWRITE_ERROR on failure.
 */
int _write_mapping(abt_io_instance_id abtio,
                   int                fd,
                   off_t              offset,
                   size_t*            entry_file,
                   off_t*             entry_offset);

#ifdef __cplusplus
}
#endif

#endif /* ABT_IO_LOG_HELPERS_H */