/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */

#include "abt-io-log-helpers.h"
#include "abt-io-macros.h"

int _abt_io_error_handler(abt_io_instance_id abtio, int fd, int error_code)
{
    switch (error_code) {
    case ABT_IO_ERROR_OPEN:
        fprintf(stderr, "[error] failed abt_io_open\n");
        break;
    case ABT_IO_ERROR_PREAD:
        fprintf(stderr, "[error] failed abt_io_pread\n");
        break;
    case ABT_IO_ERROR_PWRITE:
        fprintf(stderr, "[error] failed abt_io_pwrite\n");
        break;
    case ABT_IO_ERROR_FTRUNCATE:
        fprintf(stderr, "[error] failed abt_io_ftruncate\n");
        break;
    case ABT_IO_ERROR_CLOSE:
        fprintf(stderr, "[error] failed abt_io_close\n");
        break;
    default:
        break;
    }
    return error_code;
}

#define DEFINE_METADATA_HANDLER(funcname, ABT_IO_MACRO)                        \
    int _##funcname##_metadata(abt_io_instance_id abtio, int fd,               \
                               raft_term* term, raft_id* voted_for,            \
                               size_t* current_entries,                        \
                               size_t* deleted_entries, size_t* n_entry_files) \
    {                                                                          \
        if (term != NULL) ABT_IO_MACRO(abtio, fd, term, sizeof(*term), 0);     \
        if (voted_for != NULL)                                                 \
            ABT_IO_MACRO(abtio, fd, voted_for, sizeof(*voted_for),             \
                         sizeof(*term));                                       \
        if (current_entries != NULL)                                           \
            ABT_IO_MACRO(abtio, fd, current_entries, sizeof(*current_entries), \
                         sizeof(*term) + sizeof(*voted_for));                  \
        if (deleted_entries != NULL)                                           \
            ABT_IO_MACRO(abtio, fd, deleted_entries, sizeof(*deleted_entries), \
                         sizeof(*term) + sizeof(*voted_for)                    \
                             + sizeof(*current_entries));                      \
        if (n_entry_files != NULL)                                             \
            ABT_IO_MACRO(abtio, fd, n_entry_files, sizeof(*n_entry_files),     \
                         sizeof(*term) + sizeof(*voted_for)                    \
                             + sizeof(*current_entries)                        \
                             + sizeof(*deleted_entries));                      \
                                                                               \
        return 0;                                                              \
    }

DEFINE_METADATA_HANDLER(read, ABT_IO_PREAD)
DEFINE_METADATA_HANDLER(write, ABT_IO_PWRITE)

#define DEFINE_ENTRY_HANDLER(funcname, ABT_IO_MACRO, INSERT_CODE)           \
    int _##funcname##_entry(abt_io_instance_id abtio, int fd, off_t offset, \
                            struct raft_entry* entry)                       \
    {                                                                       \
        ABT_IO_MACRO(abtio, fd, &entry->term, sizeof(entry->term), offset); \
        ABT_IO_MACRO(abtio, fd, &entry->type, sizeof(entry->type),          \
                     offset + sizeof(entry->term));                         \
        ABT_IO_MACRO(abtio, fd, &entry->buf.len, sizeof(entry->buf.len),    \
                     offset + sizeof(entry->term) + sizeof(entry->type));   \
        entry->batch = NULL;                                                \
                                                                            \
        /* Either for allocation, or writing filesize */                    \
        INSERT_CODE                                                         \
                                                                            \
        if (entry->buf.len > 0)                                             \
            ABT_IO_MACRO(abtio, fd, entry->buf.base, entry->buf.len,        \
                         offset + sizeof(entry->term) + sizeof(entry->type) \
                             + sizeof(entry->buf.len));                     \
                                                                            \
        /* Return the number of bytes read/written */                       \
        return sizeof(entry->term) + sizeof(entry->type)                    \
             + sizeof(entry->buf.len) + entry->buf.len;                     \
    }

// clang-format off
DEFINE_ENTRY_HANDLER(read,
                     ABT_IO_PREAD,
                     entry->buf.base = (entry->buf.len > 0) ? raft_malloc(entry->buf.len) : NULL;)
DEFINE_ENTRY_HANDLER(write,
                     ABT_IO_PWRITE,
                     off_t filesize = sizeof(entry->term) + sizeof(entry->type) + sizeof(entry->buf.len) + entry->buf.len + offset;
                     ABT_IO_PWRITE(abtio, fd, &filesize, sizeof(filesize), 0);)
// clang-format on

#define DEFINE_SNAPSHOT_HEADER_HANDLER(funcname, ABT_IO_MACRO)                 \
    static int _##funcname##_snapshot_header(abt_io_instance_id abtio, int fd, \
                                             struct raft_snapshot* snap)       \
    {                                                                          \
        ABT_IO_MACRO(abtio, fd, &snap->index, sizeof(snap->index), 0);         \
        ABT_IO_MACRO(abtio, fd, &snap->term, sizeof(snap->term),               \
                     sizeof(snap->index));                                     \
        ABT_IO_MACRO(abtio, fd, &snap->n_bufs, sizeof(snap->n_bufs),           \
                     sizeof(snap->index) + sizeof(snap->term));                \
        return 0;                                                              \
    }

DEFINE_SNAPSHOT_HEADER_HANDLER(read, ABT_IO_PREAD)
DEFINE_SNAPSHOT_HEADER_HANDLER(write, ABT_IO_PWRITE)

#define DEFINE_SNAPSHOT_BUFFERS_HANDLER(funcname, ABT_IO_MACRO, ALLOC_CODE) \
    static int _##funcname##_snapshot_buffers(abt_io_instance_id abtio,     \
                                              int fd, off_t offset,         \
                                              struct raft_snapshot* snap)   \
    {                                                                       \
        for (unsigned i = 0; i < snap->n_bufs; i++) {                       \
            struct raft_buffer* buf = &snap->bufs[i];                       \
            ABT_IO_MACRO(abtio, fd, &buf->len, sizeof(buf->len),            \
                         sizeof(snap->index) + sizeof(snap->term)           \
                             + sizeof(snap->n_bufs) + offset);              \
                                                                            \
            ALLOC_CODE                                                      \
                                                                            \
            if (buf->len > 0) {                                             \
                ABT_IO_MACRO(abtio, fd, buf->base, buf->len,                \
                             sizeof(snap->index) + sizeof(snap->term)       \
                                 + sizeof(snap->n_bufs) + sizeof(buf->len)  \
                                 + offset);                                 \
            }                                                               \
            offset += sizeof(buf->len) + buf->len;                          \
        }                                                                   \
        return offset;                                                      \
    }

// clang-format off
DEFINE_SNAPSHOT_BUFFERS_HANDLER(read,
                                ABT_IO_PREAD,
                                buf->base = (buf->len > 0) ? raft_malloc(buf->len) : NULL;)
DEFINE_SNAPSHOT_BUFFERS_HANDLER(write,
                                ABT_IO_PWRITE,
                                /* NULL */)
// clang-format on

#define DEFINE_SNAPSHOT_CONFIGURATION_HEADER_HANDLER(funcname, ABT_IO_MACRO)  \
    static int _##funcname##_snapshot_configuration_header(                   \
        abt_io_instance_id abtio, int fd, off_t offset,                       \
        struct raft_snapshot* snap)                                           \
    {                                                                         \
        ABT_IO_MACRO(abtio, fd, &snap->configuration_index,                   \
                     sizeof(snap->configuration_index),                       \
                     sizeof(snap->index) + sizeof(snap->term)                 \
                         + sizeof(snap->n_bufs) + offset);                    \
        ABT_IO_MACRO(                                                         \
            abtio, fd, &snap->configuration.n, sizeof(snap->configuration.n), \
            sizeof(snap->index) + sizeof(snap->term) + sizeof(snap->n_bufs)   \
                + offset + sizeof(snap->configuration_index));                \
        return 0;                                                             \
    }

DEFINE_SNAPSHOT_CONFIGURATION_HEADER_HANDLER(read, ABT_IO_PREAD)
DEFINE_SNAPSHOT_CONFIGURATION_HEADER_HANDLER(write, ABT_IO_PWRITE)

#define DEFINE_SNAPSHOT_CONFIGURATION_HANDLER(funcname, ABT_IO_MACRO,        \
                                              INSERT_CODE_1, INSERT_CODE_2)  \
    static int _##funcname##_snapshot_configuration(                         \
        abt_io_instance_id abtio, int fd, off_t offset,                      \
        struct raft_snapshot* snap)                                          \
    {                                                                        \
        for (unsigned i = 0; i < snap->configuration.n; i++) {               \
            struct raft_server* server = &snap->configuration.servers[i];    \
            ABT_IO_MACRO(abtio, fd, &server->id, sizeof(server->id),         \
                         sizeof(snap->index) + sizeof(snap->term)            \
                             + sizeof(snap->n_bufs) + offset                 \
                             + sizeof(snap->configuration_index)             \
                             + sizeof(snap->configuration.n));               \
            ABT_IO_MACRO(abtio, fd, &server->role, sizeof(server->role),     \
                         sizeof(snap->index) + sizeof(snap->term)            \
                             + sizeof(snap->n_bufs) + offset                 \
                             + sizeof(snap->configuration_index)             \
                             + sizeof(snap->configuration.n)                 \
                             + sizeof(server->id));                          \
                                                                             \
            INSERT_CODE_1                                                    \
                                                                             \
            ABT_IO_MACRO(abtio, fd, &addr_len, sizeof(addr_len),             \
                         sizeof(snap->index) + sizeof(snap->term)            \
                             + sizeof(snap->n_bufs) + offset                 \
                             + sizeof(snap->configuration_index)             \
                             + sizeof(snap->configuration.n)                 \
                             + sizeof(server->id) + sizeof(server->role));   \
                                                                             \
            INSERT_CODE_2                                                    \
                                                                             \
            if (addr_len > 0) {                                              \
                ABT_IO_MACRO(abtio, fd, server->address, addr_len,           \
                             sizeof(snap->index) + sizeof(snap->term)        \
                                 + sizeof(snap->n_bufs) + offset             \
                                 + sizeof(snap->configuration_index)         \
                                 + sizeof(snap->configuration.n)             \
                                 + sizeof(server->id) + sizeof(server->role) \
                                 + sizeof(addr_len));                        \
            }                                                                \
            offset += sizeof(server->id) + sizeof(server->role)              \
                    + sizeof(addr_len) + addr_len;                           \
        }                                                                    \
                                                                             \
        return 0;                                                            \
    }

// clang-format off
DEFINE_SNAPSHOT_CONFIGURATION_HANDLER(read,
                                      ABT_IO_PREAD,
                                      size_t addr_len;,
                                      server->address = (addr_len > 0) ? raft_calloc(addr_len + 1, sizeof(*server->address)) : NULL;)
DEFINE_SNAPSHOT_CONFIGURATION_HANDLER(write,
                                      ABT_IO_PWRITE,
                                      size_t addr_len = strlen(server->address);,
                                      /* NULL */)
// clang-format on

int _read_snapshot(abt_io_instance_id abtio, int fd, struct raft_snapshot* snap)
{
    _read_snapshot_header(abtio, fd, snap);

    snap->bufs   = (snap->n_bufs > 0)
                     ? raft_calloc(snap->n_bufs, sizeof(*snap->bufs))
                     : NULL;
    off_t offset = 0;
    int   ret    = _read_snapshot_buffers(abtio, fd, offset, snap);
    offset += ret;

    _read_snapshot_configuration_header(abtio, fd, offset, snap);
    snap->configuration.servers
        = (snap->configuration.n != 0) ? raft_calloc(
              snap->configuration.n, sizeof(*snap->configuration.servers))
                                       : NULL;
    _read_snapshot_configuration(abtio, fd, offset, snap);
    return 0;
}

int _write_snapshot(abt_io_instance_id    abtio,
                    int                   fd,
                    struct raft_snapshot* snap)
{
    _write_snapshot_header(abtio, fd, snap);

    off_t offset = 0;
    int   ret    = _write_snapshot_buffers(abtio, fd, offset, snap);
    offset += ret;

    _write_snapshot_configuration_header(abtio, fd, offset, snap);
    _write_snapshot_configuration(abtio, fd, offset, snap);
    return 0;
}

#define DEFINE_MAPPING_HANDLER(funcname, ABT_IO_MACRO)                        \
    int _##funcname##_mapping(abt_io_instance_id abtio, int fd, off_t offset, \
                              size_t* entry_file, off_t* entry_offset)        \
    {                                                                         \
        if (entry_file != NULL)                                               \
            ABT_IO_MACRO(abtio, fd, entry_file, sizeof(*entry_file), offset); \
        if (entry_offset != NULL)                                             \
            ABT_IO_MACRO(abtio, fd, entry_offset, sizeof(*entry_offset),      \
                         offset + sizeof(*entry_file));                       \
        return 0;                                                             \
    }

DEFINE_MAPPING_HANDLER(read, ABT_IO_PREAD)
DEFINE_MAPPING_HANDLER(write, ABT_IO_PWRITE)