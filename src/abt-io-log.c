/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <abt-io.h>
#include <mochi-raft-abt-io-log.h>
#include <fcntl.h>
#include <stdlib.h>

#include "abt-io-macros.h"
#include "abt-io-log-helpers.h"

#define N_ENTRIES 100

#define FILENAME_LEN 40

#define ENTRY_FILE_MAX_SIZE (128 * 1024)

struct abt_io_log {
    raft_id            id;
    abt_io_instance_id aid;
};

static int abt_io_log_load(struct mraft_log*      log,
                           raft_term*             term,
                           raft_id*               voted_for,
                           struct raft_snapshot** snapshot,
                           raft_index*            start_index,
                           struct raft_entry*     entries[],
                           size_t*                n_entries)
{
    struct abt_io_log* abtlog = (struct abt_io_log*)log->data;
    int                fd;
    size_t             n_entry_files;
    char               filename[FILENAME_LEN];

    /* Read metadata (i.e. term, voted_for, n_entries, n_entry_files) */
    snprintf(filename, FILENAME_LEN, "metadata-%020llu", abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_RDONLY, &fd);
    _read_metadata(abtlog->aid, fd, term, voted_for, n_entries, &n_entry_files);
    ABT_IO_CLOSE(abtlog->aid, fd);
    *start_index = 1;

    /* Read entries */
    *entries = raft_malloc((*n_entries) * sizeof(**entries));
    size_t i = 0; /* index of entry in entries */
    for (size_t entry_file = 0; entry_file < n_entry_files; entry_file++) {
        snprintf(filename, FILENAME_LEN, "entry-%03lu-%020llu", entry_file,
                 abtlog->id);
        ABT_IO_OPEN(abtlog->aid, filename, O_RDONLY, &fd);

        /* First line of file is always the size of that file */
        off_t filesize;
        ABT_IO_PREAD(abtlog->aid, fd, &filesize, sizeof(filesize), 0);

        /* Read each entry until EOF */
        off_t offset = sizeof(filesize);
        while (offset < filesize) {
            struct raft_entry* entry = &(*entries)[i];
            entry->batch             = NULL;
            int ret = _read_entry(abtlog->aid, fd, offset, entry);
            i++;
            offset += ret;
        }
        ABT_IO_CLOSE(abtlog->aid, fd);
    }

    /* TODO: Deal with snapshots ???*/
    // Note: a snapshot is redundant in this memory log since we can
    // load all the entries directly from memory. In a real log, the
    // snapshot would contain whatever came before all the entries we
    // are loading.
    *snapshot = NULL;
#if 0
    /* Read last snapshot */
    snprintf(filename, FILENAME_LEN, "snp%020llu", abtlog->id);
    struct raft_snapshot* snap = raft_calloc(1, sizeof(*snap));
    ABT_IO_OPEN(abtlog->abtio, abtlog->snapshot, O_RDONLY, &fd);
    _read_snapshot(abtlog->abtio, fd, snap, NULL);
    ABT_IO_CLOSE(abtlog->abtio, fd);
    *snapshot = snap;
#endif
    return 0;
}

static int abt_io_log_bootstrap(struct mraft_log*                log,
                                const struct raft_configuration* conf)
{
    struct abt_io_log* abtlog = (struct abt_io_log*)log->data;
    int                fd;
    off_t              offset;
    char               filename[FILENAME_LEN];

    /* Persist the current term to 1 and the vote to nil */
    snprintf(filename, FILENAME_LEN, "metadata-%020llu", abtlog->id);
    static raft_term current_term  = 1;
    static raft_id   voted_for     = 0;
    static size_t    n_entries     = 1;
    static size_t    n_entry_files = 1;
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY, &fd);
    _write_metadata(abtlog->aid, fd, &current_term, &voted_for, &n_entries,
                    &n_entry_files);
    ABT_IO_CLOSE(abtlog->aid, fd);

    /* Persist the first entry with the default values */
    snprintf(filename, FILENAME_LEN, "entry-000-%020llu", abtlog->id);
    static struct raft_entry entry = {.batch = NULL,
                                      .term  = 1,
                                      .type  = RAFT_CHANGE,
                                      .buf   = {.len = 0, .base = NULL}};
    raft_configuration_encode(conf, &entry.buf);
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY, &fd);
    _write_entry(abtlog->aid, fd, sizeof(off_t), &entry);
    ABT_IO_CLOSE(abtlog->aid, fd);

    /* Map the entry to its entry_file and offset in it */
    snprintf(filename, FILENAME_LEN, "map-%020llu", abtlog->id);
    static size_t entry_number = 0;
    static size_t entry_file   = 0;
    static off_t  entry_offset = sizeof(off_t);
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY, &fd);
    _write_mapping(abtlog->aid, fd, entry_number, &entry_file, &entry_offset);
    ABT_IO_CLOSE(abtlog->aid, fd);

    /* TODO: Deal with snapshots ???*/
#if 0
    /* Persist last snapshot */
    snprintf(filename, FILENAME_LEN, "snp%020llu", abtlog->id);
    static struct raft_buffer   buffer = {.len = 0, .base = NULL};
    static struct raft_snapshot snap   = {.index               = 0,
                                          .term                = 1,
                                          .configuration_index = 0,
                                          .n_bufs              = 1,
                                          .bufs                = &buffer};
    raft_configuration_init(&snap.configuration);
    for (unsigned i = 0; i < conf->n; i++) {
        raft_configuration_add(&snap.configuration, conf->servers[i].id,
                               conf->servers[i].address, conf->servers[i].role);
    }
    raft_configuration_close(&snap.configuration);
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY, &fd);
    _write_snapshot(abtlog->aid, fd, &snap);
    ABT_IO_CLOSE(abtlog->aid, fd);
#endif
    return 0;
}

static int abt_io_log_recover(struct mraft_log*                log,
                              const struct raft_configuration* conf)
{
    // TODO:
    fprintf(stderr, "[abtlog] [error] abt_io_log_recover not implemented\n");
    return RAFT_NOTFOUND;
}

static int abt_io_log_set_term(struct mraft_log* log, raft_term term)
{
    struct abt_io_log* abtlog    = (struct abt_io_log*)log->data;
    raft_id            voted_for = 0;
    int                fd;
    char               filename[FILENAME_LEN];

    snprintf(filename, FILENAME_LEN, "metadata-%020llu", abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY, &fd);
    _write_metadata(abtlog->aid, fd, &term, &voted_for, NULL, NULL);
    abt_io_fdatasync(abtlog->aid, fd);
    ABT_IO_CLOSE(abtlog->aid, fd);

    return 0;
}

static int abt_io_log_set_vote(struct mraft_log* log, raft_id server_id)
{
    struct abt_io_log* abtlog = (struct abt_io_log*)log->data;
    int                fd;
    char               filename[FILENAME_LEN];

    snprintf(filename, FILENAME_LEN, "metadata-%020llu", abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY, &fd);
    _write_metadata(abtlog->aid, fd, NULL, &server_id, NULL, NULL);
    abt_io_fdatasync(abtlog->aid, fd);
    ABT_IO_CLOSE(abtlog->aid, fd);

    return 0;
}

static int abt_io_log_append(struct mraft_log*       log,
                             const struct raft_entry entries[],
                             unsigned                n)
{
    struct abt_io_log* abtlog = (struct abt_io_log*)log->data;
    int                metadata_fd;
    int                entry_fd;
    int                mapping_fd;
    char               filename[FILENAME_LEN];

    /* Get the current number of entries (n_entries) */
    snprintf(filename, FILENAME_LEN, "metadata-%020llu", abtlog->id);
    size_t n_entries;
    size_t n_entry_files;
    ABT_IO_OPEN(abtlog->aid, filename, O_RDWR, &metadata_fd);
    _read_metadata(abtlog->aid, metadata_fd, NULL, NULL, &n_entries,
                   &n_entry_files);

    snprintf(filename, FILENAME_LEN, "entry-%03lu-%020llu", n_entry_files - 1,
             abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_RDWR, &entry_fd);

    snprintf(filename, FILENAME_LEN, "map-%020llu", abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY, &mapping_fd);

    /* Read current entry file's size, "seek" until end of file */
    off_t offset;
    ABT_IO_PREAD(abtlog->aid, entry_fd, &offset, sizeof(offset), 0);
    for (unsigned i = 0; i < n; i++) {
        /* Check if there is enough room to write the entry to the file.*/
        const off_t entry_size
            = sizeof(entries[i].term) + sizeof(entries[i].type)
            + sizeof(entries[i].buf.len) + entries[i].buf.len;
        if (offset + entry_size > ENTRY_FILE_MAX_SIZE) {
            /* Size exceeded */
            /* Close old entry file */
            ABT_IO_CLOSE(abtlog->aid, entry_fd);

            /* Create new entry file */
            snprintf(filename, FILENAME_LEN, "entry-%03lu-%020llu",
                     n_entry_files, abtlog->id);

            /* Persist incremented number of entry files */
            n_entry_files++;
            _write_metadata(abtlog->aid, metadata_fd, NULL, NULL, NULL,
                            &n_entry_files);

            /* Open the new entry file, and write its size on the first line */
            offset = sizeof(offset);
            ABT_IO_OPEN(abtlog->aid, filename, O_RDWR | O_CREAT | O_TRUNC,
                        &entry_fd);
            ABT_IO_PWRITE(abtlog->aid, entry_fd, &offset, sizeof(offset), 0);
        }

        /* Entry offset in the current file */
        off_t entry_offset = offset;

        /* Write the entry to the entry file */
        int ret = _write_entry(abtlog->aid, entry_fd, offset,
                               (struct raft_entry*)&entries[i]);
        offset += ret;

        /* Write the mapping in the mapping file */
        size_t entry_file = n_entry_files - 1;
        _write_mapping(abtlog->aid, mapping_fd,
                       (n_entries - 1)
                           * (sizeof(entry_file) + sizeof(entry_offset)),
                       &entry_file, &entry_offset);

        /* Increment the number of entries */
        n_entries++;
        _write_metadata(abtlog->aid, metadata_fd, NULL, NULL, &n_entries, NULL);
    }
    ABT_IO_CLOSE(abtlog->aid, mapping_fd);
    ABT_IO_CLOSE(abtlog->aid, entry_fd);
    ABT_IO_CLOSE(abtlog->aid, metadata_fd);

    return 0;
}

static int abt_io_log_truncate(struct mraft_log* log, raft_index index)
{

    struct abt_io_log* abtlog = (struct abt_io_log*)log->data;
    int                fd;
    char               filename[FILENAME_LEN];

    snprintf(filename, FILENAME_LEN, "metadata-%020llu", abtlog->id);
    size_t n_entry_files;
    ABT_IO_OPEN(abtlog->aid, filename, O_RDONLY, &fd);
    _read_metadata(abtlog->aid, fd, NULL, NULL, NULL, &n_entry_files);
    ABT_IO_CLOSE(abtlog->aid, fd);

    /* Read the offset in the file */
    snprintf(filename, FILENAME_LEN, "map-%020llu", abtlog->id);
    size_t entry_file;
    off_t  entry_offset;
    ABT_IO_OPEN(abtlog->aid, filename, O_RDWR, &fd);
    _read_mapping(abtlog->aid, fd,
                  index * (sizeof(entry_file) + sizeof(entry_offset)),
                  &entry_file, &entry_offset);

    /* Truncate mapping file */
    ABT_IO_FTRUNCATE(abtlog->aid, fd,
                     index * (sizeof(entry_file) + sizeof(entry_offset)));
    ABT_IO_CLOSE(abtlog->aid, fd);

    /* Truncate the entry file */
    snprintf(filename, FILENAME_LEN, "entry-%03lu-%020llu", entry_file,
             abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY, &fd);
    ABT_IO_FTRUNCATE(abtlog->aid, fd, entry_offset);

    /* Write new truncated file size at head of entry file */
    ABT_IO_PWRITE(abtlog->aid, fd, &entry_offset, sizeof(entry_offset), 0);
    ABT_IO_CLOSE(abtlog->aid, fd);

    /* Delete extra entry files if needed */
    size_t deleted_files = 0;
    for (size_t i = entry_file + 1; i < n_entry_files; i++) {
        snprintf(filename, FILENAME_LEN, "entry-%03lu-%020llu", i, abtlog->id);
        abt_io_unlink(abtlog->aid, filename);
        deleted_files++;
    }

    /* Update n_entries and n_entry_files */
    snprintf(filename, FILENAME_LEN, "metadata-%020llu", abtlog->id);
    size_t n_entries = index;
    n_entry_files -= deleted_files;
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY, &fd);
    _write_metadata(abtlog->aid, fd, NULL, NULL, &n_entries, &n_entry_files);
    ABT_IO_CLOSE(abtlog->aid, fd);

    return 0;
}

static int abt_io_log_snapshot_put(struct mraft_log*           log,
                                   unsigned                    trailing,
                                   const struct raft_snapshot* snapshot)
{
    // TODO:
    fprintf(stderr, "[abtlog] [error] abt_io_log_recover not implemented\n");
    return RAFT_NOTFOUND;
}

static int abt_io_log_snapshot_get(struct mraft_log*            log,
                                   struct raft_io_snapshot_get* req,
                                   raft_io_snapshot_get_cb      cb)
{
    // TODO:
    fprintf(stderr, "[abtlog] [error] abt_io_log_recover not implemented\n");
    return RAFT_NOTFOUND;
}

void mraft_abt_io_log_init(struct mraft_log* log, raft_id id)
{
    /* Initialize ABT_IO */
    ABT_init(0, NULL);    /* TODO: Check what arguments to pass here*/
    abt_io_instance_id aid
        = abt_io_init(2); /* TODO: Check how many ES to create */

#define SET_FUNCTION(__name__) log->__name__ = abt_io_log_##__name__
    SET_FUNCTION(load);
    SET_FUNCTION(bootstrap);
    SET_FUNCTION(recover);
    SET_FUNCTION(set_term);
    SET_FUNCTION(set_vote);
    SET_FUNCTION(append);
    SET_FUNCTION(truncate);
    SET_FUNCTION(snapshot_put);
    SET_FUNCTION(snapshot_get);
#undef SET_FUNCTION

    struct abt_io_log* abtlog = calloc(1, sizeof(*abtlog));
    abtlog->aid               = aid;
    abtlog->id                = id;
    log->data                 = abtlog;

    /* Create each of the files */
    int    fd;
    int    flags = O_WRONLY | O_CREAT | O_EXCL;
    mode_t mode  = 0644;
    char   filename[FILENAME_LEN];

    /* Metadata file */
    snprintf(filename, FILENAME_LEN, "metadata-%020llu", id);
    fd = abt_io_open(aid, filename, flags, mode);
    abt_io_close(aid, fd);

    /* Entry file */
    snprintf(filename, FILENAME_LEN, "entry-000-%020llu", id);
    fd = abt_io_open(aid, filename, flags, mode);
    abt_io_close(aid, fd);

    /* Snapshot file */
    snprintf(filename, FILENAME_LEN, "snap-%020llu", id);
    fd = abt_io_open(aid, filename, flags, mode);
    abt_io_close(aid, fd);

    /* Entry map file */
    snprintf(filename, FILENAME_LEN, "map-%020llu", id);
    fd = abt_io_open(aid, filename, flags, mode);
    abt_io_close(aid, fd);
}

void mraft_abt_io_log_finalize(struct mraft_log* log)
{
    struct abt_io_log* abtlog = (struct abt_io_log*)log->data;
    char               filename[FILENAME_LEN];

    /* Read n_entry_files, then unlink metadata file */
    size_t n_entry_files;
    snprintf(filename, FILENAME_LEN, "metadata-%020llu", abtlog->id);
    int fd = abt_io_open(abtlog->aid, filename, O_RDONLY, 0644);
    _read_metadata(abtlog->aid, fd, NULL, NULL, NULL, &n_entry_files);
    abt_io_close(abtlog->aid, fd);
    // abt_io_unlink(abtlog->aid, filename);

    /* Entry files */
    for (size_t i = 0ULL; i < n_entry_files; i++) {
        snprintf(filename, FILENAME_LEN, "entry-%03lu-%020llu", i, abtlog->id);
        // abt_io_unlink(abtlog->aid, filename);
    }

    /* Snapshot file */
    snprintf(filename, FILENAME_LEN, "snap-%020llu", abtlog->id);
    // abt_io_unlink(abtlog->aid, filename);

    /* Entry map file */
    snprintf(filename, FILENAME_LEN, "map-%020llu", abtlog->id);
    // abt_io_unlink(abtlog->aid, filename);

    /* Finalize ABT_IO */
    abt_io_finalize(abtlog->aid);
    ABT_finalize();

    free(abtlog);
    memset(log, 0, sizeof(*log));

    // TODO: raft_configuration_close(&mlog->last_snapshot.configuration); ???
}