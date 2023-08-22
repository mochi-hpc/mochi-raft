/*
 * (C) 2023 The University of Chicago
 *
 * See COPYRIGHT in top-level directory.
 */
#include <json-c/json.h>
#include <abt-io.h>
#include <mochi-raft-abt-io-log.h>
#include <fcntl.h>
#include <stdlib.h>

#include "abt-io-macros.h"
#include "abt-io-log-helpers.h"

#define ENTRY_FILE_MAX_SIZE (128 * 1024)

struct abt_io_log {
    raft_id            id;
    char*              path;
    abt_io_instance_id aid;
    margo_instance_id  mid;
    bool               owns_aid;
};

static int abt_io_log_load(struct mraft_log*      log,
                           raft_term*             term,
                           raft_id*               voted_for,
                           struct raft_snapshot** snapshot,
                           raft_index*            start_index,
                           struct raft_entry*     entries[],
                           size_t*                current_entries)
{
    struct abt_io_log* abtlog = (struct abt_io_log*)log->data;
    int                fd;
    size_t             n_entry_files;
    char               filename[PATH_MAX];

    /* Read metadata */
    snprintf(filename, PATH_MAX, "%s/metadata-%020llu", abtlog->path, abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_RDONLY, &fd);
    _read_metadata(abtlog->aid, fd, term, voted_for, current_entries, NULL,
                   &n_entry_files);
    ABT_IO_CLOSE(abtlog->aid, fd);
    *start_index = 1;

    /* Read entries */
    *entries = raft_malloc((*current_entries) * sizeof(**entries));
    size_t i = 0; /* index of entry in entries */
    for (size_t entry_file = 0; entry_file < n_entry_files; entry_file++) {
        snprintf(filename, PATH_MAX, "%s/entry-%03lu-%020llu", abtlog->path,
                 entry_file, abtlog->id);
        ABT_IO_OPEN(abtlog->aid, filename, O_RDONLY, &fd);

        /* First line of file is always the size of that file */
        off_t filesize;
        ABT_IO_PREAD(abtlog->aid, fd, &filesize, sizeof(filesize), 0);

        /* Read each entry until EOF */
        off_t offset = sizeof(filesize);
        while (offset < filesize) {
            struct raft_entry* entry = &(*entries)[i];
            int ret = _read_entry(abtlog->aid, fd, offset, entry);
            i++;
            offset += ret;
        }
        ABT_IO_CLOSE(abtlog->aid, fd);
    }

    /* Read last snapshot */
    *snapshot = NULL;
    snprintf(filename, PATH_MAX, "%s/snap-%020llu", abtlog->path, abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_RDONLY, &fd);

    /* Read first line of snapshot file to see if there is a snapshot to read */
    raft_index index;
    ssize_t read_size = abt_io_pread(abtlog->aid, fd, &index, sizeof(index), 0);
    if (read_size != sizeof(index)) goto end;

    struct raft_snapshot* snap = raft_calloc(1, sizeof(*snap));
    _read_snapshot(abtlog->aid, fd, snap);
    *snapshot = snap;

end:
    ABT_IO_CLOSE(abtlog->aid, fd);
    return 0;
}

static int abt_io_log_bootstrap(struct mraft_log*                log,
                                const struct raft_configuration* conf)
{
    struct abt_io_log* abtlog = (struct abt_io_log*)log->data;
    int                fd;
    off_t              offset;
    char               filename[PATH_MAX];

    /* Persist the current term to 1 and the vote to nil */
    snprintf(filename, PATH_MAX, "%s/metadata-%020llu", abtlog->path, abtlog->id);
    static raft_term current_term    = 1;
    static raft_id   voted_for       = 0;
    static size_t    current_entries = 1;
    static size_t    deleted_entries = 0;
    static size_t    n_entry_files   = 1;
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY | O_TRUNC, &fd);
    _write_metadata(abtlog->aid, fd, &current_term, &voted_for,
                    &current_entries, &deleted_entries, &n_entry_files);
    ABT_IO_CLOSE(abtlog->aid, fd);

    /* Persist the first entry with the default values */
    snprintf(filename, PATH_MAX, "%s/entry-000-%020llu", abtlog->path, abtlog->id);
    static struct raft_entry entry = {.batch = NULL,
                                      .term  = 1,
                                      .type  = RAFT_CHANGE,
                                      .buf   = {.len = 0, .base = NULL}};
    raft_configuration_encode(conf, &entry.buf);
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY | O_TRUNC, &fd);
    _write_entry(abtlog->aid, fd, sizeof(off_t), &entry);
    ABT_IO_CLOSE(abtlog->aid, fd);

    /* Map the entry to its entry_file and offset in it */
    snprintf(filename, PATH_MAX, "%s/map-%020llu", abtlog->path, abtlog->id);
    static size_t entry_number = 0;
    static size_t entry_file   = 0;
    static off_t  entry_offset = sizeof(off_t);
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY | O_TRUNC, &fd);
    _write_mapping(abtlog->aid, fd, entry_number, &entry_file, &entry_offset);
    ABT_IO_CLOSE(abtlog->aid, fd);
}

static int abt_io_log_recover(struct mraft_log*                log,
                              const struct raft_configuration* conf)
{
    // TODO:
    struct abt_io_log* abtlog = (struct abt_io_log*)log->data;
    return RAFT_NOTFOUND;
}

static int abt_io_log_set_term(struct mraft_log* log, raft_term term)
{
    struct abt_io_log* abtlog    = (struct abt_io_log*)log->data;
    raft_id            voted_for = 0;
    int                fd;
    char               filename[PATH_MAX];

    snprintf(filename, PATH_MAX, "%s/metadata-%020llu", abtlog->path, abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY, &fd);
    _write_metadata(abtlog->aid, fd, &term, &voted_for, NULL, NULL, NULL);
    abt_io_fdatasync(abtlog->aid, fd);
    ABT_IO_CLOSE(abtlog->aid, fd);

    return 0;
}

static int abt_io_log_set_vote(struct mraft_log* log, raft_id server_id)
{
    struct abt_io_log* abtlog = (struct abt_io_log*)log->data;
    int                fd;
    char               filename[PATH_MAX];

    snprintf(filename, PATH_MAX, "%s/metadata-%020llu", abtlog->path, abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY, &fd);
    _write_metadata(abtlog->aid, fd, NULL, &server_id, NULL, NULL, NULL);
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
    char               filename[PATH_MAX];

    /* Get the current number of entries (current_entries) */
    snprintf(filename, PATH_MAX, "%s/metadata-%020llu", abtlog->path, abtlog->id);
    size_t current_entries;
    size_t n_entry_files;
    ABT_IO_OPEN(abtlog->aid, filename, O_RDWR, &metadata_fd);
    _read_metadata(abtlog->aid, metadata_fd, NULL, NULL, &current_entries, NULL,
                   &n_entry_files);

    snprintf(filename, PATH_MAX, "%s/entry-%03lu-%020llu", abtlog->path,
             n_entry_files - 1, abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_RDWR, &entry_fd);

    snprintf(filename, PATH_MAX, "%s/map-%020llu", abtlog->path, abtlog->id);
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
            snprintf(filename, PATH_MAX, "%s/entry-%03lu-%020llu",
                     abtlog->path, n_entry_files, abtlog->id);

            /* Persist incremented number of entry files */
            n_entry_files++;
            _write_metadata(abtlog->aid, metadata_fd, NULL, NULL, NULL, NULL,
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
                       current_entries
                           * (sizeof(entry_file) + sizeof(entry_offset)),
                       &entry_file, &entry_offset);

        /* Increment the number of entries */
        current_entries++;
        _write_metadata(abtlog->aid, metadata_fd, NULL, NULL, &current_entries,
                        NULL, NULL);
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
    char               filename[PATH_MAX];

    snprintf(filename, PATH_MAX, "%s/metadata-%020llu", abtlog->path, abtlog->id);
    size_t n_entry_files;
    size_t deleted_entries;
    ABT_IO_OPEN(abtlog->aid, filename, O_RDONLY, &fd);
    _read_metadata(abtlog->aid, fd, NULL, NULL, NULL, &deleted_entries,
                   &n_entry_files);
    ABT_IO_CLOSE(abtlog->aid, fd);

    /* Read the offset in the file */
    snprintf(filename, PATH_MAX, "%s/map-%020llu", abtlog->path, abtlog->id);
    size_t entry_file;
    off_t  entry_offset;
    ABT_IO_OPEN(abtlog->aid, filename, O_RDWR, &fd);
    _read_mapping(abtlog->aid, fd,
                  (index - deleted_entries - 1)
                      * (sizeof(entry_file) + sizeof(entry_offset)),
                  &entry_file, &entry_offset);

    /* Truncate mapping file */
    ABT_IO_FTRUNCATE(abtlog->aid, fd,
                     (index - deleted_entries - 1)
                         * (sizeof(entry_file) + sizeof(entry_offset)));
    ABT_IO_CLOSE(abtlog->aid, fd);

    /* Truncate the entry file */
    snprintf(filename, PATH_MAX, "%s/entry-%03lu-%020llu", abtlog->path,
             entry_file, abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY, &fd);
    ABT_IO_FTRUNCATE(abtlog->aid, fd, entry_offset);

    /* Write new truncated file size at head of entry file */
    ABT_IO_PWRITE(abtlog->aid, fd, &entry_offset, sizeof(entry_offset), 0);
    ABT_IO_CLOSE(abtlog->aid, fd);

    /* Delete extra entry files if needed */
    size_t deleted_files = 0;
    for (size_t i = entry_file + 1; i < n_entry_files; i++) {
        snprintf(filename, PATH_MAX, "%s/entry-%03lu-%020llu", abtlog->path,
                 i, abtlog->id);
        abt_io_unlink(abtlog->aid, filename);
        deleted_files++;
    }

    /* Update current_entries and n_entry_files */
    snprintf(filename, PATH_MAX, "%s/metadata-%020llu", abtlog->path, abtlog->id);
    size_t current_entries = index - deleted_entries - 1;
    n_entry_files -= deleted_files;
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY, &fd);
    _write_metadata(abtlog->aid, fd, NULL, NULL, &current_entries, NULL,
                    &n_entry_files);
    ABT_IO_CLOSE(abtlog->aid, fd);

    return 0;
}

static int abt_io_log_snapshot_put(struct mraft_log*           log,
                                   unsigned                    trailing,
                                   const struct raft_snapshot* snapshot)
{
    struct abt_io_log* abtlog = (struct abt_io_log*)log->data;
    int                fd;
    char               filename[PATH_MAX];

    snprintf(filename, PATH_MAX, "%s/snap-%020llu", abtlog->path, abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY, &fd);
    _write_snapshot(abtlog->aid, fd, (struct raft_snapshot*)snapshot);
    ABT_IO_CLOSE(abtlog->aid, fd);

    if (trailing >= snapshot->index) return 0;

    /* Read metadata */
    size_t current_entries;
    size_t deleted_entries;
    size_t n_entry_files;
    snprintf(filename, PATH_MAX, "%s/metadata-%020llu", abtlog->path, abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_RDONLY, &fd);
    _read_metadata(abtlog->aid, fd, NULL, NULL, &current_entries,
                   &deleted_entries, &n_entry_files);
    ABT_IO_CLOSE(abtlog->aid, fd);

    /* If trailing is 0, we must delete all the entries in the file */
    if (trailing == 0) {
        size_t deleted_entries = snapshot->index;
        size_t current_entries = 0;
        size_t n_entry_files   = 1;
        ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY, &fd);
        _write_metadata(abtlog->aid, fd, NULL, NULL, &current_entries,
                        &deleted_entries, &n_entry_files);
        ABT_IO_CLOSE(abtlog->aid, fd);
    }

    if (current_entries < snapshot->index - deleted_entries - trailing)
        return 0;

    /* Read offset of last entry to store */
    unsigned n_entries
        = current_entries - (snapshot->index - deleted_entries - trailing);

    size_t entry_file;
    off_t  entry_offset;
    snprintf(filename, PATH_MAX, "%s/map-%020llu", abtlog->path, abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_RDONLY, &fd);
    int ret = _read_mapping(abtlog->aid, fd,
                            (snapshot->index - deleted_entries - trailing)
                                * (sizeof(size_t) + sizeof(off_t)),
                            &entry_file, &entry_offset);
    ABT_IO_CLOSE(abtlog->aid, fd);

    if (ret < 0 || n_entries == 0) {
        entry_file   = 0;
        entry_offset = ULLONG_MAX;
    }

    struct raft_entry* entries
        = (n_entries > 0) ? raft_malloc(n_entries * sizeof(*entries)) : NULL;

    /* Copy all entries more recent than snap->index - trailing, and delete all
     * older entries */
    size_t i = 0; /* index of entry */
    snprintf(filename, PATH_MAX, "%s/entry-%03lu-%020llu", abtlog->path, entry_file,
             abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_RDWR, &fd);

    /* First line of file is always the size of that file */
    off_t filesize;
    ABT_IO_PREAD(abtlog->aid, fd, &filesize, sizeof(filesize), 0);

    /* Read all entries from that given offset */
    off_t offset = entry_offset;
    if (n_entries > 0) {
        while (offset < filesize) {
            struct raft_entry* entry = &entries[i];
            int ret = _read_entry(abtlog->aid, fd, offset, entry);
            i++;
            offset += ret;
        }
    }

    /* Write the file as empty. We clear the file, then re write all the entries
     * into it */
    filesize = sizeof(off_t);
    ABT_IO_PWRITE(abtlog->aid, fd, &filesize, sizeof(filesize), 0);
    ABT_IO_FTRUNCATE(abtlog->aid, fd, sizeof(filesize));
    ABT_IO_CLOSE(abtlog->aid, fd);

    /* Truncate the mapping file */
    snprintf(filename, PATH_MAX, "%s/map-%020llu", abtlog->path, abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY, &fd);
    ABT_IO_FTRUNCATE(abtlog->aid, fd, 0);
    ABT_IO_CLOSE(abtlog->aid, fd);

    entry_file++;
    if (ret < 0) {
        for (; entry_file < n_entry_files; entry_file++) {
            /* Delete the extra entry file */
            snprintf(filename, PATH_MAX, "%s/entry-%03lu-%020llu", abtlog->path, entry_file,
                     abtlog->id);
            abt_io_unlink(abtlog->aid, filename);
        }
    } else {
        /* Read all the entries from the next entry_files */
        entry_file++;
        for (; entry_file < n_entry_files; entry_file++) {
            snprintf(filename, PATH_MAX, "%s/entry-%03lu-%020llu", abtlog->path, entry_file,
                     abtlog->id);
            ABT_IO_OPEN(abtlog->aid, filename, O_RDONLY, &fd);

            /* First line of file is always the size of that file */
            off_t filesize;
            ABT_IO_PREAD(abtlog->aid, fd, &filesize, sizeof(filesize), 0);

            /* Read each entry until EOF */
            off_t offset = sizeof(filesize);
            while (offset < filesize) {
                struct raft_entry* entry = &entries[i];
                int ret = _read_entry(abtlog->aid, fd, offset, entry);
                i++;
                offset += ret;
            }
            ABT_IO_CLOSE(abtlog->aid, fd);

            /* Delete the extra entry file */
            abt_io_unlink(abtlog->aid, filename);
        }
    }

    /* Update metadata now that we cleared all the entries */
    deleted_entries += current_entries - n_entries;
    current_entries = 0;
    n_entry_files   = 1;
    snprintf(filename, PATH_MAX, "%s/metadata-%020llu", abtlog->path, abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_WRONLY, &fd);
    _write_metadata(abtlog->aid, fd, NULL, NULL, &current_entries,
                    &deleted_entries, &n_entry_files);
    ABT_IO_CLOSE(abtlog->aid, fd);

    /* Now that we deleted the old entries, re write all the recent entries */
    /* Delete entries older than snapshot->index - trailing (included) */
    if (ret < 0) { return 0; }

    ret = abt_io_log_append(log, entries, n_entries);
    for (size_t j = 0; j < i; j++) {
        if (entries[j].buf.len > 0) raft_free(entries[j].buf.base);
    }
    raft_free(entries);
    return ret;
}

static int abt_io_log_snapshot_get(struct mraft_log*            log,
                                   struct raft_io_snapshot_get* req,
                                   raft_io_snapshot_get_cb      cb)
{
    struct abt_io_log* abtlog = (struct abt_io_log*)log->data;
    int                fd;
    char               filename[PATH_MAX];

    /* Read last snapshot */
    snprintf(filename, PATH_MAX, "%s/snap-%020llu", abtlog->path, abtlog->id);
    ABT_IO_OPEN(abtlog->aid, filename, O_RDONLY, &fd);

    /* Read first line of snapshot file to see if there is a snapshot to read */
    struct raft_snapshot* snapshot = NULL;
    raft_index            index;
    ssize_t read_size = abt_io_pread(abtlog->aid, fd, &index, sizeof(index), 0);
    if (read_size != sizeof(index)) goto end;

    struct raft_snapshot* snap = raft_calloc(1, sizeof(*snap));
    _read_snapshot(abtlog->aid, fd, snap);
end:
    ABT_IO_CLOSE(abtlog->aid, fd);
    cb(req, snap, 0);
    return 0;
}

/**
 * @brief Write default values in the abt-io-log files.
 * We need to do this because if a process doesn't call bootstrap, and then
 * gets added to a cluster, it will call mraft_start, which will call
 * log->load(). Then we will read from the files and get random values,
 * because those files are empty.
 *
 * We don't have this problem is the memory log because we call calloc, which
 * default initializes all values for us.
 *
 * We wan't this to write defaults to the file IF it doesn't already exist,
 * meaning that the process has just spawned and never existed before.
 * If a process crashed but it restarts with the same ID, we don't want to
 * overwrite its metadata file
 */
static int abt_io_write_default(struct abt_io_log* abtlog)
{
    int    fd;
    mode_t mode = 0644;
    char   filename[PATH_MAX];

    snprintf(filename, PATH_MAX, "%s/metadata-%020llu", abtlog->path, abtlog->id);
    fd = abt_io_open(abtlog->aid, filename, O_CREAT | O_EXCL, mode);
    if (fd < 0) {
        /* The file already existed, meaning the process had already called
         * mraft_start, or even maybe had some entries, so we don't do anything,
         * we keep whatever data the process had stored before its crash */
        margo_warning(abtlog->mid, "metadata file already existed, not writing defaults");
        return 0;
    }

    static raft_term current_term    = 0;
    static raft_id   voted_for       = 0;
    static size_t    current_entries = 0;
    static size_t    deleted_entries = 0;
    static size_t    n_entry_files   = 1;
    ABT_IO_OPEN(abtlog->aid, filename, O_CREAT | O_WRONLY, &fd);
    _write_metadata(abtlog->aid, fd, &current_term, &voted_for,
                    &current_entries, &deleted_entries, &n_entry_files);
    ABT_IO_CLOSE(abtlog->aid, fd);

    /* Persist that there are 0 entries in the first entry file */
    snprintf(filename, PATH_MAX, "%s/entry-000-%020llu", abtlog->path, abtlog->id);
    off_t filesize = sizeof(filesize);
    ABT_IO_OPEN(abtlog->aid, filename, O_CREAT | O_WRONLY, &fd);
    ABT_IO_PWRITE(abtlog->aid, fd, &filesize, sizeof(filesize), 0);
    ABT_IO_CLOSE(abtlog->aid, fd);
    return 0;
}

int mraft_abt_io_log_init(struct mraft_log* log, raft_id id, mraft_abt_io_log_args* args)
{
    abt_io_instance_id  aid    = args ? args->abtio : ABT_IO_INSTANCE_NULL;
    margo_instance_id   mid    = args ? args->mid : MARGO_INSTANCE_NULL;
    struct json_object* config = NULL;
    bool owns_aid              = false;
    char* path                 = NULL;
    size_t max_entry_file_size = 1024 * 1024; // 1 MB by default
    int abt_io_thread_count    = 1;

    // parse configuration
    {
        struct json_tokener*    tokener = json_tokener_new();
        enum json_tokener_error jerr;

        if (args && args->config && strlen(args->config) > 0) {
            config = json_tokener_parse_ex(tokener, args->config, strlen(args->config));
            if (!config) {
                jerr = json_tokener_get_error(tokener);
                margo_error(mid, "JSON parse error: %s",
                       json_tokener_error_desc(jerr));
                json_tokener_free(tokener);
                return MRAFT_ERR_INVALID_ARGS;
            }
            if(!json_object_is_type(config, json_type_object)) {
                margo_error(mid, "JSON configuration should contain an object");
                json_object_put(config);
                return MRAFT_ERR_INVALID_ARGS;
            }
        } else {
            config = json_object_new_object();
        }
        json_tokener_free(tokener);
    }

    // read the path where log files should be stored
    struct json_object* val = NULL;
    if((val = json_object_object_get(config, "path")) != NULL) {
        if(!json_object_is_type(val, json_type_string)) {
            margo_error(mid,
                "\"path\" should be of type string in MRaft log configuration");
            json_object_put(config);
            return MRAFT_ERR_INVALID_ARGS;
        }
        const char* str = json_object_get_string(val);
        if(json_object_get_string_len(val) == 0) {
            path = strdup(".");
        } else {
            path = strdup(str);
        }
    } else {
        path = strdup(".");
    }

    // read the max_entry_file_size
    if((val = json_object_object_get(config, "max_entry_file_size")) != NULL) {
        if(!json_object_is_type(val, json_type_int)) {
            margo_error(mid,
                "\"max_entry_file_size\" should be of type int in MRaft log configuration");
            json_object_put(config);
            return MRAFT_ERR_INVALID_ARGS;
        }
        int x = json_object_get_int(val);
        if(x <= 0) {
            margo_error(mid,
                "\"max_entry_file_size\" should be > 0 in MRaft log configuration");
            json_object_put(config);
            return MRAFT_ERR_INVALID_ARGS;
        }
        max_entry_file_size = (size_t)x;
    }

    // read the abt_io_thread_count
    if((val = json_object_object_get(config, "abt_io_thread_count")) != NULL) {
        if(!json_object_is_type(val, json_type_int)) {
            margo_error(mid,
                "\"abt_io_thread_count\" should be of type int in MRaft log configuration");
            json_object_put(config);
            return MRAFT_ERR_INVALID_ARGS;
        }
        int x = json_object_get_int(val);
        if(x <= 0) {
            margo_error(mid,
                "\"abt_io_thread_count\" should be > 0 in MRaft log configuration");
            json_object_put(config);
            return MRAFT_ERR_INVALID_ARGS;
        }
        abt_io_thread_count = x;
        if(aid != ABT_IO_INSTANCE_NULL) {
            margo_warning(mid,
                "\"abt_io_thread_count\" will be ignored because the caller specified"
                " an existing ABT-IO instance");
        }
    }

    if(aid == ABT_IO_INSTANCE_NULL) {
        aid = abt_io_init(abt_io_thread_count);
        owns_aid = true;
    }

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
    abtlog->mid               = mid;
    abtlog->owns_aid          = owns_aid;
    abtlog->path              = path;
    log->data                 = abtlog;

    /* Create each of the files if not already done */
    int    fd;
    int    flags = O_CREAT;
    mode_t mode  = 0644;
    char   filename[PATH_MAX];

    /* Metadata file */
    abt_io_write_default(abtlog);

    /* Entry file */
    snprintf(filename, PATH_MAX, "%s/entry-000-%020llu", abtlog->path, id);
    fd = abt_io_open(aid, filename, flags, mode);
    abt_io_close(aid, fd);

    /* Snapshot file */
    snprintf(filename, PATH_MAX, "%s/snap-%020llu", abtlog->path, id);
    fd = abt_io_open(aid, filename, flags, mode);
    abt_io_close(aid, fd);

    /* Entry map file */
    snprintf(filename, PATH_MAX, "%s/map-%020llu", abtlog->path, id);
    fd = abt_io_open(aid, filename, flags, mode);
    abt_io_close(aid, fd);

    json_object_put(config);

    return MRAFT_SUCCESS;
}

int mraft_abt_io_log_finalize(struct mraft_log* log)
{
    struct abt_io_log* abtlog = (struct abt_io_log*)log->data;
    char               filename[PATH_MAX];

    /* Read n_entry_files, then unlink metadata file */
    size_t n_entry_files;
    snprintf(filename, PATH_MAX, "%s/metadata-%020llu", abtlog->path, abtlog->id);
    int fd = abt_io_open(abtlog->aid, filename, O_RDONLY, 0644);
    _read_metadata(abtlog->aid, fd, NULL, NULL, NULL, NULL, &n_entry_files);
    abt_io_close(abtlog->aid, fd);

    /* Snapshot file */
    snprintf(filename, PATH_MAX, "%s/snap-%020llu", abtlog->path, abtlog->id);

    /* Entry map file */
    snprintf(filename, PATH_MAX, "%s/map-%020llu", abtlog->path, abtlog->id);

    /* Finalize ABT_IO */
    if(abtlog->owns_aid)
        abt_io_finalize(abtlog->aid);

    free(abtlog);
    memset(log, 0, sizeof(*log));

    MRAFT_SUCCESS;
}
