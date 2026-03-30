#include "storage.hpp"

#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <cstring>
#include <cstdio>
#include <algorithm>
#include <stdexcept>

namespace mochi_raft {

// Little-endian encoding/decoding helpers
static void put64(uint8_t* buf, uint64_t v) {
    for (int i = 0; i < 8; i++) {
        buf[i] = static_cast<uint8_t>(v >> (i * 8));
    }
}

static uint64_t get64(const uint8_t* buf) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) {
        v |= static_cast<uint64_t>(buf[i]) << (i * 8);
    }
    return v;
}

Storage::Storage(abt_io_instance_id abt_io, const std::string& dir)
    : abt_io_(abt_io), dir_(dir) {}

Storage::~Storage() {
    if (open_fd_ >= 0) {
        abt_io_close(abt_io_, open_fd_);
        open_fd_ = -1;
    }
    for (auto& e : entry_cache_) {
        if (e.buf.base) raft_free(e.buf.base);
    }
}

int Storage::init() {
    struct stat st;
    if (stat(dir_.c_str(), &st) != 0) {
        if (mkdir(dir_.c_str(), 0755) != 0) {
            return RAFT_IOERR;
        }
    } else if (!S_ISDIR(st.st_mode)) {
        return RAFT_IOERR;
    }
    return 0;
}

int Storage::write_metadata_file(unsigned short index, const Metadata& meta) {
    uint8_t buf[METADATA_SIZE];
    put64(buf + 0, METADATA_FORMAT);
    put64(buf + 8, meta.version);
    put64(buf + 16, meta.term);
    put64(buf + 24, meta.voted_for);

    std::string path = dir_ + "/metadata" + std::to_string(index);
    int fd = abt_io_open(abt_io_, path.c_str(),
                         O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) return RAFT_IOERR;

    ssize_t nw = abt_io_pwrite(abt_io_, fd, buf, METADATA_SIZE, 0);
    if (nw != static_cast<ssize_t>(METADATA_SIZE)) {
        abt_io_close(abt_io_, fd);
        return RAFT_IOERR;
    }

    int rv = abt_io_fdatasync(abt_io_, fd);
    if (rv != 0) {
        abt_io_close(abt_io_, fd);
        return RAFT_IOERR;
    }

    abt_io_close(abt_io_, fd);
    return 0;
}

int Storage::read_metadata_file(unsigned short index, Metadata* meta) {
    std::string path = dir_ + "/metadata" + std::to_string(index);

    int fd = abt_io_open(abt_io_, path.c_str(), O_RDONLY, 0);
    if (fd < 0) {
        // File doesn't exist yet — not an error, just no metadata
        meta->version = 0;
        meta->term = 0;
        meta->voted_for = 0;
        return 0;
    }

    uint8_t buf[METADATA_SIZE];
    ssize_t nr = abt_io_pread(abt_io_, fd, buf, METADATA_SIZE, 0);
    abt_io_close(abt_io_, fd);

    if (nr != static_cast<ssize_t>(METADATA_SIZE)) {
        return RAFT_IOERR;
    }

    uint64_t format = get64(buf + 0);
    if (format != METADATA_FORMAT) {
        return RAFT_IOERR;
    }

    meta->version = get64(buf + 8);
    meta->term = static_cast<raft_term>(get64(buf + 16));
    meta->voted_for = static_cast<raft_id>(get64(buf + 24));
    return 0;
}

int Storage::load_metadata(raft_term* term, raft_id* voted_for) {
    Metadata meta1, meta2;

    int rv = read_metadata_file(1, &meta1);
    if (rv != 0) return rv;

    rv = read_metadata_file(2, &meta2);
    if (rv != 0) return rv;

    // Pick the one with the higher version
    if (meta1.version >= meta2.version) {
        metadata_ = meta1;
    } else {
        metadata_ = meta2;
    }

    *term = metadata_.term;
    *voted_for = metadata_.voted_for;
    return 0;
}

int Storage::set_term(raft_term term) {
    metadata_.term = term;
    metadata_.version++;

    // Odd version → file 1, even version → file 2
    unsigned short index = (metadata_.version % 2 == 1) ? 1 : 2;
    return write_metadata_file(index, metadata_);
}

int Storage::set_vote(raft_id voted_for) {
    metadata_.voted_for = voted_for;
    metadata_.version++;

    unsigned short index = (metadata_.version % 2 == 1) ? 1 : 2;
    return write_metadata_file(index, metadata_);
}

// --- Segment file helpers ---

std::string Storage::open_segment_path(uint64_t counter) {
    char buf[64];
    snprintf(buf, sizeof(buf), "open-%llu",
             static_cast<unsigned long long>(counter));
    return dir_ + "/" + buf;
}

std::string Storage::segment_path(raft_index first, raft_index last) {
    char buf[64];
    snprintf(buf, sizeof(buf), "%016llu-%016llu",
             static_cast<unsigned long long>(first),
             static_cast<unsigned long long>(last));
    return dir_ + "/" + buf;
}

int Storage::scan_segments(std::vector<SegmentInfo>& segments) {
    segments.clear();
    DIR* d = opendir(dir_.c_str());
    if (!d) return RAFT_IOERR;

    struct dirent* entry;
    while ((entry = readdir(d)) != nullptr) {
        std::string name = entry->d_name;

        // Closed segments: XXXXXXXXXXXXXXXXXX-XXXXXXXXXXXXXXXXX (16-1-16 chars)
        if (name.size() == 33 && name[16] == '-') {
            SegmentInfo seg;
            seg.first_index = strtoull(name.substr(0, 16).c_str(), nullptr, 10);
            seg.last_index = strtoull(name.substr(17, 16).c_str(), nullptr, 10);
            if (seg.first_index > 0 && seg.last_index >= seg.first_index) {
                seg.path = dir_ + "/" + name;
                segments.push_back(seg);
            }
        }
        // Open segments: open-N
        else if (name.substr(0, 5) == "open-") {
            uint64_t counter = strtoull(name.substr(5).c_str(), nullptr, 10);
            // We need to read the open segment to find its actual range
            // For now, store it with unknown bounds and fix during load
            SegmentInfo seg;
            seg.path = dir_ + "/" + name;
            seg.first_index = 0; // Will be determined during load
            seg.last_index = 0;
            // Track the counter for naming new segments
            if (counter >= open_counter_) {
                open_counter_ = counter + 1;
            }
            segments.push_back(seg);
        }
    }
    closedir(d);

    // Sort: closed segments by first_index, open segments at the end
    std::sort(segments.begin(), segments.end(),
              [](const SegmentInfo& a, const SegmentInfo& b) {
                  if (a.first_index == 0 && b.first_index == 0) return false;
                  if (a.first_index == 0) return false; // open segments go last
                  if (b.first_index == 0) return true;
                  return a.first_index < b.first_index;
              });

    return 0;
}

int Storage::load_segment(const SegmentInfo& seg,
                          std::vector<struct raft_entry>& entries) {
    int fd = abt_io_open(abt_io_, seg.path.c_str(), O_RDONLY, 0);
    if (fd < 0) return RAFT_IOERR;

    // Get file size
    struct stat st;
    if (fstat(fd, &st) != 0) {
        abt_io_close(abt_io_, fd);
        return RAFT_IOERR;
    }

    off_t offset = 0;
    while (offset < st.st_size) {
        // Read entry header
        uint8_t hdr[ENTRY_HEADER_SIZE];
        ssize_t nr = abt_io_pread(abt_io_, fd, hdr, ENTRY_HEADER_SIZE, offset);
        if (nr != static_cast<ssize_t>(ENTRY_HEADER_SIZE)) {
            // Incomplete header at end of file — stop reading
            break;
        }

        struct raft_entry e;
        memset(&e, 0, sizeof(e));
        e.term = static_cast<raft_term>(get64(hdr + 0));
        e.type = static_cast<enum raft_entry_type>(hdr[8]);
        uint64_t data_len = get64(hdr + 16);

        if (data_len > 0) {
            e.buf.base = raft_malloc(data_len);
            if (!e.buf.base) {
                abt_io_close(abt_io_, fd);
                return RAFT_NOMEM;
            }
            e.buf.len = data_len;

            nr = abt_io_pread(abt_io_, fd, e.buf.base,
                              data_len, offset + ENTRY_HEADER_SIZE);
            if (nr != static_cast<ssize_t>(data_len)) {
                raft_free(e.buf.base);
                abt_io_close(abt_io_, fd);
                return RAFT_IOERR;
            }
        } else {
            e.buf.base = nullptr;
            e.buf.len = 0;
        }
        e.batch = nullptr;

        entries.push_back(e);
        offset += ENTRY_HEADER_SIZE + align8(data_len);
    }

    abt_io_close(abt_io_, fd);
    return 0;
}

int Storage::ensure_open_segment() {
    if (open_fd_ >= 0) return 0;

    std::string path = open_segment_path(open_counter_);
    open_fd_ = abt_io_open(abt_io_, path.c_str(),
                           O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (open_fd_ < 0) return RAFT_IOERR;

    open_first_ = next_index_;
    open_last_ = 0;
    open_size_ = 0;
    return 0;
}

int Storage::close_current_segment() {
    if (open_fd_ < 0) return 0;

    abt_io_fdatasync(abt_io_, open_fd_);
    abt_io_close(abt_io_, open_fd_);
    open_fd_ = -1;

    if (open_last_ >= open_first_) {
        // Rename open segment to closed segment
        std::string old_path = open_segment_path(open_counter_);
        std::string new_path = segment_path(open_first_, open_last_);
        rename(old_path.c_str(), new_path.c_str());
    } else {
        // Empty segment — delete it
        std::string path = open_segment_path(open_counter_);
        unlink(path.c_str());
    }

    open_counter_++;
    return 0;
}

int Storage::append(raft_index first_index,
                    const struct raft_entry* entries, unsigned n) {
    if (n == 0) return 0;

    // Update next_index if needed (in case it wasn't set yet)
    if (first_index > 0) {
        next_index_ = first_index;
    }

    int rv = ensure_open_segment();
    if (rv != 0) return rv;

    for (unsigned i = 0; i < n; i++) {
        const struct raft_entry* e = &entries[i];

        // Encode entry header
        uint8_t hdr[ENTRY_HEADER_SIZE];
        memset(hdr, 0, ENTRY_HEADER_SIZE);
        put64(hdr + 0, e->term);
        hdr[8] = static_cast<uint8_t>(e->type);
        put64(hdr + 16, e->buf.len);

        // Write header
        ssize_t nw = abt_io_pwrite(abt_io_, open_fd_, hdr,
                                   ENTRY_HEADER_SIZE, open_size_);
        if (nw != static_cast<ssize_t>(ENTRY_HEADER_SIZE))
            return RAFT_IOERR;
        open_size_ += ENTRY_HEADER_SIZE;

        // Write data
        if (e->buf.len > 0) {
            nw = abt_io_pwrite(abt_io_, open_fd_, e->buf.base,
                               e->buf.len, open_size_);
            if (nw != static_cast<ssize_t>(e->buf.len))
                return RAFT_IOERR;
            open_size_ += align8(e->buf.len);
        }

        open_last_ = next_index_;
        next_index_++;

        // Rotate segment if it exceeds max size
        if (open_size_ >= SEGMENT_MAX_SIZE) {
            rv = close_current_segment();
            if (rv != 0) return rv;
            if (i + 1 < n) {
                rv = ensure_open_segment();
                if (rv != 0) return rv;
            }
        }
    }

    // Sync the data
    if (open_fd_ >= 0) {
        abt_io_fdatasync(abt_io_, open_fd_);
    }

    // Add to in-memory cache
    for (unsigned i = 0; i < n; i++) {
        const struct raft_entry* e = &entries[i];
        struct raft_entry cached;
        memset(&cached, 0, sizeof(cached));
        cached.term = e->term;
        cached.type = e->type;
        if (e->buf.len > 0 && e->buf.base) {
            cached.buf.base = raft_malloc(e->buf.len);
            memcpy(cached.buf.base, e->buf.base, e->buf.len);
            cached.buf.len = e->buf.len;
        }
        entry_cache_.push_back(cached);
    }

    return 0;
}

int Storage::truncate(raft_index from_index) {
    // Close current open segment to flush it
    int rv = close_current_segment();
    if (rv != 0) return rv;

    // Scan all segments (now all are closed)
    std::vector<SegmentInfo> segments;
    rv = scan_segments(segments);
    if (rv != 0) return rv;

    for (auto& seg : segments) {
        if (seg.first_index >= from_index) {
            // Entire segment is at or after truncation point — delete
            unlink(seg.path.c_str());
        } else if (seg.last_index >= from_index) {
            // Segment partially overlaps — load, keep entries before from_index
            std::vector<struct raft_entry> seg_entries;
            rv = load_segment(seg, seg_entries);
            if (rv != 0) return rv;

            unlink(seg.path.c_str());

            raft_index keep = from_index - seg.first_index;
            if (keep > 0) {
                next_index_ = seg.first_index;
                rv = append(seg.first_index, seg_entries.data(),
                            static_cast<unsigned>(keep));
                if (rv != 0) {
                    for (auto& e : seg_entries)
                        if (e.buf.base) raft_free(e.buf.base);
                    return rv;
                }
                close_current_segment();
            }

            for (auto& e : seg_entries) {
                if (e.buf.base) raft_free(e.buf.base);
            }
        }
        // else: segment entirely before from_index — keep it
    }

    next_index_ = from_index;
    return 0;
}

int Storage::load(raft_term* term, raft_id* voted_for,
                  raft_index* start_index,
                  struct raft_entry** entries, size_t* n_entries) {
    // Load metadata
    int rv = load_metadata(term, voted_for);
    if (rv != 0) return rv;

    // Scan and load segments
    std::vector<SegmentInfo> segments;
    rv = scan_segments(segments);
    if (rv != 0) return rv;

    std::vector<struct raft_entry> all_entries;

    // Determine start_index from segments
    raft_index seg_start = 0;
    for (auto& seg : segments) {
        if (seg.first_index > 0 && (seg_start == 0 || seg.first_index < seg_start)) {
            seg_start = seg.first_index;
        }
    }

    // Load entries from each segment in order
    raft_index current_index = seg_start > 0 ? seg_start : 1;
    for (auto& seg : segments) {
        if (seg.first_index == 0) {
            // Open segment — we need to determine its first_index
            // It follows after the last closed segment
            seg.first_index = current_index;
        }

        std::vector<struct raft_entry> seg_entries;
        rv = load_segment(seg, seg_entries);
        if (rv != 0) {
            // Free already loaded entries
            for (auto& e : all_entries)
                if (e.buf.base) raft_free(e.buf.base);
            return rv;
        }

        current_index = seg.first_index + seg_entries.size();
        all_entries.insert(all_entries.end(),
                          seg_entries.begin(), seg_entries.end());
    }

    // Set outputs
    *start_index = seg_start > 0 ? seg_start : 1;

    if (all_entries.empty()) {
        *entries = nullptr;
        *n_entries = 0;
    } else {
        // Allocate array with raft_malloc for c-raft compatibility
        *n_entries = all_entries.size();
        *entries = static_cast<struct raft_entry*>(
            raft_malloc(*n_entries * sizeof(struct raft_entry)));
        if (!*entries) {
            for (auto& e : all_entries)
                if (e.buf.base) raft_free(e.buf.base);
            return RAFT_NOMEM;
        }
        memcpy(*entries, all_entries.data(),
               *n_entries * sizeof(struct raft_entry));
    }

    // Update next_index for subsequent appends
    next_index_ = *start_index + all_entries.size();

    // Populate in-memory cache
    cache_start_index_ = *start_index;
    entry_cache_.clear();
    for (auto& e : all_entries) {
        struct raft_entry cached;
        memset(&cached, 0, sizeof(cached));
        cached.term = e.term;
        cached.type = e.type;
        if (e.buf.len > 0 && e.buf.base) {
            cached.buf.base = raft_malloc(e.buf.len);
            memcpy(cached.buf.base, e.buf.base, e.buf.len);
            cached.buf.len = e.buf.len;
        }
        entry_cache_.push_back(cached);
    }

    return 0;
}

int Storage::get_entries(raft_index first_index, unsigned n,
                        const struct raft_entry** entries) {
    if (n == 0) {
        *entries = nullptr;
        return 0;
    }

    if (first_index < cache_start_index_) return RAFT_IOERR;

    size_t offset = static_cast<size_t>(first_index - cache_start_index_);
    if (offset + n > entry_cache_.size()) return RAFT_IOERR;

    *entries = &entry_cache_[offset];
    return 0;
}

int Storage::bootstrap(const struct raft_configuration* conf) {
    // Serialize the configuration into a raft_buffer
    struct raft_buffer buf;
    unsigned int rv_conf = raft_configuration_encode(conf, &buf);
    if (rv_conf != 0) return rv_conf;

    // Create a RAFT_CHANGE entry with the serialized configuration
    struct raft_entry entry;
    memset(&entry, 0, sizeof(entry));
    entry.term = 1;
    entry.type = RAFT_CHANGE;
    entry.buf = buf;
    entry.batch = nullptr;

    // Write the first metadata (term=1, voted_for=0)
    int rv = set_term(1);
    if (rv != 0) {
        raft_free(buf.base);
        return rv;
    }

    // Write the entry
    rv = append(1, &entry, 1);
    raft_free(buf.base);
    if (rv != 0) return rv;

    // Close the segment
    return close_current_segment();
}

} // namespace mochi_raft
