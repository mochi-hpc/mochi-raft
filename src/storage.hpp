#pragma once

extern "C" {
#include <raft.h>
}
#include <abt-io.h>
#include <string>
#include <cstdint>
#include <vector>

namespace mraft {

class Storage {
public:
    Storage(abt_io_instance_id abt_io, const std::string& dir);
    ~Storage();

    // Initialize storage (verify/create directory)
    int init();

    // Synchronous metadata persistence (must complete before sending messages)
    int set_term(raft_term term);
    int set_vote(raft_id voted_for);

    // Load metadata from disk (term + voted_for)
    int load_metadata(raft_term* term, raft_id* voted_for);

    // Load all persistent state at startup for the RAFT_START event.
    // Caller must free returned entries with raft_free().
    int load(raft_term* term, raft_id* voted_for,
             raft_index* start_index,
             struct raft_entry** entries, size_t* n_entries);

    // Append entries to the log (synchronous for now).
    // The storage takes ownership of entry buffer data.
    int append(raft_index first_index,
               const struct raft_entry* entries, unsigned n);

    // Truncate log from the given index onwards (inclusive).
    int truncate(raft_index from_index);

    // Get entries from the in-memory cache for AppendEntries messages.
    // Returns pointers into the cache; caller must NOT free them.
    // Returns 0 on success, or an error if entries are not in cache.
    int get_entries(raft_index first_index, unsigned n,
                    const struct raft_entry** entries);

    // Bootstrap: write initial configuration as the first log entry.
    int bootstrap(const struct raft_configuration* conf);

private:
    // Metadata
    static constexpr size_t METADATA_SIZE = 32;
    static constexpr uint64_t METADATA_FORMAT = 1;

    struct Metadata {
        uint64_t version = 0;
        raft_term term = 0;
        raft_id voted_for = 0;
    };

    int write_metadata_file(unsigned short index, const Metadata& meta);
    int read_metadata_file(unsigned short index, Metadata* meta);

    // Segment file entry format:
    //   [term:8][type:1][padding:7][data_len:8][data:data_len][padding to 8-byte align]
    static constexpr size_t ENTRY_HEADER_SIZE = 24; // 8 + 1 + 7 + 8
    static constexpr size_t SEGMENT_MAX_SIZE = 8 * 1024 * 1024; // 8MB

    struct SegmentInfo {
        std::string path;
        raft_index first_index;
        raft_index last_index;
    };

    int scan_segments(std::vector<SegmentInfo>& segments);
    int load_segment(const SegmentInfo& seg,
                     std::vector<struct raft_entry>& entries);
    int ensure_open_segment();
    int close_current_segment();
    std::string segment_path(raft_index first, raft_index last);
    std::string open_segment_path(uint64_t counter);

    static size_t align8(size_t n) { return (n + 7) & ~size_t(7); }

    abt_io_instance_id abt_io_;
    std::string dir_;
    Metadata metadata_;

    // In-memory entry cache for filling AppendEntries messages
    std::vector<struct raft_entry> entry_cache_;
    raft_index cache_start_index_ = 1; // Index of first entry in cache

    // Current append state
    raft_index next_index_ = 1;   // Next log index to assign
    int open_fd_ = -1;            // Current open segment fd
    uint64_t open_counter_ = 0;   // Counter for open segment naming
    raft_index open_first_ = 0;   // First index in current open segment
    raft_index open_last_ = 0;    // Last index in current open segment
    size_t open_size_ = 0;        // Bytes written to current segment
};

} // namespace mochi_raft
