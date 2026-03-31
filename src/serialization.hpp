#pragma once

extern "C" {
#include <raft.h>
}

#include <cstring>
#include <string>
#include <vector>
#include <cstdint>

#include <cereal/types/string.hpp>
#include <cereal/types/vector.hpp>

namespace mraft {

// Serializable wrapper types for raft_message structs.
// Each type provides a cereal-compatible serialize() method and
// conversion to/from the c-raft C struct.

struct RequestVoteData {
    uint8_t version = 0;
    uint64_t term = 0;
    uint64_t candidate_id = 0;
    uint64_t last_log_index = 0;
    uint64_t last_log_term = 0;
    bool disrupt_leader = false;
    bool pre_vote = false;

    template <typename Archive>
    void serialize(Archive& ar) {
        ar(version, term, candidate_id, last_log_index, last_log_term,
           disrupt_leader, pre_vote);
    }

    static RequestVoteData from_raft(const struct raft_request_vote& rv) {
        RequestVoteData d;
        d.version = rv.version;
        d.term = rv.term;
        d.candidate_id = rv.candidate_id;
        d.last_log_index = rv.last_log_index;
        d.last_log_term = rv.last_log_term;
        d.disrupt_leader = rv.disrupt_leader;
        d.pre_vote = rv.pre_vote;
        return d;
    }

    void to_raft(struct raft_request_vote& rv) const {
        rv.version = version;
        rv.term = static_cast<raft_term>(term);
        rv.candidate_id = static_cast<raft_id>(candidate_id);
        rv.last_log_index = static_cast<raft_index>(last_log_index);
        rv.last_log_term = static_cast<raft_term>(last_log_term);
        rv.disrupt_leader = disrupt_leader;
        rv.pre_vote = pre_vote;
    }
};

struct RequestVoteResultData {
    uint8_t version = 0;
    uint64_t term = 0;
    bool vote_granted = false;
    bool pre_vote = false;
    uint16_t features = 0;
    uint16_t capacity = 0;

    template <typename Archive>
    void serialize(Archive& ar) {
        ar(version, term, vote_granted, pre_vote, features, capacity);
    }

    static RequestVoteResultData from_raft(
        const struct raft_request_vote_result& rv) {
        RequestVoteResultData d;
        d.version = rv.version;
        d.term = rv.term;
        d.vote_granted = rv.vote_granted;
        d.pre_vote = rv.pre_vote;
        d.features = rv.features;
        d.capacity = rv.capacity;
        return d;
    }

    void to_raft(struct raft_request_vote_result& rv) const {
        rv.version = version;
        rv.term = static_cast<raft_term>(term);
        rv.vote_granted = vote_granted;
        rv.pre_vote = pre_vote;
        rv.features = features;
        rv.capacity = capacity;
    }
};

// Serializable log entry (for AppendEntries RPC)
struct EntryData {
    uint64_t term = 0;
    uint8_t type = 0;
    std::vector<char> data;

    template <typename Archive>
    void serialize(Archive& ar) {
        ar(term, type, data);
    }

    static EntryData from_raft(const struct raft_entry& e) {
        EntryData d;
        d.term = e.term;
        d.type = static_cast<uint8_t>(e.type);
        if (e.buf.len > 0 && e.buf.base != nullptr) {
            d.data.assign(static_cast<char*>(e.buf.base),
                          static_cast<char*>(e.buf.base) + e.buf.len);
        }
        return d;
    }

    void to_raft(struct raft_entry& e) const {
        e.term = static_cast<raft_term>(term);
        e.type = static_cast<enum raft_entry_type>(type);
        if (!data.empty()) {
            e.buf.base = raft_malloc(data.size());
            memcpy(e.buf.base, data.data(), data.size());
            e.buf.len = data.size();
            e.batch = e.buf.base;
        } else {
            e.buf.base = nullptr;
            e.buf.len = 0;
            e.batch = nullptr;
        }
    }
};

struct AppendEntriesData {
    uint8_t version = 0;
    uint64_t term = 0;
    uint64_t prev_log_index = 0;
    uint64_t prev_log_term = 0;
    uint64_t leader_commit = 0;
    std::vector<EntryData> entries;

    template <typename Archive>
    void serialize(Archive& ar) {
        ar(version, term, prev_log_index, prev_log_term, leader_commit,
           entries);
    }

    static AppendEntriesData from_raft(
        const struct raft_append_entries& ae) {
        AppendEntriesData d;
        d.version = ae.version;
        d.term = ae.term;
        d.prev_log_index = ae.prev_log_index;
        d.prev_log_term = ae.prev_log_term;
        d.leader_commit = ae.leader_commit;
        for (unsigned i = 0; i < ae.n_entries; i++) {
            d.entries.push_back(EntryData::from_raft(ae.entries[i]));
        }
        return d;
    }

    void to_raft(struct raft_append_entries& ae) const {
        ae.version = version;
        ae.term = static_cast<raft_term>(term);
        ae.prev_log_index = static_cast<raft_index>(prev_log_index);
        ae.prev_log_term = static_cast<raft_term>(prev_log_term);
        ae.leader_commit = static_cast<raft_index>(leader_commit);
        ae.n_entries = static_cast<unsigned>(entries.size());
        if (!entries.empty()) {
            ae.entries = static_cast<struct raft_entry*>(
                raft_malloc(entries.size() * sizeof(struct raft_entry)));
            for (size_t i = 0; i < entries.size(); i++) {
                memset(&ae.entries[i], 0, sizeof(struct raft_entry));
                entries[i].to_raft(ae.entries[i]);
            }
        } else {
            ae.entries = nullptr;
        }
    }
};

struct AppendEntriesResultData {
    uint8_t version = 0;
    uint64_t term = 0;
    uint64_t rejected = 0;
    uint64_t last_log_index = 0;
    uint16_t features = 0;
    uint16_t capacity = 0;

    template <typename Archive>
    void serialize(Archive& ar) {
        ar(version, term, rejected, last_log_index, features, capacity);
    }

    static AppendEntriesResultData from_raft(
        const struct raft_append_entries_result& aer) {
        AppendEntriesResultData d;
        d.version = aer.version;
        d.term = aer.term;
        d.rejected = aer.rejected;
        d.last_log_index = aer.last_log_index;
        d.features = aer.features;
        d.capacity = aer.capacity;
        return d;
    }

    void to_raft(struct raft_append_entries_result& aer) const {
        aer.version = version;
        aer.term = static_cast<raft_term>(term);
        aer.rejected = static_cast<raft_index>(rejected);
        aer.last_log_index = static_cast<raft_index>(last_log_index);
        aer.features = features;
        aer.capacity = capacity;
    }
};

// Serializable server info for configuration within InstallSnapshot
struct ServerData {
    uint64_t id = 0;
    std::string address;
    int role = 0;

    template <typename Archive>
    void serialize(Archive& ar) {
        ar(id, address, role);
    }
};

struct InstallSnapshotData {
    uint8_t version = 0;
    uint64_t term = 0;
    uint64_t last_index = 0;
    uint64_t last_term = 0;
    std::vector<ServerData> conf_servers;
    uint64_t conf_index = 0;
    std::vector<char> data;

    template <typename Archive>
    void serialize(Archive& ar) {
        ar(version, term, last_index, last_term, conf_servers, conf_index,
           data);
    }

    static InstallSnapshotData from_raft(
        const struct raft_install_snapshot& is) {
        InstallSnapshotData d;
        d.version = is.version;
        d.term = is.term;
        d.last_index = is.last_index;
        d.last_term = is.last_term;
        d.conf_index = is.conf_index;

        for (unsigned i = 0; i < is.conf.n; i++) {
            ServerData s;
            s.id = is.conf.servers[i].id;
            s.address = is.conf.servers[i].address;
            s.role = is.conf.servers[i].role;
            d.conf_servers.push_back(s);
        }

        if (is.data.len > 0 && is.data.base != nullptr) {
            d.data.assign(static_cast<char*>(is.data.base),
                          static_cast<char*>(is.data.base) + is.data.len);
        }
        return d;
    }

    void to_raft(struct raft_install_snapshot& is) const {
        is.version = version;
        is.term = static_cast<raft_term>(term);
        is.last_index = static_cast<raft_index>(last_index);
        is.last_term = static_cast<raft_term>(last_term);
        is.conf_index = static_cast<raft_index>(conf_index);

        raft_configuration_init(&is.conf);
        for (auto& s : conf_servers) {
            raft_configuration_add(&is.conf, static_cast<raft_id>(s.id),
                                   s.address.c_str(), s.role);
        }

        if (!data.empty()) {
            is.data.base = raft_malloc(data.size());
            memcpy(is.data.base, data.data(), data.size());
            is.data.len = data.size();
        } else {
            is.data.base = nullptr;
            is.data.len = 0;
        }
    }
};

struct TimeoutNowData {
    uint8_t version = 0;
    uint64_t term = 0;
    uint64_t last_log_index = 0;
    uint64_t last_log_term = 0;

    template <typename Archive>
    void serialize(Archive& ar) {
        ar(version, term, last_log_index, last_log_term);
    }

    static TimeoutNowData from_raft(const struct raft_timeout_now& tn) {
        TimeoutNowData d;
        d.version = tn.version;
        d.term = tn.term;
        d.last_log_index = tn.last_log_index;
        d.last_log_term = tn.last_log_term;
        return d;
    }

    void to_raft(struct raft_timeout_now& tn) const {
        tn.version = version;
        tn.term = static_cast<raft_term>(term);
        tn.last_log_index = static_cast<raft_index>(last_log_index);
        tn.last_log_term = static_cast<raft_term>(last_log_term);
    }
};

// Top-level message wrapper that encapsulates any raft_message type
struct MessageData {
    uint8_t type = 0;         // raft_message_type
    uint64_t server_id = 0;   // sending/destination server ID
    std::string server_address;

    // Only one of these is populated, based on type
    RequestVoteData request_vote;
    RequestVoteResultData request_vote_result;
    AppendEntriesData append_entries;
    AppendEntriesResultData append_entries_result;
    InstallSnapshotData install_snapshot;
    TimeoutNowData timeout_now;

    template <typename Archive>
    void serialize(Archive& ar) {
        ar(type, server_id, server_address);
        switch (type) {
            case RAFT_REQUEST_VOTE:
                ar(request_vote);
                break;
            case RAFT_REQUEST_VOTE_RESULT:
                ar(request_vote_result);
                break;
            case RAFT_APPEND_ENTRIES:
                ar(append_entries);
                break;
            case RAFT_APPEND_ENTRIES_RESULT:
                ar(append_entries_result);
                break;
            case RAFT_INSTALL_SNAPSHOT:
                ar(install_snapshot);
                break;
            case RAFT_TIMEOUT_NOW:
                ar(timeout_now);
                break;
        }
    }

    static MessageData from_raft(const struct raft_message& msg) {
        MessageData d;
        d.type = static_cast<uint8_t>(msg.type);
        d.server_id = msg.server_id;
        d.server_address = msg.server_address ? msg.server_address : "";

        switch (msg.type) {
            case RAFT_REQUEST_VOTE:
                d.request_vote =
                    RequestVoteData::from_raft(msg.request_vote);
                break;
            case RAFT_REQUEST_VOTE_RESULT:
                d.request_vote_result =
                    RequestVoteResultData::from_raft(msg.request_vote_result);
                break;
            case RAFT_APPEND_ENTRIES:
                d.append_entries =
                    AppendEntriesData::from_raft(msg.append_entries);
                break;
            case RAFT_APPEND_ENTRIES_RESULT:
                d.append_entries_result =
                    AppendEntriesResultData::from_raft(
                        msg.append_entries_result);
                break;
            case RAFT_INSTALL_SNAPSHOT:
                d.install_snapshot =
                    InstallSnapshotData::from_raft(msg.install_snapshot);
                break;
            case RAFT_TIMEOUT_NOW:
                d.timeout_now =
                    TimeoutNowData::from_raft(msg.timeout_now);
                break;
        }
        return d;
    }

    void to_raft(struct raft_message& msg) const {
        memset(&msg, 0, sizeof(msg));
        msg.type = static_cast<enum raft_message_type>(type);
        msg.server_id = static_cast<raft_id>(server_id);
        // Note: server_address is set by the caller (network layer)
        // since it needs to point to stable storage

        switch (type) {
            case RAFT_REQUEST_VOTE:
                request_vote.to_raft(msg.request_vote);
                break;
            case RAFT_REQUEST_VOTE_RESULT:
                request_vote_result.to_raft(msg.request_vote_result);
                break;
            case RAFT_APPEND_ENTRIES:
                append_entries.to_raft(msg.append_entries);
                break;
            case RAFT_APPEND_ENTRIES_RESULT:
                append_entries_result.to_raft(msg.append_entries_result);
                break;
            case RAFT_INSTALL_SNAPSHOT:
                install_snapshot.to_raft(msg.install_snapshot);
                break;
            case RAFT_TIMEOUT_NOW:
                timeout_now.to_raft(msg.timeout_now);
                break;
        }
    }
};

// Per-entry metadata for the RDMA AppendEntries RPC (no inline payload).
struct RdmaEntryMeta {
    uint64_t term = 0;
    uint8_t  type = 0;
    uint64_t size = 0;  // bytes of this entry's data in the bulk

    template <typename Archive>
    void serialize(Archive& ar) { ar(term, type, size); }
};

// Arguments for the RDMA AppendEntries RPC.
// The actual entry data is transferred via the accompanying tl::bulk handle.
struct RdmaAppendEntriesArgs {
    uint64_t    server_id      = 0;
    std::string server_address;

    // AppendEntries header fields (no inline entries)
    uint8_t  version        = 0;
    uint64_t term           = 0;
    uint64_t prev_log_index = 0;
    uint64_t prev_log_term  = 0;
    uint64_t leader_commit  = 0;

    // Per-entry layout: tells the receiver how to split the bulk
    std::vector<RdmaEntryMeta> entries_meta;

    // Timing: sent_at is seconds since system_clock epoch, set right before
    // the RPC is issued so the receiver can compute remaining timeout.
    double sent_at   = 0;
    double timeout_s = 5.0;

    template <typename Archive>
    void serialize(Archive& ar) {
        ar(server_id, server_address,
           version, term, prev_log_index, prev_log_term, leader_commit,
           entries_meta, sent_at, timeout_s);
    }
};

// Helper to free entries allocated by to_raft for AppendEntries
inline void free_message_entries(struct raft_message& msg) {
    if (msg.type == RAFT_APPEND_ENTRIES && msg.append_entries.entries) {
        for (unsigned i = 0; i < msg.append_entries.n_entries; i++) {
            if (msg.append_entries.entries[i].buf.base) {
                raft_free(msg.append_entries.entries[i].buf.base);
            }
        }
        raft_free(msg.append_entries.entries);
        msg.append_entries.entries = nullptr;
        msg.append_entries.n_entries = 0;
    } else if (msg.type == RAFT_INSTALL_SNAPSHOT) {
        raft_configuration_close(&msg.install_snapshot.conf);
        if (msg.install_snapshot.data.base) {
            raft_free(msg.install_snapshot.data.base);
            msg.install_snapshot.data.base = nullptr;
        }
    }
}

} // namespace mraft
