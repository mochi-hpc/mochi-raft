#pragma once

#include <mochi-raft.hpp>

#include <map>
#include <string>
#include <mutex>
#include <optional>

// Key-value FSM for CLI testing.
// Entries are "PUT <key> <value>" strings.
class KeyValueFsm : public mraft::Fsm {
public:
    int apply(std::string_view data) override {
        std::string cmd(data);

        // Parse "PUT <key> <value>"
        if (cmd.size() < 5 || cmd.substr(0, 4) != "PUT ") {
            return 0; // Ignore malformed entries
        }

        auto rest = cmd.substr(4);
        auto sp = rest.find(' ');
        if (sp == std::string::npos) {
            return 0;
        }

        std::string key = rest.substr(0, sp);
        std::string value = rest.substr(sp + 1);

        std::lock_guard<std::mutex> lock(mutex_);
        store_[key] = value;
        return 0;
    }

    std::optional<std::string> get(const std::string& key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = store_.find(key);
        if (it != store_.end()) return it->second;
        return std::nullopt;
    }

private:
    mutable std::mutex mutex_;
    std::map<std::string, std::string> store_;
};
