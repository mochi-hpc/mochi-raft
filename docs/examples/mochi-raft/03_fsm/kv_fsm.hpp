#pragma once

#include <map>
#include <sstream>
#include <string>
#include <string_view>

#include <mochi-raft.hpp>

// A minimal key-value FSM that understands two plain-text commands:
//
//   SET <key> <value>   — insert or update a key
//   DEL <key>           — remove a key
//
// The payload format is intentionally simple; real applications would
// typically use a binary encoding such as Protocol Buffers or MessagePack.
class KvFsm : public mraft::Fsm {
public:
    int apply(std::string_view data) override {
        std::istringstream ss{std::string(data)};
        std::string op, key;
        ss >> op >> key;

        if (op == "SET") {
            std::string value;
            ss >> value;
            store_[key] = value;
        } else if (op == "DEL") {
            store_.erase(key);
        }
        // Unknown commands are silently ignored.
        return 0;
    }

    // Look up a key.  Not thread-safe with apply(); read only when quiesced.
    std::string get(const std::string& key) const {
        auto it = store_.find(key);
        return it != store_.end() ? it->second : std::string{};
    }

    const std::map<std::string, std::string>& store() const { return store_; }

private:
    std::map<std::string, std::string> store_;
};
