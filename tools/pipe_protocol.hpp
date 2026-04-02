#pragma once

#include <string>
#include <vector>
#include <sstream>

namespace proto {

// Split a line into whitespace-separated tokens.
// The last token may contain spaces if max_tokens is set
// (e.g., "PUT key value with spaces" with max_tokens=3 → ["PUT","key","value with spaces"]).
inline std::vector<std::string> split(const std::string& line,
                                       size_t max_tokens = 0) {
    std::vector<std::string> tokens;
    size_t pos = 0;

    while (pos < line.size()) {
        // Skip leading whitespace
        while (pos < line.size() && line[pos] == ' ') pos++;
        if (pos >= line.size()) break;

        // If we've reached max_tokens-1, take the rest as one token
        if (max_tokens > 0 && tokens.size() == max_tokens - 1) {
            tokens.push_back(line.substr(pos));
            break;
        }

        // Find end of token
        size_t end = line.find(' ', pos);
        if (end == std::string::npos) {
            tokens.push_back(line.substr(pos));
            break;
        }
        tokens.push_back(line.substr(pos, end - pos));
        pos = end + 1;
    }

    return tokens;
}

// Format a response line: "OK" or "OK <data>" or "ERR <msg>"
inline std::string ok() { return "OK"; }
inline std::string ok(const std::string& data) { return "OK " + data; }
inline std::string err(const std::string& msg) { return "ERR " + msg; }

} // namespace proto
