#include "http/http_request.h"
#include <algorithm>
#include <cctype>

namespace http {

static bool is_path_sep(char c) { return c == '/'; }

bool HttpRequest::is_bucket_path() const {
    std::string p = path;
    while (!p.empty() && is_path_sep(p.back())) p.pop_back();
    if (p.empty()) return true;
    size_t first = 0;
    while (first < p.size() && is_path_sep(p[first])) ++first;
    if (first >= p.size()) return true;
    size_t slash = p.find('/', first);
    return slash == std::string::npos;  // 只有一层
}

std::string HttpRequest::get_query_param(const std::string& key) const {
    if (query.empty()) return {};
    std::string q = query;
    size_t pos = 0;
    while (pos < q.size()) {
        size_t amp = q.find('&', pos);
        size_t end = (amp == std::string::npos) ? q.size() : amp;
        size_t eq = q.find('=', pos);
        if (eq != std::string::npos && eq < end) {
            std::string k = q.substr(pos, eq - pos);
            std::string v = q.substr(eq + 1, end - eq - 1);
            if (k == key) return v;
        }
        pos = end + (end < q.size() ? 1 : 0);
    }
    return {};
}

} // namespace http
