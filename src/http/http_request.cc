#include "http/http_request.h"
#include <algorithm>
#include <cctype>
#include <cstdlib>

namespace http {

static bool is_path_sep(char c) { return c == '/'; }

// 对 query 参数值做 URL 解码
static std::string urldecode_param_value(const std::string& v) {
    std::string out;
    out.reserve(v.size());
    for (size_t i = 0; i < v.size(); ++i) {
        if (v[i] == '%' && i + 2 < v.size()) {
            int hi = 0, lo = 0;
            if (v[i+1] >= '0' && v[i+1] <= '9') hi = v[i+1] - '0';
            else if (v[i+1] >= 'A' && v[i+1] <= 'F') hi = v[i+1] - 'A' + 10;
            else if (v[i+1] >= 'a' && v[i+1] <= 'f') hi = v[i+1] - 'a' + 10;
            else { out += v[i]; continue; }
            if (v[i+2] >= '0' && v[i+2] <= '9') lo = v[i+2] - '0';
            else if (v[i+2] >= 'A' && v[i+2] <= 'F') lo = v[i+2] - 'A' + 10;
            else if (v[i+2] >= 'a' && v[i+2] <= 'f') lo = v[i+2] - 'a' + 10;
            else { out += v[i]; continue; }
            out += static_cast<char>((hi << 4) | lo);
            i += 2;
        } else {
            out += v[i];  // 保留 + 等字符（Base64 签名需要）
        }
    }
    return out;
}

// 废弃
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
            std::string k = urldecode_param_value(q.substr(pos, eq - pos));  // key 也仅 %XX 解码，与 value 一致
            std::string v = q.substr(eq + 1, end - eq - 1);
            if (k == key) return urldecode_param_value(v);
        }
        pos = end + (end < q.size() ? 1 : 0);
    }
    return {};
}

}
