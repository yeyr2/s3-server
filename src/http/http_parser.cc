#include "http/http_parser.h"
#include "msg/msg_buffer4.h"
#include <cstring>
#include <algorithm>
#include <cctype>
#include <string>
#include <vector>

namespace http {

void normalize_path(std::string& path) {
    std::string out;
    size_t i = 0;
    while (i < path.size()) {
        while (i < path.size() && path[i] == '/') ++i;
        if (i >= path.size()) break;
        size_t start = i;
        while (i < path.size() && path[i] != '/') ++i;
        std::string seg = path.substr(start, i - start);
        if (seg == ".") continue;
        if (seg == "..") {
            size_t last = out.rfind('/');
            if (last != std::string::npos) out.resize(last);
            else out.clear();
            continue;
        }
        if (!out.empty() && out.back() != '/') out += '/';
        out += seg;
    }
    if (out.empty()) out = "/";
    else if (out[0] != '/') out = "/" + out;
    path = std::move(out);
}

bool parse_request(const x_msg_t& msg, HttpRequest& req) {
    uint32_t len = msg.total_length();
    if (len == 0) return false;
    std::vector<char> buf(len + 1);
    uint32_t n = msg.copy_out(buf.data(), len);
    buf[n] = '\0';
    char* p = buf.data();
    char* end = p + n;

    // 请求行: METHOD SP URI SP HTTP/1.x
    char* line_end = static_cast<char*>(std::memchr(p, '\r', end - p));
    if (!line_end || line_end + 1 >= end || line_end[1] != '\n') return false;
    *line_end = '\0';
    std::string first_line(p);
    p = line_end + 2;

    size_t sp1 = first_line.find(' ');
    if (sp1 == std::string::npos) return false;
    req.method = first_line.substr(0, sp1);
    size_t sp2 = first_line.find(' ', sp1 + 1);
    if (sp2 == std::string::npos) return false;
    std::string uri = first_line.substr(sp1 + 1, sp2 - sp1 - 1);

    size_t qm = uri.find('?');
    if (qm != std::string::npos) {
        req.path = uri.substr(0, qm);
        req.query = uri.substr(qm + 1);
    } else {
        req.path = uri;
        req.query.clear();
    }
    normalize_path(req.path);

    // 请求头
    while (p < end) {
        line_end = static_cast<char*>(std::memchr(p, '\r', end - p));
        if (!line_end || line_end + 1 >= end) return false;
        if (line_end[1] != '\n') return false;
        *line_end = '\0';
        if (p[0] == '\0') { p = line_end + 2; break; }  // 空行，头结束
        char* colon = static_cast<char*>(std::memchr(p, ':', end - p));
        if (colon) {
            std::string key(p, colon - p);
            char* val_start = colon + 1;
            while (val_start < line_end && (*val_start == ' ' || *val_start == '\t')) ++val_start;
            std::string val(val_start, line_end - val_start);
            while (!val.empty() && (val.back() == ' ' || val.back() == '\t')) val.pop_back();
            if (key.size() == 4 && (key[0] == 'H' || key[0] == 'h') && (key[1] == 'o' || key[1] == 'O') && (key[2] == 's' || key[2] == 'S') && (key[3] == 't' || key[3] == 'T'))
                req.host = val;
            else if (key.size() == 12 && std::equal(key.begin(), key.end(), "Content-Type", [](char a, char b) { return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b)); }))
                req.content_type = val;
            else if (key.size() == 14 && std::equal(key.begin(), key.end(), "Content-MD5", [](char a, char b) { return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b)); }))
                req.content_md5 = val;
            else if (key.size() == 14 && std::equal(key.begin(), key.end(), "Content-Length", [](char a, char b) { return std::tolower(static_cast<unsigned char>(a)) == std::tolower(static_cast<unsigned char>(b)); })) {
                int64_t cl = 0;
                for (char c : val) { if (c >= '0' && c <= '9') cl = cl * 10 + (c - '0'); }
                req.content_length = cl;
            }
        }
        p = line_end + 2;
    }
    return true;
}

} // namespace http
