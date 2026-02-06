#include "net/connection.h"
#include "msg/msg_buffer4.h"
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>
#include <cstring>
#include <vector>
#include <algorithm>
#include <strings.h>

namespace net {

static const size_t kMaxHeader = 65536;
// 限制 body 大小，防止 Content-Length 导致 OOM
static const int64_t kMaxContentLength = 1024 * 1024 * 1024 ;  // 1024MB


int read_request(int fd, x_msg_t& msg, x_buf_pool_t& pool, int64_t& content_length_out) {
    msg.clear();
    content_length_out = -1;
    std::vector<char> buf(4096);
    size_t total = 0;
    bool found_end = false;
    while (total < kMaxHeader) {
        ssize_t n = recv(fd, buf.data(), buf.size(), 0);
        if (n <= 0) return static_cast<int>(n);
        if (!msg.copy_in(pool, buf.data(), static_cast<uint32_t>(n)))
            return -1;
        total += static_cast<size_t>(n);
        // 查找 \r\n\r\n
        uint32_t len = msg.total_length();
        std::vector<char> linear(len + 1);
        msg.copy_out(linear.data(), len);
        linear[len] = '\0';
        const char* end = std::strstr(linear.data(), "\r\n\r\n");
        if (end) {
            found_end = true;
            break;
        }
        if (n < static_cast<ssize_t>(buf.size())) break;
    }
    if (!found_end) return total > 0 ? static_cast<int>(total) : -1;
    // 解析 Content-Length
    uint32_t len = msg.total_length();
    std::vector<char> linear(len + 1);
    msg.copy_out(linear.data(), len);
    linear[len] = '\0';
    const char* p = std::strstr(linear.data(), "\r\n\r\n");
    size_t header_len = p ? (p + 4 - linear.data()) : len;
    int64_t cl = -1;
    const char* cl_key = "Content-Length:";
    size_t cl_len = 15;
    p = linear.data();
    while (p + cl_len <= linear.data() + len) {
        if (strncasecmp(p, cl_key, cl_len) == 0) {
            p += cl_len;
            while (p < linear.data() + len && (*p == ' ' || *p == '\t')) ++p;
            int64_t v = 0;
            while (p < linear.data() + len && *p >= '0' && *p <= '9')
                v = v * 10 + (*p++ - '0');
            cl = v;
            break;
        }
        p = std::strchr(p, '\n');
        if (!p) break;
        ++p;
    }
    // 无 Content-Length 时视为无 body（如 GET）；仅当存在且非法或过大时拒绝
    if (cl >= 0 && cl > kMaxContentLength) {
        content_length_out = -1;
        return -1;  // Content-Length 过大，拒绝请求
    }
    content_length_out = (cl >= 0) ? cl : 0;
    if (cl > 0 && total < header_len + static_cast<size_t>(cl)) {
        size_t need = header_len + static_cast<size_t>(cl) - total;
        while (need > 0) {
            size_t to_read = std::min(need, buf.size());
            ssize_t n = recv(fd, buf.data(), to_read, 0);
            if (n <= 0) return static_cast<int>(n);
            if (!msg.copy_in(pool, buf.data(), static_cast<uint32_t>(n)))
                return -1;
            total += static_cast<size_t>(n);
            need -= static_cast<size_t>(n);
        }
    }
    return static_cast<int>(total);
}

int write_response(int fd, const x_msg_t& msg) {
    struct iovec iov[64];
    size_t n = msg.get_iovec(iov, 64);
    if (n == 0) return 0;
    ssize_t w = writev(fd, iov, static_cast<int>(n));
    return static_cast<int>(w);
}

void close_fd(int fd) {
    if (fd >= 0) close(fd);
}

} 
