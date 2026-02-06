#include "config/config.h"
#include "msg/msg_buffer4.h"
#include "http/http_parser.h"
#include "http/http_request.h"
#include "net/listener.h"
#include "net/connection.h"
#include "meta/meta.h"
#include "s3/auth.h"
#include "s3/handler.h"
#include "s3/response.h"
#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <thread>
#include <vector>
#include <memory>
#include <unistd.h>
#include <sys/stat.h>
#include <cerrno>
#include <cstring>

// 确保目录存在（含父级）：先检查是否已存在，再创建，用于 data_root
static bool ensure_dir_exists(const std::string& path) {
    if (path.empty()) return true;
    struct stat st;
    for (size_t i = 1; i < path.size(); ++i) {
        if (path[i] == '/') {
            std::string sub = path.substr(0, i);
            if (stat(sub.c_str(), &st) == 0) {
                if (S_ISDIR(st.st_mode)) continue;  // 已存在且为目录，跳过
            }
            if (mkdir(sub.c_str(), 0755) != 0 && errno != EEXIST) {
                std::cerr << "mkdir " << sub << " failed: " << strerror(errno) << std::endl;
                return false;
            }
        }
    }
    if (stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode))
        return true;  // 已存在且为目录
    if (mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
        std::cerr << "mkdir " << path << " failed: " << strerror(errno) << std::endl;
        return false;
    }
    return true;
}

static std::atomic<bool> g_shutdown_requested{false};

static void signal_handler(int) {
    g_shutdown_requested.store(true, std::memory_order_relaxed);
}

static void handle_client(int fd, x_buf_pool_t& pool, const s3config::Config& config, meta::MetaStore& store) {
    x_msg_t req_msg;
    int64_t content_length = -1;
    int n = net::read_request(fd, req_msg, pool, content_length);
    if (n <= 0) {
        net::close_fd(fd);
        return;
    }
    http::HttpRequest req;
    if (!http::parse_request(req_msg, req)) {
        x_msg_t resp_msg;
        s3::write_error_response(resp_msg, pool, 400, "BadRequest", "Invalid request");
        net::write_response(fd, resp_msg);
        net::close_fd(fd);
        return;  
    }
    // 显示 HTTP 请求（请求行 + 主要头）
    {
        std::string uri = req.path;
        if (!req.query.empty()) uri += "?" + req.query;
        std::cout << ">>> " << req.method << " " << uri;
        if (!req.host.empty()) std::cout << " Host: " << req.host;
        if (req.content_length >= 0) std::cout << " Content-Length: " << req.content_length;
        std::cout << std::endl;
    }
    if (!s3::verify_query_signature(req, config, store)) {
        x_msg_t resp_msg;
        s3::write_error_response(resp_msg, pool, 403, "AccessDenied", "Signature does not match");
        net::write_response(fd, resp_msg);
        net::close_fd(fd);
        return;
    }
    x_msg_t body_msg;
    const x_msg_t* body_ptr = nullptr;
    if (content_length > 0) {
        std::vector<char> linear(req_msg.total_length());
        req_msg.copy_out(linear.data(), req_msg.total_length());
        const char* end = std::strstr(linear.data(), "\r\n\r\n");
        uint32_t header_end = end ? static_cast<uint32_t>(end + 4 - linear.data()) : 0;
        if (header_end < req_msg.total_length() && static_cast<int64_t>(req_msg.total_length() - header_end) >= content_length) {
            body_msg.copy_in(pool, linear.data() + header_end, static_cast<uint32_t>(content_length));
            body_ptr = &body_msg;
        }
    }
    x_msg_t resp_msg;
    if (!s3::handle_request(req, config, store, resp_msg, pool, body_ptr)) {
        s3::write_error_response(resp_msg, pool, 503, "ServiceUnavailable", "Buffer pool exhausted");
    }
    net::write_response(fd, resp_msg);
    net::close_fd(fd);
}

int main() {
    s3config::Config config;
    s3config::load(config);
    meta::MetaStore store;
    if (!ensure_dir_exists(config.data_root)) {
        std::cerr << "cannot create data_root: " << config.data_root << std::endl;
        return 1;
    }
    if (!store.load(config.data_root)) {
        std::cerr << "meta load failed: data_root=" << config.data_root << std::endl;
        return 1;
    }
    // 先加入 root，再读取 user.dat
    store.ensure_root_user(config.access_key, config.secret_key);
    if (!store.load_user_dat()) {
        std::cerr << "meta load_user_dat failed" << std::endl;
        return 1;
    }
    if (!store.save()) {
        std::cerr << "meta save failed (user.dat): " << store.last_save_error() << std::endl;
        return 1;
    }
    x_buf_pool_t pool(config.buffer_payload_size, config.buffer_count);
    int listen_fd = net::listen_tcp(config.listen_addr, config.listen_port);
    if (listen_fd < 0) {
        std::cerr << "listen failed on " << config.listen_addr << ":" << config.listen_port << std::endl;
        return 1;
    }
    std::cout << "S3 server listening on " << config.listen_addr << ":" << config.listen_port
              << " data_root=" << config.data_root << std::endl;

    struct sigaction sa {};
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGTERM, &sa, nullptr);

    while (!g_shutdown_requested.load()) {
        int fd = net::accept_one(listen_fd);
        if (fd < 0) {
            if (g_shutdown_requested.load()) break;
            continue;
        }
        std::thread(handle_client, fd, std::ref(pool), std::cref(config), std::ref(store)).detach();
    }

    std::cout << "Shutting down: stopping accept, waiting for in-flight requests..." << std::endl;
    close(listen_fd);
    listen_fd = -1;
    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cout << "Server exited." << std::endl;
    return 0;
}
