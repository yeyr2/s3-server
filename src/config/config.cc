#include "config/config.h"
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <pwd.h>

namespace s3config {

// 获取环境变量，如果环境变量不存在，则返回默认值
static std::string getenv_default(const char* name, const char* def) {
    const char* v = std::getenv(name);
    return v && v[0] ? std::string(v) : (def ? std::string(def) : std::string());
}

// 将路径中的 ~ 展开为 HOME，便于 data_root 等正确打开文件
static std::string expand_tilde(std::string path) {
    if (path.empty() || path[0] != '~') return path;
    const char* home = std::getenv("HOME");
    if (!home || !*home) {
        struct passwd* pw = getpwuid(geteuid());
        home = pw && pw->pw_dir && pw->pw_dir[0] ? pw->pw_dir : nullptr;
    }
    if (!home || !*home) return path;
    if (path.size() == 1 || path[1] == '/')
        return std::string(home) + path.substr(1);  // ~ 或 ~/xxx
    return path;  // ~user 等不处理
}

// 端口号转换
static uint16_t parse_port(const char* s, uint16_t def) {
    if (!s || !*s) return def;
    int n = 0;
    while (*s >= '0' && *s <= '9') n = n * 10 + (*s++ - '0');
    if (*s || n <= 0 || n > 65535) return def;
    return static_cast<uint16_t>(n);
}

// 缓冲区大小转换
static uint32_t parse_uint(const char* s, uint32_t def) {
    if (!s || !*s) return def;
    uint32_t n = 0;
    while (*s >= '0' && *s <= '9') n = n * 10 + static_cast<uint32_t>(*s++ - '0');
    return *s ? def : n;
}

void load(Config& out) {
    out.data_root = expand_tilde(getenv_default("S3_DATA_ROOT", "~/s3data"));
    out.access_key = getenv_default("S3_ACCESS_KEY", "testkey");
    out.secret_key = getenv_default("S3_SECRET_KEY", "testsecret");
    out.listen_addr = getenv_default("S3_LISTEN_ADDR", "0.0.0.0");
    const std::string port_str = getenv_default("S3_LISTEN_PORT", "8080");
    out.listen_port = parse_port(port_str.c_str(), 8080);
    const std::string buf_size = getenv_default("S3_BUFFER_PAYLOAD_SIZE", "65536");
    out.buffer_payload_size = parse_uint(buf_size.c_str(), 65536);
    const std::string buf_count = getenv_default("S3_BUFFER_COUNT", "1024");
    out.buffer_count = parse_uint(buf_count.c_str(), 1024);
}

}
