#include "s3/response.h"
#include "msg/msg_buffer4.h"
#include <cstdio>
#include <cstring>

namespace s3 {

static const char* status_phrase(int code) {
    switch (code) {
        case 200: return "OK";
        case 204: return "No Content";
        case 403: return "Forbidden";
        case 404: return "Not Found";
        case 409: return "Conflict";
        case 503: return "Service Unavailable";
        default:  return "Unknown";
    }
}

void write_response(x_msg_t& out, x_buf_pool_t& pool, int status_code,
    const char* phrase, const char* body, size_t body_len,
    const char* content_type) {
    out.clear();
    char line[256];
    int n = std::snprintf(line, sizeof(line), "HTTP/1.1 %d %s\r\n", status_code, phrase ? phrase : status_phrase(status_code));
    if (n > 0 && n < (int)sizeof(line)) out.copy_in(pool, line, static_cast<uint32_t>(n));
    n = std::snprintf(line, sizeof(line), "Content-Length: %zu\r\n", body_len);
    if (n > 0 && n < (int)sizeof(line)) out.copy_in(pool, line, static_cast<uint32_t>(n));
    if (content_type && content_type[0]) {
        n = std::snprintf(line, sizeof(line), "Content-Type: %s\r\n", content_type);
        if (n > 0 && n < (int)sizeof(line)) out.copy_in(pool, line, static_cast<uint32_t>(n));
    }
    out.copy_in(pool, "\r\n", 2);
    if (body && body_len > 0) out.copy_in(pool, body, static_cast<uint32_t>(body_len));
}

void write_error_response(x_msg_t& out, x_buf_pool_t& pool, int status_code,
    const char* code, const char* message) {
    char body[512];
    int n = std::snprintf(body, sizeof(body),
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<Error><Code>%s</Code><Message>%s</Message></Error>",
        code ? code : "Error", message ? message : "");
    if (n <= 0 || n >= (int)sizeof(body)) n = static_cast<int>(std::strlen(body));
    write_response(out, pool, status_code, status_phrase(status_code), body, static_cast<size_t>(n), "application/xml");
}

} // namespace s3
