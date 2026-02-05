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
    const char* c = code ? code : "Error";
    const char* m = message ? message : "";
    char body[640];
    char msg_esc[256];
    size_t j = 0;
    for (const char* p = m; *p && j < sizeof(msg_esc) - 1; ++p) {
        if (*p == '"') { msg_esc[j++] = '\\'; msg_esc[j++] = '"'; }
        else if (*p == '\\') { msg_esc[j++] = '\\'; msg_esc[j++] = '\\'; }
        else if (*p == '\n') { msg_esc[j++] = '\\'; msg_esc[j++] = 'n'; }
        else msg_esc[j++] = *p;
    }
    msg_esc[j] = '\0';
    int n = std::snprintf(body, sizeof(body), "{\"code\":0,\"Code\":\"%s\",\"Message\":\"%s\"}", c, msg_esc);
    if (n <= 0 || n >= (int)sizeof(body)) n = static_cast<int>(std::strlen(body));
    write_response(out, pool, status_code, status_phrase(status_code), body, static_cast<size_t>(n), "application/json");
}

void write_success_response(x_msg_t& out, x_buf_pool_t& pool, const char* json_body, size_t json_len) {
    if (!json_body || json_len == 0) {
        const char* def = "{\"code\":1}";
        json_body = def;
        json_len = std::strlen(def);
    }
    write_response(out, pool, 200, "OK", json_body, json_len, "application/json");
}

} // namespace s3
