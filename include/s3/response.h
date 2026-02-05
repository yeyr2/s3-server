#ifndef S3_RESPONSE_H
#define S3_RESPONSE_H

#include <cstddef>

struct x_msg_t;
class x_buf_pool_t;

namespace s3 {

// 组装 HTTP 响应到 out（清空后写入）。status_code 如 200, 204, 403, 404, 409, 503。
void write_response(x_msg_t& out, x_buf_pool_t& pool, int status_code,
    const char* status_phrase, const char* body, size_t body_len,
    const char* content_type = "application/xml");

// 错误体：JSON，含 code:0
void write_error_response(x_msg_t& out, x_buf_pool_t& pool, int status_code,
    const char* code, const char* message);

// 成功体：HTTP 200 + JSON（若 json_body 为空则写 {"code":1}）
void write_success_response(x_msg_t& out, x_buf_pool_t& pool, const char* json_body = nullptr, size_t json_len = 0);

} // namespace s3

#endif
