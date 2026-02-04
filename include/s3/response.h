#ifndef S3_RESPONSE_H
#define S3_RESPONSE_H

struct x_msg_t;
class x_buf_pool_t;

namespace s3 {

// 组装 HTTP 响应到 out（清空后写入）。status_code 如 200, 204, 403, 404, 409, 503。
void write_response(x_msg_t& out, x_buf_pool_t& pool, int status_code,
    const char* status_phrase, const char* body, size_t body_len,
    const char* content_type = "application/xml");

// 常用错误体（S3 风格短 XML）
void write_error_response(x_msg_t& out, x_buf_pool_t& pool, int status_code,
    const char* code, const char* message);

} // namespace s3

#endif
