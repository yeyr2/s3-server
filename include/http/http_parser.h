#ifndef S3_HTTP_PARSER_H
#define S3_HTTP_PARSER_H

#include "http/http_request.h"

struct x_msg_t;
class x_buf_pool_t;

namespace http {

// 从 x_msg_t 中解析 HTTP 请求，填充 req。返回 true 表示解析成功（含完整请求头）。
bool parse_request(const x_msg_t& msg, HttpRequest& req);

// 规范化路径：去掉多余 /，禁止 ..
void normalize_path(std::string& path);

}

#endif
