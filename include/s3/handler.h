#ifndef S3_HANDLER_H
#define S3_HANDLER_H

struct x_msg_t;
class x_buf_pool_t;

namespace http { struct HttpRequest; }
namespace s3config { struct Config; }
namespace meta { class MetaStore; }

namespace s3 {

// 根据 req、config、meta 处理请求，将响应写入 out。使用 pool 分配缓冲。
// 返回 true 表示已写入响应，false 表示池耗尽等错误（调用方可返回 503）。
bool handle_request(const http::HttpRequest& req, const s3config::Config& config,
    meta::MetaStore& store, x_msg_t& out, x_buf_pool_t& pool, const x_msg_t* body_msg);

}

#endif
