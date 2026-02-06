#ifndef S3_AUTH_H
#define S3_AUTH_H

namespace http { struct HttpRequest; }
namespace s3config { struct Config; }
namespace meta { class MetaStore; }

namespace s3 {

// Query 签名验签。请求必须带 query 参数 AWSAccessKeyId, Signature, Expires。
// 先按 access_key 从 store 查用户密钥；若无则使用 config（管理员密钥）。校验 Expires 未过期。
// 返回 true 表示通过，false 表示 403。
bool verify_query_signature(const http::HttpRequest& req, const s3config::Config& config,
                            const meta::MetaStore& store);

} 

#endif
