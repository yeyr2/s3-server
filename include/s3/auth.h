#ifndef S3_AUTH_H
#define S3_AUTH_H

struct HttpRequest;

namespace s3config { struct Config; }

namespace s3 {

// Query 签名验签。请求必须带 query 参数 AWSAccessKeyId, Signature, Expires。
// 使用 config 中的 access_key/secret_key 校验，并校验 Expires 未过期。
// 返回 true 表示通过，false 表示 403。
bool verify_query_signature(const HttpRequest& req, const s3config::Config& config);

} // namespace s3

#endif
