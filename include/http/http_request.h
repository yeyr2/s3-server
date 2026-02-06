#ifndef S3_HTTP_REQUEST_H
#define S3_HTTP_REQUEST_H

#include <string>
#include <vector>
#include <map>

namespace http {

struct HttpRequest {
    std::string method;              // GET, PUT, DELETE
    std::string path;                // URI 路径（不含 query），已规范化，如 /bucket 或 /bucket/obj
    std::string query;               // 原始 query 字符串（含 ? 后的部分，不含 ?）
    std::string host;
    std::string content_type;
    std::string content_md5;
    int64_t     content_length{-1};  // 请求体长度，-1 表示未给出

    // 从 query 字符串中按 key 取值（用于 AWSAccessKeyId, Signature, Expires 等）
    std::string get_query_param(const std::string& key) const;

    // 路径是否视为桶：路径以 / 结束或只有一层（废弃：使用新规则）
    bool is_bucket_path() const;
};

}

#endif
