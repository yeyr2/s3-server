#include "s3/handler.h"
#include "s3/response.h"
#include "config/config.h"
#include "http/http_request.h"
#include "msg/msg_buffer4.h"
#include "io_uring/file_io.h"
#include "meta/meta.h"
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <ctime>
#include <vector>
#include <cerrno>
#include <cstdio>
#include <iostream>

namespace s3 {

// URL 按操作前缀区分：/getBucket/、/getObject/、/deleteBucket/、/deleteObject/、/createBucket/、/createObject/
enum class PathAction { None, GetBucket, GetObject, DeleteBucket, DeleteObject, CreateBucket, CreateObject };

// 解析路径前缀与后续 bucket_name、object_key。path 已规范化（如 /getBucket/my-bucket、/getObject/bucket/key）
static PathAction parse_action_path(const std::string& path, std::string& bucket_name, std::string& object_key) {
    bucket_name.clear();
    object_key.clear();
    std::string p = path;
    while (!p.empty() && p[0] == '/') p.erase(0, 1);
    if (p.empty()) return PathAction::None;

    auto strip_prefix = [&p](const char* prefix) -> bool {
        size_t len = std::strlen(prefix);
        if (p.size() >= len && p.compare(0, len, prefix) == 0) {
            p.erase(0, len);
            while (!p.empty() && p[0] == '/') p.erase(0, 1);
            return true;
        }
        return false;
    };

    if (strip_prefix("getBucket")) {
        if (p.empty()) return PathAction::GetBucket;  // /getBucket/ → 列所有桶
        size_t pos = p.find('/');
        if (pos == std::string::npos) { bucket_name = p; return PathAction::GetBucket; }  // /getBucket/name → 列桶内对象
        bucket_name = p.substr(0, pos);
        object_key = p.substr(pos + 1);
        return PathAction::GetBucket;  // getBucket 忽略 key，只用到 bucket_name
    }
    if (strip_prefix("getObject")) {
        if (p.empty()) return PathAction::None;
        size_t pos = p.find('/');
        if (pos == std::string::npos) return PathAction::None;  // 必须 bucket/key
        bucket_name = p.substr(0, pos);
        object_key = p.substr(pos + 1);
        return PathAction::GetObject;
    }
    if (strip_prefix("deleteBucket")) {
        if (p.empty()) return PathAction::None;
        size_t pos = p.find('/');
        if (pos != std::string::npos) return PathAction::None;  // 仅桶名
        bucket_name = p;
        return PathAction::DeleteBucket;
    }
    if (strip_prefix("deleteObject")) {
        if (p.empty()) return PathAction::None;
        size_t pos = p.find('/');
        if (pos == std::string::npos) return PathAction::None;
        bucket_name = p.substr(0, pos);
        object_key = p.substr(pos + 1);
        return PathAction::DeleteObject;
    }
    if (strip_prefix("createBucket")) {
        if (p.empty()) return PathAction::None;
        size_t pos = p.find('/');
        if (pos != std::string::npos) return PathAction::None;
        bucket_name = p;
        return PathAction::CreateBucket;
    }
    if (strip_prefix("createObject")) {
        if (p.empty()) return PathAction::None;
        size_t pos = p.find('/');
        if (pos == std::string::npos) return PathAction::None;
        bucket_name = p.substr(0, pos);
        object_key = p.substr(pos + 1);
        return PathAction::CreateObject;
    }
    return PathAction::None;
}

// 桶在磁盘上的实际目录名 = owner_id_桶名，仅用于服务端存储，不向用户暴露。
// 用户始终只使用自己创建的桶名：请求路径为 /<bucket_name> 或 /<bucket_name>/<key>，响应中的 Name 也为桶名。
static std::string bucket_folder_name(const std::string& owner_id, const std::string& bucket_name) {
    if (owner_id.empty()) return bucket_name;
    return owner_id + "_" + bucket_name;
}

// 本地对象文件路径：data_root/s3/<owner_id>_<bucket_name>/<key>
static std::string object_storage_path(const s3config::Config& config,
                                       const std::string& owner_id,
                                       const std::string& bucket_name,
                                       const std::string& key) {
    std::string p = config.data_root;
    if (!p.empty() && p.back() != '/') p += '/';
    p += "s3/";
    p += bucket_folder_name(owner_id, bucket_name);
    if (!key.empty()) {
        if (p.back() != '/') p += '/';
        p += key;
    }
    return p;
}

// 桶目录路径：data_root/s3/<owner_id>_<bucket_name>
static std::string bucket_dir_path(const s3config::Config& config,
                                  const std::string& owner_id,
                                  const std::string& bucket_name) {
    std::string p = config.data_root;
    if (!p.empty() && p.back() != '/') p += '/';
    p += "s3/";
    p += bucket_folder_name(owner_id, bucket_name);
    return p;
}

// 拒绝桶名/key 含 .. 或 /，防止路径穿越
static bool is_bucket_name_safe(const std::string& s) {
    if (s.empty()) return false;
    if (s.find("..") != std::string::npos || s.find('/') != std::string::npos) return false;
    return true;
}
static bool is_object_key_safe(const std::string& s) {
    if (s.find("..") != std::string::npos) return false;
    return true;
}

// 校验 storage_path 在 data_root 下，防止 meta 被篡改时读/删任意文件
static bool is_storage_path_safe(const std::string& storage_path, const std::string& data_root) {
    if (data_root.empty() || storage_path.empty()) return false;
    std::string root = data_root;
    if (root.back() != '/') root += '/';
    if (storage_path.size() < root.size()) return false;
    if (storage_path.compare(0, root.size(), root) != 0) return false;
    if (storage_path.find("..") != std::string::npos) return false;
    return true;
}

// 将字符串中 " \ 转义后追加到 s
static void json_escape_append(std::string& s, const std::string& raw) {
    for (char c : raw) {
        if (c == '"') s += "\\\"";
        else if (c == '\\') s += "\\\\";
        else if (c == '\n') s += "\\n";
        else s += c;
    }
}

static void write_list_json_from_meta(x_msg_t& out, x_buf_pool_t& pool,
                                      const std::string& bucket_name,
                                      const std::vector<meta::Object>& objects) {
    std::string body;
    body.reserve(256 + objects.size() * 128);
    body += "{\"code\":1,\"Name\":\"";
    json_escape_append(body, bucket_name);
    body += "\",\"Contents\":[";
    for (size_t i = 0; i < objects.size(); ++i) {
        const meta::Object& o = objects[i];
        if (i) body += ",";
        body += "{\"Key\":\"";
        json_escape_append(body, o.key);
        body += "\",\"Size\":";
        body += std::to_string(static_cast<long long>(o.size));
        body += ",\"LastModified\":\"";
        json_escape_append(body, o.last_modified);
        body += "\"}";
    }
    body += "]}";
    write_success_response(out, pool, body.data(), body.size());
}

// GET / 时返回该用户最外层所有桶
static void write_list_buckets_json(x_msg_t& out, x_buf_pool_t& pool,
                                    const std::vector<meta::Bucket>& buckets) {
    std::string body;
    body.reserve(128 + buckets.size() * 96);
    body += "{\"code\":1,\"Buckets\":[";
    for (size_t i = 0; i < buckets.size(); ++i) {
        const meta::Bucket& b = buckets[i];
        if (i) body += ",";
        body += "{\"Name\":\"";
        json_escape_append(body, b.name);
        body += "\",\"CreationDate\":\"";
        json_escape_append(body, b.created_at);
        body += "\"}";
    }
    body += "]}";
    write_success_response(out, pool, body.data(), body.size());
}

/*  
    路由：
    管理级（仅 config.access_key 管理员）
    POST	/_admin/users	创建用户
    GET	/_admin/users	列出用户
    桶/对象（均需 query 鉴权）
    GET	/getBucket/	列出当前用户所有桶
    GET	/getBucket/<bucket_name>	列出桶内对象
    GET	/getObject/<bucket_name>/<key>	获取对象内容
    PUT	/createBucket/<bucket_name>	创建桶
    PUT	/createObject/<bucket_name>/<key>	创建对象（body=文件内容）
    DELETE	/deleteBucket/<bucket_name>	删除桶
    DELETE	/deleteObject/<bucket_name>/<key>	删除对象
*/
static bool is_admin(const http::HttpRequest& req, const s3config::Config& config) {
    return req.get_query_param("AWSAccessKeyId") == config.access_key;
}

bool handle_request(const http::HttpRequest& req, const s3config::Config& config,
    meta::MetaStore& store, x_msg_t& out, x_buf_pool_t& pool, const x_msg_t* body_msg) {
    // ----- 管理级：创建/列举用户（仅管理员） -----
    if (req.path == "/_admin/users") {
        if (!is_admin(req, config)) {
            write_error_response(out, pool, 403, "AccessDenied", "Admin only");
            return true;
        }
        if (req.method == "POST") {
            std::string username = "user";
            if (body_msg && body_msg->total_length() > 0) {
                std::vector<char> buf(body_msg->total_length() + 1);
                body_msg->copy_out(buf.data(), static_cast<uint32_t>(body_msg->total_length()));
                buf[body_msg->total_length()] = '\0';
                const char* p = std::strstr(buf.data(), "\"username\"");
                if (p) {
                    p = std::strchr(p, ':');  // 跳到 "username" 后的 ':'
                    if (p) p = std::strchr(p + 1, '"');  // 跳到值的开始 '"'
                    if (p) {
                        ++p;  // 跳过 '"'，指向值首字符
                        const char* end = std::strchr(p, '"');
                        if (end && end > p) username.assign(p, end - p);
                    }
                }
            }
            std::string access_key, created_at;
            if (!store.create_user(username, access_key, created_at)) {
                write_error_response(out, pool, 409, "Conflict", "Username exists or create failed");
                return true;
            }
            if (!store.save()) {
                write_error_response(out, pool, 503, "InternalError", "Meta save failed");
                return true;
            }
            // secret_access_key 仅存于服务端 user.dat，不返回给用户
            std::string json = "{\"access_key\":\"";
            json_escape_append(json, access_key);
            json += "\",\"created_at\":\"";
            json_escape_append(json, created_at);
            json += "\"}";
            write_response(out, pool, 201, "Created", json.data(), json.size(), "application/json");
            return true;
        }
        if (req.method == "GET") {
            std::vector<meta::User> users = store.list_users();
            std::string body = "{\"code\":1,\"users\":[";
            for (size_t i = 0; i < users.size(); ++i) {
                if (i) body += ",";
                body += "{\"username\":\"";
                json_escape_append(body, users[i].username);
                body += "\",\"access_key\":\"";
                json_escape_append(body, users[i].access_key);
                body += "\",\"created_at\":\"";
                json_escape_append(body, users[i].created_at);
                body += "\"}";
            }
            body += "]}";
            write_success_response(out, pool, body.data(), body.size());
            return true;
        }
        write_error_response(out, pool, 400, "BadRequest", "Use POST to create or GET to list");
        return true;
    }

    std::string bucket_name, object_key;
    PathAction action = parse_action_path(req.path, bucket_name, object_key);
    std::string request_owner_id = req.get_query_param("AWSAccessKeyId");
    if (request_owner_id.empty()) request_owner_id = config.access_key;

    if (!bucket_name.empty() && !is_bucket_name_safe(bucket_name)) {
        write_error_response(out, pool, 400, "BadRequest", "Invalid bucket name");
        return true;
    }
    if (!object_key.empty() && !is_object_key_safe(object_key)) {
        write_error_response(out, pool, 400, "BadRequest", "Invalid object key");
        return true;
    }

    // ----- getBucket -----
    if (action == PathAction::GetBucket) {
        if (req.method != "GET") {
            write_error_response(out, pool, 400, "BadRequest", "Use GET for getBucket");
            return true;
        }
        if (bucket_name.empty()) {
            std::vector<meta::Bucket> buckets = store.list_buckets_by_owner(request_owner_id);
            write_list_buckets_json(out, pool, buckets);
            return true;
        }
        const meta::Bucket* b = store.get_bucket_by_name_and_owner(bucket_name, request_owner_id);
        if (!b) {
            write_error_response(out, pool, 404, "NoSuchBucket", "Bucket not found");
            return true;
        }
        std::vector<meta::Object> objs = store.list_objects(b->id);
        write_list_json_from_meta(out, pool, bucket_name, objs);
        return true;
    }
    // ----- getObject -----
    if (action == PathAction::GetObject) {
        if (req.method != "GET") {
            write_error_response(out, pool, 400, "BadRequest", "Use GET for getObject");
            return true;
        }
        const meta::Bucket* b = store.get_bucket_by_name_and_owner(bucket_name, request_owner_id);
        if (!b) {
            write_error_response(out, pool, 404, "NoSuchBucket", "Bucket not found");
            return true;
        }
        meta::Object obj;
        if (!store.get_object(b->id, object_key, obj)) {
            write_error_response(out, pool, 404, "NoSuchKey", "Object not found");
            return true;
        }
        if (!is_storage_path_safe(obj.storage_path, config.data_root)) {
            write_error_response(out, pool, 403, "Forbidden", "Invalid object path");
            return true;
        }
        size_t fsize = static_cast<size_t>(obj.size);
        std::vector<char> buf(fsize);
        ssize_t n = uring::read_file(obj.storage_path, buf.data(), fsize);
        if (n < 0 || static_cast<size_t>(n) != fsize) {
            write_error_response(out, pool, 503, "InternalError", "Read failed");
            return true;
        }
        write_response(out, pool, 200, "OK", buf.data(), fsize, "application/octet-stream");
        return true;
    }
    // ----- deleteBucket -----
    if (action == PathAction::DeleteBucket) {
        if (req.method != "DELETE") {
            write_error_response(out, pool, 400, "BadRequest", "Use DELETE for deleteBucket");
            return true;
        }
        const meta::Bucket* b = store.get_bucket_by_name_and_owner(bucket_name, request_owner_id);
        if (!b) {
            write_error_response(out, pool, 404, "NoSuchBucket", "Bucket not found");
            return true;
        }
        std::vector<meta::Object> objs = store.list_objects(b->id);
        if (!objs.empty()) {
            write_error_response(out, pool, 409, "BucketNotEmpty", "The bucket you tried to delete is not empty");
            return true;
        }
        store.delete_bucket(b->id);
        if (!store.save()) {
            std::cerr << "[S3] Meta save failed: " << store.last_save_error() << std::endl;
            write_error_response(out, pool, 503, "InternalError", "Meta save failed");
            return true;
        }
        std::string dir = bucket_dir_path(config, b->owner_id, bucket_name);
        rmdir(dir.c_str());
        write_success_response(out, pool);
        return true;
    }
    // ----- deleteObject -----
    if (action == PathAction::DeleteObject) {
        if (req.method != "DELETE") {
            write_error_response(out, pool, 400, "BadRequest", "Use DELETE for deleteObject");
            return true;
        }
        const meta::Bucket* b = store.get_bucket_by_name_and_owner(bucket_name, request_owner_id);
        if (!b) {
            write_error_response(out, pool, 404, "NoSuchBucket", "Bucket not found");
            return true;
        }
        meta::Object obj;
        if (!store.get_object(b->id, object_key, obj)) {
            write_error_response(out, pool, 404, "NoSuchKey", "Object not found");
            return true;
        }
        if (!is_storage_path_safe(obj.storage_path, config.data_root)) {
            write_error_response(out, pool, 403, "Forbidden", "Invalid object path");
            return true;
        }
        if (unlink(obj.storage_path.c_str()) != 0 && errno != ENOENT) {
            write_error_response(out, pool, 503, "InternalError", "Delete failed");
            return true;
        }
        store.delete_object(b->id, object_key);
        if (!store.save()) {
            std::cerr << "[S3] Meta save failed: " << store.last_save_error() << std::endl;
            write_error_response(out, pool, 503, "InternalError", "Meta save failed");
            return true;
        }
        write_success_response(out, pool);
        return true;
    }
    // ----- createBucket -----
    if (action == PathAction::CreateBucket) {
        if (req.method != "PUT") {
            write_error_response(out, pool, 400, "BadRequest", "Use PUT for createBucket");
            return true;
        }
        int64_t id = store.create_bucket(bucket_name, request_owner_id);
        if (id == 0) {
            write_error_response(out, pool, 409, "BucketAlreadyExists", "Bucket already exists");
            return true;
        }
        if (!store.save()) {
            store.delete_bucket(id);
            std::cerr << "[S3] Meta save failed: " << store.last_save_error() << std::endl;
            write_error_response(out, pool, 503, "InternalError", "Meta save failed");
            return true;
        }
        std::string dir = bucket_dir_path(config, request_owner_id, bucket_name);
        for (size_t i = 1; i < dir.size(); ++i) {
            if (dir[i] == '/') {
                std::string sub = dir.substr(0, i);
                mkdir(sub.c_str(), 0755);
            }
        }
        if (mkdir(dir.c_str(), 0755) != 0 && errno != ENOENT) {
            std::cerr << "[S3] create bucket dir failed: " << dir << " errno=" << errno << std::endl;
        }
        write_success_response(out, pool);
        return true;
    }
    // ----- createObject -----
    if (action == PathAction::CreateObject) {
        if (req.method != "PUT") {
            write_error_response(out, pool, 400, "BadRequest", "Use PUT for createObject");
            return true;
        }
        const meta::Bucket* b = store.get_bucket_by_name_and_owner(bucket_name, request_owner_id);
        if (!b) {
            write_error_response(out, pool, 404, "NoSuchBucket", "Bucket not found");
            return true;
        }
        meta::Object existing;
        if (store.get_object(b->id, object_key, existing)) {
            write_error_response(out, pool, 409, "ObjectAlreadyExists", "Object already exists");
            return true;
        }
        std::string storage_path = object_storage_path(config, b->owner_id, bucket_name, object_key);
        std::string dir = storage_path;
        size_t slash = dir.rfind('/');
        if (slash != std::string::npos) {
            dir.resize(slash);
            for (size_t i = 1; i < dir.size(); ++i) {
                if (dir[i] == '/') {
                    std::string sub = dir.substr(0, i);
                    mkdir(sub.c_str(), 0755);
                }
            }
            mkdir(dir.c_str(), 0755);
        }
        if (!body_msg || body_msg->total_length() == 0) {
            write_error_response(out, pool, 400, "BadRequest", "Missing or empty body; file content required");
            return true;
        }
        size_t need = body_msg->total_length();
        time_t now_t = time(nullptr);
        struct tm* tm = gmtime(&now_t);
        char mtime_buf[32];
        if (tm) strftime(mtime_buf, sizeof(mtime_buf), "%Y-%m-%dT%H:%M:%SZ", tm);
        else mtime_buf[0] = '\0';
        std::string last_modified(mtime_buf);
        std::vector<char> buf(need);
        body_msg->copy_out(buf.data(), static_cast<uint32_t>(need));
        ssize_t w = uring::write_file(storage_path, buf.data(), need);
        if (w < 0 || static_cast<size_t>(w) != need) {
            unlink(storage_path.c_str());
            write_error_response(out, pool, 503, "InternalError", "Write failed");
            return true;
        }
        store.put_object(b->id, object_key, static_cast<int64_t>(need), last_modified, "", storage_path, "private");
        if (!store.save()) {
            std::cerr << "[S3] Meta save failed: " << store.last_save_error() << std::endl;
            write_error_response(out, pool, 503, "InternalError", "Meta save failed");
            return true;
        }
        write_success_response(out, pool);
        return true;
    }

    write_error_response(out, pool, 400, "BadRequest", "Unsupported method or path");
    return true;
}

}
