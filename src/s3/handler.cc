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

// 从规范化路径 path（如 /bucket 或 /bucket/key）解析出桶名与对象 key；is_bucket_path 时 key 为空
static void parse_path(const std::string& path, std::string& bucket_name, std::string& object_key) {
    bucket_name.clear();
    object_key.clear();
    std::string p = path;
    while (!p.empty() && p[0] == '/') p.erase(0, 1);
    if (p.empty()) return;
    size_t pos = p.find('/');
    if (pos == std::string::npos) {
        bucket_name = p;
        return;
    }
    bucket_name = p.substr(0, pos);
    object_key = p.substr(pos + 1);
}

// 本地对象文件路径：data_root/s3/<bucket_name>/<key>
static std::string object_storage_path(const s3config::Config& config,
                                       const std::string& bucket_name,
                                       const std::string& key) {
    std::string p = config.data_root;
    if (!p.empty() && p.back() != '/') p += '/';
    p += "s3/";
    if (!bucket_name.empty()) p += bucket_name;
    if (!key.empty()) {
        if (p.back() != '/') p += '/';
        p += key;
    }
    return p;
}

// 桶目录路径：data_root/s3/<bucket_name>
static std::string bucket_dir_path(const s3config::Config& config, const std::string& bucket_name) {
    std::string p = config.data_root;
    if (!p.empty() && p.back() != '/') p += '/';
    p += "s3/";
    p += bucket_name;
    return p;
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

// 路由
/*
    桶级			
    PUT	/bucket	创建桶	CreateBucket
    DELETE	/bucket	删除桶	DeleteBucket
    GET	/bucket	列出桶内对象	ListBucket
    对象级			
    GET	/bucket/key	获取对象	Get Object
    PUT	/bucket/key	上传/覆盖对象	Put Object
    DELETE	/bucket/key	删除对象
*/
bool handle_request(const http::HttpRequest& req, const s3config::Config& config,
    meta::MetaStore& store, x_msg_t& out, x_buf_pool_t& pool, const x_msg_t* body_msg) {
    std::string bucket_name, object_key;
    parse_path(req.path, bucket_name, object_key);
    bool is_bucket = object_key.empty();

    // ----- 桶级 -----
    if (req.method == "PUT" && is_bucket) {
        if (bucket_name.empty()) {
            write_error_response(out, pool, 400, "BadRequest", "Invalid bucket name");
            return true;
        }
        int64_t id = store.create_bucket(bucket_name, config.access_key);
        if (id == 0) {
            write_success_response(out, pool);
            return true;
        }
        if (!store.save()) {
            store.delete_bucket(id);  // 回滚：磁盘未持久化，保持内存与磁盘一致
            std::cerr << "[S3] Meta save failed: " << store.last_save_error() << std::endl;
            write_error_response(out, pool, 503, "InternalError", "Meta save failed");
            return true;
        }
        // 创建桶目录（含父级 data_root、data_root/s3），与 PUT 对象时逻辑一致
        std::string dir = bucket_dir_path(config, bucket_name);
        for (size_t i = 1; i < dir.size(); ++i) {
            if (dir[i] == '/') {
                std::string sub = dir.substr(0, i);
                mkdir(sub.c_str(), 0755);
            }
        }
        if (mkdir(dir.c_str(), 0755) != 0 && errno != EEXIST) {
            std::cerr << "[S3] create bucket dir failed: " << dir << " errno=" << errno << std::endl;
        }
        write_success_response(out, pool);
        return true;
    }

    if (req.method == "DELETE" && is_bucket) {
        const meta::Bucket* b = store.get_bucket_by_name(bucket_name);
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
        std::string dir = bucket_dir_path(config, bucket_name);
        rmdir(dir.c_str()); // 可选
        write_success_response(out, pool);
        return true;
    }

    if (req.method == "GET" && is_bucket) {
        const meta::Bucket* b = store.get_bucket_by_name(bucket_name);
        if (!b) {
            write_error_response(out, pool, 404, "NoSuchBucket", "Bucket not found");
            return true;
        }
        std::vector<meta::Object> objs = store.list_objects(b->id);
        write_list_json_from_meta(out, pool, bucket_name, objs);
        return true;
    }

    // ----- 对象级 -----
    const meta::Bucket* b = store.get_bucket_by_name(bucket_name);
    if (!b) {
        write_error_response(out, pool, 404, "NoSuchBucket", "Bucket not found");
        return true;
    }
    int64_t bucket_id = b->id;

    if (req.method == "GET" && !is_bucket) {
        meta::Object obj;
        if (!store.get_object(bucket_id, object_key, obj)) {
            write_error_response(out, pool, 404, "NoSuchKey", "Object not found");
            return true;
        }
        size_t fsize = static_cast<size_t>(obj.size);
        std::vector<char> buf(fsize);
        ssize_t n = uring::read_file(obj.storage_path, buf.data(), fsize);
        if (n < 0 || static_cast<size_t>(n) != fsize) {
            write_error_response(out, pool, 503, "InternalError", "Read failed");
            return true;
        }
        // S3 规范：GET Object 直接返回对象二进制流，带 Content-Length
        write_response(out, pool, 200, "OK", buf.data(), fsize, "application/octet-stream");
        return true;
    }

    if (req.method == "PUT" && !is_bucket) {
        std::string storage_path = object_storage_path(config, bucket_name, object_key);
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
        // 接收上传的文件内容；无 body 或空 body 则报错
        std::cout << req.content_length << std::endl;
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
        store.put_object(bucket_id, object_key, static_cast<int64_t>(need), last_modified, "", storage_path, "private");
        if (!store.save()) {
            std::cerr << "[S3] Meta save failed: " << store.last_save_error() << std::endl;
            write_error_response(out, pool, 503, "InternalError", "Meta save failed");
            return true;
        }
        write_success_response(out, pool);
        return true;
    }

    if (req.method == "DELETE" && !is_bucket) {
        meta::Object obj;
        if (!store.get_object(bucket_id, object_key, obj)) {
            write_error_response(out, pool, 404, "NoSuchKey", "Object not found");
            return true;
        }
        if (unlink(obj.storage_path.c_str()) != 0 && errno != ENOENT) {
            write_error_response(out, pool, 503, "InternalError", "Delete failed");
            return true;
        }
        store.delete_object(bucket_id, object_key);
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
