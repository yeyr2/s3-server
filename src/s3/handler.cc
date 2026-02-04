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

static void write_list_xml_from_meta(x_msg_t& out, x_buf_pool_t& pool,
                                     const std::string& bucket_name,
                                     const std::vector<meta::Object>& objects) {
    out.clear();
    std::string prefix = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<ListBucketResult><Name>" + bucket_name + "</Name>";
    out.copy_in(pool, prefix.data(), static_cast<uint32_t>(prefix.size()));
    for (const meta::Object& o : objects) {
        char line[512];
        int n = std::snprintf(line, sizeof(line),
            "<Contents><Key>%s</Key><Size>%lld</Size><LastModified>%s</LastModified></Contents>",
            o.key.c_str(), static_cast<long long>(o.size), o.last_modified.c_str());
        if (n > 0 && n < (int)sizeof(line)) out.copy_in(pool, line, static_cast<uint32_t>(n));
    }
    out.copy_in(pool, "</ListBucketResult>", 19);
    uint32_t total = out.total_length();
    char header[256];
    int nc = std::snprintf(header, sizeof(header), "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: %u\r\n\r\n", total);
    if (nc <= 0 || nc >= (int)sizeof(header)) return;
    std::vector<char> body(total);
    out.copy_out(body.data(), total);
    out.clear();
    out.copy_in(pool, header, static_cast<uint32_t>(nc));
    out.copy_in(pool, body.data(), total);
}

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
            write_response(out, pool, 200, "OK", nullptr, 0, nullptr); // 已存在
            return true;
        }
        if (!store.save()) {
            write_error_response(out, pool, 503, "InternalError", "Meta save failed");
            return true;
        }
        std::string dir = bucket_dir_path(config, bucket_name);
        mkdir(dir.c_str(), 0755); // 可选目录
        write_response(out, pool, 200, "OK", nullptr, 0, nullptr);
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
            write_error_response(out, pool, 503, "InternalError", "Meta save failed");
            return true;
        }
        std::string dir = bucket_dir_path(config, bucket_name);
        rmdir(dir.c_str()); // 可选
        write_response(out, pool, 204, "No Content", nullptr, 0, nullptr);
        return true;
    }

    if (req.method == "GET" && is_bucket) {
        const meta::Bucket* b = store.get_bucket_by_name(bucket_name);
        if (!b) {
            write_error_response(out, pool, 404, "NoSuchBucket", "Bucket not found");
            return true;
        }
        std::vector<meta::Object> objs = store.list_objects(b->id);
        write_list_xml_from_meta(out, pool, bucket_name, objs);
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
        ssize_t n = io_uring::read_file(obj.storage_path, buf.data(), fsize);
        if (n < 0 || static_cast<size_t>(n) != fsize) {
            write_error_response(out, pool, 503, "InternalError", "Read failed");
            return true;
        }
        out.clear();
        char line[256];
        int nl = std::snprintf(line, sizeof(line), "HTTP/1.1 200 OK\r\nContent-Length: %zu\r\n\r\n", fsize);
        if (!out.copy_in(pool, line, static_cast<uint32_t>(nl)) || !out.copy_in(pool, buf.data(), static_cast<uint32_t>(fsize))) {
            write_error_response(out, pool, 503, "ServiceUnavailable", "Buffer pool exhausted");
            return true;
        }
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
        size_t need = body_msg ? body_msg->total_length() : 0;
        time_t now_t = time(nullptr);
        struct tm* tm = gmtime(&now_t);
        char mtime_buf[32];
        if (tm) strftime(mtime_buf, sizeof(mtime_buf), "%Y-%m-%dT%H:%M:%SZ", tm);
        else mtime_buf[0] = '\0';
        std::string last_modified(mtime_buf);

        if (body_msg && need > 0) {
            std::vector<char> buf(need);
            body_msg->copy_out(buf.data(), static_cast<uint32_t>(need));
            ssize_t w = io_uring::write_file(storage_path, buf.data(), need);
            if (w < 0 || static_cast<size_t>(w) != need) {
                unlink(storage_path.c_str());
                write_error_response(out, pool, 503, "InternalError", "Write failed");
                return true;
            }
        } else {
            ssize_t w = io_uring::write_file(storage_path, nullptr, 0);
            if (w < 0) {
                write_error_response(out, pool, 503, "InternalError", "Create file failed");
                return true;
            }
        }
        store.put_object(bucket_id, object_key, static_cast<int64_t>(need), last_modified, "", storage_path, "private");
        if (!store.save()) {
            write_error_response(out, pool, 503, "InternalError", "Meta save failed");
            return true;
        }
        write_response(out, pool, 200, "OK", nullptr, 0, nullptr);
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
            write_error_response(out, pool, 503, "InternalError", "Meta save failed");
            return true;
        }
        write_response(out, pool, 204, "No Content", nullptr, 0, nullptr);
        return true;
    }

    write_error_response(out, pool, 400, "BadRequest", "Unsupported method or path");
    return true;
}

} // namespace s3
