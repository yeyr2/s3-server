// 元数据存储层：方案 A 行式文本单文件（参见架构设计 3.5.2）
// 文件路径：<data_root>/s3_meta.dat
// 首行格式：N\t<bucket_next_id>\t<object_next_id>
// 桶行格式：首字段 B，字段顺序 id、name、created_at、owner_id，制表符 \t 分隔
// 对象行格式：首字段 O，字段顺序 id、bucket_id、key、size、last_modified、etag、storage_path、acl，制表符 \t 分隔
// 字段禁止字符：\t、\n；写回方式：先写临时文件 s3_meta.dat.tmp 再 rename 覆盖

#include "meta/meta.h"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <ctime>

namespace meta {

namespace {

// 时间戳格式 YYYY-MM-DDTHH:MM:SSZ
std::string now_iso8601() {
    time_t t = time(nullptr);
    struct tm* tm = gmtime(&t);
    if (!tm) return "1970-01-01T00:00:00Z";
    char buf[32];
    strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", tm);
    return std::string(buf);
}

// 按 \t 分割一行，返回字段向量
std::vector<std::string> split_line(const std::string& line) {
    std::vector<std::string> out;
    std::string field;
    for (char c : line) {
        if (c == '\t') {
            out.push_back(std::move(field));
            field.clear();
        } else {
            field += c;
        }
    }
    if (!field.empty() || !line.empty()) out.push_back(std::move(field));
    return out;
}

} // namespace

std::string MetaStore::meta_file_path() const {
    std::string p = data_root_;
    if (!p.empty() && p.back() != '/') p += '/';
    p += "s3_meta.dat";
    return p;
}

std::string MetaStore::meta_file_path_tmp() const {
    std::string p = data_root_;
    if (!p.empty() && p.back() != '/') p += '/';
    p += "s3_meta.dat.tmp";
    return p;
}

bool MetaStore::load(const std::string& data_root) {
    std::lock_guard<std::mutex> lock(mutex_);
    data_root_ = data_root;
    next_bucket_id_ = 1;
    next_object_id_ = 1;
    buckets_.clear();
    objects_.clear();

    std::string path = meta_file_path();
    std::ifstream f(path);
    if (!f.is_open()) {
        if (errno == ENOENT) return true; // 新库
        return false;
    }

    std::string line;
    bool first = true;
    while (std::getline(f, line)) {
        if (line.empty()) continue;
        std::vector<std::string> parts = split_line(line);
        if (parts.empty()) continue;

        if (first) {
            first = false;
            // 首行 N\t<bucket_next_id>\t<object_next_id>
            if (parts[0] == "N" && parts.size() >= 3) {
                next_bucket_id_ = static_cast<int64_t>(std::stoll(parts[1]));
                next_object_id_ = static_cast<int64_t>(std::stoll(parts[2]));
            }
            continue;
        }

        if (parts[0] == "B" && parts.size() >= 5) {
            Bucket b;
            b.id = static_cast<int64_t>(std::stoll(parts[1]));
            b.name = parts[2];
            b.created_at = parts[3];
            b.owner_id = parts[4];
            buckets_.push_back(std::move(b));
        } else if (parts[0] == "O" && parts.size() >= 10) {
            Object o;
            o.id = static_cast<int64_t>(std::stoll(parts[1]));
            o.bucket_id = static_cast<int64_t>(std::stoll(parts[2]));
            o.key = parts[3];
            o.size = static_cast<int64_t>(std::stoll(parts[4]));
            o.last_modified = parts[5];
            o.etag = parts[6];
            o.storage_path = parts[7];
            o.acl = parts[8];
            objects_.push_back(std::move(o));
        }
    }
    return true;
}

bool MetaStore::save() {
    std::lock_guard<std::mutex> lock(mutex_);
    std::string path = meta_file_path();
    std::string path_tmp = meta_file_path_tmp();
    std::ofstream f(path_tmp);
    if (!f.is_open()) return false;

    // 首行 N\t<bucket_next_id>\t<object_next_id>
    f << "N\t" << next_bucket_id_ << "\t" << next_object_id_ << "\n";
    for (const Bucket& b : buckets_)
        f << "B\t" << b.id << "\t" << b.name << "\t" << b.created_at << "\t" << b.owner_id << "\n";
    for (const Object& o : objects_)
        f << "O\t" << o.id << "\t" << o.bucket_id << "\t" << o.key << "\t" << o.size << "\t"
          << o.last_modified << "\t" << o.etag << "\t" << o.storage_path << "\t" << o.acl << "\n";

    f.close();
    if (!f.good()) return false;
    if (rename(path_tmp.c_str(), path.c_str()) != 0) return false;
    return true;
}

const Bucket* MetaStore::get_bucket_by_name(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const Bucket& b : buckets_)
        if (b.name == name) return &b;
    return nullptr;
}

int64_t MetaStore::create_bucket(const std::string& name, const std::string& owner_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const Bucket& b : buckets_)
        if (b.name == name) return 0;
    Bucket b;
    b.id = next_bucket_id_++;
    b.name = name;
    b.created_at = now_iso8601();
    b.owner_id = owner_id;
    buckets_.push_back(std::move(b));
    return buckets_.back().id;
}

bool MetaStore::delete_bucket(int64_t bucket_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::remove_if(buckets_.begin(), buckets_.end(),
        [bucket_id](const Bucket& b) { return b.id == bucket_id; });
    if (it == buckets_.end()) return false; // 未找到
    buckets_.erase(it, buckets_.end());
    return true;
}

bool MetaStore::get_object(int64_t bucket_id, const std::string& key, Object& out) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const Object& o : objects_)
        if (o.bucket_id == bucket_id && o.key == key) {
            out = o;
            return true;
        }
    return false;
}

std::vector<Object> MetaStore::list_objects(int64_t bucket_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Object> out;
    for (const Object& o : objects_)
        if (o.bucket_id == bucket_id) out.push_back(o);
    return out;
}

bool MetaStore::put_object(int64_t bucket_id, const std::string& key, int64_t size,
                           const std::string& last_modified, const std::string& etag,
                           const std::string& storage_path, const std::string& acl) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (Object& o : objects_) {
        if (o.bucket_id == bucket_id && o.key == key) {
            o.size = size;
            o.last_modified = last_modified;
            o.etag = etag;
            o.storage_path = storage_path;
            o.acl = acl;
            return true;
        }
    }
    Object o;
    o.id = next_object_id_++;
    o.bucket_id = bucket_id;
    o.key = key;
    o.size = size;
    o.last_modified = last_modified;
    o.etag = etag;
    o.storage_path = storage_path;
    o.acl = acl;
    objects_.push_back(std::move(o));
    return true;
}

bool MetaStore::delete_object(int64_t bucket_id, const std::string& key) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = std::remove_if(objects_.begin(), objects_.end(),
        [bucket_id, &key](const Object& o) { return o.bucket_id == bucket_id && o.key == key; });
    if (it == objects_.end()) return false; // 未找到
    objects_.erase(it, objects_.end());
    return true;
}

} 
