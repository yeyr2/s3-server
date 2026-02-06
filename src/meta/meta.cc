// 元数据存储层：单文件
// 文件路径：<data_root>/s3_meta.dat
// 首行格式：N\t<bucket_next_id>\t<object_next_id>（无 user_next_id，用户仅存 user.dat）
// 桶行 B、对象行 O 同上；用户仅从 user.dat 读取，s3_meta.dat 不存 U 行
// 字段禁止字符：\t、\n；写回方式：先写临时文件 s3_meta.dat.tmp 再 rename 覆盖

#include "meta/meta.h"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <ctime>
#include <openssl/rand.h>
#include <iostream>

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

} 

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

std::string MetaStore::user_dat_path() const {
    std::string p = data_root_;
    if (!p.empty() && p.back() != '/') p += '/';
    p += "user.dat";
    return p;
}

bool MetaStore::load(const std::string& data_root) {
    std::lock_guard<std::mutex> lock(mutex_);
    data_root_ = data_root;
    next_bucket_id_ = 1;
    next_object_id_ = 1;
    next_user_id_ = 1;
    buckets_.clear();
    objects_.clear();
    users_.clear();
    secret_by_access_key_.clear();

    std::string path = meta_file_path();
    std::ifstream f(path);
    if (!f.is_open()) {
        if (errno == ENOENT) {
            std::cout << "meta: no " << path << ", starting with empty buckets/objects" << std::endl;
            return true; // 新库
        }
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
            // 首行 N\t<bucket_next_id>\t<object_next_id>（user 从 user.dat 读，不在此）
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
        } else if (parts[0] == "O" && parts.size() >= 9) {
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
        // 用户仅从 user.dat 读取，在 load_user_dat() 中读（且应在 ensure_root_user 之后调用）
    }
    std::cout << "meta: loaded " << path << " buckets=" << buckets_.size() << " objects=" << objects_.size() << std::endl;
    return true;
}

bool MetaStore::load_user_dat() {
    // 在 ensure_root_user() 之后调用：从 user.dat 读取其余用户与 next_user_id，不覆盖已存在的 root
    std::lock_guard<std::mutex> lock(mutex_);
    std::string udat = user_dat_path();
    std::ifstream fu(udat);
    if (!fu.is_open()) return true;  // 文件不存在视为仅有内存中的 root
    std::string uline;
    bool u_first = true;
    int64_t uid_placeholder = 1;
    while (std::getline(fu, uline)) {
        if (uline.empty()) continue;
        std::vector<std::string> up = split_line(uline);
        if (up.empty()) continue;
        if (u_first && up[0] == "N" && up.size() >= 2) {
            int64_t file_next = static_cast<int64_t>(std::stoll(up[1]));
            if (file_next > next_user_id_) next_user_id_ = file_next;
            u_first = false;
            continue;
        }
        u_first = false;
        if (up[0] == "U" && up.size() >= 6) {
            if (up[2] == "root") continue;  // root 已由 ensure_root_user 加入，不重复
            User u;
            u.id = static_cast<int64_t>(std::stoll(up[1]));
            u.username = up[2];
            u.access_key = up[3];
            u.created_at = up[5];
            secret_by_access_key_[u.access_key] = up[4];
            users_.push_back(std::move(u));
        } else if (up.size() >= 2 && up[0] != "N") {
            // 兼容旧格式：每行 access_key\tsecret_key
            if (secret_by_access_key_.count(up[0])) continue;
            secret_by_access_key_[up[0]] = up[1];
            User u;
            u.id = uid_placeholder++;
            u.username = up[0];
            u.access_key = up[0];
            u.created_at = "";
            users_.push_back(std::move(u));
        }
    }
    if (uid_placeholder > 1 && uid_placeholder > next_user_id_)
        next_user_id_ = uid_placeholder;
    return true;
}

bool MetaStore::save() {
    std::lock_guard<std::mutex> lock(mutex_);
    last_save_error_.clear();
    std::string path = meta_file_path();
    std::string path_tmp = meta_file_path_tmp();
    std::ofstream f(path_tmp);
    if (!f.is_open()) {
        last_save_error_ = path_tmp + ": " + strerror(errno);
        return false;
    }

    // 首行仅桶与对象 next_id，用户存 user.dat
    f << "N\t" << next_bucket_id_ << "\t" << next_object_id_ << "\n";
    for (const Bucket& b : buckets_)
        f << "B\t" << b.id << "\t" << b.name << "\t" << b.created_at << "\t" << b.owner_id << "\n";
    for (const Object& o : objects_)
        f << "O\t" << o.id << "\t" << o.bucket_id << "\t" << o.key << "\t" << o.size << "\t"
          << o.last_modified << "\t" << o.etag << "\t" << o.storage_path << "\t" << o.acl << "\n";

    f.close();
    if (!f.good()) {
        last_save_error_ = "write failed: " + path_tmp;
        return false;
    }
    if (rename(path_tmp.c_str(), path.c_str()) != 0) {
        last_save_error_ = std::string("rename to ") + path + ": " + strerror(errno);
        return false;
    }
    // 用户完整信息（含 secret）仅写 user.dat：首行 N\t<next_user_id>，后续 U\t<id>\t<username>\t<access_key>\t<secret>\t<created_at>
    std::string udat = user_dat_path();
    std::string udat_tmp = udat + ".tmp";
    std::ofstream fu(udat_tmp);
    if (fu.is_open()) {
        fu << "N\t" << next_user_id_ << "\n";
        for (const User& u : users_) {
            auto it = secret_by_access_key_.find(u.access_key);
            if (it != secret_by_access_key_.end())
                fu << "U\t" << u.id << "\t" << u.username << "\t" << u.access_key << "\t" << it->second << "\t" << u.created_at << "\n";
        }
        fu.close();
        if (fu.good())
            rename(udat_tmp.c_str(), udat.c_str());
    }
    return true;
}

const Bucket* MetaStore::get_bucket_by_name_and_owner(const std::string& name, const std::string& owner_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const Bucket& b : buckets_) {
        if (b.name == name && b.owner_id == owner_id) return &b;
    }
    return nullptr;
}

std::vector<Bucket> MetaStore::list_buckets_by_owner(const std::string& owner_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<Bucket> out;
    for (const Bucket& b : buckets_)
        if (b.owner_id == owner_id) out.push_back(b);
    return out;
}

int64_t MetaStore::create_bucket(const std::string& name, const std::string& owner_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const Bucket& b : buckets_)
        if (b.name == name && b.owner_id == owner_id) return 0;  // 同一用户同名桶只记一次
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

// 同一 bucket_id+key 在 s3_meta.dat 只记一条；重复 PUT 为覆盖更新
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

std::string MetaStore::get_secret_by_access_key(const std::string& access_key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = secret_by_access_key_.find(access_key);
    return it != secret_by_access_key_.end() ? it->second : std::string{};
}

bool MetaStore::has_user_by_access_key(const std::string& access_key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const User& u : users_)
        if (u.access_key == access_key) return true;
    return false;
}

bool MetaStore::has_user_by_username(const std::string& username) const {
    std::lock_guard<std::mutex> lock(mutex_);
    for (const User& u : users_)
        if (u.username == username) return true;
    return false;
}

static const char kAlnum[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
static bool random_alnum_string(size_t len, std::string& out) {
    out.resize(len);
    std::vector<unsigned char> buf(len);
    if (RAND_bytes(buf.data(), static_cast<int>(len)) != 1) return false;
    for (size_t i = 0; i < len; ++i)
        out[i] = kAlnum[buf[i] % (sizeof(kAlnum) - 1)];
    return true;
}

bool MetaStore::create_user(const std::string& username, std::string& out_access_key, std::string& out_created_at) {
    for (char c : username) {
        if (c == '\t' || c == '\n') return false;
    }
    std::string ak, sk;
    if (!random_alnum_string(20, ak) || !random_alnum_string(40, sk)) return false;
    std::lock_guard<std::mutex> lock(mutex_);
    for (const User& u : users_) {
        if (u.access_key == ak) return false;   // 新 access_key 与已有用户冲突（极低概率）
        if (u.username == username) return false; // 用户名已存在，视为用户已存在
    }
    std::string created = now_iso8601();
    User u;
    u.id = next_user_id_++;
    u.username = username;
    u.access_key = ak;
    u.created_at = created;
    users_.push_back(std::move(u));
    secret_by_access_key_[ak] = std::move(sk);  // 仅存服务端 user.dat，不返回给调用方
    out_access_key = std::move(ak);
    out_created_at = std::move(created);
    return true;
}

void MetaStore::ensure_root_user(const std::string& access_key, const std::string& secret_key) {
    if (access_key.empty()) return;
    std::lock_guard<std::mutex> lock(mutex_);
    for (const User& u : users_) {
        if (u.username == "root") return;  // 已存在 root，不重复添加
    }
    std::string created = now_iso8601();
    User u;
    u.id = next_user_id_++;
    u.username = "root";
    u.access_key = access_key;
    u.created_at = created;
    users_.push_back(std::move(u));
    secret_by_access_key_[access_key] = secret_key;
}

std::vector<User> MetaStore::list_users() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return users_;
}

} 
