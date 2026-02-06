#ifndef S3_META_META_H
#define S3_META_META_H

#include <string>
#include <vector>
#include <map>
#include <cstdint>
#include <mutex>

namespace meta {

// buckets / objects 表结构
struct Bucket {
    int64_t id{0};
    std::string name;
    std::string created_at;
    std::string owner_id;
};

struct Object {
    int64_t id{0};
    int64_t bucket_id{0};
    std::string key;
    int64_t size{0};
    std::string last_modified;
    std::string etag;
    std::string storage_path;
    std::string acl;
};

// 用户：access_key 唯一；secret 仅存于服务端 user.dat，不发给客户端
struct User {
    int64_t id{0};
    std::string username;
    std::string access_key;
    std::string created_at;
};

// 元数据存储
// 文件路径：<data_root>/s3_meta.dat；首行 N\t<bucket_next_id>\t<object_next_id>；
// 桶行 B\t<id>\t<name>\t<created_at>\t<owner_id>；对象行 O\t<id>\t<bucket_id>\t<key>\t<size>\t<last_modified>\t<etag>\t<storage_path>\t<acl>；字段禁止 \t \n；写回先写临时文件再 rename。
class MetaStore {
public:
    MetaStore() = default;
    ~MetaStore() = default;
    MetaStore(const MetaStore&) = delete;
    MetaStore& operator=(const MetaStore&) = delete;

    // 初始化：设置 data_root，并从 <data_root>/s3_meta.dat 加载桶与对象（不读 user.dat）
    bool load(const std::string& data_root);
    // 从 <data_root>/user.dat 加载用户列表与 secret；应在 ensure_root_user() 之后调用
    bool load_user_dat();

    // 持久化：将内存数据按行式格式写回 <data_root>/s3_meta.dat（经临时文件 s3_meta.dat.tmp 再 rename）
    bool save();
    // save() 失败时原因（供日志），调用 save() 后立即读
    const std::string& last_save_error() const { return last_save_error_; }

    // 桶：按 (name, owner_id) 查，同一用户同名桶在 s3_meta.dat 只记一条；创建返回 id，已存在返回 0
    const Bucket* get_bucket_by_name_and_owner(const std::string& name, const std::string& owner_id) const;
    std::vector<Bucket> list_buckets_by_owner(const std::string& owner_id) const;
    int64_t create_bucket(const std::string& name, const std::string& owner_id);
    bool delete_bucket(int64_t bucket_id);

    // 对象：按 bucket_id+key 查；按 bucket_id 列表；插入或覆盖（同一 bucket_id+key）；删除
    bool get_object(int64_t bucket_id, const std::string& key, Object& out) const;
    std::vector<Object> list_objects(int64_t bucket_id) const;
    bool put_object(int64_t bucket_id, const std::string& key, int64_t size,
                    const std::string& last_modified, const std::string& etag,
                    const std::string& storage_path, const std::string& acl);
    bool delete_object(int64_t bucket_id, const std::string& key);

    // 用户与密钥：secret 仅存于 user.dat，按 access_key 查 secret（用于验签）
    std::string get_secret_by_access_key(const std::string& access_key) const;
    // 判断用户是否存在（不涉及 secret）
    bool has_user_by_access_key(const std::string& access_key) const;
    bool has_user_by_username(const std::string& username) const;
    // 创建用户，生成密钥；secret 仅写入 user.dat，不返回给调用方。返回 (access_key, created_at)
    bool create_user(const std::string& username, std::string& out_access_key, std::string& out_created_at);
    // 启动时确保存在 root 用户：若尚无 username 为 root 的用户，则添加（access_key/secret_key 使用传入值，一般为 config 中的值）
    void ensure_root_user(const std::string& access_key, const std::string& secret_key);
    std::vector<User> list_users() const;

private:
    std::string data_root_;
    int64_t next_bucket_id_{1};
    int64_t next_object_id_{1};
    int64_t next_user_id_{1};
    std::vector<Bucket> buckets_;
    std::vector<Object> objects_;
    std::vector<User> users_;
    std::map<std::string, std::string> secret_by_access_key_;  // 从 user.dat 加载，仅服务端保存
    mutable std::mutex mutex_;
    std::string last_save_error_;

    std::string meta_file_path() const;
    std::string meta_file_path_tmp() const;
    std::string user_dat_path() const;  // <data_root>/user.dat
};

}

#endif
