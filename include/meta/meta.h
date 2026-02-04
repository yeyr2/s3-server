#ifndef S3_META_META_H
#define S3_META_META_H

#include <string>
#include <vector>
#include <cstdint>
#include <mutex>

namespace meta {

// 参见架构设计 3.5.1：buckets / objects 表结构
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

// 元数据存储：方案 A 行式文本单文件 s3_meta.dat（参见架构设计 3.5.2）
// 文件路径：<data_root>/s3_meta.dat；首行 N\t<bucket_next_id>\t<object_next_id>；
// 桶行 B\t<id>\t<name>\t<created_at>\t<owner_id>；对象行 O\t<id>\t<bucket_id>\t<key>\t<size>\t<last_modified>\t<etag>\t<storage_path>\t<acl>；字段禁止 \t \n；写回先写临时文件再 rename。
class MetaStore {
public:
    MetaStore() = default;
    ~MetaStore() = default;
    MetaStore(const MetaStore&) = delete;
    MetaStore& operator=(const MetaStore&) = delete;

    // 初始化：设置 data_root，并尝试从 <data_root>/s3_meta.dat 加载；文件不存在或空则视为新库
    bool load(const std::string& data_root);

    // 持久化：将内存数据按行式格式写回 <data_root>/s3_meta.dat（经临时文件 s3_meta.dat.tmp 再 rename）
    bool save();

    // 桶：按名称查；创建（owner_id 为 AccessKey）；删除（按 id）；创建返回 id，已存在返回 0
    const Bucket* get_bucket_by_name(const std::string& name) const;
    int64_t create_bucket(const std::string& name, const std::string& owner_id);
    bool delete_bucket(int64_t bucket_id);

    // 对象：按 bucket_id+key 查；按 bucket_id 列表；插入或覆盖（同一 bucket_id+key）；删除
    bool get_object(int64_t bucket_id, const std::string& key, Object& out) const;
    std::vector<Object> list_objects(int64_t bucket_id) const;
    bool put_object(int64_t bucket_id, const std::string& key, int64_t size,
                    const std::string& last_modified, const std::string& etag,
                    const std::string& storage_path, const std::string& acl);
    bool delete_object(int64_t bucket_id, const std::string& key);

private:
    std::string data_root_;
    int64_t next_bucket_id_{1};
    int64_t next_object_id_{1};
    std::vector<Bucket> buckets_;
    std::vector<Object> objects_;
    mutable std::mutex mutex_;

    std::string meta_file_path() const;
    std::string meta_file_path_tmp() const;
};

} // namespace meta

#endif
