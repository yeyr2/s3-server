#ifndef S3_CONFIG_H
#define S3_CONFIG_H

#include <string>
#include <cstdint>

namespace s3config {

struct Config {
    std::string data_root;           // 配置中设定的地址，S3 存储根 = data_root + "/s3/"
    std::string access_key;
    std::string secret_key;
    std::string listen_addr;         // 如 "0.0.0.0"
    uint16_t    listen_port{8080};
    uint32_t    buffer_payload_size{65536};  //  缓冲区大小，单块 64KB
    uint32_t    buffer_count{1024}; // 缓冲区数量
};

// 从环境变量加载，缺省使用默认值
void load(Config& out);

} // namespace s3config

#endif
