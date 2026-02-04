#ifndef S3_NET_LISTENER_H
#define S3_NET_LISTENER_H

#include <cstdint>
#include <string>

namespace net {

// TCP 监听：bind + listen。返回监听 fd，失败返回 -1。
int listen_tcp(const std::string& addr, uint16_t port);

// accept，返回客户端 fd，失败返回 -1。
int accept_one(int listen_fd);

} 

#endif
