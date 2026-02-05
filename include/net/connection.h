#ifndef S3_NET_CONNECTION_H
#define S3_NET_CONNECTION_H

#include <cstdint>

struct x_msg_t;
class x_buf_pool_t;

namespace net {

// 从 fd 读取 HTTP 请求（到头部结束 \r\n\r\n），写入 msg。返回读取字节数，0 表示对端关闭，-1 表示错误。
// 若 content_length >= 0 且已读满头部，会继续读 body 直到 content_length 字节，全部放入 msg。
int read_request(int fd, x_msg_t& msg, x_buf_pool_t& pool, int64_t& content_length_out);

// 将 msg 通过 get_iovec + writev 发送到 fd。返回写入字节数，-1 表示错误。
int write_response(int fd, const x_msg_t& msg);

void close_fd(int fd);

}

#endif
