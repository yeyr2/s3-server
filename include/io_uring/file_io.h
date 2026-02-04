#ifndef S3_IO_URING_FILE_IO_H
#define S3_IO_URING_FILE_IO_H

#include <cstddef>
#include <string>

#include <sys/types.h>  /* ssize_t (POSIX, WSL/Ubuntu) */

namespace io_uring {

// 使用 io_uring 读整个文件到 buf（最多 capacity 字节）。
// 成功返回读到的字节数，失败返回 -1。
ssize_t read_file(const std::string& path, void* buf, size_t capacity);

// 使用 io_uring 将 buf 的 size 字节写入 path（创建或截断）。
// 成功返回写入的字节数（应为 size），失败返回 -1。
ssize_t write_file(const std::string& path, const void* buf, size_t size);

} 

#endif
