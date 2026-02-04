#include "io_uring/file_io.h"

#include <fcntl.h>
#include <liburing.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace io_uring {

namespace {

constexpr unsigned RING_ENTRIES = 4;

struct RingHolder {
    struct io_uring ring;
    bool inited{false};

    struct io_uring* get() {
        if (!inited) {
            if (io_uring_queue_init(RING_ENTRIES, &ring, 0) != 0)
                return nullptr;
            inited = true;
        }
        return &ring;
    }

    ~RingHolder() {
        if (inited)
            io_uring_queue_exit(&ring);
    }
};

static thread_local RingHolder t_ring;

} 

ssize_t read_file(const std::string& path, void* buf, size_t capacity) {
    if (buf == nullptr || capacity == 0)
        return -1;

    struct io_uring* ring = t_ring.get();
    if (!ring) {
        errno = ENOMEM;
        return -1;
    }

    int fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return -1;

    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        ::close(fd);
        errno = ENOMEM;
        return -1;
    }

    io_uring_prep_read(sqe, fd, buf, capacity, 0);
    io_uring_sqe_set_data(sqe, nullptr);
    io_uring_submit(ring);

    struct io_uring_cqe* cqe = nullptr;
    int ret = io_uring_wait_cqe(ring, &cqe);
    if (ret != 0) {
        ::close(fd);
        errno = -ret;
        return -1;
    }

    ssize_t res = static_cast<ssize_t>(cqe->res);
    io_uring_cqe_seen(ring, cqe);
    ::close(fd);

    if (res < 0) {
        errno = -static_cast<int>(res);
        return -1;
    }
    return res;
}

ssize_t write_file(const std::string& path, const void* buf, size_t size) {
    if (buf == nullptr && size > 0)
        return -1;

    struct io_uring* ring = t_ring.get();
    if (!ring) {
        errno = ENOMEM;
        return -1;
    }

    int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0)
        return -1;

    if (size == 0) {
        ::close(fd);
        return 0;
    }

    struct io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (!sqe) {
        ::close(fd);
        errno = ENOMEM;
        return -1;
    }

    io_uring_prep_write(sqe, fd, buf, size, 0);
    io_uring_sqe_set_data(sqe, nullptr);
    io_uring_submit(ring);

    struct io_uring_cqe* cqe = nullptr;
    int ret = io_uring_wait_cqe(ring, &cqe);
    if (ret != 0) {
        ::close(fd);
        errno = -ret;
        return -1;
    }

    ssize_t res = static_cast<ssize_t>(cqe->res);
    io_uring_cqe_seen(ring, cqe);
    ::close(fd);

    if (res < 0) {
        errno = -static_cast<int>(res);
        return -1;
    }
    return res;
}

} 
