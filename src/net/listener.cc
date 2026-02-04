#include "net/listener.h"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <stdexcept>

namespace net {

int listen_tcp(const std::string& addr, uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int on = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) < 0) {
        close(fd);
        return -1;
    }
    struct sockaddr_in sa;
    std::memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_port = htons(port);
    if (addr.empty() || addr == "0.0.0.0") {
        sa.sin_addr.s_addr = INADDR_ANY;
    } else {
        if (inet_pton(AF_INET, addr.c_str(), &sa.sin_addr) <= 0) {
            close(fd);
            return -1;
        }
    }
    if (bind(fd, reinterpret_cast<struct sockaddr*>(&sa), sizeof(sa)) < 0) {
        close(fd);
        return -1;
    }
    if (::listen(fd, 128) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

int accept_one(int listen_fd) {
    struct sockaddr_in peer;
    socklen_t len = sizeof(peer);
    return accept(listen_fd, reinterpret_cast<struct sockaddr*>(&peer), &len);
}

} 
