cmake_minimum_required(VERSION 3.10)
project(s3server)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# set(CMAKE_BUILD_TYPE Debug)
# # 使用系统 OpenSSL（避免混用 MSYS2 的 MinGW 库导致 R_AMD64_IMAGEBASE 链接错误）
# if(CMAKE_HOST_SYSTEM_NAME STREQUAL "Linux" AND EXISTS "/usr/lib/x86_64-linux-gnu/libcrypto.so")
#   set(OPENSSL_FOUND TRUE)
#   set(OPENSSL_INCLUDE_DIR "/usr/include")
#   set(OPENSSL_CRYPTO_LIBRARY "/usr/lib/x86_64-linux-gnu/libcrypto.so")
#   set(OPENSSL_SSL_LIBRARY "/usr/lib/x86_64-linux-gnu/libssl.so")
#   add_library(OpenSSL::Crypto SHARED IMPORTED)
#   set_target_properties(OpenSSL::Crypto PROPERTIES IMPORTED_LOCATION "${OPENSSL_CRYPTO_LIBRARY}" INTERFACE_INCLUDE_DIRECTORIES "${OPENSSL_INCLUDE_DIR}")
#   add_library(OpenSSL::SSL SHARED IMPORTED)
#   set_target_properties(OpenSSL::SSL PROPERTIES IMPORTED_LOCATION "${OPENSSL_SSL_LIBRARY}" INTERFACE_INCLUDE_DIRECTORIES "${OPENSSL_INCLUDE_DIR}" INTERFACE_LINK_LIBRARIES OpenSSL::Crypto)
#   message(STATUS "Using system OpenSSL: ${OPENSSL_CRYPTO_LIBRARY}")
# else()
#   find_package(OpenSSL REQUIRED)
# endif()
find_package(OpenSSL REQUIRED)

find_package(PkgConfig QUIET)
if(PkgConfig_FOUND)
  pkg_check_modules(URING QUIET liburing)
endif()
if(NOT URING_FOUND)
  find_library(URING_LIBRARY NAMES uring)
  find_path(URING_INCLUDE_DIR NAMES liburing.h)
  if(URING_LIBRARY AND URING_INCLUDE_DIR)
    set(URING_FOUND TRUE)
    set(URING_LIBRARIES ${URING_LIBRARY})
    set(URING_INCLUDE_DIRS ${URING_INCLUDE_DIR})
  endif()
endif()
if(NOT URING_FOUND)
  message(FATAL_ERROR "liburing not found. Install liburing-dev (e.g. apt install liburing-dev)")
endif()

set(SOURCES
  src/msg/msg_buffer4.cc
  src/config/config.cc
  src/http/http_parser.cc
  src/http/http_request.cc
  src/net/listener.cc
  src/net/connection.cc
  src/meta/meta.cc
  src/io_uring/file_io.cc
  src/s3/auth.cc
  src/s3/handler.cc
  src/s3/response.cc
  src/server.cc
)

add_executable(s3server ${SOURCES})

target_compile_options(s3server PRIVATE -Wall -Wextra
  $<$<CONFIG:Debug>:-O0 -g>
  $<$<CONFIG:Release>:-O2>)
target_compile_definitions(s3server PRIVATE _GNU_SOURCE)

target_include_directories(s3server PRIVATE
  ${CMAKE_SOURCE_DIR}/include
  ${OPENSSL_INCLUDE_DIR}
  ${URING_INCLUDE_DIRS}
)
target_link_libraries(s3server PRIVATE pthread OpenSSL::SSL OpenSSL::Crypto ${URING_LIBRARIES})
