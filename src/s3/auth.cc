#include "s3/auth.h"
#include "config/config.h"
#include "http/http_request.h"
#include "meta/meta.h"
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <ctime>
#include <cstring>
#include <string>
#include <iostream>

namespace s3 {

static std::string hmac_sha1_base64(const std::string& key, const std::string& data) {
    unsigned char md[EVP_MAX_MD_SIZE];
    unsigned int md_len = 0;
    HMAC(EVP_sha1(), key.data(), static_cast<int>(key.size()),
         reinterpret_cast<const unsigned char*>(data.data()), data.size(), md, &md_len);
    unsigned char buf[128];
    int n = EVP_EncodeBlock(buf, md, md_len);
    if (n <= 0) return {};
    return std::string(reinterpret_cast<char*>(buf), static_cast<size_t>(n));
}

bool verify_query_signature(const http::HttpRequest& req, const s3config::Config& config,
                            const meta::MetaStore& store) {
    std::string access_key = req.get_query_param("AWSAccessKeyId");
    std::string sig_from_client = req.get_query_param("Signature");
    std::string expires_str = req.get_query_param("Expires");
    if (access_key.empty() || sig_from_client.empty() || expires_str.empty())
        return false;
    std::string secret = store.get_secret_by_access_key(access_key);
    if (secret.empty() && access_key == config.access_key)
        secret = config.secret_key;
    if (secret.empty()){
        return false;
    }
    
    int64_t expires = 0;
    for (char c : expires_str) { if (c >= '0' && c <= '9') expires = expires * 10 + (c - '0'); }
    if (static_cast<int64_t>(std::time(nullptr)) > expires)
        return false;
    // StringToSign v2: Method + "\n" + Content-MD5 + "\n" + Content-Type + "\n" + Expires + "\n" + CanonicalizedAmzHeaders + CanonicalizedResource
    std::string string_to_sign;
    string_to_sign += req.method;
    string_to_sign += '\n';
    string_to_sign += req.content_md5;
    string_to_sign += '\n';
    string_to_sign += req.content_type;
    string_to_sign += '\n';
    string_to_sign += expires_str;
    string_to_sign += '\n';
    string_to_sign += req.path;
    std::string expected_sig = hmac_sha1_base64(secret, string_to_sign);
    std::cout << string_to_sign << " " << secret << " " << expected_sig << " " << sig_from_client << std::endl;
    if (expected_sig != sig_from_client) {
        std::cerr << "[S3 auth] Signature does not match. Server used this StringToSign (5 lines):\n"
                  << "  line1(Method):     [" << req.method << "]\n"
                  << "  line2(Content-MD5): [" << req.content_md5 << "]\n"
                  << "  line3(Content-Type):[" << req.content_type << "]\n"
                  << "  line4(Expires):     [" << expires_str << "]\n"
                  << "  line5(Path):        [" << req.path << "]\n"
                  << "  Client must sign exactly this (with \\n between lines, no trailing \\n after path).\n";
    }
    return expected_sig == sig_from_client;
}

} 
