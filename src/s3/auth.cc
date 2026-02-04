#include "s3/auth.h"
#include "config/config.h"
#include "http/http_request.h"
#include <openssl/hmac.h>
#include <openssl/evp.h>
#include <ctime>
#include <cstring>
#include <string>

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

bool verify_query_signature(const HttpRequest& req, const s3config::Config& config) {
    std::string access_key = req.get_query_param("AWSAccessKeyId");
    std::string sig_from_client = req.get_query_param("Signature");
    std::string expires_str = req.get_query_param("Expires");
    if (access_key.empty() || sig_from_client.empty() || expires_str.empty())
        return false;
    if (access_key != config.access_key)
        return false;
    int64_t expires = 0;
    for (char c : expires_str) { if (c >= '0' && c <= '9') expires = expires * 10 + (c - '0'); }
    if (::time(nullptr) > static_cast<time_t>(expires))
        return false;
    // StringToSign for query auth v2: Method + "\n" + Content-MD5 + "\n" + Content-Type + "\n" + Expires + "\n" + CanonicalizedAmzHeaders + CanonicalizedResource
    std::string string_to_sign;
    string_to_sign += req.method;
    string_to_sign += '\n';
    string_to_sign += req.content_md5;
    string_to_sign += '\n';
    string_to_sign += req.content_type;
    string_to_sign += '\n';
    string_to_sign += expires_str;
    string_to_sign += '\n';
    // CanonicalizedAmzHeaders: empty if no x-amz-* headers (we don't parse them in http_parser for now)
    // CanonicalizedResource: path-style = path only
    string_to_sign += req.path;
    std::string expected_sig = hmac_sha1_base64(config.secret_key, string_to_sign);
    return expected_sig == sig_from_client;
}

} // namespace s3
