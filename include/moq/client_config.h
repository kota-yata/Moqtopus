// Transport configuration shared by the subscriber and publisher sessions
#pragma once

#include <chrono>
#include <cstdint>
#include <string>

namespace moq {

struct MsQuicClientConfig {
  std::string host;
  uint16_t port = 0;
  std::string alpn = "moqt-18";
  std::string authority;
  std::string path = "/";
  bool disable_certificate_validation = true;
  std::chrono::milliseconds idle_timeout{30000};
};

} // namespace moq
