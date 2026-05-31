#pragma once

#include <cstdint>
#include <stdexcept>
#include <string>

namespace moq {

enum class SessionCloseErrorCode : uint64_t {
  NoError = 0x0,
  InternalError = 0x1,
  Unauthorized = 0x2,
  ProtocolViolation = 0x3,
  InvalidRequestId = 0x4,
  DuplicateTrackAlias = 0x5,
  KeyValueFormattingError = 0x6,
};

struct ReceiveError {
  uint64_t code = 0;
  std::string message;
};

class ProtocolError : public std::runtime_error {
public:
  explicit ProtocolError(const std::string &message) : std::runtime_error(message) {}
};

class RequestRejected : public std::runtime_error {
public:
  RequestRejected(uint64_t code, uint64_t retry_interval, std::string reason);

  uint64_t code() const noexcept;
  uint64_t retry_interval() const noexcept;
  const std::string &reason() const noexcept;

private:
  uint64_t code_;
  uint64_t retry_interval_;
  std::string reason_;
};

} // namespace moq
