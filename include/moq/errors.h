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

enum class RequestErrorCode : uint64_t {
  InternalError = 0x0,
  Unauthorized = 0x1,
  Timeout = 0x2,
  NotSupported = 0x3,
  MalformedAuthToken = 0x4,
  ExpiredAuthToken = 0x5,
  GoingAway = 0x6,
  ExcessiveLoad = 0x9,
  DoesNotExist = 0x10,
  InvalidRange = 0x11,
  MalformedTrack = 0x12,
  DuplicateSubscription = 0x19,
  Uninterested = 0x20,
  PrefixOverlap = 0x30,
  NamespaceTooLarge = 0x31,
  InvalidJoiningRequestId = 0x32,
  UnsupportedExtension = 0x33,
  Redirect = 0x34,
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
  RequestRejected(RequestErrorCode code, uint64_t retry_interval, std::string reason);

  RequestErrorCode code() const noexcept;
  uint64_t retry_interval() const noexcept;
  const std::string &reason() const noexcept;

private:
  RequestErrorCode code_;
  uint64_t retry_interval_;
  std::string reason_;
};

} // namespace moq
