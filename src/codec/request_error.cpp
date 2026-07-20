#include "codec_internal.h"

namespace moq::codec {

ByteBuffer encode_request_error(RequestErrorCode code, std::string reason, uint64_t retry_interval) {
  ByteBuffer payload;
  write_varint(payload, static_cast<uint64_t>(code));
  write_varint(payload, retry_interval);
  write_varint(payload, reason.size());
  detail::append_bytes(payload, reason);
  ByteBuffer framed;
  append_control_message(framed, kMessageRequestError, payload);
  return framed;
}

std::optional<RequestError> decode_request_error(const ByteBuffer &payload, std::string &error) {
  detail::Cursor cursor{payload};
  RequestError decoded;
  uint64_t raw_code = 0;
  if (!cursor.read_varint(raw_code) || !cursor.read_varint(decoded.retry_interval) ||
      !detail::read_reason(cursor, decoded.reason)) {
    error = "invalid REQUEST_ERROR";
    return std::nullopt;
  }
  decoded.code = static_cast<RequestErrorCode>(raw_code);
  if (decoded.code != RequestErrorCode::Redirect && cursor.remaining() != 0) {
    error = "trailing REQUEST_ERROR bytes";
    return std::nullopt;
  }
  return decoded;
}

} // namespace moq::codec
