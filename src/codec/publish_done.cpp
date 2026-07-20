#include "codec_internal.h"

namespace moq::codec {

std::optional<PublishDone> decode_publish_done(const ByteBuffer &payload, std::string &error) {
  detail::Cursor cursor{payload};
  PublishDone decoded;
  if (!cursor.read_varint(decoded.status_code) || !cursor.read_varint(decoded.stream_count) ||
      !detail::read_reason(cursor, decoded.reason) || cursor.remaining() != 0) {
    error = "invalid PUBLISH_DONE";
    return std::nullopt;
  }
  return decoded;
}

ByteBuffer encode_publish_done(uint64_t status_code, uint64_t stream_count, const std::string &reason) {
  ByteBuffer payload;
  write_varint(payload, status_code);
  write_varint(payload, stream_count);
  write_varint(payload, reason.size());
  detail::append_bytes(payload, reason);
  ByteBuffer framed;
  append_control_message(framed, kMessagePublishDone, payload);
  return framed;
}

} // namespace moq::codec
