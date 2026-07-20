#include "codec_internal.h"

#include <stdexcept>

namespace moq::codec {

ByteBuffer encode_request_update(RequestId request_id, const RequestUpdate &update) {
  ByteBuffer payload;
  write_varint(payload, request_id);
  if (!detail::encode_parameters(payload, update.parameters)) {
    throw std::invalid_argument("REQUEST_UPDATE includes an unsupported or duplicate parameter");
  }
  ByteBuffer framed;
  append_control_message(framed, kMessageRequestUpdate, payload);
  return framed;
}

std::optional<RequestUpdateMessage> decode_request_update(const ByteBuffer &payload, std::string &error) {
  detail::Cursor cursor{payload};
  RequestUpdateMessage update;
  if (!cursor.read_varint(update.request_id)) {
    error = "invalid REQUEST_UPDATE Request ID";
    return std::nullopt;
  }
  uint64_t parameter_count = 0;
  if (!cursor.read_varint(parameter_count) ||
      !detail::read_parameters(cursor, parameter_count, update.parameters, error)) {
    if (error.empty()) {
      error = "invalid REQUEST_UPDATE parameters";
    }
    return std::nullopt;
  }
  if (cursor.remaining() != 0) {
    error = "trailing REQUEST_UPDATE bytes";
    return std::nullopt;
  }
  return update;
}

} // namespace moq::codec
