#include "moq/codec.h"

#include "codec/internal.h"

#include <stdexcept>

namespace moq::codec {

ByteBuffer encode_request_update(RequestId request_id, const RequestUpdate &update) {
  ByteBuffer payload;
  write_varint(payload, request_id);
  if (!internal::encode_parameters(payload, update.parameters)) {
    throw std::invalid_argument("REQUEST_UPDATE includes an unsupported or duplicate parameter");
  }
  ByteBuffer framed;
  append_control_message(framed, kMessageRequestUpdate, payload);
  return framed;
}

} // namespace moq::codec
