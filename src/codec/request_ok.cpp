#include "codec_internal.h"

#include <stdexcept>
#include <utility>

namespace moq::codec {

std::optional<RequestOk> decode_request_ok(const ByteBuffer &payload, std::string &error) {
  detail::Cursor cursor{payload};
  RequestOk ok;
  if (!detail::read_parameters_and_properties(cursor, ok.parameters, ok.track_properties, "REQUEST_OK", error)) {
    return std::nullopt;
  }
  return ok;
}

ByteBuffer encode_request_ok(std::vector<Parameter> parameters, const ObjectProperties &track_properties) {
  ByteBuffer payload;
  if (!detail::encode_parameters(payload, std::move(parameters))) {
    throw std::invalid_argument("REQUEST_OK includes an unsupported or duplicate parameter");
  }
  payload.insert(payload.end(), track_properties.begin(), track_properties.end());
  ByteBuffer framed;
  append_control_message(framed, kMessageRequestOk, payload);
  return framed;
}

} // namespace moq::codec
