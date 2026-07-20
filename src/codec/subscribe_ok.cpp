#include "codec_internal.h"

#include <stdexcept>
#include <utility>

namespace moq::codec {

std::optional<SubscribeOk> decode_subscribe_ok(const ByteBuffer &payload, std::string &error) {
  detail::Cursor cursor{payload};
  SubscribeOk ok;
  if (!cursor.read_varint(ok.track_alias) ||
      !detail::read_parameters_and_properties(cursor, ok.parameters, ok.track_properties, "SUBSCRIBE_OK", error)) {
    if (error.empty()) {
      error = "invalid SUBSCRIBE_OK";
    }
    return std::nullopt;
  }
  return ok;
}

ByteBuffer encode_subscribe_ok(TrackAlias track_alias, std::vector<Parameter> parameters,
                               const ObjectProperties &track_properties) {
  ByteBuffer payload;
  write_varint(payload, track_alias);
  if (!detail::encode_parameters(payload, std::move(parameters))) {
    throw std::invalid_argument("SUBSCRIBE_OK includes an unsupported or duplicate parameter");
  }
  payload.insert(payload.end(), track_properties.begin(), track_properties.end());
  ByteBuffer framed;
  append_control_message(framed, kMessageSubscribeOk, payload);
  return framed;
}

} // namespace moq::codec
