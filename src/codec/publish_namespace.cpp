#include "codec_internal.h"

#include <stdexcept>
#include <utility>

namespace moq::codec {

ByteBuffer encode_publish_namespace(RequestId request_id, const TrackNamespace &track_namespace,
                                    std::vector<Parameter> parameters) {
  ByteBuffer payload;
  write_varint(payload, request_id);
  write_track_namespace(payload, track_namespace);
  if (!detail::encode_parameters(payload, std::move(parameters))) {
    throw std::invalid_argument("PUBLISH_NAMESPACE includes an unsupported or duplicate parameter");
  }
  ByteBuffer framed;
  append_control_message(framed, kMessagePublishNamespace, payload);
  return framed;
}

} // namespace moq::codec
