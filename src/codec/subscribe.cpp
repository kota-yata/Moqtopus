#include "moq/codec.h"

#include "codec/internal.h"

#include <stdexcept>

namespace moq::codec {

ByteBuffer encode_subscribe(RequestId request_id, const SubscribeRequest& request) {
    ByteBuffer payload;
    write_varint(payload, request_id);
    write_track_namespace(payload, request.track_namespace);
    write_varint(payload, request.track_name.size());
    internal::append_bytes(payload, request.track_name);
    if (!internal::encode_parameters(payload, request.parameters)) {
        throw std::invalid_argument("SUBSCRIBE includes an unsupported or duplicate parameter");
    }

    ByteBuffer framed;
    append_control_message(framed, kMessageSubscribe, payload);
    return framed;
}

} // namespace moq::codec
