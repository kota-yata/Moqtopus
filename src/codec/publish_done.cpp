#include "moq/codec.h"

#include "codec/internal.h"

#include <optional>

namespace moq::codec {

std::optional<PublishDone> decode_publish_done(
    const ByteBuffer& payload, std::string& error) {
    internal::Cursor cursor{payload};
    PublishDone decoded;
    if (!cursor.read_varint(decoded.status_code) || !cursor.read_varint(decoded.stream_count) ||
        !internal::read_reason(cursor, decoded.reason) || cursor.remaining() != 0) {
        error = "invalid PUBLISH_DONE";
        return std::nullopt;
    }
    return decoded;
}

} // namespace moq::codec
