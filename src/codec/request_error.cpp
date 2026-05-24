#include "moq/codec.h"

#include "codec/internal.h"

#include <cstddef>
#include <optional>

namespace moq::codec {

ByteBuffer encode_request_error(uint64_t code, std::string reason, uint64_t retry_interval) {
    ByteBuffer payload;
    write_varint(payload, code);
    write_varint(payload, retry_interval);
    write_varint(payload, reason.size());
    internal::append_bytes(payload, reason);
    ByteBuffer framed;
    append_control_message(framed, kMessageRequestError, payload);
    return framed;
}

std::optional<RequestError> decode_request_error(
    const ByteBuffer& payload, std::string& error) {
    internal::Cursor cursor{payload};
    RequestError decoded;
    if (!cursor.read_varint(decoded.code) || !cursor.read_varint(decoded.retry_interval) ||
        !internal::read_reason(cursor, decoded.reason)) {
        error = "invalid REQUEST_ERROR";
        return std::nullopt;
    }
    if (decoded.code == 0x34) {
        uint64_t uri_size = 0;
        std::string uri;
        uint64_t track_name_size = 0;
        std::string track_name;
        if (!cursor.read_varint(uri_size) || uri_size > 8192 ||
            !cursor.read_string(static_cast<size_t>(uri_size), uri) ||
            !internal::read_track_namespace(cursor) ||
            !cursor.read_varint(track_name_size) || track_name_size > 4096 ||
            !cursor.read_string(static_cast<size_t>(track_name_size), track_name)) {
            error = "invalid REQUEST_ERROR redirect";
            return std::nullopt;
        }
    }
    if (cursor.remaining() != 0) {
        error = "trailing REQUEST_ERROR bytes";
        return std::nullopt;
    }
    return decoded;
}

} // namespace moq::codec
