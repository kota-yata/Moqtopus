#include "moq/codec.h"

#include "codec/internal.h"

#include <cstddef>
#include <optional>

namespace moq::codec {

std::optional<RequestOk> decode_request_ok(
    const ByteBuffer& payload, std::string& error) {
    internal::Cursor cursor{payload};
    RequestOk ok;
    uint64_t parameter_count = 0;
    if (!cursor.read_varint(parameter_count) ||
        !internal::read_parameters(cursor, parameter_count, ok.parameters, error)) {
        if (error.empty()) {
            error = "invalid REQUEST_OK";
        }
        return std::nullopt;
    }
    ok.track_properties.assign(
        payload.begin() + static_cast<std::ptrdiff_t>(cursor.offset), payload.end());
    return ok;
}

} // namespace moq::codec
