#include "moq/codec.h"

#include "codec/internal.h"
#include "moq/errors.h"

#include <algorithm>
#include <array>
#include <limits>
#include <optional>
#include <stdexcept>
#include <utility>

namespace moq {

Parameter Parameter::uint8(uint64_t type, uint8_t value) {
    return Parameter{type, ByteBuffer{value}};
}

Parameter Parameter::varint(uint64_t type, uint64_t value) {
    Parameter parameter;
    parameter.type = type;
    codec::write_varint(parameter.encoded_value, value);
    return parameter;
}

Parameter Parameter::location(uint64_t type, Location value) {
    Parameter parameter;
    parameter.type = type;
    codec::write_varint(parameter.encoded_value, value.group);
    codec::write_varint(parameter.encoded_value, value.object);
    return parameter;
}

Parameter Parameter::length_prefixed(uint64_t type, ByteBuffer value) {
    Parameter parameter;
    parameter.type = type;
    codec::write_varint(parameter.encoded_value, value.size());
    parameter.encoded_value.insert(
        parameter.encoded_value.end(), value.begin(), value.end());
    return parameter;
}

Parameter Parameter::track_namespace(uint64_t type, TrackNamespace value) {
    Parameter parameter;
    parameter.type = type;
    codec::write_track_namespace(parameter.encoded_value, value);
    return parameter;
}

RequestRejected::RequestRejected(
    uint64_t code, uint64_t retry_interval, std::string reason)
    : std::runtime_error(
          "MOQT request rejected: code=" + std::to_string(code) +
          (reason.empty() ? "" : " reason=" + reason)),
      code_(code),
      retry_interval_(retry_interval),
      reason_(std::move(reason)) {}

uint64_t RequestRejected::code() const noexcept {
    return code_;
}

uint64_t RequestRejected::retry_interval() const noexcept {
    return retry_interval_;
}

const std::string& RequestRejected::reason() const noexcept {
    return reason_;
}

} // namespace moq

namespace moq::codec {
namespace {

size_t varint_length(uint64_t value) {
    static constexpr std::array<uint64_t, 8> kMax = {
        0x7fULL,
        0x3fffULL,
        0x1fffffULL,
        0xfffffffULL,
        0x7ffffffffULL,
        0x3ffffffffffULL,
        0x1ffffffffffffULL,
        0xffffffffffffffULL,
    };
    for (size_t index = 0; index < kMax.size(); ++index) {
        if (value <= kMax[index]) {
            return index + 1;
        }
    }
    return 9;
}

uint8_t varint_value_bits(size_t length) {
    return length == 9 ? 0 : static_cast<uint8_t>(8 - length);
}

uint8_t varint_prefix(size_t length) {
    if (length == 9) {
        return 0xff;
    }
    if (length == 1) {
        return 0;
    }
    return static_cast<uint8_t>(0xff << (9 - length));
}

void append_u16(ByteBuffer& out, uint16_t value) {
    out.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
    out.push_back(static_cast<uint8_t>(value & 0xff));
}

} // namespace

namespace internal {
namespace {

enum class ParameterEncoding {
    Uint8,
    Varint,
    Location,
    LengthPrefixed,
    TrackNamespace,
};

std::optional<ParameterEncoding> parameter_encoding(uint64_t type) {
    switch (type) {
    case 0x02:
    case 0x04:
    case 0x06:
    case 0x08:
    case 0x0a:
    case 0x32:
        return ParameterEncoding::Varint;
    case 0x03:
    case 0x21:
        return ParameterEncoding::LengthPrefixed;
    case 0x09:
        return ParameterEncoding::Location;
    case 0x10:
    case 0x20:
    case 0x22:
        return ParameterEncoding::Uint8;
    case 0x34:
        return ParameterEncoding::TrackNamespace;
    default:
        return std::nullopt;
    }
}

} // namespace

void append_bytes(ByteBuffer& out, const std::string& value) {
    out.insert(out.end(), value.begin(), value.end());
}

bool Cursor::read_byte(uint8_t& value) {
    if (offset >= bytes.size()) {
        return false;
    }
    value = bytes[offset++];
    return true;
}

bool Cursor::read_varint(uint64_t& value) {
    const VarintResult parsed = codec::read_varint(bytes, offset);
    if (parsed.status != DecodeStatus::Done) {
        return false;
    }
    offset += parsed.bytes;
    value = parsed.value;
    return true;
}

bool Cursor::read_bytes(size_t size, ByteBuffer& value) {
    if (size > bytes.size() - offset) {
        return false;
    }
    value.assign(bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                 bytes.begin() + static_cast<std::ptrdiff_t>(offset + size));
    offset += size;
    return true;
}

bool Cursor::read_string(size_t size, std::string& value) {
    if (size > bytes.size() - offset) {
        return false;
    }
    value.assign(reinterpret_cast<const char*>(bytes.data() + offset), size);
    offset += size;
    return true;
}

size_t Cursor::remaining() const {
    return bytes.size() - offset;
}

bool read_reason(Cursor& cursor, std::string& reason) {
    uint64_t size = 0;
    if (!cursor.read_varint(size) || size > 1024) {
        return false;
    }
    return cursor.read_string(static_cast<size_t>(size), reason);
}

bool read_track_namespace(Cursor& cursor, TrackNamespace* result) {
    uint64_t fields = 0;
    if (!cursor.read_varint(fields) || fields > 32) {
        return false;
    }
    TrackNamespace decoded;
    size_t total = 0;
    for (uint64_t index = 0; index < fields; ++index) {
        uint64_t size = 0;
        std::string field;
        if (!cursor.read_varint(size) || size == 0 || size > cursor.remaining() ||
            size > 4096 || total + size > 4096 ||
            !cursor.read_string(static_cast<size_t>(size), field)) {
            return false;
        }
        total += static_cast<size_t>(size);
        decoded.push_back(std::move(field));
    }
    if (result) {
        *result = std::move(decoded);
    }
    return true;
}

bool skip_key_value_pairs(Cursor& cursor) {
    uint64_t last_type = 0;
    bool has_type = false;
    while (cursor.remaining() != 0) {
        uint64_t delta = 0;
        if (!cursor.read_varint(delta) ||
            (has_type && delta > std::numeric_limits<uint64_t>::max() - last_type)) {
            return false;
        }
        const uint64_t type = has_type ? last_type + delta : delta;
        has_type = true;
        last_type = type;
        if ((type & 1U) == 0) {
            uint64_t ignored = 0;
            if (!cursor.read_varint(ignored)) {
                return false;
            }
            continue;
        }
        uint64_t size = 0;
        ByteBuffer ignored;
        if (!cursor.read_varint(size) || size > 65535 ||
            !cursor.read_bytes(static_cast<size_t>(size), ignored)) {
            return false;
        }
    }
    return true;
}

bool read_parameters(
    Cursor& cursor,
    uint64_t count,
    std::vector<Parameter>& parameters,
    std::string& error) {
    uint64_t previous_type = 0;
    bool have_previous = false;
    for (uint64_t index = 0; index < count; ++index) {
        const size_t value_start_with_delta = cursor.offset;
        uint64_t delta = 0;
        if (!cursor.read_varint(delta) ||
            (have_previous &&
             delta > std::numeric_limits<uint64_t>::max() - previous_type)) {
            error = "invalid message parameter type delta";
            return false;
        }
        const uint64_t type = have_previous ? previous_type + delta : delta;
        if (have_previous && type <= previous_type) {
            error = "message parameters are not strictly ascending";
            return false;
        }
        previous_type = type;
        have_previous = true;

        const std::optional<ParameterEncoding> encoding = parameter_encoding(type);
        if (!encoding) {
            error = "unknown message parameter " + std::to_string(type);
            return false;
        }

        const size_t value_start = cursor.offset;
        switch (*encoding) {
        case ParameterEncoding::Uint8: {
            uint8_t ignored = 0;
            if (!cursor.read_byte(ignored)) {
                error = "truncated uint8 message parameter";
                return false;
            }
            break;
        }
        case ParameterEncoding::Varint: {
            uint64_t ignored = 0;
            if (!cursor.read_varint(ignored)) {
                error = "truncated varint message parameter";
                return false;
            }
            break;
        }
        case ParameterEncoding::Location: {
            uint64_t group = 0;
            uint64_t object = 0;
            if (!cursor.read_varint(group) || !cursor.read_varint(object)) {
                error = "truncated location message parameter";
                return false;
            }
            break;
        }
        case ParameterEncoding::LengthPrefixed: {
            uint64_t size = 0;
            ByteBuffer ignored;
            if (!cursor.read_varint(size) || size > 65535 ||
                !cursor.read_bytes(static_cast<size_t>(size), ignored)) {
                error = "truncated length-prefixed message parameter";
                return false;
            }
            break;
        }
        case ParameterEncoding::TrackNamespace:
            if (!read_track_namespace(cursor)) {
                error = "invalid track namespace message parameter";
                return false;
            }
            break;
        }

        Parameter parameter;
        parameter.type = type;
        parameter.encoded_value.assign(
            cursor.bytes.begin() + static_cast<std::ptrdiff_t>(value_start),
            cursor.bytes.begin() + static_cast<std::ptrdiff_t>(cursor.offset));
        parameters.push_back(std::move(parameter));
        (void)value_start_with_delta;
    }
    return true;
}

bool encode_parameters(ByteBuffer& payload, std::vector<Parameter> parameters) {
    std::stable_sort(parameters.begin(), parameters.end(),
                     [](const Parameter& left, const Parameter& right) {
                         return left.type < right.type;
                     });
    write_varint(payload, parameters.size());
    uint64_t previous = 0;
    bool have_previous = false;
    for (const Parameter& parameter : parameters) {
        if (have_previous && parameter.type <= previous) {
            return false;
        }
        const std::optional<ParameterEncoding> encoding = parameter_encoding(parameter.type);
        if (!encoding) {
            return false;
        }
        write_varint(payload, have_previous ? parameter.type - previous : parameter.type);
        payload.insert(
            payload.end(), parameter.encoded_value.begin(), parameter.encoded_value.end());
        previous = parameter.type;
        have_previous = true;
    }
    return true;
}

void append_setup_option_bytes(
    ByteBuffer& payload, uint64_t type, uint64_t previous_type, const std::string& value) {
    write_varint(payload, type - previous_type);
    write_varint(payload, value.size());
    append_bytes(payload, value);
}

} // namespace internal

void write_varint(ByteBuffer& out, uint64_t value) {
    const size_t length = varint_length(value);
    if (length == 9) {
        out.push_back(0xff);
        for (int shift = 56; shift >= 0; shift -= 8) {
            out.push_back(static_cast<uint8_t>((value >> shift) & 0xff));
        }
        return;
    }

    const uint8_t usable_first_bits = varint_value_bits(length);
    const uint64_t first_mask =
        usable_first_bits == 0 ? 0 : ((uint64_t{1} << usable_first_bits) - 1);
    const int following_bytes = static_cast<int>(length - 1);
    out.push_back(static_cast<uint8_t>(
        varint_prefix(length) | ((value >> (following_bytes * 8)) & first_mask)));
    for (int index = following_bytes - 1; index >= 0; --index) {
        out.push_back(static_cast<uint8_t>((value >> (index * 8)) & 0xff));
    }
}

VarintResult read_varint(const ByteBuffer& bytes, size_t offset) {
    if (offset >= bytes.size()) {
        return {};
    }

    const uint8_t first = bytes[offset];
    size_t leading_ones = 0;
    while (leading_ones < 8 && (first & (0x80 >> leading_ones)) != 0) {
        ++leading_ones;
    }
    const size_t length = leading_ones == 8 ? 9 : leading_ones + 1;
    if (length > bytes.size() - offset) {
        return {};
    }

    uint64_t value = 0;
    if (length == 9) {
        for (size_t index = 1; index < 9; ++index) {
            value = (value << 8) | bytes[offset + index];
        }
    } else {
        const uint8_t first_bits = varint_value_bits(length);
        value = first_bits == 0 ? 0 : first & ((uint8_t{1} << first_bits) - 1);
        for (size_t index = 1; index < length; ++index) {
            value = (value << 8) | bytes[offset + index];
        }
    }
    return VarintResult{DecodeStatus::Done, value, length, {}};
}

void append_control_message(ByteBuffer& out, uint64_t type, const ByteBuffer& payload) {
    if (payload.size() > std::numeric_limits<uint16_t>::max()) {
        throw std::length_error("MOQT control message payload exceeds 65535 bytes");
    }
    write_varint(out, type);
    append_u16(out, static_cast<uint16_t>(payload.size()));
    out.insert(out.end(), payload.begin(), payload.end());
}

ControlMessageResult read_control_message(const ByteBuffer& bytes, size_t offset) {
    const VarintResult type = read_varint(bytes, offset);
    if (type.status != DecodeStatus::Done) {
        return {};
    }
    if (bytes.size() - offset < type.bytes + 2) {
        return {};
    }
    const size_t length_offset = offset + type.bytes;
    const uint16_t length = static_cast<uint16_t>(
        (static_cast<uint16_t>(bytes[length_offset]) << 8) | bytes[length_offset + 1]);
    const size_t frame_size = type.bytes + 2 + length;
    if (bytes.size() - offset < frame_size) {
        return {};
    }

    ControlMessage message;
    message.type = type.value;
    message.payload.assign(
        bytes.begin() + static_cast<std::ptrdiff_t>(length_offset + 2),
        bytes.begin() + static_cast<std::ptrdiff_t>(length_offset + 2 + length));
    return ControlMessageResult{DecodeStatus::Done, std::move(message), frame_size, {}};
}

void write_track_namespace(ByteBuffer& out, const TrackNamespace& name_space) {
    if (name_space.size() > 32) {
        throw std::invalid_argument("MOQT track namespace has more than 32 fields");
    }
    write_varint(out, name_space.size());
    size_t total_size = 0;
    for (const std::string& field : name_space) {
        if (field.empty() || total_size + field.size() > 4096) {
            throw std::invalid_argument("invalid MOQT track namespace field");
        }
        write_varint(out, field.size());
        internal::append_bytes(out, field);
        total_size += field.size();
    }
}

// Subgroup header is not a single fixed value
bool is_subgroup_stream_type(uint64_t type) {
    if ((type & 0x10) == 0 || type > 0x7f) {
        return false;
    }
    return ((type & 0x06) >> 1) != 0x03;
}

} // namespace moq::codec
