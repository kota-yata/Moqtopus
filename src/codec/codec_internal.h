#pragma once

#include "moq/codec.h"

#include <algorithm>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace moq::codec::detail {

struct Cursor {
  const ByteBuffer &bytes;
  size_t offset = 0;

  size_t remaining() const { return bytes.size() - offset; }

  bool read_byte(uint8_t &value) {
    if (offset >= bytes.size()) {
      return false;
    }
    value = bytes[offset++];
    return true;
  }

  bool read_varint(uint64_t &value) {
    const VarintResult parsed = codec::read_varint(bytes, offset);
    if (parsed.status != DecodeStatus::Done) {
      return false;
    }
    offset += parsed.bytes;
    value = parsed.value;
    return true;
  }

  bool read_bytes(size_t size, ByteBuffer &value) {
    if (size > remaining()) {
      return false;
    }
    value.assign(bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                 bytes.begin() + static_cast<std::ptrdiff_t>(offset + size));
    offset += size;
    return true;
  }

  bool read_string(size_t size, std::string &value) {
    if (size > remaining()) {
      return false;
    }
    value.assign(reinterpret_cast<const char *>(bytes.data() + offset), size);
    offset += size;
    return true;
  }
};

inline void append_bytes(ByteBuffer &out, const std::string &value) {
  out.insert(out.end(), value.begin(), value.end());
}

inline bool read_reason(Cursor &cursor, std::string &reason) {
  uint64_t size = 0;
  if (!cursor.read_varint(size) || size > 1024) {
    return false;
  }
  return cursor.read_string(static_cast<size_t>(size), reason);
}

inline bool read_track_namespace(Cursor &cursor, TrackNamespace *result = nullptr) {
  uint64_t fields = 0;
  if (!cursor.read_varint(fields) || fields > 32) {
    return false;
  }
  TrackNamespace decoded;
  size_t total = 0;
  for (uint64_t index = 0; index < fields; ++index) {
    uint64_t size = 0;
    std::string field;
    if (!cursor.read_varint(size) || size == 0 || size > cursor.remaining() || size > 4096 || total + size > 4096 ||
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

enum class ParameterEncoding {
  Uint8,
  Varint,
  Location,
  LengthPrefixed,
  TrackNamespace,
};

inline std::optional<ParameterEncoding> parameter_encoding(uint64_t type) {
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

inline bool skip_parameter_value(Cursor &cursor, ParameterEncoding encoding) {
  uint8_t byte = 0;
  uint64_t first = 0;
  uint64_t second = 0;
  ByteBuffer bytes;
  switch (encoding) {
  case ParameterEncoding::Uint8:
    return cursor.read_byte(byte);
  case ParameterEncoding::Varint:
    return cursor.read_varint(first);
  case ParameterEncoding::Location:
    return cursor.read_varint(first) && cursor.read_varint(second);
  case ParameterEncoding::LengthPrefixed:
    return cursor.read_varint(first) && first <= 65535 && cursor.read_bytes(static_cast<size_t>(first), bytes);
  case ParameterEncoding::TrackNamespace:
    return read_track_namespace(cursor);
  }
  return false;
}

inline bool read_parameters(Cursor &cursor, uint64_t count, std::vector<Parameter> &parameters, std::string &error) {
  uint64_t previous_type = 0;
  bool have_previous = false;
  for (uint64_t index = 0; index < count; ++index) {
    uint64_t delta = 0;
    if (!cursor.read_varint(delta) || (have_previous && delta > std::numeric_limits<uint64_t>::max() - previous_type)) {
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
    if (!skip_parameter_value(cursor, *encoding)) {
      error = "truncated message parameter " + std::to_string(type);
      return false;
    }

    Parameter parameter;
    parameter.type = type;
    parameter.encoded_value.assign(cursor.bytes.begin() + static_cast<std::ptrdiff_t>(value_start),
                                   cursor.bytes.begin() + static_cast<std::ptrdiff_t>(cursor.offset));
    parameters.push_back(std::move(parameter));
  }
  return true;
}

inline bool read_parameters_and_properties(Cursor &cursor, std::vector<Parameter> &parameters,
                                           ObjectProperties &properties, const char *what, std::string &error) {
  uint64_t parameter_count = 0;
  if (!cursor.read_varint(parameter_count) || !read_parameters(cursor, parameter_count, parameters, error)) {
    if (error.empty()) {
      error = std::string("invalid ") + what;
    }
    return false;
  }
  properties.assign(cursor.bytes.begin() + static_cast<std::ptrdiff_t>(cursor.offset), cursor.bytes.end());
  return true;
}

inline bool encode_parameters(ByteBuffer &payload, std::vector<Parameter> parameters) {
  std::stable_sort(parameters.begin(), parameters.end(),
                   [](const Parameter &left, const Parameter &right) { return left.type < right.type; });
  write_varint(payload, parameters.size());
  uint64_t previous = 0;
  bool have_previous = false;
  for (const Parameter &parameter : parameters) {
    if ((have_previous && parameter.type <= previous) || !parameter_encoding(parameter.type)) {
      return false;
    }
    write_varint(payload, have_previous ? parameter.type - previous : parameter.type);
    payload.insert(payload.end(), parameter.encoded_value.begin(), parameter.encoded_value.end());
    previous = parameter.type;
    have_previous = true;
  }
  return true;
}

} // namespace moq::codec::detail
