#pragma once

#include "moq/codec.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace moq::codec::internal {

struct Cursor {
  const ByteBuffer &bytes;
  size_t offset = 0;

  bool read_byte(uint8_t &value);
  bool read_varint(uint64_t &value);
  bool read_bytes(size_t size, ByteBuffer &value);
  bool read_string(size_t size, std::string &value);
  size_t remaining() const;
};

void append_bytes(ByteBuffer &out, const std::string &value);
bool read_reason(Cursor &cursor, std::string &reason);
bool read_track_namespace(Cursor &cursor, TrackNamespace *result = nullptr);
bool skip_key_value_pairs(Cursor &cursor);
bool read_parameters(Cursor &cursor, uint64_t count, std::vector<Parameter> &parameters, std::string &error);
bool encode_parameters(ByteBuffer &payload, std::vector<Parameter> parameters);
void append_setup_option_bytes(ByteBuffer &payload, codec::SetupOption type, codec::SetupOption previous_type,
                               const std::string &value);

} // namespace moq::codec::internal
