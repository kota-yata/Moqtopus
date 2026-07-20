#include "codec_internal.h"

#include <limits>

namespace moq::codec {
namespace {

void append_setup_option_bytes(ByteBuffer &payload, SetupOption type, SetupOption previous_type,
                               const std::string &value) {
  write_varint(payload, static_cast<uint64_t>(type) - static_cast<uint64_t>(previous_type));
  write_varint(payload, value.size());
  detail::append_bytes(payload, value);
}

} // namespace

ByteBuffer encode_setup(std::string authority, std::string path) {
  ByteBuffer payload;
  SetupOption previous = SetupOption::None;
  if (!path.empty()) {
    append_setup_option_bytes(payload, SetupOption::Path, previous, path);
    previous = SetupOption::Path;
  }
  if (!authority.empty()) {
    append_setup_option_bytes(payload, SetupOption::Authority, previous, authority);
    previous = SetupOption::Authority;
  }
  append_setup_option_bytes(payload, SetupOption::MoqtImplementation, previous, "kota-moqtopus");

  ByteBuffer stream_bytes;
  append_control_message(stream_bytes, kMessageSetup, payload);
  return stream_bytes;
}

bool decode_setup(const ByteBuffer &payload, std::string &error) {
  detail::Cursor cursor{payload};
  uint64_t last_type = 0;
  bool has_type = false;
  while (cursor.remaining() != 0) {
    uint64_t delta = 0;
    if (!cursor.read_varint(delta) || (has_type && delta > std::numeric_limits<uint64_t>::max() - last_type)) {
      error = "invalid SETUP option type delta";
      return false;
    }
    last_type = has_type ? last_type + delta : delta;
    has_type = true;

    if ((last_type & 1U) == 0) {
      uint64_t value = 0;
      if (!cursor.read_varint(value)) {
        error = "truncated SETUP varint option";
        return false;
      }
      continue;
    }
    uint64_t size = 0;
    ByteBuffer value;
    if (!cursor.read_varint(size) || size > 65535 || !cursor.read_bytes(static_cast<size_t>(size), value)) {
      error = "truncated SETUP length-prefixed option";
      return false;
    }
  }
  return true;
}

} // namespace moq::codec
