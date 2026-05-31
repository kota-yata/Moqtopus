#include "moq/codec.h"

#include "codec/internal.h"

#include <limits>
#include <optional>
#include <utility>

namespace moq::codec {

ByteBuffer encode_setup(std::string authority, std::string path) {
  ByteBuffer payload;
  if (!path.empty()) {
    internal::append_setup_option_bytes(payload, codec::SetupOption::Path, codec::SetupOption::None, path);
  }
  if (!authority.empty()) {
    internal::append_setup_option_bytes(payload, codec::SetupOption::Authority,
                                        path.empty() ? codec::SetupOption::None : codec::SetupOption::Path, authority);
  }
  // Fixed value for MOQT_IMPLEMENTATION
  internal::append_setup_option_bytes(payload, codec::SetupOption::MoqtImplementation,
                                      authority.empty()
                                          ? (path.empty() ? codec::SetupOption::None : codec::SetupOption::Path)
                                          : codec::SetupOption::Authority,
                                      "kota-moqtopus");

  ByteBuffer stream_bytes;
  append_control_message(stream_bytes, kMessageSetup, payload);
  return stream_bytes;
}

std::optional<Setup> decode_setup(const ByteBuffer &payload, std::string &error) {
  internal::Cursor cursor{payload};
  Setup setup;
  uint64_t last_type = 0;
  bool has_type = false;
  while (cursor.remaining() != 0) {
    uint64_t delta = 0;
    if (!cursor.read_varint(delta) || (has_type && delta > std::numeric_limits<uint64_t>::max() - last_type)) {
      error = "invalid SETUP option type delta";
      return std::nullopt;
    }

    const uint64_t type = has_type ? last_type + delta : delta;
    has_type = true;
    last_type = type;

    ByteBuffer value;
    if ((type & 1U) == 0) {
      uint64_t encoded = 0;
      if (!cursor.read_varint(encoded)) {
        error = "truncated SETUP varint option";
        return std::nullopt;
      }
      write_varint(value, encoded);
    } else {
      uint64_t size = 0;
      if (!cursor.read_varint(size) || size > 65535 || !cursor.read_bytes(static_cast<size_t>(size), value)) {
        error = "truncated SETUP length-prefixed option";
        return std::nullopt;
      }
    }

    if (type == static_cast<uint64_t>(SetupOption::Path) || type == static_cast<uint64_t>(SetupOption::Authority) ||
        type == static_cast<uint64_t>(SetupOption::MoqtImplementation)) {
      std::string decoded(reinterpret_cast<const char *>(value.data()), value.size());
      if (type == static_cast<uint64_t>(SetupOption::Path)) {
        setup.path = std::move(decoded);
      } else if (type == static_cast<uint64_t>(SetupOption::Authority)) {
        setup.authority = std::move(decoded);
      } else {
        setup.moqt_implementation = std::move(decoded);
      }
    }

    setup.options.push_back(Setup::Option{type, std::move(value)});
  }

  return setup;
}

} // namespace moq::codec
