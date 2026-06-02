#include "peer_stream_demux.h"

#include <spdlog/spdlog.h>
#include <utility>

namespace moq::detail {
namespace {

uint16_t read_u16(const ByteBuffer &bytes, size_t offset) {
  return static_cast<uint16_t>((static_cast<uint16_t>(bytes[offset]) << 8) | bytes[offset + 1]);
}

} // namespace

PeerUniDemux::PeerUniDemux(std::shared_ptr<StreamContext> stream, SessionCallbacks callbacks)
    : stream_(std::move(stream)), on_setup_(std::move(callbacks.on_setup)),
      on_subgroup_(std::move(callbacks.on_subgroup)), on_padding_(std::move(callbacks.on_padding)),
      on_fetch_(std::move(callbacks.on_fetch)), on_protocol_violation_(std::move(callbacks.on_protocol_violation)) {}

// dispatches bytes on a unidirectional stream based on the prefix varint
void PeerUniDemux::feed(ByteBuffer input, bool fin) {
  spdlog::debug("PeerUniDemux feed called with {} bytes (fin={})", input.size(), fin);
  bytes_.insert(bytes_.end(), input.begin(), input.end());

  const codec::VarintResult type = codec::read_varint(bytes_);
  if (type.status != codec::DecodeStatus::Done) {
    if (fin) {
      on_protocol_violation_("peer unidirectional stream ended before type");
    }
    return;
  }

  const std::shared_ptr<StreamContext> stream = stream_.lock();
  if (!stream) {
    return;
  }

  switch (type.value) {
  case codec::kSetupStreamType: {
    spdlog::debug("PeerUniDemux received stream with SETUP type");
    const size_t length_offset = type.bytes;
    if (bytes_.size() < length_offset + 2) {
      if (fin) {
        on_protocol_violation_("peer setup stream ended before setup length");
      }
      return;
    }

    const uint16_t length = read_u16(bytes_, length_offset);
    const size_t frame_size = type.bytes + 2 + length;
    if (bytes_.size() < frame_size) {
      if (fin) {
        on_protocol_violation_("peer setup stream ended before setup payload");
      }
      return;
    }

    ByteBuffer payload(bytes_.begin() + length_offset + 2, bytes_.begin() + frame_size);
    std::string error;
    std::optional<codec::Setup> setup = codec::decode_setup(payload, error);
    if (!setup) {
      on_protocol_violation_(std::move(error));
      return;
    }

    spdlog::debug("PeerUniDemux decoded SETUP message");
    on_setup_(std::move(*setup));
    bytes_.erase(bytes_.begin(), bytes_.begin() + frame_size);
    break;
  }
  case codec::kFetchStreamType: {
    on_fetch_(stream);
    break;
  }
  case codec::kPaddingStreamType: {
    on_padding_(stream, std::move(bytes_), type.bytes);
    break;
  }
  default: {
    if (codec::is_subgroup_stream_type(type.value)) {
      on_subgroup_(stream, std::move(bytes_), fin);
    } else {
      on_protocol_violation_("unknown peer unidirectional stream type " + std::to_string(type.value));
    }
    break;
  }
  }
}

PeerBidiDemux::PeerBidiDemux(std::shared_ptr<StreamContext> stream, SessionCallbacksBidi callbacks)
    : stream_(std::move(stream)), on_request_(std::move(callbacks.on_request)),
      on_protocol_violation_(std::move(callbacks.on_protocol_violation)) {}

// dispatches bytes on a bidirectional stream as a single request message
void PeerBidiDemux::feed(ByteBuffer input, bool fin) {
  bytes_.insert(bytes_.end(), input.begin(), input.end());
  const codec::ControlMessageResult message = codec::read_control_message(bytes_);
  if (message.status != codec::DecodeStatus::Done) {
    if (fin) {
      on_protocol_violation_("peer bidirectional stream ended before first request");
    }
    return;
  }

  const std::shared_ptr<StreamContext> stream = stream_.lock();
  if (!stream) {
    return;
  }

  on_request_(message.message.type, stream);
}

} // namespace moq::detail
