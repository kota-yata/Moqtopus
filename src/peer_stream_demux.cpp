#include "peer_stream_demux.h"

#include <spdlog/spdlog.h>
#include <utility>

namespace moq::detail {

PeerUniDemux::PeerUniDemux(std::shared_ptr<TransportStream> stream, SessionCallbacks callbacks)
    : stream_(std::move(stream)), on_setup_(std::move(callbacks.on_setup)),
      on_subgroup_(std::move(callbacks.on_subgroup)), on_padding_(std::move(callbacks.on_padding)),
      on_fetch_(std::move(callbacks.on_fetch)), on_protocol_violation_(std::move(callbacks.on_protocol_violation)) {}

// dispatches bytes on a unidirectional stream based on the prefix varint
void PeerUniDemux::feed(ByteBuffer input, bool fin) {
  bytes_.insert(bytes_.end(), input.begin(), input.end());
  const codec::VarintResult type = codec::read_varint(bytes_);
  if (type.status != codec::DecodeStatus::Done) {
    if (fin) {
      on_protocol_violation_("peer unidirectional stream ended before type");
    }
    return;
  }

  const std::shared_ptr<TransportStream> stream = stream_.lock();
  if (!stream) {
    return;
  }

  ByteBuffer initial = std::move(bytes_);
  switch (type.value) {
  case codec::kSetupStreamType: {
    ByteBuffer payload(initial.begin() + static_cast<std::ptrdiff_t>(type.bytes), initial.end());
    on_setup_(std::move(payload), fin);
    break;
  }
  case codec::kFetchStreamType: {
    on_fetch_(stream);
    break;
  }
  case codec::kPaddingStreamType: {
    on_padding_(stream, std::move(initial), type.bytes);
    break;
  }
  default: {
    if (codec::is_subgroup_stream_type(type.value)) {
      on_subgroup_(stream, std::move(initial), fin);
    } else {
      on_protocol_violation_("unknown peer unidirectional stream type " + std::to_string(type.value));
    }
    break;
  }
  }
}

PeerBidiDemux::PeerBidiDemux(std::shared_ptr<TransportStream> stream, SessionCallbacksBidi callbacks)
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

  const std::shared_ptr<TransportStream> stream = stream_.lock();
  if (!stream) {
    return;
  }

  on_request_(message.message.type, stream);
}

} // namespace moq::detail