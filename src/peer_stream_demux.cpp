#include "peer_stream_demux.h"

#include <utility>

namespace moq::detail {

PeerUniDemux::PeerUniDemux(
    std::shared_ptr<TransportStream> stream,
    OnSetup on_setup,
    OnSubgroup on_subgroup,
    OnPadding on_padding,
    OnFetch on_fetch,
    OnProtocolViolation on_protocol_violation)
    : stream_(std::move(stream)),
      on_setup_(std::move(on_setup)),
      on_subgroup_(std::move(on_subgroup)),
      on_padding_(std::move(on_padding)),
      on_fetch_(std::move(on_fetch)),
      on_protocol_violation_(std::move(on_protocol_violation)) {}

// dispatches bytes on a unidirectional stream based on the prefix varint
void PeerUniDemux::feed(ByteBuffer input, bool fin) {
    bytes_.insert(bytes_.end(), input.begin(), input.end());
    const codec::VarintResult type = codec::read_varint(bytes_);
    if (type.status != codec::DecodeStatus::Done) {
        if (fin && on_protocol_violation_) {
            on_protocol_violation_("peer unidirectional stream ended before type");
        }
        return;
    }

    const std::shared_ptr<TransportStream> stream = stream_.lock();
    if (!stream) {
        return;
    }

    ByteBuffer initial = std::move(bytes_);
    if (type.value == codec::kSetupStreamType) {
        ByteBuffer payload(
            initial.begin() + static_cast<std::ptrdiff_t>(type.bytes), initial.end());
        stream->on_bytes([on_setup = on_setup_](ByteBuffer next, bool bytes_fin) mutable {
            if (on_setup) {
                on_setup(std::move(next), bytes_fin);
            }
        });
        if (on_setup_) {
            on_setup_(std::move(payload), fin);
        }
        return;
    }

    if (codec::is_subgroup_stream_type(type.value)) {
        if (on_subgroup_) {
            on_subgroup_(stream, std::move(initial), fin);
        }
        return;
    }

    if (type.value == codec::kFetchStreamType) {
        if (on_fetch_) {
            on_fetch_(stream);
        }
        return;
    }

    if (type.value == codec::kPaddingStreamType) {
        if (on_padding_) {
            on_padding_(stream, std::move(initial), type.bytes);
        }
        return;
    }

    if (on_protocol_violation_) {
        on_protocol_violation_(
            "unknown peer unidirectional stream type " + std::to_string(type.value));
    }
}

PeerBidiDemux::PeerBidiDemux(
    std::shared_ptr<TransportStream> stream,
    OnRequest on_request,
    OnProtocolViolation on_protocol_violation)
    : stream_(std::move(stream)),
      on_request_(std::move(on_request)),
      on_protocol_violation_(std::move(on_protocol_violation)) {}

// dispatches bytes on a bidirectional stream as a single request message
void PeerBidiDemux::feed(ByteBuffer input, bool fin) {
    bytes_.insert(bytes_.end(), input.begin(), input.end());
    const codec::ControlMessageResult message = codec::read_control_message(bytes_);
    if (message.status != codec::DecodeStatus::Done) {
        if (fin && on_protocol_violation_) {
            on_protocol_violation_("peer bidirectional stream ended before first request");
        }
        return;
    }

    const std::shared_ptr<TransportStream> stream = stream_.lock();
    if (!stream) {
        return;
    }

    if (on_request_) {
        on_request_(message.message.type, stream);
    }
}

} // namespace moq::detail