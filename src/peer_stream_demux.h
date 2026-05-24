#pragma once

#include "moq/codec.h"
#include "msquic_transport_adapter.h"

#include <functional>
#include <memory>
#include <string>

namespace moq::detail {

class PeerUniDemux final {
public:
    using OnSetup = std::function<void(ByteBuffer, bool)>;
    using OnSubgroup = std::function<void(std::shared_ptr<TransportStream>, ByteBuffer, bool)>;
    using OnPadding = std::function<void(std::shared_ptr<TransportStream>, ByteBuffer, size_t)>;
    using OnFetch = std::function<void(std::shared_ptr<TransportStream>)>;
    using OnProtocolViolation = std::function<void(std::string)>;

    PeerUniDemux(
        std::shared_ptr<TransportStream> stream,
        OnSetup on_setup,
        OnSubgroup on_subgroup,
        OnPadding on_padding,
        OnFetch on_fetch,
        OnProtocolViolation on_protocol_violation);

    void feed(ByteBuffer input, bool fin);

private:
    std::weak_ptr<TransportStream> stream_;
    OnSetup on_setup_;
    OnSubgroup on_subgroup_;
    OnPadding on_padding_;
    OnFetch on_fetch_;
    OnProtocolViolation on_protocol_violation_;
    ByteBuffer bytes_;
};

class PeerBidiDemux final {
public:
    using OnRequest = std::function<void(uint64_t, std::shared_ptr<TransportStream>)>;
    using OnProtocolViolation = std::function<void(std::string)>;

    PeerBidiDemux(
        std::shared_ptr<TransportStream> stream,
        OnRequest on_request,
        OnProtocolViolation on_protocol_violation);

    void feed(ByteBuffer input, bool fin);

private:
    std::weak_ptr<TransportStream> stream_;
    OnRequest on_request_;
    OnProtocolViolation on_protocol_violation_;
    ByteBuffer bytes_;
};

} // namespace moq::detail