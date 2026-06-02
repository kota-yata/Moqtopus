#pragma once

#include "moq/codec.h"
#include "stream_context.h"

#include <functional>
#include <memory>
#include <string>

namespace moq::detail {

class PeerUniDemux final {
public:
  using OnSetup = std::function<void(codec::Setup)>;
  using OnSubgroup = std::function<void(std::shared_ptr<StreamContext>, ByteBuffer, bool)>;
  using OnPadding = std::function<void(std::shared_ptr<StreamContext>, ByteBuffer, size_t)>;
  using OnFetch = std::function<void(std::shared_ptr<StreamContext>)>;
  using OnProtocolViolation = std::function<void(std::string)>;

  struct SessionCallbacks {
    OnSetup on_setup;
    OnSubgroup on_subgroup;
    OnPadding on_padding;
    OnFetch on_fetch;
    OnProtocolViolation on_protocol_violation;
  };

  PeerUniDemux(std::shared_ptr<StreamContext> stream, SessionCallbacks callbacks);

  void feed(ByteBuffer input, bool fin);

private:
  std::weak_ptr<StreamContext> stream_;
  OnSetup on_setup_;
  OnSubgroup on_subgroup_;
  OnPadding on_padding_;
  OnFetch on_fetch_;
  OnProtocolViolation on_protocol_violation_;
  ByteBuffer bytes_;
};

class PeerBidiDemux final {
public:
  using OnRequest = std::function<void(uint64_t, std::shared_ptr<StreamContext>)>;
  using OnProtocolViolation = std::function<void(std::string)>;

  struct SessionCallbacksBidi {
    OnRequest on_request;
    OnProtocolViolation on_protocol_violation;
  };

  PeerBidiDemux(std::shared_ptr<StreamContext> stream, SessionCallbacksBidi callbacks);

  void feed(ByteBuffer input, bool fin);

private:
  std::weak_ptr<StreamContext> stream_;
  OnRequest on_request_;
  OnProtocolViolation on_protocol_violation_;
  ByteBuffer bytes_;
};

} // namespace moq::detail
