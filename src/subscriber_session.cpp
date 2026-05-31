#include "moq/subscriber_session.h"

#include "data_plane.h"
#include "moq/codec.h"
#include "msquic_transport_adapter.h"
#include "peer_stream_demux.h"
#include "request/subscription.h"

#include <algorithm>
#include <deque>
#include <exception>
#include <future>
#include <limits>
#include <mutex>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

namespace moq::detail {
namespace {

template <typename T> void set_exception(const std::shared_ptr<std::promise<T>> &promise, const std::string &message) {
  try {
    throw std::runtime_error(message);
  } catch (...) {
    try {
      promise->set_exception(std::current_exception());
    } catch (const std::future_error &) {
      // squash because promise is already satisfied with other value or exception
    }
  }
}

template <typename T>
void set_exception(const std::shared_ptr<std::promise<T>> &promise, std::exception_ptr exception) {
  try {
    promise->set_exception(std::move(exception));
  } catch (const std::future_error &) {
    // squash because promise is already satisfied with other value or exception
  }
}

// converts REQUEST_ERROR to c++ exception
std::exception_ptr rejected_exception(const RequestError &error) {
  return std::make_exception_ptr(RequestRejected(error.code, error.retry_interval, error.reason));
}

std::string default_authority(const MsQuicClientConfig &config) {
  if (!config.authority.empty()) {
    return config.authority;
  }
  return config.host + ":" + std::to_string(config.port);
}

bool known_peer_request_type(uint64_t type) {
  switch (type) {
  case codec::kMessageSubscribe:
  case codec::kMessagePublish:
  case codec::kMessagePublishNamespace:
  case codec::kMessageTrackStatus:
  case codec::kMessageFetch:
  case codec::kMessageSubscribeNamespace:
  case codec::kMessageSubscribeTracks:
    return true;
  default:
    return false;
  }
}

} // namespace

class SessionImpl : public std::enable_shared_from_this<SessionImpl> {
public:
  SessionImpl(MsQuicClientConfig msquic_config, SubscriberConfig subscriber_config)
      : msquic_config_(std::move(msquic_config)), subscriber_config_(std::move(subscriber_config)),
        data_plane_(
            subscriber_config_, [this](std::string error) { protocol_violation(std::move(error)); },
            [this](RequestId request_id, std::string error) { malformed_track(request_id, std::move(error)); }) {
    refresh_session_snapshot();
  }

private:
  PeerUniDemux::SessionCallbacks peer_demux_callbacks_;
  PeerBidiDemux::SessionCallbacksBidi peer_demux_callbacks_bidi_;

public:
  ~SessionImpl() {
    close_and_wait(SessionCloseErrorCode::NoError);
    executor_.stop();
  }

  void start() {
    MsQuicTransportAdapter::Callbacks callbacks;
    callbacks.connected = [this] { on_connected(); };
    callbacks.peer_stream_started = [this](std::shared_ptr<TransportStream> stream) {
      on_peer_stream_started(std::move(stream));
    };
    callbacks.datagram_received = [this](ByteBuffer bytes) { data_plane_.on_datagram(std::move(bytes)); };
    callbacks.transport_error = [this](std::string error) { transport_error(std::move(error)); };
    callbacks.shutdown_complete = [this](bool handshake) { shutdown_complete(handshake); };
    transport_ = std::make_unique<MsQuicTransportAdapter>(executor_, msquic_config_, std::move(callbacks));
    transport_->start();

    // prepare a reusable set of callbacks for demux so we don't recreate
    // identical lambdas per incoming stream.
    const std::weak_ptr<SessionImpl> weak = weak_from_this();
    peer_demux_callbacks_.on_setup = [weak](ByteBuffer bytes, bool fin) mutable {
      if (auto active = weak.lock()) {
        active->on_peer_control_bytes(std::move(bytes), fin);
      }
    };
    peer_demux_callbacks_.on_subgroup = [weak](std::shared_ptr<TransportStream> peer_stream, ByteBuffer initial,
                                               bool fin) mutable {
      if (auto active = weak.lock()) {
        active->data_plane_.start_subgroup_stream(std::move(peer_stream), std::move(initial), fin);
      }
    };
    peer_demux_callbacks_.on_padding = [weak](std::shared_ptr<TransportStream> peer_stream, ByteBuffer initial,
                                              size_t type_bytes) mutable {
      if (auto active = weak.lock()) {
        active->start_padding_stream(std::move(peer_stream), std::move(initial), type_bytes);
      }
    };
    peer_demux_callbacks_.on_fetch = [](std::shared_ptr<TransportStream> peer_stream) mutable {
      if (peer_stream) {
        peer_stream->abort_receive(0);
      }
    };
    peer_demux_callbacks_.on_protocol_violation = [weak](std::string error) mutable {
      if (auto active = weak.lock()) {
        active->protocol_violation(std::move(error));
      }
    };
    // bidi callbacks reuse protocol_violation and define request handling
    peer_demux_callbacks_bidi_.on_protocol_violation = peer_demux_callbacks_.on_protocol_violation;
    peer_demux_callbacks_bidi_.on_request = [weak](uint64_t request_type,
                                                   std::shared_ptr<TransportStream> peer_stream) mutable {
      if (auto active = weak.lock()) {
        if (!known_peer_request_type(request_type)) {
          active->protocol_violation("unknown peer request type " + std::to_string(request_type));
          return;
        }
        const uint64_t error_code = request_type == codec::kMessagePublish ? 0x20 : 0x03;
        const std::string reason = request_type == codec::kMessagePublish ? "subscriber is not accepting PUBLISH"
                                                                          : "subscriber-only implementation";
        peer_stream->send(codec::encode_request_error(error_code, reason), true);
        peer_stream->abort_receive(0);
      }
    };
  }

  // waits until session is either ready or closed
  std::future<void> ready() {
    auto promise = std::make_shared<std::promise<void>>();
    std::future<void> future = promise->get_future();
    executor_.post([self = shared_from_this(), promise] {
      if (self->phase_ == SessionPhase::Ready || self->phase_ == SessionPhase::Draining) {
        promise->set_value();
        return;
      }
      if (self->phase_ == SessionPhase::Closing || self->phase_ == SessionPhase::Closed) {
        set_exception(promise, "MOQT session closed before SETUP completed");
        return;
      }
      self->ready_waiters_.push_back(promise);
    });
    return future;
  }

  SessionStateSnapshot state() const {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    return session_snapshot_;
  }

  // returns a state of the subscription with the given request ID
  SubscriptionStateSnapshot subscription_state(RequestId request_id) const {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    const auto snapshot = subscription_snapshots_.find(request_id);
    if (snapshot != subscription_snapshots_.end()) {
      return snapshot->second;
    }
    SubscriptionStateSnapshot terminated;
    terminated.request_id = request_id;
    return terminated;
  }

  std::future<SubscriptionHandle> subscribe(SubscribeRequest request, std::shared_ptr<ObjectHandler> handler) {
    auto promise = std::make_shared<std::promise<SubscriptionHandle>>();
    std::future<SubscriptionHandle> future = promise->get_future();
    executor_.post([self = shared_from_this(), request = std::move(request), handler, promise]() mutable {
      self->subscribe_on_executor(std::move(request), std::move(handler), std::move(promise));
    });
    return future;
  }

  std::future<RequestOk> request_update(RequestId existing_request_id, RequestUpdate update) {
    auto promise = std::make_shared<std::promise<RequestOk>>();
    std::future<RequestOk> future = promise->get_future();
    executor_.post([self = shared_from_this(), existing_request_id, update = std::move(update), promise]() mutable {
      self->request_update_on_executor(existing_request_id, std::move(update), std::move(promise));
    });
    return future;
  }

  std::future<void> stop_subscription(RequestId request_id) {
    auto promise = std::make_shared<std::promise<void>>();
    std::future<void> future = promise->get_future();
    executor_.post([self = shared_from_this(), request_id, promise] {
      self->stop_subscription_on_executor(request_id, false);
      promise->set_value();
    });
    return future;
  }

  void close(SessionCloseErrorCode error) {
    spdlog::debug("Session close requested: code={}", static_cast<uint64_t>(error));
    executor_.post([self = shared_from_this(), error] { self->begin_close(error, "local close"); });
  }

  void close_and_wait(SessionCloseErrorCode error) {
    spdlog::debug("Session close-and-wait requested: code={}", static_cast<uint64_t>(error));
    if (executor_.on_thread()) {
      begin_close(error, "local close");
      return;
    }

    auto promise = std::make_shared<std::promise<void>>();
    std::future<void> future = promise->get_future();
    executor_.post([this, error, promise] {
      begin_close(error, "local close");
      promise->set_value();
    });
    future.wait();
  }

private:
  // Send SETUP on connection establishment
  void on_connected() {
    if (phase_ != SessionPhase::Init) {
      return;
    }
    phase_ = SessionPhase::SetupInProgress;
    try {
      spdlog::debug("MOQT connected; sending SETUP authority={} path={}", default_authority(msquic_config_),
                    msquic_config_.path);
      local_setup_stream_ = transport_->open_stream(true);
      if (!local_setup_stream_->send(codec::encode_setup(default_authority(msquic_config_), msquic_config_.path))) {
        throw std::runtime_error("StreamSend failed for SETUP");
      }
      spdlog::debug("Local SETUP sent; waiting for peer SETUP");
      local_setup_sent_ = true;
      refresh_session_snapshot();
    } catch (const std::exception &error) {
      begin_close(SessionCloseErrorCode::InternalError, error.what());
    }
  }

  void on_peer_stream_started(std::shared_ptr<TransportStream> stream) {
    if (stream->unidirectional()) {
      spdlog::debug("Peer started a unidirectional stream (id={})", stream->id());
      auto demux = std::make_shared<PeerUniDemux>(stream, peer_demux_callbacks_);
      stream->on_bytes([demux](ByteBuffer bytes, bool fin) mutable { demux->feed(std::move(bytes), fin); });
      return;
    }
    spdlog::debug("Peer started a bidirectional stream (id={})", stream->id());
    auto demux = std::make_shared<PeerBidiDemux>(stream, peer_demux_callbacks_bidi_);
    stream->on_bytes([demux](ByteBuffer bytes, bool fin) mutable { demux->feed(std::move(bytes), fin); });
  }

  void start_padding_stream(const std::shared_ptr<TransportStream> &stream, ByteBuffer initial, size_t type_bytes) {
    if (!std::all_of(initial.begin() + static_cast<std::ptrdiff_t>(type_bytes), initial.end(),
                     [](uint8_t byte) { return byte == 0; })) {
      protocol_violation("padding stream contains non-zero bytes");
      return;
    }
    std::weak_ptr<SessionImpl> weak = weak_from_this();
    stream->on_bytes([weak](ByteBuffer bytes, bool) {
      if (!std::all_of(bytes.begin(), bytes.end(), [](uint8_t byte) { return byte == 0; })) {
        if (auto active = weak.lock()) {
          active->protocol_violation("padding stream contains non-zero bytes");
        }
      }
    });
  }

  void on_peer_control_bytes(ByteBuffer bytes, bool fin) {
    peer_control_buffer_.insert(peer_control_buffer_.end(), bytes.begin(), bytes.end());
    for (;;) {
      const codec::ControlMessageResult parsed = codec::read_control_message(peer_control_buffer_);
      if (parsed.status != codec::DecodeStatus::Done) {
        break;
      }
      spdlog::debug("Received peer control message type={} payload={} bytes", parsed.message.type,
                    parsed.message.payload.size());
      peer_control_buffer_.erase(peer_control_buffer_.begin(),
                                 peer_control_buffer_.begin() + static_cast<std::ptrdiff_t>(parsed.bytes));
      handle_peer_control_message(parsed.message);
      if (phase_ == SessionPhase::Closing || phase_ == SessionPhase::Closed) {
        return;
      }
    }
    if (fin && !peer_control_buffer_.empty()) {
      protocol_violation("peer control stream ended mid-message");
    }
  }

  // handles session-level control messages
  void handle_peer_control_message(const codec::ControlMessage &message) {
    if (!peer_setup_received_) {
      if (message.type != codec::kMessageSetup) {
        protocol_violation("first peer control message was not SETUP");
        return;
      }
      spdlog::debug("Peer SETUP received");
      std::string error;
      if (!codec::decode_setup(message.payload, error)) {
        protocol_violation(std::move(error));
        return;
      }
      peer_setup_received_ = true;
      maybe_ready();
      return;
    }
    if (message.type == codec::kMessageGoAway) {
      if (phase_ == SessionPhase::Ready) {
        phase_ = SessionPhase::Draining;
        refresh_session_snapshot();
      }
      return;
    }
    protocol_violation("unsupported control message " + std::to_string(message.type));
  }

  void maybe_ready() {
    if (!local_setup_sent_ || !peer_setup_received_ || phase_ == SessionPhase::Closing ||
        phase_ == SessionPhase::Closed) {
      refresh_session_snapshot();
      return;
    }
    spdlog::debug("MOQT SETUP completed; session ready");
    phase_ = SessionPhase::Ready;
    for (const auto &waiter : ready_waiters_) {
      waiter->set_value();
    }
    ready_waiters_.clear();
    refresh_session_snapshot();
  }

  RequestId allocate_request_id() {
    const RequestId request_id = next_request_id_;
    if (request_id > std::numeric_limits<RequestId>::max() - 2) {
      throw std::overflow_error("MOQT Request ID space exhausted");
    }
    next_request_id_ += 2;
    return request_id;
  }

  void subscribe_on_executor(SubscribeRequest request, std::shared_ptr<ObjectHandler> handler,
                             std::shared_ptr<std::promise<SubscriptionHandle>> promise) {
    if (!handler) {
      set_exception(promise, "SUBSCRIBE requires an ObjectHandler");
      return;
    }
    if (phase_ != SessionPhase::Ready) {
      set_exception(promise, "MOQT session is not ready for SUBSCRIBE");
      return;
    }
    try {
      RequestId request_id = 0;
      if (request.request_id) {
        if (!subscriber_config_.allow_explicit_request_ids || (*request.request_id & 1U) != 0 ||
            subscriptions_.find(*request.request_id) != subscriptions_.end()) {
          throw std::invalid_argument("explicit SUBSCRIBE Request ID is not allowed");
        }
        request_id = *request.request_id;
        if (request_id >= next_request_id_) {
          if (request_id > std::numeric_limits<RequestId>::max() - 2) {
            throw std::overflow_error("MOQT Request ID space exhausted");
          }
          next_request_id_ = request_id + 2;
        }
      } else {
        request_id = allocate_request_id();
      }
      auto stream = transport_->open_stream(false);
      const std::weak_ptr<SessionImpl> weak = weak_from_this();

      // install route callback: if duplicate alias detected, close session
      auto install_cb = [weak](TrackAlias alias, std::shared_ptr<ReceiveRoute> route) {
        if (auto active = weak.lock()) {
          if (!active->data_plane_.install_route(alias, route)) {
            active->begin_close(SessionCloseErrorCode::DuplicateTrackAlias,
                                "SUBSCRIBE_OK reused an established Track Alias");
            return false;
          }
          return true;
        }
        return false;
      };

      auto deactivate_cb = [weak](TrackAlias alias) {
        if (auto active = weak.lock()) {
          active->data_plane_.deactivate_route(alias);
        }
      };

      auto remove_cb = [weak](TrackAlias alias) {
        if (auto active = weak.lock()) {
          active->data_plane_.remove_route(alias);
        }
      };

      // subscribe result callback: settle promise and refresh snapshots
      auto subscribe_result_cb = [weak, promise, request_id](std::optional<RequestError> rejected,
                                                             std::optional<TrackAlias> alias) {
        if (auto active = weak.lock()) {
          if (rejected) {
            set_exception(promise, rejected_exception(*rejected));
          } else {
            try {
              promise->set_value(SubscriptionHandle(request_id, weak));
            } catch (...) {
              set_exception(promise, std::current_exception());
            }
          }
          active->refresh_session_snapshot();
          active->update_subscription_snapshot_from_fsm(request_id);
        }
      };

      auto fsm = std::make_shared<SubscriptionFSM>(request_id, std::move(request), std::move(handler), stream,
                                                   std::move(install_cb), std::move(deactivate_cb),
                                                   std::move(remove_cb), std::move(subscribe_result_cb));

      // wire transport stream callbacks into FSM
      stream->on_bytes([fsm](ByteBuffer bytes, bool fin) mutable { fsm->on_bytes(std::move(bytes), fin); });
      stream->on_peer_send_aborted([fsm](uint64_t error_code) mutable { fsm->on_peer_send_aborted(error_code); });
      stream->on_shutdown([fsm] { fsm->on_shutdown(); });

      subscriptions_.emplace(request_id, fsm);
      update_subscription_snapshot_from_fsm(request_id);
      refresh_session_snapshot();
      if (!stream->send(codec::encode_subscribe(request_id, request))) {
        throw std::runtime_error("StreamSend failed for SUBSCRIBE");
      }
    } catch (...) {
      set_exception(promise, std::current_exception());
    }
  }

  void request_update_on_executor(RequestId existing_request_id, RequestUpdate update,
                                  std::shared_ptr<std::promise<RequestOk>> promise) {
    const auto found = subscriptions_.find(existing_request_id);
    if (found == subscriptions_.end() || found->second->phase() != moq::SubscriptionPhase::Established) {
      set_exception(promise, "REQUEST_UPDATE requires an established subscription");
      return;
    }
    auto fsm = found->second;
    try {
      RequestId request_id = allocate_request_id();
      auto stream = fsm->stream();
      auto sender = [stream](ByteBuffer bytes) { return stream->send(std::move(bytes)); };
      fsm->send_request_update(request_id, std::move(update), promise, sender);
      update_subscription_snapshot_from_fsm(existing_request_id);
    } catch (...) {
      set_exception(promise, std::current_exception());
    }
  }

  void on_subscription_bytes(RequestId request_id, ByteBuffer bytes, bool fin) {
    const auto found = subscriptions_.find(request_id);
    if (found == subscriptions_.end())
      return;
    auto fsm = found->second;
    fsm->on_bytes(std::move(bytes), fin);
    if (fin && fsm->phase() == moq::SubscriptionPhase::Terminated) {
      update_subscription_snapshot_from_fsm(request_id);
      refresh_session_snapshot();
      // keep entry until owner removes it explicitly
    }
  }

  void subscription_aborted(RequestId request_id, uint64_t error_code) {
    const auto found = subscriptions_.find(request_id);
    if (found == subscriptions_.end())
      return;
    auto fsm = found->second;
    fsm->on_peer_send_aborted(error_code);
    if (fsm->phase() == moq::SubscriptionPhase::Terminated) {
      update_subscription_snapshot_from_fsm(request_id);
      refresh_session_snapshot();
    }
  }

  void subscription_shutdown(RequestId request_id) {
    const auto found = subscriptions_.find(request_id);
    if (found == subscriptions_.end())
      return;
    auto fsm = found->second;
    fsm->on_shutdown();
    if (fsm->phase() == moq::SubscriptionPhase::Terminated) {
      update_subscription_snapshot_from_fsm(request_id);
      refresh_session_snapshot();
    }
  }

  void stop_subscription_on_executor(RequestId request_id, bool report_error) {
    const auto found = subscriptions_.find(request_id);
    if (found == subscriptions_.end())
      return;
    auto fsm = found->second;
    fsm->terminate(report_error, report_error ? "subscription stopped after receive error" : std::string{});
    if (auto s = fsm->stream())
      s->abort_receive(0);
    update_subscription_snapshot_from_fsm(request_id);
    refresh_session_snapshot();
    subscriptions_.erase(found);
  }

  void malformed_track(RequestId request_id, std::string error) {
    const auto found = subscriptions_.find(request_id);
    if (found == subscriptions_.end())
      return;
    auto fsm = found->second;
    fsm->report_handler_error(ReceiveError{0x12, error});
    stop_subscription_on_executor(request_id, false);
  }

  void protocol_violation(std::string error) {
    begin_close(SessionCloseErrorCode::ProtocolViolation, std::move(error));
  }

  void transport_error(std::string error) { begin_close(SessionCloseErrorCode::InternalError, std::move(error)); }

  void begin_close(SessionCloseErrorCode code, std::string reason) {
    if (phase_ == SessionPhase::Closed || phase_ == SessionPhase::Closing) {
      return;
    }
    spdlog::debug("Session beginning close: code={} reason={}", static_cast<uint64_t>(code), reason);
    phase_ = SessionPhase::Closing;
    close_reason_ = SessionCloseReason{code, reason};
    fail_ready_waiters(reason.empty() ? "MOQT session closing" : reason);
    for (auto &entry : subscriptions_) {
      if (entry.second)
        entry.second->terminate(true, reason);
    }
    refresh_session_snapshot();
    if (transport_) {
      transport_->shutdown(code);
    }
  }

  void shutdown_complete(bool handshake_completed) {
    spdlog::debug("Session shutdown complete: handshake_completed={} close_reason_set={}", handshake_completed,
                  static_cast<bool>(close_reason_));
    if (!handshake_completed && !close_reason_) {
      close_reason_ = SessionCloseReason{SessionCloseErrorCode::InternalError, "QUIC handshake did not complete"};
    }
    phase_ = SessionPhase::Closed;
    fail_ready_waiters("MOQT session closed");
    refresh_session_snapshot();
  }

  void fail_ready_waiters(const std::string &reason) {
    for (const auto &waiter : ready_waiters_) {
      set_exception(waiter, reason);
    }
    ready_waiters_.clear();
  }

  void update_subscription_snapshot_from_fsm(RequestId request_id) {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    const auto it = subscriptions_.find(request_id);
    if (it == subscriptions_.end())
      return;
    SubscriptionStateSnapshot snapshot;
    snapshot.request_id = request_id;
    const auto &fsm = it->second;
    snapshot.phase = fsm->phase();
    snapshot.track_alias = fsm->track_alias();
    snapshot.inflight_updates = fsm->inflight_updates();
    subscription_snapshots_[request_id] = snapshot;
  }

  void refresh_session_snapshot() {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    session_snapshot_.phase = phase_;
    session_snapshot_.local_setup_sent = local_setup_sent_;
    session_snapshot_.peer_setup_received = peer_setup_received_;
    session_snapshot_.negotiated_version = (phase_ == SessionPhase::Ready || phase_ == SessionPhase::Draining ||
                                            phase_ == SessionPhase::Closing || phase_ == SessionPhase::Closed)
                                               ? std::optional<std::string>(msquic_config_.alpn)
                                               : std::nullopt;
    size_t active = 0;
    for (const auto &entry : subscriptions_) {
      const auto &fsm = entry.second;
      if (fsm && fsm->phase() != moq::SubscriptionPhase::Terminated) {
        ++active;
      }
    }
    session_snapshot_.active_subscriptions = active;
    session_snapshot_.close_reason = close_reason_;
  }

  Executor executor_;
  MsQuicClientConfig msquic_config_;
  SubscriberConfig subscriber_config_;
  DataPlane data_plane_;
  std::unique_ptr<MsQuicTransportAdapter> transport_;

  SessionPhase phase_ = SessionPhase::Init;
  RequestId next_request_id_ = 0;
  bool local_setup_sent_ = false;
  bool peer_setup_received_ = false;
  std::optional<SessionCloseReason> close_reason_;
  std::shared_ptr<TransportStream> local_setup_stream_;
  ByteBuffer peer_control_buffer_;
  std::vector<std::shared_ptr<std::promise<void>>> ready_waiters_;
  std::unordered_map<RequestId, std::shared_ptr<SubscriptionFSM>> subscriptions_;

  mutable std::mutex snapshot_mutex_;
  SessionStateSnapshot session_snapshot_;
  std::unordered_map<RequestId, SubscriptionStateSnapshot> subscription_snapshots_;
};

} // namespace moq::detail

namespace moq {

SubscriptionHandle::SubscriptionHandle(RequestId request_id, std::weak_ptr<detail::SessionImpl> session)
    : request_id_(request_id), session_(std::move(session)) {}

RequestId SubscriptionHandle::request_id() const { return request_id_; }

std::optional<TrackAlias> SubscriptionHandle::track_alias() const { return state().track_alias; }

SubscriptionStateSnapshot SubscriptionHandle::state() const {
  const std::shared_ptr<detail::SessionImpl> session = session_.lock();
  if (!session) {
    SubscriptionStateSnapshot terminated;
    terminated.request_id = request_id_;
    return terminated;
  }
  return session->subscription_state(request_id_);
}

MoqSubscriberSession::MoqSubscriberSession(std::shared_ptr<detail::SessionImpl> impl) : impl_(std::move(impl)) {}

std::future<std::unique_ptr<MoqSubscriberSession>> MoqSubscriberSession::connect(MsQuicClientConfig msquic_config,
                                                                                 SubscriberConfig subscriber_config) {
  std::promise<std::unique_ptr<MoqSubscriberSession>> promise;
  std::future<std::unique_ptr<MoqSubscriberSession>> future = promise.get_future();
  try {
    auto impl = std::make_shared<detail::SessionImpl>(std::move(msquic_config), std::move(subscriber_config));
    impl->start();
    promise.set_value(std::unique_ptr<MoqSubscriberSession>(new MoqSubscriberSession(std::move(impl))));
  } catch (...) {
    promise.set_exception(std::current_exception());
  }
  return future;
}

std::future<void> MoqSubscriberSession::ready() { return impl_->ready(); }

SessionStateSnapshot MoqSubscriberSession::state() const { return impl_->state(); }

std::future<SubscriptionHandle> MoqSubscriberSession::subscribe(SubscribeRequest request,
                                                                std::shared_ptr<ObjectHandler> handler) {
  return impl_->subscribe(std::move(request), std::move(handler));
}

std::future<RequestOk> MoqSubscriberSession::request_update(RequestId existing_request_id, RequestUpdate update) {
  return impl_->request_update(existing_request_id, std::move(update));
}

std::future<void> MoqSubscriberSession::stop_subscription(RequestId request_id) {
  return impl_->stop_subscription(request_id);
}

void MoqSubscriberSession::close(SessionCloseErrorCode error) { impl_->close(error); }

MoqSubscriberSession::~MoqSubscriberSession() {
  if (impl_) {
    spdlog::debug("MoqSubscriberSession destructor initiating shutdown wait");
    impl_->close_and_wait(SessionCloseErrorCode::NoError);
  }
}

} // namespace moq
