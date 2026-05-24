#include "moq/subscriber_session.h"

#include "data_plane.h"
#include "moq/codec.h"
#include "msquic_transport_adapter.h"

#include <algorithm>
#include <deque>
#include <exception>
#include <future>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

namespace moq::detail {
namespace {

template <typename T>
void set_exception(
    const std::shared_ptr<std::promise<T>>& promise,
    const std::string& message) {
    try {
        throw std::runtime_error(message);
    } catch (...) {
        try {
            promise->set_exception(std::current_exception());
        } catch (const std::future_error&) {
            // squash because promise is already satisfied with other value or exception
        }
    }
}

template <typename T>
void set_exception(
    const std::shared_ptr<std::promise<T>>& promise,
    std::exception_ptr exception) {
    try {
        promise->set_exception(std::move(exception));
    } catch (const std::future_error&) {
        // squash because promise is already satisfied with other value or exception
    }
}

// converts REQUEST_ERROR to future-gettable exception
std::exception_ptr rejected_exception(const RequestError& error) {
    return std::make_exception_ptr(RequestRejected(error.code, error.retry_interval, error.reason));
}

std::string default_authority(const MsQuicClientConfig& config) {
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

// Session plane
class SessionImpl : public std::enable_shared_from_this<SessionImpl> {
public:
    SessionImpl(MsQuicClientConfig msquic_config, SubscriberConfig subscriber_config)
        : msquic_config_(std::move(msquic_config)),
          subscriber_config_(std::move(subscriber_config)),
          data_plane_(
              subscriber_config_,
              [this](std::string error) { protocol_violation(std::move(error)); },
              [this](RequestId request_id, std::string error) {
                  malformed_track(request_id, std::move(error));
              }) {
        refresh_session_snapshot();
    }

    ~SessionImpl() {
        close_and_wait(SessionCloseErrorCode::NoError);
        executor_.stop();
    }

    void start() {
        MsQuicTransportAdapter::Callbacks callbacks;
        callbacks.connected = [this] { on_connected(); };
        callbacks.peer_stream_started =
            [this](std::shared_ptr<TransportStream> stream) {
                on_peer_stream_started(std::move(stream));
            };
        callbacks.datagram_received =
            [this](ByteBuffer bytes) { data_plane_.on_datagram(std::move(bytes)); };
        callbacks.transport_error =
            [this](std::string error) { transport_error(std::move(error)); };
        callbacks.shutdown_complete =
            [this](bool handshake) { shutdown_complete(handshake); };
        transport_ = std::make_unique<MsQuicTransportAdapter>(
            executor_, msquic_config_, std::move(callbacks));
        transport_->start();
    }

    std::future<void> ready() {
        auto promise = std::make_shared<std::promise<void>>();
        std::future<void> future = promise->get_future();
        executor_.post([self = shared_from_this(), promise] {
            if (self->phase_ == SessionPhase::Ready ||
                self->phase_ == SessionPhase::Draining) {
                promise->set_value();
                return;
            }
            if (self->phase_ == SessionPhase::Closing ||
                self->phase_ == SessionPhase::Closed) {
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

    std::future<SubscriptionHandle> subscribe(
        SubscribeRequest request,
        std::shared_ptr<ObjectHandler> handler) {
        auto promise = std::make_shared<std::promise<SubscriptionHandle>>();
        std::future<SubscriptionHandle> future = promise->get_future();
        executor_.post(
            [self = shared_from_this(), request = std::move(request), handler, promise]() mutable {
                self->subscribe_on_executor(
                    std::move(request), std::move(handler), std::move(promise));
            });
        return future;
    }

    std::future<RequestOk> request_update(RequestId existing_request_id, RequestUpdate update) {
        auto promise = std::make_shared<std::promise<RequestOk>>();
        std::future<RequestOk> future = promise->get_future();
        executor_.post(
            [self = shared_from_this(), existing_request_id, update = std::move(update),
             promise]() mutable {
                self->request_update_on_executor(
                    existing_request_id, std::move(update), std::move(promise));
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
        executor_.post([self = shared_from_this(), error] {
            self->begin_close(static_cast<uint64_t>(error), "local close");
        });
    }

    void close_and_wait(SessionCloseErrorCode error) {
        if (executor_.on_thread()) {
            begin_close(static_cast<uint64_t>(error), "local close");
            return;
        }

        auto promise = std::make_shared<std::promise<void>>();
        std::future<void> future = promise->get_future();
        executor_.post([this, error, promise] {
            begin_close(static_cast<uint64_t>(error), "local close");
            promise->set_value();
        });
        future.wait();
    }

private:
    struct PendingUpdate {
        RequestId request_id = 0;
        std::shared_ptr<std::promise<RequestOk>> promise;
    };

    struct Subscription {
        RequestId request_id = 0;
        SubscribeRequest request;
        SubscriptionPhase phase = SubscriptionPhase::Pending;
        std::optional<TrackAlias> track_alias;
        std::shared_ptr<TransportStream> stream;
        ByteBuffer response_buffer;
        std::shared_ptr<ObjectHandler> handler;
        std::shared_ptr<ReceiveRoute> route;
        std::shared_ptr<std::promise<SubscriptionHandle>> subscribe_promise;
        bool subscribe_settled = false;
        std::deque<PendingUpdate> updates;
    };

    struct PeerUniDemux {
        std::weak_ptr<SessionImpl> session;
        std::shared_ptr<TransportStream> stream;
        ByteBuffer bytes;

        void feed(ByteBuffer input, bool fin) {
            bytes.insert(bytes.end(), input.begin(), input.end());
            const codec::VarintResult type = codec::read_varint(bytes);
            if (type.status != codec::DecodeStatus::Done) {
                if (fin) {
                    if (auto locked = session.lock()) {
                        locked->protocol_violation("peer unidirectional stream ended before type");
                    }
                }
                return;
            }
            auto locked = session.lock();
            if (!locked) {
                return;
            }
            ByteBuffer initial = std::move(bytes);
            if (type.value == codec::kSetupStreamType) {
                ByteBuffer payload(
                    initial.begin() + static_cast<std::ptrdiff_t>(type.bytes),
                    initial.end());
                std::weak_ptr<SessionImpl> weak = locked;
                stream->on_bytes([weak](ByteBuffer next, bool bytes_fin) mutable {
                    if (auto active = weak.lock()) {
                        active->on_peer_control_bytes(std::move(next), bytes_fin);
                    }
                });
                locked->on_peer_control_bytes(std::move(payload), fin);
                return;
            }
            if (codec::is_subgroup_stream_type(type.value)) {
                locked->data_plane_.start_subgroup_stream(stream, std::move(initial), fin);
                return;
            }
            if (type.value == codec::kFetchStreamType) {
                stream->abort_receive(0);
                return;
            }
            if (type.value == codec::kPaddingStreamType) {
                locked->start_padding_stream(stream, std::move(initial), type.bytes);
                return;
            }
            locked->protocol_violation(
                "unknown peer unidirectional stream type " + std::to_string(type.value));
        }
    };

    struct PeerBidiDemux {
        std::weak_ptr<SessionImpl> session;
        std::shared_ptr<TransportStream> stream;
        ByteBuffer bytes;

        void feed(ByteBuffer input, bool fin) {
            bytes.insert(bytes.end(), input.begin(), input.end());
            const codec::ControlMessageResult message = codec::read_control_message(bytes);
            if (message.status != codec::DecodeStatus::Done) {
                if (fin) {
                    if (auto active = session.lock()) {
                        active->protocol_violation(
                            "peer bidirectional stream ended before first request");
                    }
                }
                return;
            }
            auto active = session.lock();
            if (!active) {
                return;
            }
            if (!known_peer_request_type(message.message.type)) {
                active->protocol_violation(
                    "unknown peer request type " + std::to_string(message.message.type));
                return;
            }
            const uint64_t error_code =
                message.message.type == codec::kMessagePublish ? 0x20 : 0x03;
            const std::string reason =
                message.message.type == codec::kMessagePublish
                    ? "subscriber is not accepting PUBLISH"
                    : "subscriber-only implementation";
            stream->send(codec::encode_request_error(error_code, reason), true);
            stream->abort_receive(0);
        }
    };

    void on_connected() {
        if (phase_ != SessionPhase::Init) {
            return;
        }
        phase_ = SessionPhase::SetupInProgress;
        try {
            local_setup_stream_ = transport_->open_stream(true);
            if (!local_setup_stream_->send(
                    codec::encode_setup(
                        default_authority(msquic_config_), msquic_config_.path))) {
                throw std::runtime_error("StreamSend failed for SETUP");
            }
            local_setup_sent_ = true;
            refresh_session_snapshot();
        } catch (const std::exception& error) {
            begin_close(
                static_cast<uint64_t>(SessionCloseErrorCode::InternalError),
                error.what());
        }
    }

    void on_peer_stream_started(std::shared_ptr<TransportStream> stream) {
        if (stream->unidirectional()) {
            auto demux = std::make_shared<PeerUniDemux>();
            demux->session = weak_from_this();
            demux->stream = stream;
            stream->on_bytes([demux](ByteBuffer bytes, bool fin) mutable {
                demux->feed(std::move(bytes), fin);
            });
            return;
        }
        auto demux = std::make_shared<PeerBidiDemux>();
        demux->session = weak_from_this();
        demux->stream = stream;
        stream->on_bytes([demux](ByteBuffer bytes, bool fin) mutable {
            demux->feed(std::move(bytes), fin);
        });
    }

    void start_padding_stream(
        const std::shared_ptr<TransportStream>& stream,
        ByteBuffer initial,
        size_t type_bytes) {
        if (!std::all_of(
                initial.begin() + static_cast<std::ptrdiff_t>(type_bytes), initial.end(),
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
        peer_control_buffer_.insert(
            peer_control_buffer_.end(), bytes.begin(), bytes.end());
        for (;;) {
            const codec::ControlMessageResult parsed =
                codec::read_control_message(peer_control_buffer_);
            if (parsed.status != codec::DecodeStatus::Done) {
                break;
            }
            peer_control_buffer_.erase(
                peer_control_buffer_.begin(),
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

    void handle_peer_control_message(const codec::ControlMessage& message) {
        if (!peer_setup_received_) {
            if (message.type != codec::kMessageSetup) {
                protocol_violation("first peer control message was not SETUP");
                return;
            }
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
        protocol_violation(
            "unsupported control message " + std::to_string(message.type));
    }

    void maybe_ready() {
        if (!local_setup_sent_ || !peer_setup_received_ ||
            phase_ == SessionPhase::Closing || phase_ == SessionPhase::Closed) {
            refresh_session_snapshot();
            return;
        }
        phase_ = SessionPhase::Ready;
        for (const auto& waiter : ready_waiters_) {
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

    void subscribe_on_executor(
        SubscribeRequest request,
        std::shared_ptr<ObjectHandler> handler,
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
                if (!subscriber_config_.allow_explicit_request_ids ||
                    (*request.request_id & 1U) != 0 ||
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
            auto subscription = std::make_shared<Subscription>();
            subscription->request_id = request_id;
            subscription->request = request;
            subscription->handler = std::move(handler);
            subscription->subscribe_promise = promise;
            subscription->stream = transport_->open_stream(false);
            const std::weak_ptr<SessionImpl> weak = weak_from_this();
            subscription->stream->on_bytes(
                [weak, request_id](ByteBuffer bytes, bool fin) mutable {
                    if (auto active = weak.lock()) {
                        active->on_subscription_bytes(request_id, std::move(bytes), fin);
                    }
                });
            subscription->stream->on_peer_send_aborted(
                [weak, request_id](uint64_t error_code) {
                    if (auto active = weak.lock()) {
                        active->subscription_aborted(request_id, error_code);
                    }
                });
            subscription->stream->on_shutdown([weak, request_id] {
                if (auto active = weak.lock()) {
                    active->subscription_shutdown(request_id);
                }
            });
            subscriptions_.emplace(request_id, subscription);
            update_subscription_snapshot(*subscription);
            refresh_session_snapshot();
            if (!subscription->stream->send(codec::encode_subscribe(request_id, request))) {
                throw std::runtime_error("StreamSend failed for SUBSCRIBE");
            }
        } catch (...) {
            set_exception(promise, std::current_exception());
        }
    }

    void request_update_on_executor(
        RequestId existing_request_id,
        RequestUpdate update,
        std::shared_ptr<std::promise<RequestOk>> promise) {
        const auto found = subscriptions_.find(existing_request_id);
        if (found == subscriptions_.end() ||
            found->second->phase != SubscriptionPhase::Established) {
            set_exception(promise, "REQUEST_UPDATE requires an established subscription");
            return;
        }
        std::shared_ptr<Subscription> subscription = found->second;
        try {
            PendingUpdate pending;
            pending.request_id = allocate_request_id();
            pending.promise = promise;
            if (!subscription->stream->send(
                    codec::encode_request_update(pending.request_id, update))) {
                throw std::runtime_error("StreamSend failed for REQUEST_UPDATE");
            }
            subscription->updates.push_back(std::move(pending));
            update_subscription_snapshot(*subscription);
        } catch (...) {
            set_exception(promise, std::current_exception());
        }
    }

    void on_subscription_bytes(RequestId request_id, ByteBuffer bytes, bool fin) {
        const auto found = subscriptions_.find(request_id);
        if (found == subscriptions_.end()) {
            return;
        }
        const std::shared_ptr<Subscription> subscription = found->second;
        subscription->response_buffer.insert(
            subscription->response_buffer.end(), bytes.begin(), bytes.end());
        for (;;) {
            const codec::ControlMessageResult parsed =
                codec::read_control_message(subscription->response_buffer);
            if (parsed.status != codec::DecodeStatus::Done) {
                break;
            }
            subscription->response_buffer.erase(
                subscription->response_buffer.begin(),
                subscription->response_buffer.begin() +
                    static_cast<std::ptrdiff_t>(parsed.bytes));
            handle_subscription_message(subscription, parsed.message);
            if (phase_ == SessionPhase::Closing || phase_ == SessionPhase::Closed) {
                return;
            }
        }
        if (!fin) {
            return;
        }
        if (!subscription->response_buffer.empty()) {
            protocol_violation("request stream ended mid-message");
            return;
        }
        if (subscription->phase != SubscriptionPhase::Terminated) {
            terminate_subscription(
                subscription, true, "publisher closed request stream");
        }
    }

    void handle_subscription_message(
        const std::shared_ptr<Subscription>& subscription,
        const codec::ControlMessage& message) {
        if (subscription->phase == SubscriptionPhase::Pending) {
            if (message.type == codec::kMessageSubscribeOk) {
                accept_subscribe_ok(subscription, message.payload);
                return;
            }
            if (message.type == codec::kMessageRequestError) {
                reject_initial_subscribe(subscription, message.payload);
                return;
            }
            protocol_violation("invalid first response on SUBSCRIBE stream");
            return;
        }

        if (subscription->phase == SubscriptionPhase::Terminated) {
            return;
        }
        if (message.type == codec::kMessageRequestOk) {
            accept_request_ok(subscription, message.payload);
            return;
        }
        if (message.type == codec::kMessageRequestError) {
            reject_request_update(subscription, message.payload);
            return;
        }
        if (message.type == codec::kMessagePublishDone) {
            accept_publish_done(subscription, message.payload);
            return;
        }
        if (message.type == codec::kMessageGoAway) {
            return;
        }
        protocol_violation(
            "invalid response on SUBSCRIBE stream: " + std::to_string(message.type));
    }

    void accept_subscribe_ok(
        const std::shared_ptr<Subscription>& subscription,
        const ByteBuffer& payload) {
        std::string error;
        const std::optional<codec::SubscribeOk> ok =
            codec::decode_subscribe_ok(payload, error);
        if (!ok) {
            protocol_violation(std::move(error));
            return;
        }
        auto route = std::make_shared<ReceiveRoute>();
        route->request_id = subscription->request_id;
        route->track_alias = ok->track_alias;
        route->handler = subscription->handler;
        if (!data_plane_.install_route(ok->track_alias, route)) {
            begin_close(
                static_cast<uint64_t>(SessionCloseErrorCode::DuplicateTrackAlias),
                "SUBSCRIBE_OK reused an established Track Alias");
            return;
        }
        subscription->track_alias = ok->track_alias;
        subscription->route = std::move(route);
        subscription->phase = SubscriptionPhase::Established;
        if (!subscription->subscribe_settled) {
            subscription->subscribe_promise->set_value(
                SubscriptionHandle(subscription->request_id, weak_from_this()));
            subscription->subscribe_settled = true;
        }
        update_subscription_snapshot(*subscription);
        refresh_session_snapshot();
    }

    void reject_initial_subscribe(
        const std::shared_ptr<Subscription>& subscription,
        const ByteBuffer& payload) {
        std::string error;
        const std::optional<RequestError> rejected =
            codec::decode_request_error(payload, error);
        if (!rejected) {
            protocol_violation(std::move(error));
            return;
        }
        if (!subscription->subscribe_settled) {
            set_exception(subscription->subscribe_promise, rejected_exception(*rejected));
            subscription->subscribe_settled = true;
        }
        terminate_subscription(subscription, false, {});
    }

    void accept_request_ok(
        const std::shared_ptr<Subscription>& subscription,
        const ByteBuffer& payload) {
        if (subscription->updates.empty()) {
            protocol_violation("REQUEST_OK arrived without an in-flight REQUEST_UPDATE");
            return;
        }
        std::string error;
        const std::optional<RequestOk> ok = codec::decode_request_ok(payload, error);
        if (!ok) {
            protocol_violation(std::move(error));
            return;
        }
        if (!ok->track_properties.empty()) {
            protocol_violation("REQUEST_UPDATE_OK included Track Properties");
            return;
        }
        PendingUpdate pending = std::move(subscription->updates.front());
        subscription->updates.pop_front();
        pending.promise->set_value(*ok);
        update_subscription_snapshot(*subscription);
    }

    void reject_request_update(
        const std::shared_ptr<Subscription>& subscription,
        const ByteBuffer& payload) {
        if (subscription->updates.empty()) {
            protocol_violation("REQUEST_ERROR arrived without an in-flight request");
            return;
        }
        std::string error;
        const std::optional<RequestError> rejected =
            codec::decode_request_error(payload, error);
        if (!rejected) {
            protocol_violation(std::move(error));
            return;
        }
        const std::exception_ptr exception = rejected_exception(*rejected);
        while (!subscription->updates.empty()) {
            PendingUpdate pending = std::move(subscription->updates.front());
            subscription->updates.pop_front();
            set_exception(pending.promise, exception);
        }
        subscription->phase = SubscriptionPhase::UpdateFailed;
        update_subscription_snapshot(*subscription);
    }

    void accept_publish_done(
        const std::shared_ptr<Subscription>& subscription,
        const ByteBuffer& payload) {
        std::string error;
        const std::optional<PublishDone> done =
            codec::decode_publish_done(payload, error);
        if (!done) {
            protocol_violation(std::move(error));
            return;
        }
        if (subscription->route) {
            subscription->route->expected_stream_count.store(done->stream_count);
        }
        subscription->handler->on_publish_done(*done);
        terminate_subscription(subscription, false, {});
    }

    void subscription_aborted(RequestId request_id, uint64_t error_code) {
        const auto found = subscriptions_.find(request_id);
        if (found == subscriptions_.end()) {
            return;
        }
        terminate_subscription(
            found->second,
            true,
            "publisher reset request stream with error " + std::to_string(error_code));
    }

    void subscription_shutdown(RequestId request_id) {
        const auto found = subscriptions_.find(request_id);
        if (found == subscriptions_.end() ||
            found->second->phase == SubscriptionPhase::Terminated) {
            return;
        }
        terminate_subscription(
            found->second, true, "request stream shut down before subscription ended");
    }

    void stop_subscription_on_executor(RequestId request_id, bool report_error) {
        const auto found = subscriptions_.find(request_id);
        if (found == subscriptions_.end()) {
            return;
        }
        terminate_subscription(
            found->second,
            report_error,
            report_error ? "subscription stopped after receive error" : std::string{});
        found->second->stream->abort_receive(0);
    }

    void terminate_subscription(
        const std::shared_ptr<Subscription>& subscription,
        bool report_error,
        std::string reason) {
        if (subscription->phase == SubscriptionPhase::Terminated) {
            return;
        }
        if (!subscription->subscribe_settled) {
            set_exception(
                subscription->subscribe_promise,
                reason.empty() ? "subscription terminated before SUBSCRIBE_OK" : reason);
            subscription->subscribe_settled = true;
        }
        while (!subscription->updates.empty()) {
            PendingUpdate pending = std::move(subscription->updates.front());
            subscription->updates.pop_front();
            set_exception(
                pending.promise,
                reason.empty() ? "subscription terminated before REQUEST_UPDATE completed"
                               : reason);
        }
        if (subscription->track_alias) {
            data_plane_.deactivate_route(*subscription->track_alias);
            data_plane_.remove_route(*subscription->track_alias);
        }
        subscription->phase = SubscriptionPhase::Terminated;
        if (report_error && !reason.empty()) {
            subscription->handler->on_error(ReceiveError{0, std::move(reason)});
        }
        update_subscription_snapshot(*subscription);
        refresh_session_snapshot();
    }

    void malformed_track(RequestId request_id, std::string error) {
        const auto found = subscriptions_.find(request_id);
        if (found == subscriptions_.end()) {
            return;
        }
        found->second->handler->on_error(ReceiveError{0x12, error});
        stop_subscription_on_executor(request_id, false);
    }

    void protocol_violation(std::string error) {
        begin_close(
            static_cast<uint64_t>(SessionCloseErrorCode::ProtocolViolation),
            std::move(error));
    }

    void transport_error(std::string error) {
        begin_close(
            static_cast<uint64_t>(SessionCloseErrorCode::InternalError),
            std::move(error));
    }

    void begin_close(uint64_t code, std::string reason) {
        if (phase_ == SessionPhase::Closed || phase_ == SessionPhase::Closing) {
            return;
        }
        phase_ = SessionPhase::Closing;
        close_reason_ = SessionCloseReason{code, reason};
        fail_ready_waiters(reason.empty() ? "MOQT session closing" : reason);
        for (auto& entry : subscriptions_) {
            terminate_subscription(entry.second, true, reason);
        }
        refresh_session_snapshot();
        if (transport_) {
            transport_->shutdown(code);
        }
    }

    void shutdown_complete(bool handshake_completed) {
        if (!handshake_completed && !close_reason_) {
            close_reason_ = SessionCloseReason{
                static_cast<uint64_t>(SessionCloseErrorCode::InternalError),
                "QUIC handshake did not complete"};
        }
        phase_ = SessionPhase::Closed;
        fail_ready_waiters("MOQT session closed");
        refresh_session_snapshot();
    }

    void fail_ready_waiters(const std::string& reason) {
        for (const auto& waiter : ready_waiters_) {
            set_exception(waiter, reason);
        }
        ready_waiters_.clear();
    }

    void update_subscription_snapshot(const Subscription& subscription) {
        std::lock_guard<std::mutex> lock(snapshot_mutex_);
        SubscriptionStateSnapshot snapshot;
        snapshot.phase = subscription.phase;
        snapshot.request_id = subscription.request_id;
        snapshot.track_alias = subscription.track_alias;
        snapshot.inflight_updates = subscription.updates.size();
        subscription_snapshots_[subscription.request_id] = snapshot;
    }

    void refresh_session_snapshot() {
        std::lock_guard<std::mutex> lock(snapshot_mutex_);
        session_snapshot_.phase = phase_;
        session_snapshot_.local_setup_sent = local_setup_sent_;
        session_snapshot_.peer_setup_received = peer_setup_received_;
        session_snapshot_.negotiated_version =
            (phase_ == SessionPhase::Ready || phase_ == SessionPhase::Draining ||
             phase_ == SessionPhase::Closing || phase_ == SessionPhase::Closed)
                ? std::optional<std::string>(msquic_config_.alpn)
                : std::nullopt;
        size_t active = 0;
        for (const auto& subscription : subscriptions_) {
            if (subscription.second->phase != SubscriptionPhase::Terminated) {
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
    std::unordered_map<RequestId, std::shared_ptr<Subscription>> subscriptions_;

    mutable std::mutex snapshot_mutex_;
    SessionStateSnapshot session_snapshot_;
    std::unordered_map<RequestId, SubscriptionStateSnapshot> subscription_snapshots_;
};

} // namespace moq::detail

namespace moq {

SubscriptionHandle::SubscriptionHandle(
    RequestId request_id, std::weak_ptr<detail::SessionImpl> session)
    : request_id_(request_id),
      session_(std::move(session)) {}

RequestId SubscriptionHandle::request_id() const {
    return request_id_;
}

std::optional<TrackAlias> SubscriptionHandle::track_alias() const {
    return state().track_alias;
}

SubscriptionStateSnapshot SubscriptionHandle::state() const {
    const std::shared_ptr<detail::SessionImpl> session = session_.lock();
    if (!session) {
        SubscriptionStateSnapshot terminated;
        terminated.request_id = request_id_;
        return terminated;
    }
    return session->subscription_state(request_id_);
}

MoqSubscriberSession::MoqSubscriberSession(std::shared_ptr<detail::SessionImpl> impl)
    : impl_(std::move(impl)) {}

MoqSubscriberSession::~MoqSubscriberSession() {
    if (impl_) {
        impl_->close_and_wait(SessionCloseErrorCode::NoError);
    }
}

std::future<std::unique_ptr<MoqSubscriberSession>> MoqSubscriberSession::connect(
    MsQuicClientConfig msquic_config,
    SubscriberConfig subscriber_config) {
    std::promise<std::unique_ptr<MoqSubscriberSession>> promise;
    std::future<std::unique_ptr<MoqSubscriberSession>> future = promise.get_future();
    try {
        auto impl = std::make_shared<detail::SessionImpl>(
            std::move(msquic_config), std::move(subscriber_config));
        impl->start();
        promise.set_value(
            std::unique_ptr<MoqSubscriberSession>(new MoqSubscriberSession(std::move(impl))));
    } catch (...) {
        promise.set_exception(std::current_exception());
    }
    return future;
}

std::future<void> MoqSubscriberSession::ready() {
    return impl_->ready();
}

SessionStateSnapshot MoqSubscriberSession::state() const {
    return impl_->state();
}

std::future<SubscriptionHandle> MoqSubscriberSession::subscribe(
    SubscribeRequest request,
    std::shared_ptr<ObjectHandler> handler) {
    return impl_->subscribe(std::move(request), std::move(handler));
}

std::future<RequestOk> MoqSubscriberSession::request_update(
    RequestId existing_request_id,
    RequestUpdate update) {
    return impl_->request_update(existing_request_id, std::move(update));
}

std::future<void> MoqSubscriberSession::stop_subscription(RequestId request_id) {
    return impl_->stop_subscription(request_id);
}

void MoqSubscriberSession::close(SessionCloseErrorCode error) {
    impl_->close(error);
}

} // namespace moq
