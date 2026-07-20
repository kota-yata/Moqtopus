#pragma once

#include "data_plane.h"
#include "moq/codec.h"
#include "moq/errors.h"
#include "moq/object_handler.h"
#include "moq/types.h"
#include "stream_context.h"

#include <deque>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <string>

namespace moq::detail {

// converts REQUEST_ERROR to a C++ exception
inline std::exception_ptr rejected_exception(const RequestError &error) {
  return std::make_exception_ptr(RequestRejected(error.code, error.retry_interval, error.reason));
}

// Route registry operations the FSM needs from its owning session.
class SubscriptionOwner {
public:
  virtual ~SubscriptionOwner() = default;

  virtual bool install_route(TrackAlias alias, std::shared_ptr<ReceiveRoute> route) = 0;
  virtual void retire_route(TrackAlias alias) = 0;
};

class SubscriptionFSM final : public StreamSink {
public:
  using SubscribeResultCb = std::function<void(std::optional<RequestError> rejected, std::optional<TrackAlias> alias)>;

  SubscriptionFSM(RequestId request_id, std::shared_ptr<ObjectHandler> handler, std::shared_ptr<StreamContext> stream,
                  std::weak_ptr<SubscriptionOwner> owner, SubscribeResultCb subscribe_result_cb);

  // StreamSink: control responses arriving on the SUBSCRIBE bidi stream.
  void on_receive(const BytesView *chunks, size_t count, bool fin) override;
  void on_peer_send_aborted(uint64_t error_code) override;
  void on_stream_closed() override;

  // Enqueue a request-update to be sent on this stream.
  void send_request_update(RequestId allocated_request_id, RequestUpdate update, std::promise<RequestOk> promise,
                           std::function<bool(ByteBuffer)> sender);

  // Stop the subscription and optionally report error to handler.
  void terminate(bool report_error, std::string reason);

  moq::SubscriptionPhase phase() const { return phase_; }
  RequestId request_id() const { return request_id_; }
  std::shared_ptr<StreamContext> stream() const { return stream_; }
  std::optional<TrackAlias> track_alias() const { return track_alias_; }
  size_t inflight_updates() const { return updates_.size(); }

  // Allow owner to ask FSM to report an error to the handler.
  void report_handler_error(const ReceiveError &err) {
    if (handler_)
      handler_->on_error(err);
  }

private:
  void fail_updates(std::exception_ptr error);
  void handle_control_message(const codec::ControlMessage &message);
  void accept_subscribe_ok(const codec::ControlMessage &message);
  void reject_initial_subscribe(const codec::ControlMessage &message);
  void accept_request_ok(const codec::ControlMessage &message);
  void reject_request_update(const codec::ControlMessage &message);
  void accept_publish_done(const codec::ControlMessage &message);

  RequestId request_id_;
  moq::SubscriptionPhase phase_ = moq::SubscriptionPhase::Pending;
  std::optional<TrackAlias> track_alias_;
  std::shared_ptr<StreamContext> stream_;
  ByteBuffer response_buffer_;
  std::shared_ptr<ObjectHandler> handler_;
  std::shared_ptr<ReceiveRoute> route_;
  bool subscribe_settled_ = false;
  std::deque<std::promise<RequestOk>> updates_; // settled in FIFO order by REQUEST_OK / REQUEST_ERROR
  std::weak_ptr<SubscriptionOwner> owner_;
  SubscribeResultCb subscribe_result_cb_;
};

} // namespace moq::detail
