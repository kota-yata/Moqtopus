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

// FSM for a single request stream (SUBSCRIBE / FETCH / ...).
class SubscriptionFSM {
public:
  struct PendingUpdate {
    RequestId request_id = 0;
    std::shared_ptr<std::promise<RequestOk>> promise;
  };

  using InstallRouteCb = std::function<bool(TrackAlias, std::shared_ptr<ReceiveRoute>)>;
  using DeactivateRouteCb = std::function<void(TrackAlias)>;
  using RemoveRouteCb = std::function<void(TrackAlias)>;
  using SubscribeResultCb = std::function<void(std::optional<RequestError> rejected, std::optional<TrackAlias> alias)>;

  SubscriptionFSM(RequestId request_id, SubscribeRequest request, std::shared_ptr<ObjectHandler> handler,
                  std::shared_ptr<StreamContext> stream, InstallRouteCb install_cb, DeactivateRouteCb deactivate_cb,
                  RemoveRouteCb remove_cb, SubscribeResultCb subscribe_result_cb);

  ~SubscriptionFSM();

  // Feed raw bytes from the peer into this request stream.
  void on_bytes(ByteBuffer bytes, bool fin);

  // Called when the peer aborts sending on this stream.
  void on_peer_send_aborted(uint64_t error_code);

  // Called when the stream is shutdown by the peer.
  void on_shutdown();

  // Enqueue a request-update to be sent on this stream. Returns the generated id.
  RequestId send_request_update(RequestId allocated_request_id, RequestUpdate update,
                                std::shared_ptr<std::promise<RequestOk>> promise,
                                std::function<bool(ByteBuffer)> sender);

  // Stop the subscription and optionally report error to handler.
  void terminate(bool report_error, std::string reason);

  moq::SubscriptionPhase phase() const { return phase_; }
  RequestId request_id() const { return request_id_; }

private:
  void handle_control_message(const codec::ControlMessage &message);
  void accept_subscribe_ok(const codec::ControlMessage &message);
  void reject_initial_subscribe(const codec::ControlMessage &message);
  void accept_request_ok(const codec::ControlMessage &message);
  void reject_request_update(const codec::ControlMessage &message);
  void accept_publish_done(const codec::ControlMessage &message);

  RequestId request_id_;
  SubscribeRequest request_;
  moq::SubscriptionPhase phase_ = moq::SubscriptionPhase::Pending;
  std::optional<TrackAlias> track_alias_;
  std::shared_ptr<StreamContext> stream_;
  ByteBuffer response_buffer_;
  std::shared_ptr<ObjectHandler> handler_;
  std::shared_ptr<ReceiveRoute> route_;
  bool subscribe_settled_ = false;
  std::deque<PendingUpdate> updates_;

  InstallRouteCb install_route_cb_;
  DeactivateRouteCb deactivate_route_cb_;
  RemoveRouteCb remove_route_cb_;
  SubscribeResultCb subscribe_result_cb_;

public:
  std::shared_ptr<StreamContext> stream() const { return stream_; }
  std::optional<TrackAlias> track_alias() const { return track_alias_; }
  size_t inflight_updates() const { return updates_.size(); }
  // Allow owner to ask FSM to report an error to the handler.
  void report_handler_error(const ReceiveError &err) {
    if (handler_)
      handler_->on_error(err);
  }
};

} // namespace moq::detail
