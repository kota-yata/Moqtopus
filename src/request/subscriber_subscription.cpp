#include "request/subscriber_subscription.h"

#include "moq/codec.h"
#include "moq/errors.h"

#include <utility>

namespace moq::detail {

SubscriptionFSM::SubscriptionFSM(RequestId request_id, std::shared_ptr<ObjectHandler> handler,
                                 std::shared_ptr<StreamContext> stream, std::weak_ptr<SubscriptionOwner> owner,
                                 SubscribeResultCb subscribe_result_cb)
    : request_id_(request_id), stream_(std::move(stream)), handler_(std::move(handler)), owner_(std::move(owner)),
      subscribe_result_cb_(std::move(subscribe_result_cb)) {}

void SubscriptionFSM::on_receive(const BytesView *chunks, size_t count, bool fin) {
  for (size_t index = 0; index < count; ++index) {
    response_buffer_.insert(response_buffer_.end(), chunks[index].begin(), chunks[index].end());
  }
  while (phase_ == moq::SubscriptionPhase::Pending || phase_ == moq::SubscriptionPhase::Established) {
    const codec::ControlMessageResult parsed = codec::read_control_message(response_buffer_);
    if (parsed.status != codec::DecodeStatus::Done) {
      if (fin && !response_buffer_.empty()) {
        terminate(true, "request stream ended mid-message");
      }
      return;
    }
    response_buffer_.erase(response_buffer_.begin(), response_buffer_.begin() + parsed.bytes);
    handle_control_message(parsed.message);
  }
}

void SubscriptionFSM::on_peer_send_aborted(uint64_t error_code) {
  handler_->on_error(ReceiveError{0, "publisher reset request stream with error " + std::to_string(error_code)});
  terminate(true, "publisher reset request stream");
}

void SubscriptionFSM::on_stream_closed() { terminate(true, "request stream shut down before subscription ended"); }

void SubscriptionFSM::send_request_update(RequestId allocated_request_id, RequestUpdate update,
                                          std::promise<RequestOk> promise, std::function<bool(ByteBuffer)> sender) {
  if (!sender(codec::encode_request_update(allocated_request_id, update))) {
    throw std::runtime_error("StreamSend failed for REQUEST_UPDATE");
  }
  updates_.push_back(std::move(promise));
}

// Settles every in-flight REQUEST_UPDATE promise with the same error.
void SubscriptionFSM::fail_updates(std::exception_ptr error) {
  while (!updates_.empty()) {
    try {
      updates_.front().set_exception(error);
    } catch (const std::future_error &) {
    }
    updates_.pop_front();
  }
}

void SubscriptionFSM::terminate(bool report_error, std::string reason) {
  if (phase_ == moq::SubscriptionPhase::Terminated)
    return;
  subscribe_settled_ = true; // owner settles the subscribe promise
  const auto message = reason.empty() ? "subscription terminated before REQUEST_UPDATE completed" : reason;
  fail_updates(std::make_exception_ptr(std::runtime_error(message)));
  if (track_alias_) {
    if (const auto owner = owner_.lock()) {
      owner->retire_route(*track_alias_);
    }
  }
  phase_ = moq::SubscriptionPhase::Terminated;
  if (report_error && !reason.empty()) {
    handler_->on_error(ReceiveError{0, std::move(reason)});
  }
}

void SubscriptionFSM::handle_control_message(const codec::ControlMessage &message) {
  if (phase_ == moq::SubscriptionPhase::Pending) {
    if (message.type == codec::kMessageSubscribeOk) {
      accept_subscribe_ok(message);
    } else if (message.type == codec::kMessageRequestError) {
      reject_initial_subscribe(message);
    } else {
      handler_->on_error(ReceiveError{0, "invalid first response on SUBSCRIBE stream"});
      terminate(true, "invalid first response on SUBSCRIBE stream");
    }
    return;
  }

  switch (message.type) {
  case codec::kMessageRequestOk:
    accept_request_ok(message);
    return;
  case codec::kMessageRequestError:
    reject_request_update(message);
    return;
  case codec::kMessagePublishDone:
    accept_publish_done(message);
    return;
  case codec::kMessageGoAway:
    // ignore GoAway for request streams
    return;
  default:
    handler_->on_error(ReceiveError{0, "invalid response on SUBSCRIBE stream: " + std::to_string(message.type)});
    return;
  }
}

void SubscriptionFSM::accept_subscribe_ok(const codec::ControlMessage &message) {
  std::string error;
  const std::optional<codec::SubscribeOk> ok = codec::decode_subscribe_ok(message.payload, error);
  if (!ok) {
    terminate(true, error);
    return;
  }
  auto route = std::make_shared<ReceiveRoute>();
  route->request_id = request_id_;
  route->track_alias = ok->track_alias;
  route->handler = handler_;
  const auto owner = owner_.lock();
  if (!owner || !owner->install_route(ok->track_alias, route)) {
    terminate(true, "SUBSCRIBE_OK reused an established Track Alias");
    return;
  }
  track_alias_ = ok->track_alias;
  route_ = std::move(route);
  phase_ = moq::SubscriptionPhase::Established;
  subscribe_settled_ = true; // owner settles the subscribe promise via the callback
  if (subscribe_result_cb_) {
    subscribe_result_cb_(std::nullopt, track_alias_);
  }
}

void SubscriptionFSM::reject_initial_subscribe(const codec::ControlMessage &message) {
  std::string error;
  const std::optional<RequestError> rejected = codec::decode_request_error(message.payload, error);
  if (!rejected) {
    terminate(true, error);
    return;
  }
  subscribe_settled_ = true; // owner settles the subscribe promise via the callback
  if (subscribe_result_cb_) {
    subscribe_result_cb_(rejected, std::nullopt);
  }
  terminate(false, {});
}

void SubscriptionFSM::accept_request_ok(const codec::ControlMessage &message) {
  if (updates_.empty()) {
    terminate(true, "REQUEST_OK arrived without an in-flight REQUEST_UPDATE");
    return;
  }
  std::string error;
  const std::optional<RequestOk> ok = codec::decode_request_ok(message.payload, error);
  if (!ok) {
    terminate(true, error);
    return;
  }
  if (!ok->track_properties.empty()) {
    terminate(true, "REQUEST_UPDATE_OK included Track Properties");
    return;
  }
  try {
    updates_.front().set_value(*ok);
  } catch (const std::future_error &) {
  }
  updates_.pop_front();
}

void SubscriptionFSM::reject_request_update(const codec::ControlMessage &message) {
  if (updates_.empty()) {
    terminate(true, "REQUEST_ERROR arrived without an in-flight request");
    return;
  }
  std::string error;
  const std::optional<RequestError> rejected = codec::decode_request_error(message.payload, error);
  if (!rejected) {
    terminate(true, error);
    return;
  }
  fail_updates(rejected_exception(*rejected));
  phase_ = moq::SubscriptionPhase::UpdateFailed;
}

void SubscriptionFSM::accept_publish_done(const codec::ControlMessage &message) {
  std::string error;
  const std::optional<PublishDone> done = codec::decode_publish_done(message.payload, error);
  if (!done) {
    terminate(true, error);
    return;
  }
  if (route_) {
    route_->expected_stream_count.store(done->stream_count);
  }
  handler_->on_publish_done(*done);
  terminate(false, {});
}

} // namespace moq::detail
