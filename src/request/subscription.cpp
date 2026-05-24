#include "request/subscription.h"

#include "moq/codec.h"
#include "moq/errors.h"

#include <utility>

namespace moq::detail
{

    static std::exception_ptr rejected_exception(const RequestError &error)
    {
        return std::make_exception_ptr(RequestRejected(error.code, error.retry_interval, error.reason));
    }

    SubscriptionFSM::SubscriptionFSM(RequestId request_id, SubscribeRequest request, std::shared_ptr<ObjectHandler> handler,
                                     std::shared_ptr<TransportStream> stream, InstallRouteCb install_cb, DeactivateRouteCb deactivate_cb,
                                     RemoveRouteCb remove_cb, SubscribeResultCb subscribe_result_cb)
        : request_id_(request_id),
          request_(std::move(request)),
          stream_(std::move(stream)),
          handler_(std::move(handler)),
          install_route_cb_(std::move(install_cb)),
          deactivate_route_cb_(std::move(deactivate_cb)),
          remove_route_cb_(std::move(remove_cb)),
          subscribe_result_cb_(std::move(subscribe_result_cb)) {}

    SubscriptionFSM::~SubscriptionFSM() = default;

    void SubscriptionFSM::on_bytes(ByteBuffer bytes, bool fin)
    {
        response_buffer_.insert(response_buffer_.end(), bytes.begin(), bytes.end());
        for (;;)
        {
            const codec::ControlMessageResult parsed = codec::read_control_message(response_buffer_);
            if (parsed.status != codec::DecodeStatus::Done)
            {
                break;
            }
            response_buffer_.erase(response_buffer_.begin(), response_buffer_.begin() +
                                                                 static_cast<std::ptrdiff_t>(parsed.bytes));
            handle_control_message(parsed.message);
            if (phase_ == moq::SubscriptionPhase::Terminated)
            {
                return;
            }
        }
        if (fin && !response_buffer_.empty())
        {
            // Protocol violation; notify owner through handler
            handler_->on_error(ReceiveError{0, "request stream ended mid-message"});
            terminate(true, "request stream ended mid-message");
        }
    }

    void SubscriptionFSM::on_peer_send_aborted(uint64_t error_code)
    {
        handler_->on_error(ReceiveError{0, "publisher reset request stream with error " + std::to_string(error_code)});
        terminate(true, "publisher reset request stream");
    }

    void SubscriptionFSM::on_shutdown()
    {
        terminate(true, "request stream shut down before subscription ended");
    }

    RequestId SubscriptionFSM::send_request_update(RequestId allocated_request_id,
                                                   RequestUpdate update,
                                                   std::shared_ptr<std::promise<RequestOk>> promise,
                                                   std::function<bool(ByteBuffer)> sender)
    {
        PendingUpdate pending;
        pending.request_id = allocated_request_id;
        pending.promise = promise;
        // encode and send via provided sender; failure should surface to caller
        if (!sender(codec::encode_request_update(allocated_request_id, update)))
        {
            throw std::runtime_error("StreamSend failed for REQUEST_UPDATE");
        }
        updates_.push_back(std::move(pending));
        return allocated_request_id;
    }

    void SubscriptionFSM::terminate(bool report_error, std::string reason)
    {
        if (phase_ == moq::SubscriptionPhase::Terminated)
            return;
        if (!subscribe_settled_)
        {
            // Owner is responsible for promise settling; mark as settled locally.
            subscribe_settled_ = true;
        }
        while (!updates_.empty())
        {
            PendingUpdate pending = std::move(updates_.front());
            updates_.pop_front();
            try
            {
                throw std::runtime_error(reason.empty() ? "subscription terminated before REQUEST_UPDATE completed" : reason);
            }
            catch (...)
            {
                try
                {
                    pending.promise->set_exception(std::current_exception());
                }
                catch (const std::future_error &)
                {
                }
            }
        }
        if (track_alias_)
        {
            if (deactivate_route_cb_)
                deactivate_route_cb_(*track_alias_);
            if (remove_route_cb_)
                remove_route_cb_(*track_alias_);
        }
        phase_ = moq::SubscriptionPhase::Terminated;
        if (report_error && !reason.empty())
        {
            handler_->on_error(ReceiveError{0, std::move(reason)});
        }
    }

    void SubscriptionFSM::handle_control_message(const codec::ControlMessage &message)
    {
        if (phase_ == moq::SubscriptionPhase::Pending)
        {
            if (message.type == codec::kMessageSubscribeOk)
            {
                accept_subscribe_ok(message);
                return;
            }
            if (message.type == codec::kMessageRequestError)
            {
                reject_initial_subscribe(message);
                return;
            }
            handler_->on_error(ReceiveError{0, "invalid first response on SUBSCRIBE stream"});
            terminate(true, "invalid first response on SUBSCRIBE stream");
            return;
        }

        if (phase_ == moq::SubscriptionPhase::Terminated)
            return;
        switch (message.type)
        {
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

    void SubscriptionFSM::accept_subscribe_ok(const codec::ControlMessage &message)
    {
        std::string error;
        const std::optional<codec::SubscribeOk> ok = codec::decode_subscribe_ok(message.payload, error);
        if (!ok)
        {
            terminate(true, error);
            return;
        }
        auto route = std::make_shared<ReceiveRoute>();
        route->request_id = request_id_;
        route->track_alias = ok->track_alias;
        route->handler = handler_;
        if (install_route_cb_)
        {
            if (!install_route_cb_(ok->track_alias, route))
            {
                terminate(true, "SUBSCRIBE_OK reused an established Track Alias");
                return;
            }
        }
        track_alias_ = ok->track_alias;
        route_ = std::move(route);
        phase_ = moq::SubscriptionPhase::Established;
        // Notify owner that subscribe was accepted. Owner will settle promises.
        subscribe_settled_ = true;
        if (subscribe_result_cb_)
        {
            subscribe_result_cb_(std::nullopt, track_alias_);
        }
    }

    void SubscriptionFSM::reject_initial_subscribe(const codec::ControlMessage &message)
    {
        std::string error;
        const std::optional<RequestError> rejected = codec::decode_request_error(message.payload, error);
        if (!rejected)
        {
            terminate(true, error);
            return;
        }
        if (!subscribe_settled_)
        {
            // Notify owner that subscribe was rejected. Owner will settle promises.
            subscribe_settled_ = true;
            if (subscribe_result_cb_)
            {
                subscribe_result_cb_(rejected, std::nullopt);
            }
        }
        terminate(false, {});
    }

    void SubscriptionFSM::accept_request_ok(const codec::ControlMessage &message)
    {
        if (updates_.empty())
        {
            terminate(true, "REQUEST_OK arrived without an in-flight REQUEST_UPDATE");
            return;
        }
        std::string error;
        const std::optional<RequestOk> ok = codec::decode_request_ok(message.payload, error);
        if (!ok)
        {
            terminate(true, error);
            return;
        }
        if (!ok->track_properties.empty())
        {
            terminate(true, "REQUEST_UPDATE_OK included Track Properties");
            return;
        }
        PendingUpdate pending = std::move(updates_.front());
        updates_.pop_front();
        try
        {
            pending.promise->set_value(*ok);
        }
        catch (...)
        {
        }
    }

    void SubscriptionFSM::reject_request_update(const codec::ControlMessage &message)
    {
        if (updates_.empty())
        {
            terminate(true, "REQUEST_ERROR arrived without an in-flight request");
            return;
        }
        std::string error;
        const std::optional<RequestError> rejected = codec::decode_request_error(message.payload, error);
        if (!rejected)
        {
            terminate(true, error);
            return;
        }
        const std::exception_ptr exception = rejected_exception(*rejected);
        while (!updates_.empty())
        {
            PendingUpdate pending = std::move(updates_.front());
            updates_.pop_front();
            try
            {
                pending.promise->set_exception(exception);
            }
            catch (...)
            {
            }
        }
        phase_ = moq::SubscriptionPhase::UpdateFailed;
    }

    void SubscriptionFSM::accept_publish_done(const codec::ControlMessage &message)
    {
        std::string error;
        const std::optional<PublishDone> done = codec::decode_publish_done(message.payload, error);
        if (!done)
        {
            terminate(true, error);
            return;
        }
        if (route_)
        {
            route_->expected_stream_count.store(done->stream_count);
        }
        handler_->on_publish_done(*done);
        terminate(false, {});
    }

} // namespace moq::detail
