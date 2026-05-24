// Public API
#pragma once

#include "moq/errors.h"
#include "moq/object_handler.h"
#include "moq/types.h"

#include <chrono>
#include <future>
#include <memory>
#include <string>

namespace moq
{

    struct MsQuicClientConfig
    {
        std::string host;
        uint16_t port = 0;
        std::string alpn = "moqt-18";
        std::string authority;
        std::string path = "/";
        bool disable_certificate_validation = true;
        std::chrono::milliseconds idle_timeout{30000};
    };

    enum class UnknownAliasPolicy
    {
        Drop,
        BufferDatagrams,
        Error,
    };

    struct SubscriberConfig
    {
        UnknownAliasPolicy unknown_alias_policy = UnknownAliasPolicy::BufferDatagrams;
        size_t max_buffered_datagrams_per_alias = 16;
        size_t max_buffered_datagram_bytes = 256 * 1024;
        bool allow_explicit_request_ids = false;
    };

    namespace detail
    {
        class SessionImpl;
    }

    class SubscriptionHandle
    {
    public:
        SubscriptionHandle() = default;

        RequestId request_id() const;
        std::optional<TrackAlias> track_alias() const;
        SubscriptionStateSnapshot state() const;

    private:
        friend class detail::SessionImpl;

        SubscriptionHandle(RequestId request_id, std::weak_ptr<detail::SessionImpl> session);

        RequestId request_id_ = 0;
        std::weak_ptr<detail::SessionImpl> session_;
    };

    class MoqSubscriberSession
    {
    public:
        ~MoqSubscriberSession();

        MoqSubscriberSession(const MoqSubscriberSession &) = delete;
        MoqSubscriberSession &operator=(const MoqSubscriberSession &) = delete;

        static std::future<std::unique_ptr<MoqSubscriberSession>> connect(
            MsQuicClientConfig msquic_config,
            SubscriberConfig subscriber_config = {});

        std::future<void> ready();
        SessionStateSnapshot state() const;

        std::future<SubscriptionHandle> subscribe(
            SubscribeRequest request,
            std::shared_ptr<ObjectHandler> handler);
        std::future<RequestOk> request_update(RequestId existing_request_id, RequestUpdate update);
        std::future<void> stop_subscription(RequestId request_id);
        void close(SessionCloseErrorCode error = SessionCloseErrorCode::NoError);

    private:
        explicit MoqSubscriberSession(std::shared_ptr<detail::SessionImpl> impl);

        std::shared_ptr<detail::SessionImpl> impl_;
    };

} // namespace moq
