#pragma once

#include "executor.h"
#include "moq/subscriber_session.h"
#include "moq/types.h"

#include <msquic.h>

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace moq::detail
{

    class MsQuicTransportAdapter;

    class TransportStream : public std::enable_shared_from_this<TransportStream>
    {
    public:
        using BytesCallback = std::function<void(ByteBuffer, bool)>;
        using ErrorCallback = std::function<void(uint64_t)>;
        using ShutdownCallback = std::function<void()>;

        ~TransportStream();

        TransportStream(const TransportStream &) = delete;
        TransportStream &operator=(const TransportStream &) = delete;

        bool unidirectional() const;
        uint64_t id() const;
        void set_id(uint64_t id);
        void on_bytes(BytesCallback callback);
        void on_peer_send_aborted(ErrorCallback callback);
        void on_peer_receive_aborted(ErrorCallback callback);
        void on_shutdown(ShutdownCallback callback);

        bool send(ByteBuffer bytes, bool fin = false);
        void abort_receive(uint64_t error_code);
        void abort(uint64_t error_code);
        void set_receive_enabled(bool enabled);

    private:
        friend class MsQuicTransportAdapter;

        TransportStream(
            MsQuicTransportAdapter &adapter,
            HQUIC handle,
            bool unidirectional);
        static QUIC_STATUS QUIC_API stream_callback(
            HQUIC stream, void *context, QUIC_STREAM_EVENT *event);
        QUIC_STATUS handle_event(HQUIC stream, QUIC_STREAM_EVENT *event);
        void close_handle(HQUIC stream);

        MsQuicTransportAdapter &adapter_;
        HQUIC handle_ = nullptr;
        bool unidirectional_ = false;
        uint64_t id_ = 0;
        BytesCallback bytes_callback_;
        ErrorCallback peer_send_aborted_callback_;
        ErrorCallback peer_receive_aborted_callback_;
        ShutdownCallback shutdown_callback_;
    };

    class MsQuicTransportAdapter
    {
    public:
        struct Callbacks
        {
            std::function<void()> connected;
            std::function<void(std::shared_ptr<TransportStream>)> peer_stream_started;
            std::function<void(ByteBuffer)> datagram_received;
            std::function<void(std::string)> transport_error;
            std::function<void(bool)> shutdown_complete;
        };

        MsQuicTransportAdapter(
            Executor &executor,
            MsQuicClientConfig config,
            Callbacks callbacks);
        ~MsQuicTransportAdapter();

        MsQuicTransportAdapter(const MsQuicTransportAdapter &) = delete;
        MsQuicTransportAdapter &operator=(const MsQuicTransportAdapter &) = delete;

        void start();
        std::shared_ptr<TransportStream> open_stream(bool unidirectional);
        void shutdown(moq::SessionCloseErrorCode error_code);

        const QUIC_API_TABLE *api() const;
        Executor &executor();

    private:
        friend class TransportStream;

        static QUIC_STATUS QUIC_API connection_callback(
            HQUIC connection, void *context, QUIC_CONNECTION_EVENT *event);
        QUIC_STATUS handle_connection_event(HQUIC connection, QUIC_CONNECTION_EVENT *event);
        void remove_stream(TransportStream *stream);
        void close_connection_handle(HQUIC connection);

        Executor &executor_;
        MsQuicClientConfig config_;
        Callbacks callbacks_;
        const QUIC_API_TABLE *api_ = nullptr;
        HQUIC registration_ = nullptr;
        HQUIC configuration_ = nullptr;
        HQUIC connection_ = nullptr;
        std::mutex streams_mutex_;
        std::unordered_map<TransportStream *, std::shared_ptr<TransportStream>> streams_;
        bool started_ = false;
    };

} // namespace moq::detail
