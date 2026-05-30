#include "msquic_transport_adapter.h"

#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <utility>

#include <msquichelper.h>

namespace moq::detail
{
    namespace
    {

        struct PendingSend
        {
            explicit PendingSend(ByteBuffer input)
                : bytes(std::move(input))
            {
                buffer.Length = static_cast<uint32_t>(bytes.size());
                buffer.Buffer = bytes.data();
            }

            ByteBuffer bytes;
            QUIC_BUFFER buffer{};
        };

        std::string status_to_string(QUIC_STATUS status)
        {
            switch (status)
            {
            case QUIC_STATUS_BAD_CERTIFICATE:
                return "BAD_CERTIFICATE";
            case QUIC_STATUS_UNSUPPORTED_CERTIFICATE:
                return "UNSUPPORTED_CERTIFICATE";
            case QUIC_STATUS_REVOKED_CERTIFICATE:
                return "REVOKED_CERTIFICATE";
            case QUIC_STATUS_EXPIRED_CERTIFICATE:
                return "EXPIRED_CERTIFICATE";
            case QUIC_STATUS_UNKNOWN_CERTIFICATE:
                return "UNKNOWN_CERTIFICATE";
            case QUIC_STATUS_REQUIRED_CERTIFICATE:
                return "REQUIRED_CERTIFICATE";
            default:
                return QuicStatusToString(status);
            }
        }

        std::string status_message(const char *operation, QUIC_STATUS status)
        {
            std::ostringstream message;
            message << operation << " failed with status " << status_to_string(status) << " (0x" << std::hex << status << ")";
            return message.str();
        }

        ByteBuffer copy_buffers(const QUIC_BUFFER *buffers, uint32_t count)
        {
            ByteBuffer bytes;
            size_t total = 0;
            for (uint32_t index = 0; index < count; ++index)
            {
                total += buffers[index].Length;
            }
            bytes.reserve(total);
            for (uint32_t index = 0; index < count; ++index)
            {
                const uint8_t *begin = buffers[index].Buffer;
                bytes.insert(bytes.end(), begin, begin + buffers[index].Length);
            }
            return bytes;
        }

    } // namespace

    TransportStream::TransportStream(
        MsQuicTransportAdapter &adapter, HQUIC handle, bool unidirectional)
        : adapter_(adapter),
          handle_(handle),
          unidirectional_(unidirectional) {}

    TransportStream::~TransportStream() = default;

    bool TransportStream::unidirectional() const
    {
        return unidirectional_;
    }

    uint64_t TransportStream::id() const
    {
        return id_;
    }

    void TransportStream::set_id(uint64_t id)
    {
        id_ = id;
    }

    void TransportStream::on_bytes(BytesCallback callback)
    {
        bytes_callback_ = std::move(callback);
    }

    void TransportStream::on_peer_send_aborted(ErrorCallback callback)
    {
        peer_send_aborted_callback_ = std::move(callback);
    }

    void TransportStream::on_peer_receive_aborted(ErrorCallback callback)
    {
        peer_receive_aborted_callback_ = std::move(callback);
    }

    void TransportStream::on_shutdown(ShutdownCallback callback)
    {
        shutdown_callback_ = std::move(callback);
    }

    bool TransportStream::send(ByteBuffer bytes, bool fin)
    {
        if (!handle_)
        {
            return false;
        }
        auto *pending = new PendingSend(std::move(bytes));
        const QUIC_SEND_FLAGS flags =
            fin ? QUIC_SEND_FLAG_FIN : QUIC_SEND_FLAG_NONE;
        const QUIC_STATUS status = adapter_.api()->StreamSend(
            handle_, &pending->buffer, 1, flags, pending);
        if (QUIC_FAILED(status))
        {
            delete pending;
            return false;
        }
        return true;
    }

    void TransportStream::abort_receive(uint64_t error_code)
    {
        if (handle_)
        {
            adapter_.api()->StreamShutdown(
                handle_, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE, error_code);
        }
    }

    void TransportStream::abort(uint64_t error_code)
    {
        if (handle_)
        {
            adapter_.api()->StreamShutdown(handle_, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, error_code);
        }
    }

    void TransportStream::set_receive_enabled(bool enabled)
    {
        if (handle_)
        {
            adapter_.api()->StreamReceiveSetEnabled(handle_, enabled ? TRUE : FALSE);
        }
    }

    QUIC_STATUS QUIC_API TransportStream::stream_callback(
        HQUIC stream, void *context, QUIC_STREAM_EVENT *event)
    {
        return static_cast<TransportStream *>(context)->handle_event(stream, event);
    }

    QUIC_STATUS TransportStream::handle_event(HQUIC stream, QUIC_STREAM_EVENT *event)
    {
        switch (event->Type)
        {
        case QUIC_STREAM_EVENT_START_COMPLETE:
        {
            set_id(event->START_COMPLETE.ID);
            if (QUIC_FAILED(event->START_COMPLETE.Status))
            {
                const std::string message =
                    status_message("StreamStart", event->START_COMPLETE.Status);
                adapter_.executor().post([callback = adapter_.callbacks_.transport_error, message]
                                         {
                if (callback) {
                    callback(message);
                } });
            }
            break;
        }
        case QUIC_STREAM_EVENT_RECEIVE:
        {
            ByteBuffer bytes = copy_buffers(event->RECEIVE.Buffers, event->RECEIVE.BufferCount);
            const bool fin = (event->RECEIVE.Flags & QUIC_RECEIVE_FLAG_FIN) != 0;
            const std::shared_ptr<TransportStream> self = shared_from_this();
            adapter_.executor().post([self, bytes = std::move(bytes), fin]() mutable
                                     {
            if (self->bytes_callback_) {
                self->bytes_callback_(std::move(bytes), fin);
            } });
            break;
        }
        case QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN:
        {
            const std::shared_ptr<TransportStream> self = shared_from_this();
            adapter_.executor().post([self]() mutable
                                     {
            if (self->bytes_callback_) {
                self->bytes_callback_(ByteBuffer{}, true);
            } });
            break;
        }
        case QUIC_STREAM_EVENT_SEND_COMPLETE:
            delete static_cast<PendingSend *>(event->SEND_COMPLETE.ClientContext);
            break;
        case QUIC_STREAM_EVENT_PEER_SEND_ABORTED:
        {
            const uint64_t error_code = event->PEER_SEND_ABORTED.ErrorCode;
            const std::shared_ptr<TransportStream> self = shared_from_this();
            adapter_.executor().post([self, error_code]
                                     {
            if (self->peer_send_aborted_callback_) {
                self->peer_send_aborted_callback_(error_code);
            } });
            break;
        }
        case QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED:
        {
            const uint64_t error_code = event->PEER_RECEIVE_ABORTED.ErrorCode;
            const std::shared_ptr<TransportStream> self = shared_from_this();
            adapter_.executor().post([self, error_code]
                                     {
            if (self->peer_receive_aborted_callback_) {
                self->peer_receive_aborted_callback_(error_code);
            } });
            break;
        }
        case QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE:
        {
            const std::shared_ptr<TransportStream> self = shared_from_this();
            close_handle(stream);
            adapter_.executor().post([self]
                                     {
            if (self->shutdown_callback_) {
                self->shutdown_callback_();
            } });
            adapter_.remove_stream(this);
            break;
        }
        default:
            break;
        }
        return QUIC_STATUS_SUCCESS;
    }

    void TransportStream::close_handle(HQUIC stream)
    {
        if (handle_)
        {
            adapter_.api()->StreamClose(stream);
            handle_ = nullptr;
        }
    }

    MsQuicTransportAdapter::MsQuicTransportAdapter(
        Executor &executor, MsQuicClientConfig config, Callbacks callbacks)
        : executor_(executor),
          config_(std::move(config)),
          callbacks_(std::move(callbacks))
    {
        QUIC_STATUS status = MsQuicOpen2(&api_);
        if (QUIC_FAILED(status))
        {
            throw std::runtime_error(status_message("MsQuicOpen2", status));
        }

        QUIC_REGISTRATION_CONFIG registration_config{};
        registration_config.AppName = "moqtopus";
        registration_config.ExecutionProfile = QUIC_EXECUTION_PROFILE_LOW_LATENCY;
        status = api_->RegistrationOpen(&registration_config, &registration_);
        if (QUIC_FAILED(status))
        {
            throw std::runtime_error(status_message("RegistrationOpen", status));
        }

        QUIC_BUFFER alpn{};
        alpn.Length = static_cast<uint32_t>(config_.alpn.size());
        alpn.Buffer = reinterpret_cast<uint8_t *>(config_.alpn.data());

        QUIC_SETTINGS settings{};
        settings.IdleTimeoutMs = static_cast<uint64_t>(config_.idle_timeout.count());
        settings.IsSet.IdleTimeoutMs = TRUE;
        settings.PeerBidiStreamCount = 128;
        settings.IsSet.PeerBidiStreamCount = TRUE;
        settings.PeerUnidiStreamCount = 1024;
        settings.IsSet.PeerUnidiStreamCount = TRUE;
        settings.DatagramReceiveEnabled = TRUE;
        settings.IsSet.DatagramReceiveEnabled = TRUE;

        status = api_->ConfigurationOpen(
            registration_, &alpn, 1, &settings, sizeof(settings), nullptr, &configuration_);
        if (QUIC_FAILED(status))
        {
            throw std::runtime_error(status_message("ConfigurationOpen", status));
        }

        QUIC_CREDENTIAL_CONFIG credential{};
        credential.Type = QUIC_CREDENTIAL_TYPE_NONE;
        credential.Flags = QUIC_CREDENTIAL_FLAG_CLIENT;
        if (config_.disable_certificate_validation)
        {
            credential.Flags |= QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION;
        }
        status = api_->ConfigurationLoadCredential(configuration_, &credential);
        if (QUIC_FAILED(status))
        {
            throw std::runtime_error(status_message("ConfigurationLoadCredential", status));
        }

        status = api_->ConnectionOpen(
            registration_, connection_callback, this, &connection_);
        if (QUIC_FAILED(status))
        {
            throw std::runtime_error(status_message("ConnectionOpen", status));
        }
    }

    MsQuicTransportAdapter::~MsQuicTransportAdapter()
    {
        {
            std::lock_guard<std::mutex> lock(streams_mutex_);
            for (auto &entry : streams_)
            {
                if (entry.second->handle_)
                {
                    api_->StreamClose(entry.second->handle_);
                    entry.second->handle_ = nullptr;
                }
            }
            streams_.clear();
        }
        if (connection_)
        {
            api_->ConnectionClose(connection_);
            connection_ = nullptr;
        }
        if (configuration_)
        {
            api_->ConfigurationClose(configuration_);
            configuration_ = nullptr;
        }
        if (registration_)
        {
            api_->RegistrationClose(registration_);
            registration_ = nullptr;
        }
        if (api_)
        {
            MsQuicClose(api_);
            api_ = nullptr;
        }
    }

    void MsQuicTransportAdapter::start()
    {
        if (started_)
        {
            return;
        }
        const QUIC_STATUS status = api_->ConnectionStart(
            connection_,
            configuration_,
            QUIC_ADDRESS_FAMILY_UNSPEC,
            config_.host.c_str(),
            config_.port);
        if (QUIC_FAILED(status))
        {
            throw std::runtime_error(status_message("ConnectionStart", status));
        }
        started_ = true;
    }

    std::shared_ptr<TransportStream> MsQuicTransportAdapter::open_stream(bool unidirectional)
    {
        HQUIC handle = nullptr;
        const QUIC_STREAM_OPEN_FLAGS open_flags =
            unidirectional ? QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL : QUIC_STREAM_OPEN_FLAG_NONE;
        QUIC_STATUS status = api_->StreamOpen(
            connection_, open_flags, TransportStream::stream_callback, nullptr, &handle);
        if (QUIC_FAILED(status))
        {
            throw std::runtime_error(status_message("StreamOpen", status));
        }

        auto stream = std::shared_ptr<TransportStream>(
            new TransportStream(*this, handle, unidirectional));
        api_->SetCallbackHandler(
            handle,
            reinterpret_cast<void *>(TransportStream::stream_callback),
            stream.get());
        {
            std::lock_guard<std::mutex> lock(streams_mutex_);
            streams_.emplace(stream.get(), stream);
        }
        status = api_->StreamStart(handle, QUIC_STREAM_START_FLAG_IMMEDIATE);
        if (QUIC_FAILED(status))
        {
            remove_stream(stream.get());
            api_->StreamClose(handle);
            stream->handle_ = nullptr;
            throw std::runtime_error(status_message("StreamStart", status));
        }
        return stream;
    }

    void MsQuicTransportAdapter::shutdown(moq::SessionCloseErrorCode error_code)
    {
        if (connection_)
        {
            api_->ConnectionShutdown(
                connection_, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE,
                static_cast<uint64_t>(error_code));
        }
    }

    const QUIC_API_TABLE *MsQuicTransportAdapter::api() const
    {
        return api_;
    }

    Executor &MsQuicTransportAdapter::executor()
    {
        return executor_;
    }

    QUIC_STATUS QUIC_API MsQuicTransportAdapter::connection_callback(
        HQUIC connection, void *context, QUIC_CONNECTION_EVENT *event)
    {
        return static_cast<MsQuicTransportAdapter *>(context)
            ->handle_connection_event(connection, event);
    }

    QUIC_STATUS MsQuicTransportAdapter::handle_connection_event(
        HQUIC connection, QUIC_CONNECTION_EVENT *event)
    {
        switch (event->Type)
        {
        case QUIC_CONNECTION_EVENT_CONNECTED:
        {
            executor_.post([callback = callbacks_.connected]
                           {
            if (callback) {
                callback();
            } });
            break;
        }
        case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED:
        {
            const bool unidirectional =
                (event->PEER_STREAM_STARTED.Flags & QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL) != 0;
            auto stream = std::shared_ptr<TransportStream>(
                new TransportStream(*this, event->PEER_STREAM_STARTED.Stream, unidirectional));
            api_->SetCallbackHandler(
                event->PEER_STREAM_STARTED.Stream,
                reinterpret_cast<void *>(TransportStream::stream_callback),
                stream.get());
            {
                std::lock_guard<std::mutex> lock(streams_mutex_);
                streams_.emplace(stream.get(), stream);
            }
            executor_.post([callback = callbacks_.peer_stream_started, stream]
                           {
            if (callback) {
                callback(stream);
            } });
            break;
        }
        case QUIC_CONNECTION_EVENT_DATAGRAM_RECEIVED:
        {
            ByteBuffer bytes(
                event->DATAGRAM_RECEIVED.Buffer->Buffer,
                event->DATAGRAM_RECEIVED.Buffer->Buffer +
                    event->DATAGRAM_RECEIVED.Buffer->Length);
            executor_.post([callback = callbacks_.datagram_received, bytes = std::move(bytes)]() mutable
                           {
            if (callback) {
                callback(std::move(bytes));
            } });
            break;
        }
        case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT:
        {
            const std::string message = "Connection shutdown initiated by transport: " + status_to_string(event->SHUTDOWN_INITIATED_BY_TRANSPORT.ErrorCode);
            executor_.post([callback = callbacks_.transport_error, message]
                           {
            if (callback) {
                callback(message);
            } });
            break;
        }
        case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER:
        {
            const std::string message = "peer shutdown: " + status_to_string(event->SHUTDOWN_INITIATED_BY_PEER.ErrorCode);
            executor_.post([callback = callbacks_.transport_error, message]
                           {
            if (callback) {
                callback(message);
            } });
            break;
        }
        case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE:
        {
            const bool handshake_completed = event->SHUTDOWN_COMPLETE.HandshakeCompleted != FALSE;
            close_connection_handle(connection);
            executor_.post([callback = callbacks_.shutdown_complete, handshake_completed]
                           {
            if (callback) {
                callback(handshake_completed);
            } });
            break;
        }
        default:
            break;
        }
        return QUIC_STATUS_SUCCESS;
    }

    void MsQuicTransportAdapter::remove_stream(TransportStream *stream)
    {
        std::lock_guard<std::mutex> lock(streams_mutex_);
        streams_.erase(stream);
    }

    void MsQuicTransportAdapter::close_connection_handle(HQUIC connection)
    {
        if (connection_)
        {
            api_->ConnectionClose(connection);
            connection_ = nullptr;
        }
    }

} // namespace moq::detail
