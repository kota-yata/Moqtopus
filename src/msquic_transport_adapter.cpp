#include "msquic_transport_adapter.h"

#include <spdlog/spdlog.h>
#include <stdexcept>
#include <utility>

#include <msquichelper.h>

namespace moq::detail {
namespace {

std::string status_to_string(QUIC_STATUS status) {
  switch (status) {
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

} // namespace

MsQuicTransportAdapter::MsQuicTransportAdapter(MsQuicClientConfig config, Callbacks callbacks)
    : config_(std::move(config)), callbacks_(std::move(callbacks)) {
  QUIC_STATUS status = MsQuicOpen2(&api_);
  if (QUIC_FAILED(status)) {
    throw std::runtime_error(std::string("MsQuicOpen2 failed: ") + status_to_string(status));
  }

  QUIC_REGISTRATION_CONFIG registration_config{};
  registration_config.AppName = "moqtopus";
  registration_config.ExecutionProfile = QUIC_EXECUTION_PROFILE_LOW_LATENCY;
  status = api_->RegistrationOpen(&registration_config, &registration_);
  if (QUIC_FAILED(status)) {
    throw std::runtime_error(std::string("RegistrationOpen failed: ") + status_to_string(status));
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

  status = api_->ConfigurationOpen(registration_, &alpn, 1, &settings, sizeof(settings), nullptr, &configuration_);
  if (QUIC_FAILED(status)) {
    throw std::runtime_error(std::string("ConfigurationOpen failed: ") + status_to_string(status));
  }

  QUIC_CREDENTIAL_CONFIG credential{};
  credential.Type = QUIC_CREDENTIAL_TYPE_NONE;
  credential.Flags = QUIC_CREDENTIAL_FLAG_CLIENT;
  if (config_.disable_certificate_validation) {
    credential.Flags |= QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION;
  }
  status = api_->ConfigurationLoadCredential(configuration_, &credential);
  if (QUIC_FAILED(status)) {
    throw std::runtime_error(std::string("ConfigurationLoadCredential failed: ") + status_to_string(status));
  }

  status = api_->ConnectionOpen(registration_, connection_callback, this, &connection_);
  if (QUIC_FAILED(status)) {
    throw std::runtime_error(std::string("ConnectionOpen failed: ") + status_to_string(status));
  }
}

MsQuicTransportAdapter::~MsQuicTransportAdapter() {
  {
    std::unique_lock<std::mutex> lock(shutdown_mutex_);
    if (shutdown_requested_) {
      shutdown_cv_.wait_for(lock, config_.idle_timeout, [this] { return shutdown_complete_; });
    }
  }

  std::unordered_map<StreamContext *, std::shared_ptr<StreamContext>> streams;
  {
    std::lock_guard<std::mutex> lock(streams_mutex_);
    streams.swap(streams_);
  }
  // StreamClose may synchronously invoke a shutdown callback that calls
  // remove_stream(), so it must run without streams_mutex_ held.
  for (auto &entry : streams) {
    if (entry.second->handle_) {
      api_->StreamClose(entry.second->handle_);
      entry.second->handle_ = nullptr;
    }
  }
  if (connection_) {
    api_->ConnectionClose(connection_);
    connection_ = nullptr;
  }
  if (configuration_) {
    api_->ConfigurationClose(configuration_);
    configuration_ = nullptr;
  }
  if (registration_) {
    api_->RegistrationClose(registration_);
    registration_ = nullptr;
  }
  if (api_) {
    MsQuicClose(api_);
    api_ = nullptr;
  }
}

void MsQuicTransportAdapter::start() {
  if (started_) {
    return;
  }
  const QUIC_STATUS status = api_->ConnectionStart(connection_, configuration_, QUIC_ADDRESS_FAMILY_UNSPEC,
                                                   config_.host.c_str(), config_.port);
  if (QUIC_FAILED(status)) {
    throw std::runtime_error(std::string("ConnectionStart failed: ") + status_to_string(status));
  }
  started_ = true;
}

std::shared_ptr<StreamContext> MsQuicTransportAdapter::open_stream(bool unidirectional) {
  HQUIC handle = nullptr;
  const QUIC_STREAM_OPEN_FLAGS open_flags =
      unidirectional ? QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL : QUIC_STREAM_OPEN_FLAG_NONE;
  QUIC_STATUS status = api_->StreamOpen(connection_, open_flags, StreamContext::stream_callback, nullptr, &handle);
  if (QUIC_FAILED(status)) {
    throw std::runtime_error(std::string("StreamOpen failed: ") + status_to_string(status));
  }

  auto stream = std::shared_ptr<StreamContext>(new StreamContext(*this, handle, unidirectional));
  api_->SetCallbackHandler(handle, reinterpret_cast<void *>(StreamContext::stream_callback), stream.get());
  {
    std::lock_guard<std::mutex> lock(streams_mutex_);
    streams_.emplace(stream.get(), stream);
  }
  status = api_->StreamStart(handle, QUIC_STREAM_START_FLAG_IMMEDIATE);
  if (QUIC_FAILED(status)) {
    remove_stream(stream.get());
    api_->StreamClose(handle);
    stream->handle_ = nullptr;
    throw std::runtime_error(std::string("StreamStart failed: ") + status_to_string(status));
  }
  return stream;
}

void MsQuicTransportAdapter::shutdown(moq::SessionCloseErrorCode error_code) {
  if (connection_) {
    spdlog::debug("MsQuicTransportAdapter shutdown requested: code={}", static_cast<uint64_t>(error_code));
    {
      std::lock_guard<std::mutex> lock(shutdown_mutex_);
      shutdown_requested_ = true;
    }
    api_->ConnectionShutdown(connection_, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, static_cast<uint64_t>(error_code));
  }
}

const QUIC_API_TABLE *MsQuicTransportAdapter::api() const { return api_; }

QUIC_STATUS QUIC_API MsQuicTransportAdapter::connection_callback(HQUIC connection, void *context,
                                                                 QUIC_CONNECTION_EVENT *event) {
  return static_cast<MsQuicTransportAdapter *>(context)->handle_connection_event(connection, event);
}

QUIC_STATUS MsQuicTransportAdapter::handle_connection_event(HQUIC connection, QUIC_CONNECTION_EVENT *event) {
  switch (event->Type) {
  case QUIC_CONNECTION_EVENT_CONNECTED: {
    callbacks_.connected();
    break;
  }
  case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED: {
    const bool unidirectional = (event->PEER_STREAM_STARTED.Flags & QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL) != 0;
    auto stream =
        std::shared_ptr<StreamContext>(new StreamContext(*this, event->PEER_STREAM_STARTED.Stream, unidirectional));
    stream->set_id(GetStreamID(api_, event->PEER_STREAM_STARTED.Stream));
    api_->SetCallbackHandler(event->PEER_STREAM_STARTED.Stream,
                             reinterpret_cast<void *>(StreamContext::stream_callback), stream.get());
    {
      std::lock_guard<std::mutex> lock(streams_mutex_);
      streams_.emplace(stream.get(), stream);
    }
    callbacks_.peer_stream_started(stream);
    break;
  }
  case QUIC_CONNECTION_EVENT_DATAGRAM_RECEIVED: {
    ByteBuffer bytes(event->DATAGRAM_RECEIVED.Buffer->Buffer,
                     event->DATAGRAM_RECEIVED.Buffer->Buffer + event->DATAGRAM_RECEIVED.Buffer->Length);
    callbacks_.datagram_received(std::move(bytes));
    break;
  }
  case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT: {
    const QUIC_STATUS status = event->SHUTDOWN_INITIATED_BY_TRANSPORT.Status;
    const uint64_t error_code = event->SHUTDOWN_INITIATED_BY_TRANSPORT.ErrorCode;
    const std::string message = "Connection shutdown initiated by transport: status=" + status_to_string(status) +
                                " (" + std::to_string(status) + "), error_code=" + std::to_string(error_code);
    callbacks_.transport_error(message);
    break;
  }
  case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER: {
    const std::string message =
        "peer shutdown: error_code=" + std::to_string(event->SHUTDOWN_INITIATED_BY_PEER.ErrorCode);
    callbacks_.transport_error(message);
    break;
  }
  case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE: {
    const bool handshake_completed = event->SHUTDOWN_COMPLETE.HandshakeCompleted != FALSE;
    spdlog::debug("MsQuic connection shutdown complete: handshake_completed={}", handshake_completed);
    callbacks_.shutdown_complete(handshake_completed);
    {
      std::lock_guard<std::mutex> lock(shutdown_mutex_);
      shutdown_complete_ = true;
    }
    shutdown_cv_.notify_all();
    break;
  }
  default:
    break;
  }
  return QUIC_STATUS_SUCCESS;
}

void MsQuicTransportAdapter::remove_stream(StreamContext *stream) {
  std::lock_guard<std::mutex> lock(streams_mutex_);
  streams_.erase(stream);
}

void MsQuicTransportAdapter::close_connection_handle(HQUIC connection) {
  if (connection_) {
    api_->ConnectionClose(connection);
    connection_ = nullptr;
  }
}

} // namespace moq::detail
