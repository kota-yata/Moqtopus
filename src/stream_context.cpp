#include "stream_context.h"

#include "msquic_transport_adapter.h"

#include <msquichelper.h>

#include <string>
#include <utility>

namespace moq::detail {
namespace {

struct PendingSend {
  explicit PendingSend(ByteBuffer input) : bytes(std::move(input)) {
    buffer.Length = static_cast<uint32_t>(bytes.size());
    buffer.Buffer = bytes.data();
  }

  ByteBuffer bytes;
  QUIC_BUFFER buffer{};
};

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

ByteBuffer copy_buffers(const QUIC_BUFFER *buffers, uint32_t count) {
  ByteBuffer bytes;
  size_t total = 0;
  for (uint32_t index = 0; index < count; ++index) {
    total += buffers[index].Length;
  }
  bytes.reserve(total);
  for (uint32_t index = 0; index < count; ++index) {
    const uint8_t *begin = buffers[index].Buffer;
    bytes.insert(bytes.end(), begin, begin + buffers[index].Length);
  }
  return bytes;
}

} // namespace

StreamContext::StreamContext(MsQuicTransportAdapter &adapter, HQUIC handle, bool unidirectional)
    : adapter_(adapter), handle_(handle), unidirectional_(unidirectional) {}

StreamContext::~StreamContext() = default;

bool StreamContext::unidirectional() const { return unidirectional_; }

uint64_t StreamContext::id() const { return id_; }

void StreamContext::set_id(uint64_t id) { id_ = id; }

void StreamContext::on_bytes(BytesCallback callback) { bytes_callback_ = std::move(callback); }

void StreamContext::on_peer_send_aborted(ErrorCallback callback) {
  peer_send_aborted_callback_ = std::move(callback);
}

void StreamContext::on_peer_receive_aborted(ErrorCallback callback) {
  peer_receive_aborted_callback_ = std::move(callback);
}

void StreamContext::on_shutdown(ShutdownCallback callback) { shutdown_callback_ = std::move(callback); }

bool StreamContext::send(ByteBuffer bytes, bool fin) {
  if (!handle_) {
    return false;
  }
  auto *pending = new PendingSend(std::move(bytes));
  const QUIC_SEND_FLAGS flags = fin ? QUIC_SEND_FLAG_FIN : QUIC_SEND_FLAG_NONE;
  const QUIC_STATUS status = adapter_.api()->StreamSend(handle_, &pending->buffer, 1, flags, pending);
  if (QUIC_FAILED(status)) {
    delete pending;
    return false;
  }
  return true;
}

void StreamContext::abort_receive(uint64_t error_code) {
  if (handle_) {
    adapter_.api()->StreamShutdown(handle_, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE, error_code);
  }
}

void StreamContext::abort(uint64_t error_code) {
  if (handle_) {
    adapter_.api()->StreamShutdown(handle_, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, error_code);
  }
}

void StreamContext::set_receive_enabled(bool enabled) {
  if (handle_) {
    adapter_.api()->StreamReceiveSetEnabled(handle_, enabled ? TRUE : FALSE);
  }
}

QUIC_STATUS QUIC_API StreamContext::stream_callback(HQUIC stream, void *context, QUIC_STREAM_EVENT *event) {
  return static_cast<StreamContext *>(context)->handle_event(stream, event);
}

QUIC_STATUS StreamContext::handle_event(HQUIC stream, QUIC_STREAM_EVENT *event) {
  switch (event->Type) {
  case QUIC_STREAM_EVENT_START_COMPLETE: {
    set_id(event->START_COMPLETE.ID);
    if (QUIC_FAILED(event->START_COMPLETE.Status)) {
      const std::string message = std::string("StreamStart failed: ") + status_to_string(event->START_COMPLETE.Status);
      adapter_.callbacks_.transport_error(message);
    }
    break;
  }
  case QUIC_STREAM_EVENT_RECEIVE: {
    ByteBuffer bytes = copy_buffers(event->RECEIVE.Buffers, event->RECEIVE.BufferCount);
    const bool fin = (event->RECEIVE.Flags & QUIC_RECEIVE_FLAG_FIN) != 0;
    bytes_callback_(std::move(bytes), fin);
    break;
  }
  case QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN: {
    bytes_callback_(ByteBuffer{}, true);
    break;
  }
  case QUIC_STREAM_EVENT_SEND_COMPLETE:
    delete static_cast<PendingSend *>(event->SEND_COMPLETE.ClientContext);
    break;
  case QUIC_STREAM_EVENT_PEER_SEND_ABORTED: {
    const uint64_t error_code = event->PEER_SEND_ABORTED.ErrorCode;
    peer_send_aborted_callback_(error_code);
    break;
  }
  case QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED: {
    const uint64_t error_code = event->PEER_RECEIVE_ABORTED.ErrorCode;
    peer_receive_aborted_callback_(error_code);
    break;
  }
  case QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE: {
    close_handle(stream);
    shutdown_callback_();
    adapter_.remove_stream(this);
    break;
  }
  default:
    break;
  }
  return QUIC_STATUS_SUCCESS;
}

void StreamContext::close_handle(HQUIC stream) {
  if (handle_) {
    adapter_.api()->StreamClose(stream);
    handle_ = nullptr;
  }
}

} // namespace moq::detail
