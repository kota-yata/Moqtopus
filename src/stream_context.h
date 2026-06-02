#pragma once

#include "moq/types.h"

#include <msquic.h>

#include <cstdint>
#include <functional>
#include <memory>

namespace moq::detail {

class MsQuicTransportAdapter;

class StreamContext : public std::enable_shared_from_this<StreamContext> {
public:
  using BytesCallback = std::function<void(ByteBuffer, bool)>;
  using ErrorCallback = std::function<void(uint64_t)>;
  using ShutdownCallback = std::function<void()>;

  ~StreamContext();

  StreamContext(const StreamContext &) = delete;
  StreamContext &operator=(const StreamContext &) = delete;

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

  StreamContext(MsQuicTransportAdapter &adapter, HQUIC handle, bool unidirectional);
  static QUIC_STATUS QUIC_API stream_callback(HQUIC stream, void *context, QUIC_STREAM_EVENT *event);
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

} // namespace moq::detail
