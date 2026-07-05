#pragma once

#include "moq/types.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace moq::codec {

constexpr uint64_t kSetupStreamType = 0x2f00;
constexpr uint64_t kFetchStreamType = 0x05;
constexpr uint64_t kPaddingStreamType = 0x132b3e28;
constexpr uint64_t kPaddingDatagramType = 0x132b3e29;

constexpr uint64_t kMessageRequestUpdate = 0x02;
constexpr uint64_t kMessageSubscribe = 0x03;
constexpr uint64_t kMessageSubscribeOk = 0x04;
constexpr uint64_t kMessageRequestError = 0x05;
constexpr uint64_t kMessageRequestOk = 0x07;
constexpr uint64_t kMessagePublishDone = 0x0b;
constexpr uint64_t kMessageGoAway = 0x10;
constexpr uint64_t kMessagePublish = 0x1d;
constexpr uint64_t kMessageSetup = 0x2f00;
constexpr uint64_t kMessagePublishNamespace = 0x06;
constexpr uint64_t kMessageTrackStatus = 0x0d;
constexpr uint64_t kMessageFetch = 0x16;
constexpr uint64_t kMessageSubscribeNamespace = 0x50;
constexpr uint64_t kMessageSubscribeTracks = 0x51;

// Setup option identifiers
enum class SetupOption : uint64_t {
  // Setup option = None does not exist on the wire;
  // it is only used as a placeholder for "no previous option"
  None = 0x00,
  Path = 0x01,
  Authority = 0x05,
  MoqtImplementation = 0x07,
};

enum class DecodeStatus {
  Done,
  NeedMoreData,
  Error,
};

struct VarintResult {
  DecodeStatus status = DecodeStatus::NeedMoreData;
  uint64_t value = 0;
  size_t bytes = 0;
};

struct ControlMessage {
  uint64_t type = 0;
  ByteBuffer payload;
};

struct ControlMessageResult {
  DecodeStatus status = DecodeStatus::NeedMoreData;
  ControlMessage message;
  size_t bytes = 0;
};

struct SubscribeOk {
  TrackAlias track_alias = 0;
  std::vector<Parameter> parameters;
  ObjectProperties track_properties;
};

void write_varint(ByteBuffer &out, uint64_t value);
VarintResult read_varint(const uint8_t *data, size_t size, size_t offset = 0);
inline VarintResult read_varint(const ByteBuffer &bytes, size_t offset = 0) {
  return read_varint(bytes.data(), bytes.size(), offset);
}
void append_control_message(ByteBuffer &out, uint64_t type, const ByteBuffer &payload);
ControlMessageResult read_control_message(const ByteBuffer &bytes, size_t offset = 0);

ByteBuffer encode_setup(std::string authority, std::string path);
// The subscriber ignores peer SETUP contents; decoding only validates the encoding.
bool decode_setup(const ByteBuffer &payload, std::string &error);
ByteBuffer encode_subscribe(RequestId request_id, const SubscribeRequest &request);
ByteBuffer encode_request_update(RequestId request_id, const RequestUpdate &update);
ByteBuffer encode_request_error(RequestErrorCode code, std::string reason, uint64_t retry_interval = 0);

std::optional<SubscribeOk> decode_subscribe_ok(const ByteBuffer &payload, std::string &error);
std::optional<RequestOk> decode_request_ok(const ByteBuffer &payload, std::string &error);
std::optional<RequestError> decode_request_error(const ByteBuffer &payload, std::string &error);
std::optional<PublishDone> decode_publish_done(const ByteBuffer &payload, std::string &error);

void write_track_namespace(ByteBuffer &out, const TrackNamespace &name_space);
bool is_subgroup_stream_type(uint64_t type);

} // namespace moq::codec
