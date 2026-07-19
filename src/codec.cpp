#include "moq/codec.h"

#include "moq/errors.h"

#include <algorithm>
#include <limits>
#include <optional>
#include <stdexcept>
#include <utility>

namespace moq {

Parameter Parameter::uint8(uint64_t type, uint8_t value) { return Parameter{type, ByteBuffer{value}}; }

Parameter Parameter::varint(uint64_t type, uint64_t value) {
  Parameter parameter{type, {}};
  codec::write_varint(parameter.encoded_value, value);
  return parameter;
}

Parameter Parameter::location(uint64_t type, Location value) {
  Parameter parameter{type, {}};
  codec::write_varint(parameter.encoded_value, value.group);
  codec::write_varint(parameter.encoded_value, value.object);
  return parameter;
}

Parameter Parameter::length_prefixed(uint64_t type, ByteBuffer value) {
  Parameter parameter{type, {}};
  codec::write_varint(parameter.encoded_value, value.size());
  parameter.encoded_value.insert(parameter.encoded_value.end(), value.begin(), value.end());
  return parameter;
}

Parameter Parameter::track_namespace(uint64_t type, TrackNamespace value) {
  Parameter parameter{type, {}};
  codec::write_track_namespace(parameter.encoded_value, value);
  return parameter;
}

RequestRejected::RequestRejected(RequestErrorCode code, uint64_t retry_interval, std::string reason)
    : std::runtime_error("MOQT request rejected: code=" + std::to_string(static_cast<uint64_t>(code)) +
                         (reason.empty() ? "" : " reason=" + reason)),
      code_(code), retry_interval_(retry_interval), reason_(std::move(reason)) {}

} // namespace moq

namespace moq::codec {
namespace {

// Length n encodes n-1 leading one bits, leaving 8-n value bits in the first byte.
size_t varint_length(uint64_t value) {
  size_t length = 1;
  for (uint64_t max = 0x7f; length < 9 && value > max; max = (max << 7) | 0x7f) {
    ++length;
  }
  return length;
}

uint8_t varint_value_bits(size_t length) { return length == 9 ? 0 : static_cast<uint8_t>(8 - length); }

void append_u16(ByteBuffer &out, uint16_t value) {
  out.push_back(static_cast<uint8_t>((value >> 8) & 0xff));
  out.push_back(static_cast<uint8_t>(value & 0xff));
}

struct Cursor {
  const ByteBuffer &bytes;
  size_t offset = 0;

  size_t remaining() const { return bytes.size() - offset; }

  bool read_byte(uint8_t &value) {
    if (offset >= bytes.size()) {
      return false;
    }
    value = bytes[offset++];
    return true;
  }

  bool read_varint(uint64_t &value) {
    const VarintResult parsed = codec::read_varint(bytes, offset);
    if (parsed.status != DecodeStatus::Done) {
      return false;
    }
    offset += parsed.bytes;
    value = parsed.value;
    return true;
  }

  bool read_bytes(size_t size, ByteBuffer &value) {
    if (size > remaining()) {
      return false;
    }
    value.assign(bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                 bytes.begin() + static_cast<std::ptrdiff_t>(offset + size));
    offset += size;
    return true;
  }

  bool read_string(size_t size, std::string &value) {
    if (size > remaining()) {
      return false;
    }
    value.assign(reinterpret_cast<const char *>(bytes.data() + offset), size);
    offset += size;
    return true;
  }
};

void append_bytes(ByteBuffer &out, const std::string &value) { out.insert(out.end(), value.begin(), value.end()); }

bool read_reason(Cursor &cursor, std::string &reason) {
  uint64_t size = 0;
  if (!cursor.read_varint(size) || size > 1024) {
    return false;
  }
  return cursor.read_string(static_cast<size_t>(size), reason);
}

bool read_track_namespace(Cursor &cursor, TrackNamespace *result = nullptr) {
  uint64_t fields = 0;
  if (!cursor.read_varint(fields) || fields > 32) {
    return false;
  }
  TrackNamespace decoded;
  size_t total = 0;
  for (uint64_t index = 0; index < fields; ++index) {
    uint64_t size = 0;
    std::string field;
    if (!cursor.read_varint(size) || size == 0 || size > cursor.remaining() || size > 4096 || total + size > 4096 ||
        !cursor.read_string(static_cast<size_t>(size), field)) {
      return false;
    }
    total += static_cast<size_t>(size);
    decoded.push_back(std::move(field));
  }
  if (result) {
    *result = std::move(decoded);
  }
  return true;
}

enum class ParameterEncoding {
  Uint8,
  Varint,
  Location,
  LengthPrefixed,
  TrackNamespace,
};

std::optional<ParameterEncoding> parameter_encoding(uint64_t type) {
  switch (type) {
  case 0x02:
  case 0x04:
  case 0x06:
  case 0x08:
  case 0x0a:
  case 0x32:
    return ParameterEncoding::Varint;
  case 0x03:
  case 0x21:
    return ParameterEncoding::LengthPrefixed;
  case 0x09:
    return ParameterEncoding::Location;
  case 0x10:
  case 0x20:
  case 0x22:
    return ParameterEncoding::Uint8;
  case 0x34:
    return ParameterEncoding::TrackNamespace;
  default:
    return std::nullopt;
  }
}

// skips one parameter value of the given encoding; returns false on truncation
bool skip_parameter_value(Cursor &cursor, ParameterEncoding encoding) {
  uint8_t byte = 0;
  uint64_t first = 0;
  uint64_t second = 0;
  ByteBuffer bytes;
  switch (encoding) {
  case ParameterEncoding::Uint8:
    return cursor.read_byte(byte);
  case ParameterEncoding::Varint:
    return cursor.read_varint(first);
  case ParameterEncoding::Location:
    return cursor.read_varint(first) && cursor.read_varint(second);
  case ParameterEncoding::LengthPrefixed:
    return cursor.read_varint(first) && first <= 65535 && cursor.read_bytes(static_cast<size_t>(first), bytes);
  case ParameterEncoding::TrackNamespace:
    return read_track_namespace(cursor);
  }
  return false;
}

bool read_parameters(Cursor &cursor, uint64_t count, std::vector<Parameter> &parameters, std::string &error) {
  uint64_t previous_type = 0;
  bool have_previous = false;
  for (uint64_t index = 0; index < count; ++index) {
    uint64_t delta = 0;
    if (!cursor.read_varint(delta) || (have_previous && delta > std::numeric_limits<uint64_t>::max() - previous_type)) {
      error = "invalid message parameter type delta";
      return false;
    }
    const uint64_t type = have_previous ? previous_type + delta : delta;
    if (have_previous && type <= previous_type) {
      error = "message parameters are not strictly ascending";
      return false;
    }
    previous_type = type;
    have_previous = true;

    const std::optional<ParameterEncoding> encoding = parameter_encoding(type);
    if (!encoding) {
      error = "unknown message parameter " + std::to_string(type);
      return false;
    }
    const size_t value_start = cursor.offset;
    if (!skip_parameter_value(cursor, *encoding)) {
      error = "truncated message parameter " + std::to_string(type);
      return false;
    }

    Parameter parameter;
    parameter.type = type;
    parameter.encoded_value.assign(cursor.bytes.begin() + static_cast<std::ptrdiff_t>(value_start),
                                   cursor.bytes.begin() + static_cast<std::ptrdiff_t>(cursor.offset));
    parameters.push_back(std::move(parameter));
  }
  return true;
}

// reads "param count + params" and treats the rest of the payload as track properties
bool read_parameters_and_properties(Cursor &cursor, std::vector<Parameter> &parameters, ObjectProperties &properties,
                                    const char *what, std::string &error) {
  uint64_t parameter_count = 0;
  if (!cursor.read_varint(parameter_count) || !read_parameters(cursor, parameter_count, parameters, error)) {
    if (error.empty()) {
      error = std::string("invalid ") + what;
    }
    return false;
  }
  properties.assign(cursor.bytes.begin() + static_cast<std::ptrdiff_t>(cursor.offset), cursor.bytes.end());
  return true;
}

bool encode_parameters(ByteBuffer &payload, std::vector<Parameter> parameters) {
  std::stable_sort(parameters.begin(), parameters.end(),
                   [](const Parameter &left, const Parameter &right) { return left.type < right.type; });
  write_varint(payload, parameters.size());
  uint64_t previous = 0;
  bool have_previous = false;
  for (const Parameter &parameter : parameters) {
    if ((have_previous && parameter.type <= previous) || !parameter_encoding(parameter.type)) {
      return false;
    }
    write_varint(payload, have_previous ? parameter.type - previous : parameter.type);
    payload.insert(payload.end(), parameter.encoded_value.begin(), parameter.encoded_value.end());
    previous = parameter.type;
    have_previous = true;
  }
  return true;
}

void append_setup_option_bytes(ByteBuffer &payload, SetupOption type, SetupOption previous_type,
                               const std::string &value) {
  write_varint(payload, static_cast<uint64_t>(type) - static_cast<uint64_t>(previous_type)); // option number delta
  write_varint(payload, value.size());
  append_bytes(payload, value);
}

} // namespace

void write_varint(ByteBuffer &out, uint64_t value) {
  const size_t length = varint_length(value);
  if (length == 9) {
    out.push_back(0xff);
    for (int shift = 56; shift >= 0; shift -= 8) {
      out.push_back(static_cast<uint8_t>((value >> shift) & 0xff));
    }
    return;
  }

  const uint8_t usable_first_bits = varint_value_bits(length);
  const uint64_t first_mask = usable_first_bits == 0 ? 0 : ((uint64_t{1} << usable_first_bits) - 1);
  const uint8_t prefix = length == 1 ? 0 : static_cast<uint8_t>(0xff << (9 - length));
  const int following_bytes = static_cast<int>(length - 1);
  out.push_back(static_cast<uint8_t>(prefix | ((value >> (following_bytes * 8)) & first_mask)));
  for (int index = following_bytes - 1; index >= 0; --index) {
    out.push_back(static_cast<uint8_t>((value >> (index * 8)) & 0xff));
  }
}

VarintResult read_varint(const uint8_t *data, size_t size, size_t offset) {
  if (offset >= size) {
    return {};
  }

  const uint8_t first = data[offset];
  size_t leading_ones = 0;
  while (leading_ones < 8 && (first & (0x80 >> leading_ones)) != 0) {
    ++leading_ones;
  }
  const size_t length = leading_ones == 8 ? 9 : leading_ones + 1;
  if (length > size - offset) {
    return {};
  }

  uint64_t value = 0;
  if (length == 9) {
    for (size_t index = 1; index < 9; ++index) {
      value = (value << 8) | data[offset + index];
    }
  } else {
    const uint8_t first_bits = varint_value_bits(length);
    value = first_bits == 0 ? 0 : first & ((uint8_t{1} << first_bits) - 1);
    for (size_t index = 1; index < length; ++index) {
      value = (value << 8) | data[offset + index];
    }
  }
  return VarintResult{DecodeStatus::Done, value, length};
}

void append_control_message(ByteBuffer &out, uint64_t type, const ByteBuffer &payload) {
  if (payload.size() > std::numeric_limits<uint16_t>::max()) {
    throw std::length_error("MOQT control message payload exceeds 65535 bytes");
  }
  write_varint(out, type);
  append_u16(out, static_cast<uint16_t>(payload.size()));
  out.insert(out.end(), payload.begin(), payload.end());
}

ControlMessageResult read_control_message(const ByteBuffer &bytes, size_t offset) {
  const VarintResult type = read_varint(bytes, offset);
  if (type.status != DecodeStatus::Done || bytes.size() - offset < type.bytes + 2) {
    return {};
  }
  const size_t length_offset = offset + type.bytes;
  const uint16_t length =
      static_cast<uint16_t>((static_cast<uint16_t>(bytes[length_offset]) << 8) | bytes[length_offset + 1]);
  const size_t frame_size = type.bytes + 2 + length;
  if (bytes.size() - offset < frame_size) {
    return {};
  }

  ControlMessage message;
  message.type = type.value;
  message.payload.assign(bytes.begin() + (length_offset + 2), bytes.begin() + (length_offset + 2 + length));
  return ControlMessageResult{DecodeStatus::Done, std::move(message), frame_size};
}

void write_track_namespace(ByteBuffer &out, const TrackNamespace &name_space) {
  if (name_space.size() > 32) {
    throw std::invalid_argument("MOQT track namespace has more than 32 fields");
  }
  write_varint(out, name_space.size());
  size_t total_size = 0;
  for (const std::string &field : name_space) {
    if (field.empty() || total_size + field.size() > 4096) {
      throw std::invalid_argument("invalid MOQT track namespace field");
    }
    write_varint(out, field.size());
    append_bytes(out, field);
    total_size += field.size();
  }
}

// Subgroup header is not a single fixed value
bool is_subgroup_stream_type(uint64_t type) {
  if ((type & 0x10) == 0 || type > 0x7f) {
    return false;
  }
  return ((type & 0x06) >> 1) != 0x03;
}

ByteBuffer encode_setup(std::string authority, std::string path) {
  ByteBuffer payload;
  SetupOption previous = SetupOption::None;
  if (!path.empty()) {
    append_setup_option_bytes(payload, SetupOption::Path, previous, path);
    previous = SetupOption::Path;
  }
  if (!authority.empty()) {
    append_setup_option_bytes(payload, SetupOption::Authority, previous, authority);
    previous = SetupOption::Authority;
  }
  // Fixed value for MOQT_IMPLEMENTATION
  append_setup_option_bytes(payload, SetupOption::MoqtImplementation, previous, "kota-moqtopus");

  ByteBuffer stream_bytes;
  append_control_message(stream_bytes, kMessageSetup, payload);
  return stream_bytes;
}

bool decode_setup(const ByteBuffer &payload, std::string &error) {
  Cursor cursor{payload};
  uint64_t last_type = 0;
  bool has_type = false;
  while (cursor.remaining() != 0) {
    uint64_t delta = 0;
    if (!cursor.read_varint(delta) || (has_type && delta > std::numeric_limits<uint64_t>::max() - last_type)) {
      error = "invalid SETUP option type delta";
      return false;
    }
    last_type = has_type ? last_type + delta : delta;
    has_type = true;

    if ((last_type & 1U) == 0) { // even-numbered options carry a bare varint
      uint64_t value = 0;
      if (!cursor.read_varint(value)) {
        error = "truncated SETUP varint option";
        return false;
      }
      continue;
    }
    uint64_t size = 0;
    ByteBuffer value;
    if (!cursor.read_varint(size) || size > 65535 || !cursor.read_bytes(static_cast<size_t>(size), value)) {
      error = "truncated SETUP length-prefixed option";
      return false;
    }
  }
  return true;
}

ByteBuffer encode_subscribe(RequestId request_id, const SubscribeRequest &request) {
  ByteBuffer payload;
  write_varint(payload, request_id);
  write_track_namespace(payload, request.track_namespace);
  write_varint(payload, request.track_name.size());
  append_bytes(payload, request.track_name);
  if (!encode_parameters(payload, request.parameters)) {
    throw std::invalid_argument("SUBSCRIBE includes an unsupported or duplicate parameter");
  }

  ByteBuffer framed;
  append_control_message(framed, kMessageSubscribe, payload);
  return framed;
}

std::optional<SubscribeOk> decode_subscribe_ok(const ByteBuffer &payload, std::string &error) {
  Cursor cursor{payload};
  SubscribeOk ok;
  if (!cursor.read_varint(ok.track_alias) ||
      !read_parameters_and_properties(cursor, ok.parameters, ok.track_properties, "SUBSCRIBE_OK", error)) {
    if (error.empty()) {
      error = "invalid SUBSCRIBE_OK";
    }
    return std::nullopt;
  }
  return ok;
}

std::optional<RequestOk> decode_request_ok(const ByteBuffer &payload, std::string &error) {
  Cursor cursor{payload};
  RequestOk ok;
  if (!read_parameters_and_properties(cursor, ok.parameters, ok.track_properties, "REQUEST_OK", error)) {
    return std::nullopt;
  }
  return ok;
}

ByteBuffer encode_request_error(RequestErrorCode code, std::string reason, uint64_t retry_interval) {
  ByteBuffer payload;
  write_varint(payload, static_cast<uint64_t>(code));
  write_varint(payload, retry_interval);
  write_varint(payload, reason.size());
  append_bytes(payload, reason);
  ByteBuffer framed;
  append_control_message(framed, kMessageRequestError, payload);
  return framed;
}

std::optional<RequestError> decode_request_error(const ByteBuffer &payload, std::string &error) {
  Cursor cursor{payload};
  RequestError decoded;
  uint64_t raw_code = 0;
  if (!cursor.read_varint(raw_code) || !cursor.read_varint(decoded.retry_interval) ||
      !read_reason(cursor, decoded.reason)) {
    error = "invalid REQUEST_ERROR";
    return std::nullopt;
  }
  decoded.code = static_cast<RequestErrorCode>(raw_code);
  // Redirect carries a new URI and track that this subscriber does not follow.
  if (decoded.code != RequestErrorCode::Redirect && cursor.remaining() != 0) {
    error = "trailing REQUEST_ERROR bytes";
    return std::nullopt;
  }
  return decoded;
}

ByteBuffer encode_request_update(RequestId request_id, const RequestUpdate &update) {
  ByteBuffer payload;
  write_varint(payload, request_id);
  if (!encode_parameters(payload, update.parameters)) {
    throw std::invalid_argument("REQUEST_UPDATE includes an unsupported or duplicate parameter");
  }
  ByteBuffer framed;
  append_control_message(framed, kMessageRequestUpdate, payload);
  return framed;
}

std::optional<PublishDone> decode_publish_done(const ByteBuffer &payload, std::string &error) {
  Cursor cursor{payload};
  PublishDone decoded;
  if (!cursor.read_varint(decoded.status_code) || !cursor.read_varint(decoded.stream_count) ||
      !read_reason(cursor, decoded.reason) || cursor.remaining() != 0) {
    error = "invalid PUBLISH_DONE";
    return std::nullopt;
  }
  return decoded;
}

std::optional<Subscribe> decode_subscribe(const ByteBuffer &payload, std::string &error) {
  Cursor cursor{payload};
  Subscribe subscribe;
  if (!cursor.read_varint(subscribe.request_id)) {
    error = "invalid SUBSCRIBE Request ID";
    return std::nullopt;
  }
  if (!read_track_namespace(cursor, &subscribe.track_namespace)) {
    error = "invalid SUBSCRIBE Track Namespace";
    return std::nullopt;
  }
  uint64_t name_size = 0;
  if (!cursor.read_varint(name_size) || name_size > 4096 ||
      !cursor.read_string(static_cast<size_t>(name_size), subscribe.track_name)) {
    error = "invalid SUBSCRIBE Track Name";
    return std::nullopt;
  }
  uint64_t parameter_count = 0;
  if (!cursor.read_varint(parameter_count) || !read_parameters(cursor, parameter_count, subscribe.parameters, error)) {
    if (error.empty()) {
      error = "invalid SUBSCRIBE parameters";
    }
    return std::nullopt;
  }
  if (cursor.remaining() != 0) {
    error = "trailing SUBSCRIBE bytes";
    return std::nullopt;
  }
  return subscribe;
}

std::optional<RequestUpdateMessage> decode_request_update(const ByteBuffer &payload, std::string &error) {
  Cursor cursor{payload};
  RequestUpdateMessage update;
  if (!cursor.read_varint(update.request_id)) {
    error = "invalid REQUEST_UPDATE Request ID";
    return std::nullopt;
  }
  uint64_t parameter_count = 0;
  if (!cursor.read_varint(parameter_count) || !read_parameters(cursor, parameter_count, update.parameters, error)) {
    if (error.empty()) {
      error = "invalid REQUEST_UPDATE parameters";
    }
    return std::nullopt;
  }
  if (cursor.remaining() != 0) {
    error = "trailing REQUEST_UPDATE bytes";
    return std::nullopt;
  }
  return update;
}

bool decode_subscription_options(const std::vector<Parameter> &parameters, SubscriptionOptions &options,
                                 std::string &error) {
  for (const Parameter &parameter : parameters) {
    Cursor cursor{parameter.encoded_value};
    switch (parameter.type) {
    case kParameterForward: {
      uint8_t value = 0;
      cursor.read_byte(value);
      if (value > 1) {
        error = "invalid FORWARD parameter value";
        return false;
      }
      options.forward = value;
      break;
    }
    case kParameterSubscriberPriority: {
      uint8_t value = 0;
      cursor.read_byte(value);
      options.subscriber_priority = value;
      break;
    }
    case kParameterGroupOrder: {
      uint8_t value = 0;
      cursor.read_byte(value);
      if (value != 0x1 && value != 0x2) {
        error = "invalid GROUP_ORDER parameter value";
        return false;
      }
      options.group_order = value;
      break;
    }
    case kParameterSubscriptionFilter: {
      uint64_t size = 0;
      cursor.read_varint(size); // shape already validated by read_parameters
      SubscriptionFilter filter;
      if (!cursor.read_varint(filter.filter_type)) {
        error = "truncated SUBSCRIPTION_FILTER";
        return false;
      }
      switch (filter.filter_type) {
      case kFilterLargestObject:
      case kFilterNextGroupStart:
        break;
      case kFilterAbsoluteStart:
        if (!cursor.read_varint(filter.start.group) || !cursor.read_varint(filter.start.object)) {
          error = "truncated SUBSCRIPTION_FILTER Start Location";
          return false;
        }
        break;
      case kFilterAbsoluteRange:
        if (!cursor.read_varint(filter.start.group) || !cursor.read_varint(filter.start.object) ||
            !cursor.read_varint(filter.end_group_delta)) {
          error = "truncated SUBSCRIPTION_FILTER range";
          return false;
        }
        if (filter.end_group_delta > std::numeric_limits<uint64_t>::max() - filter.start.group) {
          error = "SUBSCRIPTION_FILTER End Group overflows";
          return false;
        }
        break;
      default:
        error = "unknown subscription filter type " + std::to_string(filter.filter_type);
        return false;
      }
      if (cursor.remaining() != 0) {
        error = "trailing SUBSCRIPTION_FILTER bytes";
        return false;
      }
      options.filter = filter;
      break;
    }
    case kParameterSubgroupDeliveryTimeout: {
      uint64_t value = 0;
      cursor.read_varint(value);
      options.subgroup_delivery_timeout = value;
      break;
    }
    case kParameterObjectDeliveryTimeout: {
      uint64_t value = 0;
      cursor.read_varint(value);
      options.object_delivery_timeout = value;
      break;
    }
    case kParameterNewGroupRequest: {
      uint64_t value = 0;
      cursor.read_varint(value);
      options.new_group_request = value;
      break;
    }
    default:
      // Known to read_parameters but not subscription-scoped (e.g. AUTHORIZATION
      // TOKEN, RENDEZVOUS_TIMEOUT); accepted and ignored.
      break;
    }
  }
  return true;
}

ByteBuffer encode_subscribe_ok(TrackAlias track_alias, std::vector<Parameter> parameters,
                               const ObjectProperties &track_properties) {
  ByteBuffer payload;
  write_varint(payload, track_alias);
  if (!encode_parameters(payload, std::move(parameters))) {
    throw std::invalid_argument("SUBSCRIBE_OK includes an unsupported or duplicate parameter");
  }
  payload.insert(payload.end(), track_properties.begin(), track_properties.end());
  ByteBuffer framed;
  append_control_message(framed, kMessageSubscribeOk, payload);
  return framed;
}

ByteBuffer encode_request_ok(std::vector<Parameter> parameters, const ObjectProperties &track_properties) {
  ByteBuffer payload;
  if (!encode_parameters(payload, std::move(parameters))) {
    throw std::invalid_argument("REQUEST_OK includes an unsupported or duplicate parameter");
  }
  payload.insert(payload.end(), track_properties.begin(), track_properties.end());
  ByteBuffer framed;
  append_control_message(framed, kMessageRequestOk, payload);
  return framed;
}

ByteBuffer encode_publish_done(uint64_t status_code, uint64_t stream_count, const std::string &reason) {
  ByteBuffer payload;
  write_varint(payload, status_code);
  write_varint(payload, stream_count);
  write_varint(payload, reason.size());
  append_bytes(payload, reason);
  ByteBuffer framed;
  append_control_message(framed, kMessagePublishDone, payload);
  return framed;
}

ByteBuffer encode_publish_namespace(RequestId request_id, const TrackNamespace &track_namespace,
                                    std::vector<Parameter> parameters) {
  ByteBuffer payload;
  write_varint(payload, request_id);
  write_track_namespace(payload, track_namespace);
  if (!encode_parameters(payload, std::move(parameters))) {
    throw std::invalid_argument("PUBLISH_NAMESPACE includes an unsupported or duplicate parameter");
  }
  ByteBuffer framed;
  append_control_message(framed, kMessagePublishNamespace, payload);
  return framed;
}

ByteBuffer encode_object_datagram(TrackAlias track_alias, GroupId group_id, ObjectId object_id, uint8_t priority,
                                  BytesView properties, const std::optional<ObjectStatusCode> &status,
                                  BytesView payload, bool end_of_group) {
  uint64_t type = 0;
  if (!properties.empty()) {
    type |= 0x01; // PROPERTIES
  }
  if (status) {
    type |= 0x20; // STATUS
  } else if (end_of_group) {
    type |= 0x02; // END_OF_GROUP
  }
  if (object_id == 0) {
    type |= 0x04; // ZERO_OBJECT_ID
  }

  ByteBuffer out;
  write_varint(out, type);
  write_varint(out, track_alias);
  write_varint(out, group_id);
  if (object_id != 0) {
    write_varint(out, object_id);
  }
  out.push_back(priority);
  if (!properties.empty()) {
    write_varint(out, properties.size);
    out.insert(out.end(), properties.begin(), properties.end());
  }
  if (status) {
    write_varint(out, *status);
  } else {
    out.insert(out.end(), payload.begin(), payload.end());
  }
  return out;
}

void encode_subgroup_header(ByteBuffer &out, TrackAlias track_alias, GroupId group_id, SubgroupId subgroup_id,
                            uint8_t priority) {
  // PROPERTIES always set: objects without properties carry a zero length.
  // Subgroup 0 uses the implicit mode (0b00); anything else is explicit (0b10).
  const uint64_t subgroup_id_mode = subgroup_id == 0 ? 0x0 : 0x2;
  const uint64_t type = 0x10 | 0x01 | (subgroup_id_mode << 1);
  write_varint(out, type);
  write_varint(out, track_alias);
  write_varint(out, group_id);
  if (subgroup_id != 0) {
    write_varint(out, subgroup_id);
  }
  out.push_back(priority);
}

void encode_subgroup_object(ByteBuffer &out, uint64_t object_id_delta, BytesView properties,
                            const std::optional<ObjectStatusCode> &status, BytesView payload) {
  write_varint(out, object_id_delta);
  write_varint(out, properties.size);
  out.insert(out.end(), properties.begin(), properties.end());
  if (status || payload.empty()) {
    write_varint(out, 0); // zero payload length introduces an explicit status
    write_varint(out, status.value_or(kObjectStatusNormal));
  } else {
    write_varint(out, payload.size);
    out.insert(out.end(), payload.begin(), payload.end());
  }
}

} // namespace moq::codec
