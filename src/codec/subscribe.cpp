#include "codec_internal.h"

#include <limits>
#include <stdexcept>

namespace moq::codec {

ByteBuffer encode_subscribe(RequestId request_id, const SubscribeRequest &request) {
  ByteBuffer payload;
  write_varint(payload, request_id);
  write_track_namespace(payload, request.track_namespace);
  write_varint(payload, request.track_name.size());
  detail::append_bytes(payload, request.track_name);
  if (!detail::encode_parameters(payload, request.parameters)) {
    throw std::invalid_argument("SUBSCRIBE includes an unsupported or duplicate parameter");
  }

  ByteBuffer framed;
  append_control_message(framed, kMessageSubscribe, payload);
  return framed;
}

std::optional<Subscribe> decode_subscribe(const ByteBuffer &payload, std::string &error) {
  detail::Cursor cursor{payload};
  Subscribe subscribe;
  if (!cursor.read_varint(subscribe.request_id)) {
    error = "invalid SUBSCRIBE Request ID";
    return std::nullopt;
  }
  if (!detail::read_track_namespace(cursor, &subscribe.track_namespace)) {
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
  if (!cursor.read_varint(parameter_count) ||
      !detail::read_parameters(cursor, parameter_count, subscribe.parameters, error)) {
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

bool decode_subscription_options(const std::vector<Parameter> &parameters, SubscriptionOptions &options,
                                 std::string &error) {
  for (const Parameter &parameter : parameters) {
    detail::Cursor cursor{parameter.encoded_value};
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
      cursor.read_varint(size);
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
      break;
    }
  }
  return true;
}

} // namespace moq::codec
