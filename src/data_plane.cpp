#include "data_plane.h"

#include "moq/codec.h"

#include <algorithm>
#include <limits>
#include <mutex>
#include <optional>
#include <utility>

namespace moq::detail {
namespace {

constexpr uint64_t kNormalStatus = 0x0;
constexpr uint64_t kEndOfGroupStatus = 0x3;
constexpr uint64_t kEndOfTrackStatus = 0x4;

enum class ParseStatus {
  Done,
  NeedMore,
  Error,
};

struct ParseResult {
  ParseStatus status = ParseStatus::NeedMore;
  std::string error;
};

bool is_valid_object_status(uint64_t status) {
  return status == kNormalStatus || status == kEndOfGroupStatus || status == kEndOfTrackStatus;
}

ParseStatus read_varint_at(const ByteBuffer &bytes, size_t &offset, uint64_t &value, std::string &error) {
  const codec::VarintResult decoded = codec::read_varint(bytes, offset);
  if (decoded.status == codec::DecodeStatus::NeedMoreData) {
    return ParseStatus::NeedMore;
  }
  if (decoded.status == codec::DecodeStatus::Error) {
    error = decoded.error;
    return ParseStatus::Error;
  }
  offset += decoded.bytes;
  value = decoded.value;
  return ParseStatus::Done;
}

ParseStatus read_byte_at(const ByteBuffer &bytes, size_t &offset, uint8_t &value) {
  if (offset >= bytes.size()) {
    return ParseStatus::NeedMore;
  }
  value = bytes[offset++];
  return ParseStatus::Done;
}

ParseStatus read_properties_at(const ByteBuffer &bytes, size_t &offset, bool require_non_empty,
                               ObjectProperties &properties, std::string &error) {
  uint64_t length = 0;
  const ParseStatus length_status = read_varint_at(bytes, offset, length, error);
  if (length_status != ParseStatus::Done) {
    return length_status;
  }
  if ((require_non_empty && length == 0) || length > 65535) {
    error = "invalid object properties length";
    return ParseStatus::Error;
  }
  if (length > bytes.size() - offset) {
    return ParseStatus::NeedMore;
  }
  properties.assign(bytes.begin() + static_cast<std::ptrdiff_t>(offset),
                    bytes.begin() + static_cast<std::ptrdiff_t>(offset + length));
  offset += static_cast<size_t>(length);
  return ParseStatus::Done;
}

bool all_zero_after(const ByteBuffer &bytes, size_t offset) {
  return std::all_of(bytes.begin() + static_cast<std::ptrdiff_t>(offset), bytes.end(),
                     [](uint8_t byte) { return byte == 0; });
}

class DataStreamReceiver : public std::enable_shared_from_this<DataStreamReceiver> {
public:
  DataStreamReceiver(DataPlane &plane, std::shared_ptr<TransportStream> stream)
      : plane_(plane), stream_(std::move(stream)) {}

  void feed(ByteBuffer bytes, bool fin) {
    buffer_.insert(buffer_.end(), bytes.begin(), bytes.end());
    if (closed_) {
      return;
    }
    if (!header_done_) {
      const ParseResult header = parse_header();
      if (header.status == ParseStatus::Error) {
        fail_protocol(header.error);
        return;
      }
      if (header.status == ParseStatus::NeedMore) {
        if (fin) {
          fail_protocol("SUBGROUP_HEADER ended mid-header");
        }
        return;
      }
    }

    for (;;) {
      const ParseResult object = parse_object();
      if (object.status == ParseStatus::Error) {
        fail_protocol(object.error);
        return;
      }
      if (object.status == ParseStatus::NeedMore) {
        if (fin && !buffer_.empty()) {
          fail_protocol("subgroup stream ended mid-object");
          return;
        }
        break;
      }
    }

    if (fin) {
      close_cleanly();
    }
  }

  void aborted(uint64_t) { closed_ = true; }

private:
  ParseResult parse_header() {
    size_t offset = 0;
    std::string error;
    uint64_t type = 0;
    uint64_t alias = 0;
    uint64_t group = 0;
    ParseStatus status = read_varint_at(buffer_, offset, type, error);
    if (status != ParseStatus::Done) {
      return ParseResult{status, error};
    }
    if (!codec::is_subgroup_stream_type(type)) {
      return ParseResult{ParseStatus::Error, "invalid SUBGROUP_HEADER stream type"};
    }
    status = read_varint_at(buffer_, offset, alias, error);
    if (status != ParseStatus::Done) {
      return ParseResult{status, error};
    }
    status = read_varint_at(buffer_, offset, group, error);
    if (status != ParseStatus::Done) {
      return ParseResult{status, error};
    }

    subgroup_id_mode_ = static_cast<uint8_t>((type & 0x06) >> 1);
    if (subgroup_id_mode_ == 0x02) {
      uint64_t subgroup = 0;
      status = read_varint_at(buffer_, offset, subgroup, error);
      if (status != ParseStatus::Done) {
        return ParseResult{status, error};
      }
      subgroup_id_ = subgroup;
    } else if (subgroup_id_mode_ == 0x00) {
      subgroup_id_ = 0;
    }

    const bool default_priority = (type & 0x20) != 0;
    if (!default_priority) {
      uint8_t priority = 0;
      status = read_byte_at(buffer_, offset, priority);
      if (status != ParseStatus::Done) {
        return ParseResult{status, error};
      }
      publisher_priority_ = priority;
    }

    alias_ = alias;
    group_id_ = group;
    properties_per_object_ = (type & 0x01) != 0;
    end_of_group_on_fin_ = (type & 0x08) != 0;
    route_ = plane_.find_route(alias_);
    if (!route_) {
      if (plane_.unknown_alias_policy() == UnknownAliasPolicy::Error) {
        return ParseResult{ParseStatus::Error,
                           "subgroup stream referenced unknown track alias " + std::to_string(alias_)};
      }
      stream_->abort_receive(0);
      closed_ = true;
      return ParseResult{ParseStatus::Done, {}};
    }
    if (default_priority) {
      publisher_priority_ = route_->default_publisher_priority.load();
    }
    route_->received_stream_count.fetch_add(1);
    erase_prefix(offset);
    header_done_ = true;
    return ParseResult{ParseStatus::Done, {}};
  }

  ParseResult parse_object() {
    if (closed_ || buffer_.empty()) {
      return ParseResult{ParseStatus::NeedMore, {}};
    }

    size_t offset = 0;
    std::string error;
    uint64_t object_delta = 0;
    ParseStatus status = read_varint_at(buffer_, offset, object_delta, error);
    if (status != ParseStatus::Done) {
      return ParseResult{status, error};
    }

    ObjectProperties properties;
    if (properties_per_object_) {
      status = read_properties_at(buffer_, offset, false, properties, error);
      if (status != ParseStatus::Done) {
        return ParseResult{status, error};
      }
    }

    uint64_t payload_length = 0;
    status = read_varint_at(buffer_, offset, payload_length, error);
    if (status != ParseStatus::Done) {
      return ParseResult{status, error};
    }

    std::optional<ObjectStatusCode> object_status;
    ByteBuffer payload;
    if (payload_length == 0) {
      uint64_t status_code = 0;
      status = read_varint_at(buffer_, offset, status_code, error);
      if (status != ParseStatus::Done) {
        return ParseResult{status, error};
      }
      if (!is_valid_object_status(status_code) || (status_code != kNormalStatus && !properties.empty())) {
        return ParseResult{ParseStatus::Error, "invalid subgroup object status"};
      }
      object_status = status_code;
    } else {
      if (payload_length > buffer_.size() - offset) {
        return ParseResult{ParseStatus::NeedMore, {}};
      }
      payload.assign(buffer_.begin() + static_cast<std::ptrdiff_t>(offset),
                     buffer_.begin() + static_cast<std::ptrdiff_t>(offset + payload_length));
      offset += static_cast<size_t>(payload_length);
    }

    ObjectId object_id = object_delta;
    if (last_object_id_) {
      if (*last_object_id_ == std::numeric_limits<uint64_t>::max() ||
          object_delta > std::numeric_limits<uint64_t>::max() - (*last_object_id_ + 1)) {
        return ParseResult{ParseStatus::Error, "subgroup Object ID delta overflow"};
      }
      object_id = *last_object_id_ + object_delta + 1;
    }
    if (subgroup_id_mode_ == 0x01 && !subgroup_id_) {
      subgroup_id_ = object_id;
    }

    Object object;
    object.request_id = route_->request_id;
    object.track_alias = alias_;
    object.group_id = group_id_;
    object.subgroup_id = subgroup_id_.value_or(0);
    object.object_id = object_id;
    object.publisher_priority = publisher_priority_;
    object.status = object_status;
    object.properties = std::move(properties);
    object.payload = std::move(payload);
    object.delivery_kind = DeliveryKind::SubgroupStream;
    object.stream_id = stream_->id();
    last_object_id_ = object_id;
    erase_prefix(offset);
    plane_.deliver(route_, std::move(object));
    return ParseResult{ParseStatus::Done, {}};
  }

  void erase_prefix(size_t offset) {
    buffer_.erase(buffer_.begin(), buffer_.begin() + static_cast<std::ptrdiff_t>(offset));
  }

  void fail_protocol(std::string error) {
    closed_ = true;
    plane_.protocol_error(std::move(error));
  }

  void close_cleanly() {
    closed_ = true;
    if (route_ && end_of_group_on_fin_ && last_object_id_) {
      route_->validation->mark_final_object_in_group(group_id_, *last_object_id_);
    }
  }

  DataPlane &plane_;
  std::shared_ptr<TransportStream> stream_;
  ByteBuffer buffer_;
  std::shared_ptr<ReceiveRoute> route_;
  bool header_done_ = false;
  bool closed_ = false;
  bool properties_per_object_ = false;
  bool end_of_group_on_fin_ = false;
  uint8_t subgroup_id_mode_ = 0;
  TrackAlias alias_ = 0;
  GroupId group_id_ = 0;
  std::optional<SubgroupId> subgroup_id_;
  uint8_t publisher_priority_ = 128;
  std::optional<ObjectId> last_object_id_;
};

} // namespace

bool TrackReceiveValidation::validate(const Object &object, std::string &error) const {
  const auto final_in_group = final_object_in_group_.find(object.group_id);
  if (final_in_group != final_object_in_group_.end() && object.object_id > final_in_group->second) {
    error = "object followed final object in group";
    return false;
  }
  if (final_object_in_track_ &&
      (object.group_id > final_object_in_track_->group ||
       (object.group_id == final_object_in_track_->group && object.object_id > final_object_in_track_->object))) {
    error = "object followed final object in track";
    return false;
  }
  return true;
}

void TrackReceiveValidation::mark_final_object_in_group(GroupId group_id, ObjectId object_id) {
  final_object_in_group_[group_id] = object_id;
}

void TrackReceiveValidation::mark_final_object_in_track(Location location) { final_object_in_track_ = location; }

DataPlane::DataPlane(SubscriberConfig config, ProtocolErrorCallback protocol_error, TrackErrorCallback track_error)
    : config_(std::move(config)), protocol_error_callback_(std::move(protocol_error)),
      track_error_callback_(std::move(track_error)) {}

bool DataPlane::install_route(TrackAlias alias, std::shared_ptr<ReceiveRoute> route) {
  {
    std::unique_lock<std::shared_mutex> lock(routes_mutex_);
    if (!routes_by_alias_.emplace(alias, std::move(route)).second) {
      return false;
    }
  }
  auto buffered = unknown_datagrams_.find(alias);
  if (buffered != unknown_datagrams_.end()) {
    std::vector<ByteBuffer> datagrams = std::move(buffered->second);
    unknown_datagrams_.erase(buffered);
    for (ByteBuffer &datagram : datagrams) {
      buffered_datagram_bytes_ -= datagram.size();
      deliver_datagram(std::move(datagram), false);
    }
  }
  return true;
}

void DataPlane::deactivate_route(TrackAlias alias) {
  const std::shared_ptr<ReceiveRoute> route = find_route(alias);
  if (route) {
    route->active.store(false);
  }
}

void DataPlane::remove_route(TrackAlias alias) {
  std::unique_lock<std::shared_mutex> lock(routes_mutex_);
  routes_by_alias_.erase(alias);
}

std::shared_ptr<ReceiveRoute> DataPlane::find_route(TrackAlias alias) const {
  std::shared_lock<std::shared_mutex> lock(routes_mutex_);
  const auto route = routes_by_alias_.find(alias);
  return route == routes_by_alias_.end() ? nullptr : route->second;
}

void DataPlane::on_datagram(ByteBuffer bytes) { deliver_datagram(std::move(bytes), true); }

void DataPlane::start_subgroup_stream(std::shared_ptr<TransportStream> stream, ByteBuffer initial_bytes, bool fin) {
  auto receiver = std::make_shared<DataStreamReceiver>(*this, stream);
  stream->on_bytes(
      [receiver](ByteBuffer bytes, bool bytes_fin) mutable { receiver->feed(std::move(bytes), bytes_fin); });
  stream->on_peer_send_aborted([receiver](uint64_t error_code) { receiver->aborted(error_code); });
  receiver->feed(std::move(initial_bytes), fin);
}

UnknownAliasPolicy DataPlane::unknown_alias_policy() const { return config_.unknown_alias_policy; }

void DataPlane::deliver_datagram(ByteBuffer bytes, bool allow_buffer) {
  size_t offset = 0;
  std::string error;
  uint64_t type = 0;
  ParseStatus status = read_varint_at(bytes, offset, type, error);
  if (status != ParseStatus::Done) {
    protocol_error("datagram has malformed type");
    return;
  }
  if (type == codec::kPaddingDatagramType) {
    if (!all_zero_after(bytes, offset)) {
      protocol_error("padding datagram contains non-zero bytes");
    }
    return;
  }
  if ((type > 0x0f && (type < 0x20 || type > 0x2f)) || ((type & 0x20) != 0 && (type & 0x02) != 0)) {
    protocol_error("invalid object datagram type");
    return;
  }

  uint64_t alias = 0;
  uint64_t group = 0;
  if (read_varint_at(bytes, offset, alias, error) != ParseStatus::Done ||
      read_varint_at(bytes, offset, group, error) != ParseStatus::Done) {
    protocol_error("truncated object datagram header");
    return;
  }

  uint64_t object_id = 0;
  if ((type & 0x04) == 0 && read_varint_at(bytes, offset, object_id, error) != ParseStatus::Done) {
    protocol_error("truncated object datagram Object ID");
    return;
  }

  const std::shared_ptr<ReceiveRoute> route = find_route(alias);
  if (!route) {
    if (allow_buffer) {
      buffer_unknown_datagram(alias, std::move(bytes));
    }
    return;
  }

  uint8_t publisher_priority = route->default_publisher_priority.load();
  if ((type & 0x08) == 0 && read_byte_at(bytes, offset, publisher_priority) != ParseStatus::Done) {
    protocol_error("truncated object datagram priority");
    return;
  }

  ObjectProperties properties;
  if ((type & 0x01) != 0) {
    status = read_properties_at(bytes, offset, true, properties, error);
    if (status != ParseStatus::Done) {
      protocol_error("invalid object datagram properties");
      return;
    }
  }

  std::optional<ObjectStatusCode> object_status;
  ByteBuffer payload;
  if ((type & 0x20) != 0) {
    uint64_t status_code = 0;
    if (read_varint_at(bytes, offset, status_code, error) != ParseStatus::Done ||
        !is_valid_object_status(status_code) || (status_code != kNormalStatus && !properties.empty()) ||
        offset != bytes.size()) {
      protocol_error("invalid object datagram status");
      return;
    }
    object_status = status_code;
  } else {
    payload.assign(bytes.begin() + static_cast<std::ptrdiff_t>(offset), bytes.end());
  }

  Object object;
  object.request_id = route->request_id;
  object.track_alias = alias;
  object.group_id = group;
  object.object_id = object_id;
  object.publisher_priority = publisher_priority;
  object.status = object_status;
  object.properties = std::move(properties);
  object.payload = std::move(payload);
  object.delivery_kind = DeliveryKind::Datagram;
  if ((type & 0x02) != 0) {
    route->validation->mark_final_object_in_group(group, object_id);
  }
  deliver(route, std::move(object));
}

void DataPlane::buffer_unknown_datagram(TrackAlias alias, ByteBuffer bytes) {
  switch (config_.unknown_alias_policy) {
  case UnknownAliasPolicy::Drop:
    return;
  case UnknownAliasPolicy::Error:
    protocol_error("datagram referenced unknown track alias " + std::to_string(alias));
    return;
  case UnknownAliasPolicy::BufferDatagrams:
    break;
  }
  if (bytes.size() > config_.max_buffered_datagram_bytes ||
      buffered_datagram_bytes_ + bytes.size() > config_.max_buffered_datagram_bytes) {
    return;
  }
  std::vector<ByteBuffer> &datagrams = unknown_datagrams_[alias];
  if (datagrams.size() >= config_.max_buffered_datagrams_per_alias) {
    return;
  }
  buffered_datagram_bytes_ += bytes.size();
  datagrams.push_back(std::move(bytes));
}

void DataPlane::deliver(std::shared_ptr<ReceiveRoute> route, Object object) {
  if (!route || !route->active.load()) {
    return;
  }
  std::string error;
  if (!route->validation->validate(object, error)) {
    track_error(route->request_id, std::move(error));
    return;
  }
  if (object.status && *object.status == kEndOfGroupStatus) {
    route->validation->mark_final_object_in_group(object.group_id, object.object_id);
  }
  if (object.status && *object.status == kEndOfTrackStatus) {
    route->validation->mark_final_object_in_track(Location{object.group_id, object.object_id});
  }
  route->handler->on_object(std::move(object));
}

void DataPlane::protocol_error(std::string message) {
  if (protocol_error_callback_) {
    protocol_error_callback_(std::move(message));
  }
}

void DataPlane::track_error(RequestId request_id, std::string message) {
  if (track_error_callback_) {
    track_error_callback_(request_id, std::move(message));
  }
}

} // namespace moq::detail
