#pragma once

#include "moq/subscriber_session.h"
#include "stream_context.h"

#include <atomic>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace moq::detail {

class TrackReceiveValidation {
public:
  bool validate(const Object &object, std::string &error) const;
  void mark_final_object_in_group(GroupId group_id, ObjectId object_id) { final_object_in_group_[group_id] = object_id; }
  void mark_final_object_in_track(Location location) { final_object_in_track_ = location; }

private:
  std::unordered_map<GroupId, ObjectId> final_object_in_group_;
  std::optional<Location> final_object_in_track_;
};

struct ReceiveRoute {
  RequestId request_id = 0;
  TrackAlias track_alias = 0;
  std::atomic_bool active{true};
  std::shared_ptr<ObjectHandler> handler;
  TrackReceiveValidation validation;
  std::atomic<uint8_t> default_publisher_priority{128};
  std::atomic<uint64_t> received_stream_count{0};
  std::atomic<uint64_t> expected_stream_count{0};
};

class DataPlane {
public:
  using ProtocolErrorCallback = std::function<void(std::string)>;
  using TrackErrorCallback = std::function<void(RequestId, std::string)>;

  DataPlane(SubscriberConfig config, ProtocolErrorCallback protocol_error, TrackErrorCallback track_error);

  bool install_route(TrackAlias alias, std::shared_ptr<ReceiveRoute> route);
  void retire_route(TrackAlias alias);
  std::shared_ptr<ReceiveRoute> find_route(TrackAlias alias) const;

  // Parses the datagram in place; bytes are only valid during the call.
  void on_datagram(BytesView datagram);
  // Takes over the stream: installs the hot-path SubgroupReceiver as its sink.
  // `prefix` holds bytes the cold-path gate already buffered ahead of the header.
  void start_subgroup_stream(const std::shared_ptr<StreamContext> &stream, ByteBuffer prefix, bool fin);

  UnknownAliasPolicy unknown_alias_policy() const { return config_.unknown_alias_policy; }
  void deliver(ReceiveRoute &route, const Object &object);
  void protocol_error(std::string message);
  void track_error(RequestId request_id, std::string message);

private:
  void deliver_datagram(BytesView bytes, bool allow_buffer);
  void buffer_unknown_datagram(TrackAlias alias, BytesView bytes);

  SubscriberConfig config_;
  ProtocolErrorCallback protocol_error_callback_;
  TrackErrorCallback track_error_callback_;
  mutable std::shared_mutex routes_mutex_;
  std::unordered_map<TrackAlias, std::shared_ptr<ReceiveRoute>> routes_by_alias_;
  std::unordered_map<TrackAlias, std::vector<ByteBuffer>> unknown_datagrams_;
  size_t buffered_datagram_bytes_ = 0;
};

} // namespace moq::detail
