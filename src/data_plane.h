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
  void mark_final_object_in_group(GroupId group_id, ObjectId object_id);
  void mark_final_object_in_track(Location location);

private:
  std::unordered_map<GroupId, ObjectId> final_object_in_group_;
  std::optional<Location> final_object_in_track_;
};

struct ReceiveRoute {
  RequestId request_id = 0;
  TrackAlias track_alias = 0;
  std::atomic_bool active{true};
  std::shared_ptr<ObjectHandler> handler;
  std::shared_ptr<TrackReceiveValidation> validation = std::make_shared<TrackReceiveValidation>();
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
  void deactivate_route(TrackAlias alias);
  void remove_route(TrackAlias alias);
  std::shared_ptr<ReceiveRoute> find_route(TrackAlias alias) const;

  void on_datagram(ByteBuffer bytes);
  void start_subgroup_stream(std::shared_ptr<StreamContext> stream, ByteBuffer initial_bytes, bool fin);

  UnknownAliasPolicy unknown_alias_policy() const;
  void deliver(std::shared_ptr<ReceiveRoute> route, Object object);
  void protocol_error(std::string message);

private:
  void deliver_datagram(ByteBuffer bytes, bool allow_buffer);
  void buffer_unknown_datagram(TrackAlias alias, ByteBuffer bytes);
  void track_error(RequestId request_id, std::string message);

  SubscriberConfig config_;
  ProtocolErrorCallback protocol_error_callback_;
  TrackErrorCallback track_error_callback_;
  mutable std::shared_mutex routes_mutex_;
  std::unordered_map<TrackAlias, std::shared_ptr<ReceiveRoute>> routes_by_alias_;
  std::unordered_map<TrackAlias, std::vector<ByteBuffer>> unknown_datagrams_;
  size_t buffered_datagram_bytes_ = 0;
};

} // namespace moq::detail
