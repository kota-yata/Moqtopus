#include "moq/subscriber_session.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <memory>
#include <spdlog/spdlog.h>
#include <thread>

namespace {

constexpr const char *kHost = "tcam.sfc.wide.ad.jp";
constexpr uint16_t kPort = 4433;
constexpr const char *kPath = "/moq";
constexpr const char *kNamespace = "moqtail";
constexpr const char *kTrackName = "video0";

std::atomic_bool interrupted{false};

const char *DeliveryName(moq::DeliveryKind delivery) {
  switch (delivery) {
  case moq::DeliveryKind::SubgroupStream:
    return "subgroup";
  case moq::DeliveryKind::Datagram:
    return "datagram";
  case moq::DeliveryKind::FetchStream:
    return "fetch";
  }
  return "unknown";
}

class VideoObjectPrinter final : public moq::ObjectHandler {
public:
  void on_object(moq::Object object) override {
    ++object_count_;
    total_payload_bytes_ += object.payload.size();

    std::cout << "object #" << object_count_.load() << " delivery=" << DeliveryName(object.delivery_kind)
              << " group=" << object.group_id << " object=" << object.object_id;
    if (object.subgroup_id) {
      std::cout << " subgroup=" << *object.subgroup_id;
    }
    if (object.status) {
      std::cout << " status=" << *object.status;
    } else {
      std::cout << " payload=" << object.payload.size() << " bytes";
    }
    std::cout << '\n';
  }

  void on_publish_done(moq::PublishDone done) override {
    std::cout << "publisher finished track status=" << done.status_code << " streams=" << done.stream_count;
    if (!done.reason.empty()) {
      std::cout << " reason=\"" << done.reason << '"';
    }
    std::cout << '\n';
    stopped_.store(true);
  }

  void on_error(moq::ReceiveError error) override {
    std::cerr << "receive error code=" << error.code << " message=\"" << error.message << "\"\n";
    stopped_.store(true);
  }

  bool stopped() const { return stopped_.load(); }

  uint64_t object_count() const { return object_count_.load(); }

  uint64_t total_payload_bytes() const { return total_payload_bytes_.load(); }

private:
  std::atomic_bool stopped_{false};
  std::atomic_uint64_t object_count_{0};
  std::atomic_uint64_t total_payload_bytes_{0};
};

} // namespace

int main() {
  spdlog::set_level(spdlog::level::debug);

  try {
    moq::MsQuicClientConfig client;
    client.host = kHost;
    client.port = kPort;
    client.path = kPath;
    client.alpn = "moqt-18";
    client.disable_certificate_validation = false;

    auto session = moq::MoqSubscriberSession::connect(client).get();
    session->ready().get();

    moq::SubscribeRequest subscribe;
    subscribe.track_namespace = {kNamespace};
    subscribe.track_name = kTrackName;

    auto handler = std::make_shared<VideoObjectPrinter>();
    const moq::SubscriptionHandle handle = session->subscribe(std::move(subscribe), handler).get();

    if (handle.track_alias()) {
      std::cout << " alias=" << *handle.track_alias();
    }
    std::cout << '\n';

    while (!interrupted.load() && !handler->stopped()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    if (!interrupted.load()) {
      session->stop_subscription(handle.request_id()).get();
    }
    session->close();

    return 0;
  } catch (const std::exception &error) {
    std::cerr << "subscriber failed: " << error.what() << '\n';
    return 1;
  }
}
