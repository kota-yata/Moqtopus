#include "moq/subscriber_session.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include <spdlog/spdlog.h>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>

namespace {

std::atomic_bool interrupted{false};

void OnSignal(int) { interrupted.store(true); }

bool ParsePort(const char *value, uint16_t &port) {
  try {
    const unsigned long parsed = std::stoul(value);
    if (parsed == 0 || parsed > 65535) {
      return false;
    }
    port = static_cast<uint16_t>(parsed);
    return true;
  } catch (...) {
    return false;
  }
}

moq::TrackNamespace ParseNamespace(const std::string &value) {
  moq::TrackNamespace fields;
  size_t start = 0;
  while (start < value.size()) {
    const size_t slash = value.find('/', start);
    const size_t end = slash == std::string::npos ? value.size() : slash;
    if (end == start) {
      throw std::invalid_argument("namespace fields must not be empty");
    }
    fields.push_back(value.substr(start, end - start));
    if (slash == std::string::npos) {
      break;
    }
    start = slash + 1;
  }
  return fields;
}

std::string PayloadPreview(const moq::ByteBuffer &payload) {
  std::ostringstream text;
  const size_t max_bytes = 24;
  for (size_t index = 0; index < payload.size() && index < max_bytes; ++index) {
    const uint8_t byte = payload[index];
    if (byte >= 0x20 && byte <= 0x7e) {
      text << static_cast<char>(byte);
    } else {
      text << "\\x" << std::hex << std::setw(2) << std::setfill('0') << static_cast<unsigned>(byte) << std::dec
           << std::setfill(' ');
    }
  }
  if (payload.size() > max_bytes) {
    text << "...";
  }
  return text.str();
}

class PrintingHandler final : public moq::ObjectHandler {
public:
  void on_object(moq::Object object) override {
    std::cout << "object request=" << object.request_id << " alias=" << object.track_alias
              << " group=" << object.group_id << " object=" << object.object_id;
    if (object.subgroup_id) {
      std::cout << " subgroup=" << *object.subgroup_id;
    }
    if (object.status) {
      std::cout << " status=" << *object.status;
    } else {
      std::cout << " bytes=" << object.payload.size() << " preview=\"" << PayloadPreview(object.payload) << '"';
    }
    std::cout << '\n';
  }

  void on_publish_done(moq::PublishDone done) override {
    std::cout << "publish done status=" << done.status_code << " streams=" << done.stream_count;
    if (!done.reason.empty()) {
      std::cout << " reason=\"" << done.reason << '"';
    }
    std::cout << '\n';
    done_.store(true);
  }

  void on_error(moq::ReceiveError error) override {
    std::cerr << "receive error code=" << error.code << " message=\"" << error.message << "\"\n";
    done_.store(true);
  }

  bool done() const { return done_.load(); }

private:
  std::atomic_bool done_{false};
};

void Usage(const char *argv0) {
  std::cerr << "usage: " << argv0
            << " <host> <port> <namespace[/field...]> <track-name>"
               " [path] [alpn]\n"
            << "example: " << argv0 << " localhost 4433 camera/front video / moqt-18\n";
}

} // namespace

int main(int argc, char **argv) {
  spdlog::set_level(spdlog::level::debug);

  if (argc < 5 || argc > 7) {
    Usage(argv[0]);
    return 2;
  }

  try {
    moq::MsQuicClientConfig client_config;
    client_config.host = argv[1];
    if (!ParsePort(argv[2], client_config.port)) {
      std::cerr << "invalid port: " << argv[2] << '\n';
      return 2;
    }
    client_config.path = argc >= 6 ? argv[5] : "/";
    client_config.alpn = argc >= 7 ? argv[6] : "moqt-18";

    auto session = moq::MoqSubscriberSession::connect(client_config).get();
    session->ready().get();

    moq::SubscribeRequest request;
    request.track_namespace = ParseNamespace(argv[3]);
    request.track_name = argv[4];
    auto handler = std::make_shared<PrintingHandler>();
    const moq::SubscriptionHandle subscription = session->subscribe(std::move(request), handler).get();
    std::cout << "subscribed request=" << subscription.request_id();
    if (subscription.track_alias()) {
      std::cout << " alias=" << *subscription.track_alias();
    }
    std::cout << '\n';

    std::signal(SIGINT, OnSignal);
    std::signal(SIGTERM, OnSignal);
    while (!interrupted.load() && !handler->done()) {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    session->stop_subscription(subscription.request_id()).get();
    session->close();
    return 0;
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
