#pragma once

#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>

namespace moq::detail {

class Executor {
public:
  Executor();
  ~Executor();

  Executor(const Executor &) = delete;
  Executor &operator=(const Executor &) = delete;

  void post(std::function<void()> task);
  bool on_thread() const;
  void stop();

private:
  struct State;

  static void run(std::shared_ptr<State> state);

  std::shared_ptr<State> state_;
  std::thread thread_;
};

} // namespace moq::detail
