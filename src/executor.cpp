#include "executor.h"

#include <utility>

namespace moq::detail {

struct Executor::State {
    mutable std::mutex mutex;
    std::condition_variable cv;
    std::queue<std::function<void()>> tasks;
    bool stopped = false;
    std::thread::id thread_id;
};

Executor::Executor()
    : state_(std::make_shared<State>()),
      thread_([state = state_] { run(std::move(state)); }) {}

Executor::~Executor() {
    stop();
}

void Executor::stop() {
    std::shared_ptr<State> state = state_;
    if (!state) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        state->stopped = true;
        std::queue<std::function<void()>> empty;
        state->tasks.swap(empty);
    }
    state->cv.notify_one();
    if (thread_.joinable()) {
        if (std::this_thread::get_id() == thread_.get_id()) {
            thread_.detach();
        } else {
            thread_.join();
        }
    }
}

void Executor::post(std::function<void()> task) {
    std::shared_ptr<State> state = state_;
    if (!state) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        if (state->stopped) {
            return;
        }
        state->tasks.push(std::move(task));
    }
    state->cv.notify_one();
}

bool Executor::on_thread() const {
    std::shared_ptr<State> state = state_;
    return state && std::this_thread::get_id() == state->thread_id;
}

void Executor::run(std::shared_ptr<State> state) {
    state->thread_id = std::this_thread::get_id();
    for (;;) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(state->mutex);
            state->cv.wait(lock, [&state] {
                return state->stopped || !state->tasks.empty();
            });
            if (state->stopped) {
                return;
            }
            task = std::move(state->tasks.front());
            state->tasks.pop();
        }
        task();
    }
}

} // namespace moq::detail
