#include <list>
#include "nixl_thread.h"
#include "nixl_log.h"

nixlThread::nixlThread() {}

nixlThread::~nixlThread() {
    NIXL_ASSERT(!threadActive_);
}

void
nixlThread::runThread(nixlThread &t) {
    t.threadActive_->set_value();
    t.run();
}

void
nixlThread::start() {
    NIXL_ASSERT(!threadActive_);
    threadActive_ = std::make_unique<std::promise<void>>();
    auto active = threadActive_->get_future();
    thread_ = std::make_unique<std::thread>(&runThread, std::ref(*this));
    active.wait();
}

void
nixlThread::join() {
    NIXL_ASSERT(threadActive_);
    threadActive_.reset();
    thread_->join();
}

nixlWorkQueueThread::nixlWorkQueueThread() : nixlThread() {}

nixlWorkQueueThread::~nixlWorkQueueThread() {}

void
nixlWorkQueueThread::queue(nixlWork *work) {
    mutex_.lock();
    to_equeue_.push_back(work);
    mutex_.unlock();
    cond_.notify_one();
}

void
nixlWorkQueueThread::run() {
    while (true) {
        std::list<nixlWork *> to_equeue;
        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock, [this] { return !to_equeue_.empty() || !to_poll_.empty() || !isActive(); });
        to_equeue.swap(to_equeue_);
        lock.unlock();

        if (!isActive()) {
            break;
        }

        // Execute class-specific poll()
        poll();

        // Enqueue the works
        while (!to_equeue.empty()) {
            auto work = std::move(to_equeue.front());
            to_equeue.pop_front();
            to_poll_.push_back(work);
        }

        // Poll the works
        for (auto it = to_poll_.begin(); it != to_poll_.end(); ) {
            auto work = *it;
            if (!work->poll()) {
                it = to_poll_.erase(it);
                work->destroy();
            } else {
                it++;
            }
        }
    }
}

void
nixlWorkQueueThread::reset() {
    std::list<nixlWork *> to_equeue;
    std::list<nixlWork *> to_poll;
    std::unique_lock<std::mutex> lock(mutex_);
    to_equeue.swap(to_equeue_);
    to_poll.swap(to_poll_);
    mutex_.unlock();
    while (!to_equeue.empty()) {
        auto work = to_equeue.front();
        to_equeue.pop_front();
        work->destroy();
    }
    while (!to_poll.empty()) {
        auto work = to_poll.front();
        to_poll.pop_front();
        work->destroy();
    }
}
