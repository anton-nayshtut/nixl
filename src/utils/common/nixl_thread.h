/*
 * SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _NIXL_THREAD_H
#define _NIXL_THREAD_H

#include <string.h>
#include <list>
#include <memory>
#include <thread>
#include <mutex>
#include <future>
#include <vector>
#include <list>
#include "nixl_log.h"

class nixlThread {
public:
    nixlThread();
    virtual ~nixlThread();

    virtual void
    start();

    virtual void
    join();

    bool
    isActive() const {
        return threadActive_.get() != nullptr;
    }

protected:
    virtual void
    run() = 0;

    static void
    runThread(nixlThread &);

private:
    std::unique_ptr<std::thread> thread_;
    std::unique_ptr<std::promise<void>> threadActive_;
};

template<typename T> class nixlThreadStaticPool {
public:
    template<class... Args> nixlThreadStaticPool(size_t size, Args... args) : started_(false) {
        for (size_t i = 0; i < size; i++) {
            auto thread = new T(args...);
            threads_.push_back(std::move(thread));
        }
    }

    virtual ~nixlThreadStaticPool() {
        if (started_) {
            stop();
        }

        while (!threads_.empty()) {
            auto thread = std::move(threads_.back());
            thread->join();
            delete thread;
            threads_.pop_back();
        }
    }

    virtual void
    start() {
        NIXL_ASSERT(!started_);
        started_ = true;
        for (auto &thread : threads_) {
            thread->start();
        }
    }

    virtual void
    stop() {
        NIXL_ASSERT(started_);
        started_ = false;
        for (auto &thread : threads_) {
            thread->join();
        }
    }

    nixlThread &
    getAt(size_t idx) const {
        return *threads_[idx];
    }

    size_t
    size() const {
        return threads_.size();
    }

private:
    std::vector<nixlThread *> threads_;
    std::mutex threadsMutex_;
    std::atomic<bool> started_;
};

class nixlPollerThread : public nixlThread {
public:
    nixlPollerThread() : nixlThread() {}

    virtual ~nixlPollerThread() {}

protected:
    virtual void
    run() override {
        while (!isActive()) {
            poll();
        }
    }

    virtual void
    poll() = 0;
};

class nixlWork {
public:
    nixlWork() {}

    virtual ~nixlWork() {}

    virtual bool
    poll() = 0; // Return true to continue polling, false to stop polling

    virtual void
    destroy() = 0;
};

class nixlWorkQueueThread : public nixlThread {
public:
    nixlWorkQueueThread();
    virtual ~nixlWorkQueueThread();

    void
    queue(nixlWork *work);

    void
    reset();

protected:
    virtual void
    run() override;

    virtual void
    poll() {} // Called on each iteration of the main loop

private:
    std::list<nixlWork *> to_equeue_;
    std::list<nixlWork *> to_poll_;
    std::mutex mutex_;
    std::condition_variable cond_;
};


#endif // _NIXL_THREAD_H
