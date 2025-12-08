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

#ifndef POSIX_BACKEND_H
#define POSIX_BACKEND_H

#include <memory>
#include <string>
#include <vector>
#include <absl/strings/str_format.h>
#include "backend/backend_engine.h"
#include "io_queue.h"
#include "nixl_thread.h"

class nixlPosixBackendReqH;

class nixlPosixWork : public nixlWork {
public:
    nixlPosixWork(nixlPosixBackendReqH *req_h);
    virtual ~nixlPosixWork() = default;

    virtual bool
    poll() override;

    virtual void
    destroy() override;

private:
    nixlPosixBackendReqH *req_h_;
};

class nixlPosixWorkQueueThread : public nixlWorkQueueThread {
public:
    nixlPosixWorkQueueThread(const std::string &io_queue_type, uint32_t max_ios);
    ~nixlPosixWorkQueueThread() = default;

    nixlPosixIOQueue *
    ioQueue() const {
        return io_queue_.get(); // Return the io queue
    }

    virtual void
    poll() override;

private:
    nixl_status_t
    initIoQueue(const std::string &io_queue_type, uint32_t max_ios);

    std::unique_ptr<nixlPosixIOQueue> io_queue_;
};


class nixlPosixBackendReqH : public nixlBackendReqH {
    enum class ReqState {
        IDLE,
        ENQUEUEING,
        IN_PROGRESS,
        COMPLETED,
    };
private:
    const nixl_xfer_op_t &operation; // The transfer operation (read/write)
    const nixl_meta_dlist_t &local; // Local memory descriptor list
    const nixl_meta_dlist_t &remote; // Remote memory descriptor list
    const nixl_opt_b_args_t *opt_args; // Optional backend-specific arguments
    const nixl_b_params_t *custom_params_; // Custom backend parameters
    const int queue_depth_; // Queue depth for async I/O
    std::atomic<ReqState> req_state_; // Request state
    size_t num_confirmed_ios_; // Number of confirmed IOs
    size_t num_submitted_ios_; // Number of submitted IOs
    nixlPosixWork work_; // Work item for the backend request
    nixlPosixWorkQueueThread *thread_; // Thread where the work is queued

    void
    ioDone(uint32_t data_size, int error);
    static void
    ioDoneClb(void *ctx, uint32_t data_size, int error);

    bool
    enqueueXfer();

public:
    nixlPosixBackendReqH(const nixl_xfer_op_t &operation,
                         const nixl_meta_dlist_t &local,
                         const nixl_meta_dlist_t &remote,
                         const nixl_opt_b_args_t *opt_args,
                         const nixl_b_params_t *custom_params);
    ~nixlPosixBackendReqH(){};

    void
    queue(nixlPosixWorkQueueThread *thread) {
        thread_ = thread;
        num_submitted_ios_ = 0;
        num_confirmed_ios_ = 0;
        req_state_ = ReqState::ENQUEUEING;
        thread_->queue(&work_);
    }

    bool
    pollXfer();
    nixl_status_t
    checkXfer();

    // Exception classes
    class exception : public std::exception {
    private:
        const nixl_status_t code_;

    public:
        exception(const std::string &msg, nixl_status_t code) : std::exception(), code_(code) {}

        nixl_status_t
        code() const noexcept {
            return code_;
        }
    };
};

class nixlPosixEngine : public nixlBackendEngine {
private:
    const char *io_queue_type_;
    uint32_t num_threads_;
    uint32_t max_ios_;
    nixlThreadStaticPool<nixlPosixWorkQueueThread> thread_pool_;

public:
    nixlPosixEngine(const nixlBackendInitParams *init_params);
    virtual ~nixlPosixEngine() = default;

    bool
    supportsRemote() const override {
        return false;
    }

    bool
    supportsLocal() const override {
        return true;
    }

    bool
    supportsNotif() const override {
        return false;
    }

    nixl_mem_list_t
    getSupportedMems() const override {
        return {FILE_SEG, DRAM_SEG};
    }

    nixl_status_t
    registerMem(const nixlBlobDesc &mem, const nixl_mem_t &nixl_mem, nixlBackendMD *&out) override;

    nixl_status_t
    deregisterMem(nixlBackendMD *meta) override;

    nixl_status_t
    connect(const std::string &remote_agent) override {
        return NIXL_SUCCESS;
    }

    nixl_status_t
    disconnect(const std::string &remote_agent) override {
        return NIXL_SUCCESS;
    }

    nixl_status_t
    unloadMD(nixlBackendMD *input) override {
        return NIXL_SUCCESS;
    }

    nixl_status_t
    prepXfer(const nixl_xfer_op_t &operation,
             const nixl_meta_dlist_t &local,
             const nixl_meta_dlist_t &remote,
             const std::string &remote_agent,
             nixlBackendReqH *&handle,
             const nixl_opt_b_args_t *opt_args = nullptr) const override;

    nixl_status_t
    postXfer(const nixl_xfer_op_t &operation,
             const nixl_meta_dlist_t &local,
             const nixl_meta_dlist_t &remote,
             const std::string &remote_agent,
             nixlBackendReqH *&handle,
             const nixl_opt_b_args_t *opt_args = nullptr) const override;

    nixl_status_t
    checkXfer(nixlBackendReqH *handle) const override;
    nixl_status_t
    releaseReqH(nixlBackendReqH *handle) const override;

    nixl_status_t
    queryMem(const nixl_reg_dlist_t &descs, std::vector<nixl_query_resp_t> &resp) const override;

    nixl_status_t
    loadLocalMD(nixlBackendMD *input, nixlBackendMD *&output) override {
        output = input;
        return NIXL_SUCCESS;
    }
};

#endif // POSIX_BACKEND_H
