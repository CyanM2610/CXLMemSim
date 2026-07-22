/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */

#include "cxl_bulk_server.h"

#include <cerrno>
#include <csignal>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

namespace {
constexpr uint64_t kOrphanCheckIntervalNs = 100'000'000;
constexpr uint32_t kOwnerPidBits = 24;
constexpr uint64_t kOwnerPidMask = 0xffffff;
constexpr uint64_t kOwnerStartMask = 0xffffffffff;

bool read_process_identity(uint32_t pid, uint64_t &ticks, char &process_state) {
    std::ifstream stat("/proc/" + std::to_string(pid) + "/stat");
    std::string line;
    if (!stat || !std::getline(stat, line)) {
        return false;
    }
    const auto close_paren = line.rfind(')');
    if (close_paren == std::string::npos || close_paren + 2 >= line.size()) {
        return false;
    }
    std::istringstream fields(line.substr(close_paren + 2));
    fields >> process_state;
    std::string value;
    for (int field = 4; field <= 22; ++field) {
        if (!(fields >> value)) {
            return false;
        }
        if (field == 22) {
            try {
                ticks = std::stoull(value);
            } catch (const std::exception &) {
                return false;
            }
            return ticks != 0;
        }
    }
    return false;
}

bool owner_token_is_alive(uint64_t owner_token) {
    const uint32_t pid = owner_token & kOwnerPidMask;
    const uint64_t start_ticks = owner_token >> kOwnerPidBits;
    if (pid == 0 || start_ticks == 0) {
        return false;
    }
    if (pid > static_cast<uint32_t>(std::numeric_limits<pid_t>::max())) {
        return false;
    }
    uint64_t observed_ticks = 0;
    char process_state = '\0';
    if (read_process_identity(pid, observed_ticks, process_state)) {
        return process_state != 'Z' && process_state != 'X' && (observed_ticks & kOwnerStartMask) == start_ticks;
    }
    if (kill(static_cast<pid_t>(pid), 0) == 0 || errno == EPERM) {
        return true;
    }
    return errno != ESRCH;
}
} // namespace

CxlBulkShmServer::CxlBulkShmServer(Config config)
    : config_(std::move(config)), model_(config_.read_bandwidth_gbps, config_.write_bandwidth_gbps,
                                         config_.read_latency_ns, config_.write_latency_ns),
      lock_name_(config_.control_name + ".lock") {}

CxlBulkShmServer::~CxlBulkShmServer() { shutdown(); }

bool CxlBulkShmServer::initialize() {
    if (header_ != nullptr || config_.control_name.empty() || config_.data_name.empty() || config_.data_size == 0 ||
        config_.data_offset > config_.data_mapped_size ||
        config_.data_size > config_.data_mapped_size - config_.data_offset ||
        config_.data_name.size() >= CXL_BULK_DATA_NAME_SIZE) {
        return false;
    }

    lock_fd_ = shm_open(lock_name_.c_str(), O_CREAT | O_RDWR, 0660);
    if (lock_fd_ < 0 || flock(lock_fd_, LOCK_EX | LOCK_NB) != 0) {
        if (lock_fd_ >= 0) {
            close(lock_fd_);
            lock_fd_ = -1;
        }
        return false;
    }
    if (ftruncate(lock_fd_, sizeof(server_generation_)) != 0 ||
        pwrite(lock_fd_, &server_generation_, sizeof(server_generation_), 0) !=
            static_cast<ssize_t>(sizeof(server_generation_))) {
        shutdown();
        return false;
    }

    shm_unlink(config_.control_name.c_str());
    control_fd_ = shm_open(config_.control_name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0660);
    if (control_fd_ < 0) {
        shutdown();
        return false;
    }
    owns_control_name_ = true;
    if (ftruncate(control_fd_, sizeof(cxl_bulk_header_t)) != 0) {
        shutdown();
        return false;
    }
    void *mapping = mmap(nullptr, sizeof(cxl_bulk_header_t), PROT_READ | PROT_WRITE, MAP_SHARED, control_fd_, 0);
    if (mapping == MAP_FAILED) {
        shutdown();
        return false;
    }
    header_ = static_cast<cxl_bulk_header_t *>(mapping);
    std::memset(header_, 0, sizeof(*header_));
    header_->magic = CXL_BULK_MAGIC;
    header_->version = CXL_BULK_VERSION;
    header_->header_size = sizeof(*header_);
    header_->slot_count = CXL_BULK_MAX_SLOTS;
    header_->cacheline_size = CXL_BULK_CACHELINE_SIZE;
    server_generation_ = monotonic_time_ns() ^ (static_cast<uint64_t>(getpid()) << 32);
    if (server_generation_ == 0) {
        server_generation_ = 1;
    }
    header_->server_generation = server_generation_;
    header_->data_mapped_size = config_.data_mapped_size;
    header_->data_offset = config_.data_offset;
    header_->data_size = config_.data_size;
    header_->read_bandwidth_gbps = config_.read_bandwidth_gbps;
    header_->write_bandwidth_gbps = config_.write_bandwidth_gbps;
    header_->read_latency_ns = config_.read_latency_ns;
    header_->write_latency_ns = config_.write_latency_ns;
    std::memcpy(header_->data_name, config_.data_name.c_str(), config_.data_name.size() + 1);
    __atomic_store_n(&header_->server_ready, 1U, __ATOMIC_RELEASE);
    if (pwrite(lock_fd_, &server_generation_, sizeof(server_generation_), 0) !=
        static_cast<ssize_t>(sizeof(server_generation_))) {
        shutdown();
        return false;
    }
    return true;
}

uint64_t CxlBulkShmServer::monotonic_time_ns() {
    timespec value{};
    clock_gettime(CLOCK_MONOTONIC, &value);
    return static_cast<uint64_t>(value.tv_sec) * 1'000'000'000ULL + static_cast<uint64_t>(value.tv_nsec);
}

void CxlBulkShmServer::accept_request(cxl_bulk_slot_t &slot) {
    const bool valid_operation = slot.operation == CXL_BULK_OP_READ || slot.operation == CXL_BULK_OP_WRITE;
    const bool valid_range =
        slot.size != 0 && slot.offset <= config_.data_size && slot.size <= config_.data_size - slot.offset;
    if (!valid_operation || !valid_range) {
        slot.status = valid_operation ? CXL_BULK_STATUS_OUT_OF_RANGE : CXL_BULK_STATUS_INVALID_REQUEST;
        slot.completion_ns = slot.submitted_ns;
        failed_requests_.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    const auto direction = slot.operation == CXL_BULK_OP_READ ? CxlBulkDirection::Read : CxlBulkDirection::Write;
    const auto timing = model_.schedule(direction, slot.size, slot.submitted_ns, slot.offset);
    slot.status = CXL_BULK_STATUS_OK;
    slot.service_start_ns = timing.service_start_ns;
    slot.completion_ns = timing.completion_ns;
    slot.model_latency_ns = timing.model_latency_ns;
    slot.serialization_ns = timing.serialization_ns;
    slot.cacheline_count = timing.cacheline_count;

    total_model_latency_ns_.fetch_add(timing.model_latency_ns, std::memory_order_relaxed);
    if (direction == CxlBulkDirection::Read) {
        read_requests_.fetch_add(1, std::memory_order_relaxed);
        read_bytes_.fetch_add(slot.size, std::memory_order_relaxed);
        read_cachelines_.fetch_add(slot.cacheline_count, std::memory_order_relaxed);
    } else {
        write_requests_.fetch_add(1, std::memory_order_relaxed);
        write_bytes_.fetch_add(slot.size, std::memory_order_relaxed);
        write_cachelines_.fetch_add(slot.cacheline_count, std::memory_order_relaxed);
    }
}

void CxlBulkShmServer::finish_request(cxl_bulk_slot_t &slot) {
    __atomic_thread_fence(__ATOMIC_RELEASE);
    if (__atomic_load_n(&slot.abandoned, __ATOMIC_ACQUIRE) != 0) {
        abandoned_requests_.fetch_add(1, std::memory_order_relaxed);
        if (slot.status == CXL_BULK_STATUS_OK) {
            failed_requests_.fetch_add(1, std::memory_order_relaxed);
        }
        __atomic_store_n(&slot.state, CXL_BULK_SLOT_FREE, __ATOMIC_RELEASE);
        __atomic_store_n(&slot.owner_token, 0, __ATOMIC_RELEASE);
    } else {
        __atomic_store_n(&slot.state, CXL_BULK_SLOT_COMPLETE, __ATOMIC_RELEASE);
    }
}

int CxlBulkShmServer::poll_once() {
    if (header_ == nullptr) {
        return 0;
    }
    int processed = 0;
    const uint64_t now = monotonic_time_ns();
    for (uint32_t index = 0; index < header_->slot_count; ++index) {
        cxl_bulk_slot_t &slot = header_->slots[index];
        const uint32_t initial_state = __atomic_load_n(&slot.state, __ATOMIC_ACQUIRE);
        const uint64_t owner_token = __atomic_load_n(&slot.owner_token, __ATOMIC_ACQUIRE);
        if (initial_state == CXL_BULK_SLOT_FREE) {
            if (owner_token == 0) {
                orphan_check_after_ns_[index] = 0;
                continue;
            }
            if (orphan_check_after_ns_[index] == 0) {
                orphan_check_after_ns_[index] = now + kOrphanCheckIntervalNs;
            } else if (now >= orphan_check_after_ns_[index]) {
                if (!owner_token_is_alive(owner_token)) {
                    uint64_t expected_owner = owner_token;
                    if (__atomic_compare_exchange_n(&slot.owner_token, &expected_owner, 0, false, __ATOMIC_ACQ_REL,
                                                    __ATOMIC_ACQUIRE)) {
                        failed_requests_.fetch_add(1, std::memory_order_relaxed);
                        orphaned_requests_.fetch_add(1, std::memory_order_relaxed);
                        orphan_check_after_ns_[index] = 0;
                        ++processed;
                        continue;
                    }
                }
                orphan_check_after_ns_[index] = now + kOrphanCheckIntervalNs;
            }
            continue;
        }
        if (initial_state == CXL_BULK_SLOT_COMPLETE && __atomic_load_n(&slot.abandoned, __ATOMIC_ACQUIRE) != 0) {
            uint32_t expected = CXL_BULK_SLOT_COMPLETE;
            if (__atomic_compare_exchange_n(&slot.state, &expected, CXL_BULK_SLOT_FREE, false, __ATOMIC_ACQ_REL,
                                            __ATOMIC_ACQUIRE)) {
                abandoned_requests_.fetch_add(1, std::memory_order_relaxed);
                if (slot.status == CXL_BULK_STATUS_OK) {
                    failed_requests_.fetch_add(1, std::memory_order_relaxed);
                }
                uint64_t expected_owner = owner_token;
                __atomic_compare_exchange_n(&slot.owner_token, &expected_owner, 0, false, __ATOMIC_ACQ_REL,
                                            __ATOMIC_ACQUIRE);
                orphan_check_after_ns_[index] = 0;
                ++processed;
                continue;
            }
        }
        if (initial_state == CXL_BULK_SLOT_RESERVED || initial_state == CXL_BULK_SLOT_SUBMITTED ||
            initial_state == CXL_BULK_SLOT_IN_PROGRESS || initial_state == CXL_BULK_SLOT_COMPLETE) {
            if (orphan_check_after_ns_[index] == 0) {
                orphan_check_after_ns_[index] = now + kOrphanCheckIntervalNs;
            } else if (now >= orphan_check_after_ns_[index]) {
                if (!owner_token_is_alive(owner_token)) {
                    uint32_t expected = initial_state;
                    if (__atomic_compare_exchange_n(&slot.state, &expected, CXL_BULK_SLOT_FREE, false, __ATOMIC_ACQ_REL,
                                                    __ATOMIC_ACQUIRE)) {
                        failed_requests_.fetch_add(1, std::memory_order_relaxed);
                        orphaned_requests_.fetch_add(1, std::memory_order_relaxed);
                        uint64_t expected_owner = owner_token;
                        __atomic_compare_exchange_n(&slot.owner_token, &expected_owner, 0, false, __ATOMIC_ACQ_REL,
                                                    __ATOMIC_ACQUIRE);
                        orphan_check_after_ns_[index] = 0;
                        ++processed;
                        continue;
                    }
                }
                orphan_check_after_ns_[index] = now + kOrphanCheckIntervalNs;
            }
        }
        uint32_t expected = CXL_BULK_SLOT_SUBMITTED;
        if (__atomic_compare_exchange_n(&slot.state, &expected, CXL_BULK_SLOT_IN_PROGRESS, false, __ATOMIC_ACQ_REL,
                                        __ATOMIC_ACQUIRE)) {
            accept_request(slot);
            ++processed;
        }
        if (__atomic_load_n(&slot.state, __ATOMIC_ACQUIRE) != CXL_BULK_SLOT_IN_PROGRESS) {
            continue;
        }
        const bool copy_done = __atomic_load_n(&slot.copy_done, __ATOMIC_ACQUIRE) != 0;
        if (copy_done && now >= slot.completion_ns) {
            finish_request(slot);
            ++processed;
        }
    }
    return processed;
}

bool CxlBulkShmServer::has_pending() const {
    if (header_ == nullptr) {
        return false;
    }
    for (uint32_t index = 0; index < header_->slot_count; ++index) {
        const uint32_t state = __atomic_load_n(&header_->slots[index].state, __ATOMIC_ACQUIRE);
        if (state != CXL_BULK_SLOT_FREE && state != CXL_BULK_SLOT_COMPLETE) {
            return true;
        }
    }
    return false;
}

CxlBulkServerStats CxlBulkShmServer::stats() const {
    return {
        .read_requests = read_requests_.load(std::memory_order_relaxed),
        .write_requests = write_requests_.load(std::memory_order_relaxed),
        .read_bytes = read_bytes_.load(std::memory_order_relaxed),
        .write_bytes = write_bytes_.load(std::memory_order_relaxed),
        .read_cachelines = read_cachelines_.load(std::memory_order_relaxed),
        .write_cachelines = write_cachelines_.load(std::memory_order_relaxed),
        .total_model_latency_ns = total_model_latency_ns_.load(std::memory_order_relaxed),
        .failed_requests = failed_requests_.load(std::memory_order_relaxed),
        .abandoned_requests = abandoned_requests_.load(std::memory_order_relaxed),
        .orphaned_requests = orphaned_requests_.load(std::memory_order_relaxed),
    };
}

void CxlBulkShmServer::shutdown() {
    if (header_ != nullptr) {
        __atomic_store_n(&header_->server_ready, 0U, __ATOMIC_RELEASE);
        for (uint32_t index = 0; index < header_->slot_count; ++index) {
            cxl_bulk_slot_t &slot = header_->slots[index];
            const uint32_t state = __atomic_load_n(&slot.state, __ATOMIC_ACQUIRE);
            if (state == CXL_BULK_SLOT_SUBMITTED || state == CXL_BULK_SLOT_IN_PROGRESS) {
                slot.status = CXL_BULK_STATUS_SERVER_STOPPED;
                finish_request(slot);
            }
        }
        munmap(header_, sizeof(*header_));
        header_ = nullptr;
    }
    server_generation_ = 0;
    if (lock_fd_ >= 0) {
        pwrite(lock_fd_, &server_generation_, sizeof(server_generation_), 0);
    }
    if (owns_control_name_ && !config_.control_name.empty()) {
        shm_unlink(config_.control_name.c_str());
        owns_control_name_ = false;
    }
    if (control_fd_ >= 0) {
        close(control_fd_);
        control_fd_ = -1;
    }
    if (lock_fd_ >= 0) {
        flock(lock_fd_, LOCK_UN);
        close(lock_fd_);
        lock_fd_ = -1;
    }
}
