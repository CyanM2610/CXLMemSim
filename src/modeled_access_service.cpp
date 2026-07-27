/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */

#include "modeled_access_service.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace cxlmemsim {

namespace {

CxlBulkDirection bulkDirection(ModeledAccessDirection direction) {
    return direction == ModeledAccessDirection::ReadFromCxl ? CxlBulkDirection::Read : CxlBulkDirection::Write;
}

bool validDirection(ModeledAccessDirection direction) {
    return direction == ModeledAccessDirection::ReadFromCxl || direction == ModeledAccessDirection::WriteToCxl;
}

ModeledAccessPollResult pollResult(ModeledAccessStatus status, const ModeledAccessRecord *record = nullptr) {
    if (record == nullptr) {
        return {status, ModeledAccessState::Error, 0, 0, 0, 0};
    }
    return {status,           record->state,      record->error_code,
            record->queue_ns, record->service_ns, record->modeled_complete_ns};
}

} // namespace

ModeledAccessService::ModeledAccessService(Config config)
    : config_(config), timing_model_(config.read_bandwidth_gbps, config.write_bandwidth_gbps, config.read_latency_ns,
                                     config.write_latency_ns) {
    if (config_.provider_capacity == 0 || config_.provider_alignment == 0 ||
        config_.provider != ModeledAccessProvider::PosixShm) {
        throw std::invalid_argument("invalid modeled-access provider configuration");
    }
}

ModeledRegionResult ModeledAccessService::registerRegion(const ModeledRegionRequest &request) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (request.region_id == 0 || request.capacity == 0 || request.alignment == 0 ||
        request.provider != config_.provider) {
        return {ModeledAccessStatus::InvalidRequest, 0};
    }
    if (request.capacity > config_.provider_capacity || request.alignment > config_.provider_alignment ||
        config_.provider_alignment % request.alignment != 0) {
        return {ModeledAccessStatus::OutOfRange, 0};
    }
    for (const auto &[token, existing] : regions_) {
        if (existing.region_id == request.region_id) {
            if (existing.capacity == request.capacity && existing.alignment == request.alignment &&
                existing.provider == request.provider) {
                return {ModeledAccessStatus::Ok, token};
            }
            return {ModeledAccessStatus::Duplicate, 0};
        }
    }
    const uint64_t token = next_region_token_++;
    regions_.emplace(token, request);
    return {ModeledAccessStatus::Ok, token};
}

ModeledAccessBeginResult ModeledAccessService::beginAccess(const ModeledAccessRequest &request) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (request.op_id == 0 || request.client_id == 0 || request.start_ns == 0 || request.bytes == 0 ||
        !validDirection(request.direction)) {
        return {ModeledAccessStatus::InvalidRequest, 0, 0};
    }
    const auto duplicate = op_tokens_.find(request.op_id);
    if (duplicate != op_tokens_.end()) {
        const auto &record = accesses_.at(duplicate->second);
        return {ModeledAccessStatus::Duplicate, record.access_token, record.modeled_complete_ns};
    }
    const auto region = regions_.find(request.server_region_token);
    if (region == regions_.end()) {
        return {ModeledAccessStatus::NotFound, 0, 0};
    }
    if (request.offset > region->second.capacity || request.bytes > region->second.capacity - request.offset) {
        return {ModeledAccessStatus::OutOfRange, 0, 0};
    }

    const auto estimate = timing_model_.estimate(bulkDirection(request.direction), request.bytes, request.offset);
    if (estimate.service_ns > std::numeric_limits<uint64_t>::max() - request.start_ns) {
        return {ModeledAccessStatus::OutOfRange, 0, 0};
    }
    const uint64_t token = next_access_token_++;
    ModeledAccessRecord record{
        .op_id = request.op_id,
        .client_id = request.client_id,
        .access_token = token,
        .server_region_token = request.server_region_token,
        .direction = request.direction,
        .offset = request.offset,
        .bytes = request.bytes,
        .start_ns = request.start_ns,
        .service_start_ns = request.start_ns,
        .queue_ns = 0,
        .service_ns = estimate.service_ns,
        .modeled_complete_ns = request.start_ns + estimate.service_ns,
        .data_complete_ns = 0,
        .cuda_status = ModeledAccessCudaStatus::Pending,
        .state = ModeledAccessState::Pending,
        .error_code = 0,
    };
    accesses_.emplace(token, record);
    op_tokens_.emplace(request.op_id, token);
    auto &queue = request.direction == ModeledAccessDirection::ReadFromCxl ? read_queue_ : write_queue_;
    queue.push_back(token);
    recomputeQueue(request.direction);
    stats_.queue_high_watermark = std::max<uint64_t>(stats_.queue_high_watermark, queue.size());
    if (request.direction == ModeledAccessDirection::ReadFromCxl) {
        ++stats_.read_operations;
        stats_.read_bytes += request.bytes;
    } else {
        ++stats_.write_operations;
        stats_.write_bytes += request.bytes;
    }
    const auto &scheduled = accesses_.at(token);
    std::clog << "MODELED_ACCESS begin op=" << request.op_id << " client=" << request.client_id
              << " direction=" << static_cast<uint32_t>(request.direction) << " region=" << request.server_region_token
              << " offset=" << request.offset << " bytes=" << request.bytes << " queue_ns=" << scheduled.queue_ns
              << " service_ns=" << scheduled.service_ns << " modeled_complete_ns=" << scheduled.modeled_complete_ns
              << " payload_copy_bytes=0\n";
    return {ModeledAccessStatus::Ok, token, scheduled.modeled_complete_ns};
}

ModeledAccessStatus ModeledAccessService::dataComplete(uint64_t access_token, ModeledAccessCudaStatus cuda_status,
                                                       uint64_t data_complete_ns) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = accesses_.find(access_token);
    if (found == accesses_.end()) {
        return ModeledAccessStatus::NotFound;
    }
    auto &record = found->second;
    if (cuda_status != ModeledAccessCudaStatus::Success && cuda_status != ModeledAccessCudaStatus::Error) {
        return ModeledAccessStatus::InvalidRequest;
    }
    if (data_complete_ns < record.start_ns) {
        return ModeledAccessStatus::InvalidRequest;
    }
    if (record.cuda_status != ModeledAccessCudaStatus::Pending) {
        return record.cuda_status == cuda_status && record.data_complete_ns == data_complete_ns
                   ? ModeledAccessStatus::Ok
                   : ModeledAccessStatus::Duplicate;
    }
    if (record.state == ModeledAccessState::Cancelled) {
        return ModeledAccessStatus::Duplicate;
    }
    record.cuda_status = cuda_status;
    record.data_complete_ns = data_complete_ns;
    updateState(record, data_complete_ns);
    return ModeledAccessStatus::Ok;
}

ModeledAccessPollResult ModeledAccessService::pollAccess(uint64_t access_token, uint64_t now_ns) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = accesses_.find(access_token);
    if (found == accesses_.end()) {
        return pollResult(ModeledAccessStatus::NotFound);
    }
    updateState(found->second, now_ns);
    return pollResult(ModeledAccessStatus::Ok, &found->second);
}

ModeledAccessPollResult ModeledAccessService::cancelAccess(uint64_t access_token, const std::string &reason,
                                                           uint64_t now_ns) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto found = accesses_.find(access_token);
    if (found == accesses_.end()) {
        return pollResult(ModeledAccessStatus::NotFound);
    }
    auto &record = found->second;
    updateState(record, now_ns);
    if (record.state == ModeledAccessState::Complete || record.state == ModeledAccessState::Error ||
        record.state == ModeledAccessState::Cancelled) {
        return pollResult(ModeledAccessStatus::Ok, &record);
    }
    record.state = ModeledAccessState::Cancelled;
    ++stats_.cancels;
    auto &queue = record.direction == ModeledAccessDirection::ReadFromCxl ? read_queue_ : write_queue_;
    queue.erase(std::remove(queue.begin(), queue.end(), access_token), queue.end());
    recomputeQueue(record.direction);
    std::clog << "MODELED_ACCESS cancel op=" << record.op_id << " token=" << access_token << " reason=" << reason
              << " payload_copy_bytes=0\n";
    return pollResult(ModeledAccessStatus::Ok, &record);
}

ModeledAccessStats ModeledAccessService::stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto result = stats_;
    result.queue_depth = 0;
    for (const auto &[token, record] : accesses_) {
        (void)token;
        if (record.state == ModeledAccessState::Pending || record.state == ModeledAccessState::ModeledComplete ||
            record.state == ModeledAccessState::DataComplete) {
            ++result.queue_depth;
        }
    }
    return result;
}

void ModeledAccessService::recomputeQueue(ModeledAccessDirection direction) {
    auto &queue = direction == ModeledAccessDirection::ReadFromCxl ? read_queue_ : write_queue_;
    uint64_t next_free_ns = 0;
    for (uint64_t token : queue) {
        auto &record = accesses_.at(token);
        const auto estimate = timing_model_.estimate(bulkDirection(direction), record.bytes, record.offset);
        record.service_start_ns = std::max(record.start_ns, next_free_ns);
        record.queue_ns = record.service_start_ns - record.start_ns;
        if (record.service_ns > std::numeric_limits<uint64_t>::max() - record.service_start_ns) {
            throw std::overflow_error("modeled-access completion timestamp overflow");
        }
        record.modeled_complete_ns = record.service_start_ns + record.service_ns;
        if (estimate.serialization_ns > std::numeric_limits<uint64_t>::max() - record.service_start_ns) {
            throw std::overflow_error("modeled-access queue timestamp overflow");
        }
        next_free_ns = record.service_start_ns + estimate.serialization_ns;
    }
}

void ModeledAccessService::updateState(ModeledAccessRecord &record, uint64_t now_ns) {
    if (record.state == ModeledAccessState::Cancelled || record.state == ModeledAccessState::Complete ||
        record.state == ModeledAccessState::Error) {
        return;
    }
    if (record.cuda_status == ModeledAccessCudaStatus::Error) {
        record.state = ModeledAccessState::Error;
        record.error_code = 1;
        ++stats_.errors;
        return;
    }
    const bool modeled_done = now_ns >= record.modeled_complete_ns;
    const bool data_done = record.cuda_status == ModeledAccessCudaStatus::Success;
    if (modeled_done && data_done) {
        record.state = ModeledAccessState::Complete;
    } else if (modeled_done) {
        record.state = ModeledAccessState::ModeledComplete;
    } else if (data_done) {
        record.state = ModeledAccessState::DataComplete;
    } else {
        record.state = ModeledAccessState::Pending;
    }
}

ModeledAccessShmServer::ModeledAccessShmServer(Config config) : config_(std::move(config)), service_(config_.service) {}

ModeledAccessShmServer::~ModeledAccessShmServer() { shutdown(); }

bool ModeledAccessShmServer::initialize() {
    if (header_ != nullptr || config_.control_name.empty() || config_.control_name[0] != '/') {
        return false;
    }
    shm_unlink(config_.control_name.c_str());
    control_fd_ = shm_open(config_.control_name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0660);
    if (control_fd_ < 0) {
        return false;
    }
    owns_control_name_ = true;
    if (ftruncate(control_fd_, sizeof(ModeledAccessHeader)) != 0) {
        shutdown();
        return false;
    }
    void *mapping = mmap(nullptr, sizeof(ModeledAccessHeader), PROT_READ | PROT_WRITE, MAP_SHARED, control_fd_, 0);
    if (mapping == MAP_FAILED) {
        shutdown();
        return false;
    }
    header_ = static_cast<ModeledAccessHeader *>(mapping);
    std::memset(header_, 0, sizeof(*header_));
    header_->magic = kModeledAccessMagic;
    header_->version = kModeledAccessVersion;
    header_->header_size = sizeof(*header_);
    header_->endian_marker = kModeledAccessEndianMarker;
    header_->slot_count = kModeledAccessMaxSlots;
    header_->capability_bits = kGpuDirectModeledAccessV1;
    header_->server_generation = monotonicTimeNs() ^ (static_cast<uint64_t>(getpid()) << 32);
    if (header_->server_generation == 0) {
        header_->server_generation = 1;
    }
    header_->data_capacity = config_.service.provider_capacity;
    header_->data_alignment = config_.service.provider_alignment;
    header_->provider_kind = static_cast<uint32_t>(config_.service.provider);
    __atomic_store_n(&header_->server_ready, 1U, __ATOMIC_RELEASE);
    return true;
}

uint64_t ModeledAccessShmServer::monotonicTimeNs() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

int ModeledAccessShmServer::pollOnce() {
    if (header_ == nullptr) {
        return 0;
    }
    int processed = 0;
    for (uint32_t index = 0; index < kModeledAccessMaxSlots; ++index) {
        auto &slot = header_->slots[index];
        uint32_t expected = static_cast<uint32_t>(ModeledAccessSlotState::Submitted);
        if (!__atomic_compare_exchange_n(&slot.state, &expected,
                                         static_cast<uint32_t>(ModeledAccessSlotState::Reserved), false,
                                         __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
            continue;
        }
        handle(slot);
        uint32_t expected_reserved = static_cast<uint32_t>(ModeledAccessSlotState::Reserved);
        if (!__atomic_compare_exchange_n(&slot.state, &expected_reserved,
                                         static_cast<uint32_t>(ModeledAccessSlotState::Complete), false,
                                         __ATOMIC_RELEASE, __ATOMIC_ACQUIRE) &&
            expected_reserved == static_cast<uint32_t>(ModeledAccessSlotState::Abandoned)) {
            __atomic_store_n(&slot.state, static_cast<uint32_t>(ModeledAccessSlotState::Free), __ATOMIC_RELEASE);
        }
        ++processed;
    }
    return processed;
}

bool ModeledAccessShmServer::hasPending() const {
    if (header_ == nullptr) {
        return false;
    }
    for (uint32_t index = 0; index < kModeledAccessMaxSlots; ++index) {
        const uint32_t state = __atomic_load_n(&header_->slots[index].state, __ATOMIC_ACQUIRE);
        if (state == static_cast<uint32_t>(ModeledAccessSlotState::Submitted) ||
            state == static_cast<uint32_t>(ModeledAccessSlotState::Reserved)) {
            return true;
        }
    }
    return false;
}

ModeledAccessStats ModeledAccessShmServer::stats() const { return service_.stats(); }

void ModeledAccessShmServer::handle(ModeledAccessSlot &slot) {
    slot.status = static_cast<uint32_t>(ModeledAccessStatus::InvalidRequest);
    try {
        switch (static_cast<ModeledAccessOperation>(slot.operation)) {
        case ModeledAccessOperation::RegisterRegion: {
            const auto result = service_.registerRegion({slot.region_id, slot.capacity, slot.alignment,
                                                         static_cast<ModeledAccessProvider>(slot.provider_kind)});
            slot.status = static_cast<uint32_t>(result.status);
            slot.server_region_token = result.server_region_token;
            break;
        }
        case ModeledAccessOperation::BeginAccess: {
            const auto result =
                service_.beginAccess({slot.op_id, slot.client_id, static_cast<ModeledAccessDirection>(slot.direction),
                                      slot.server_region_token, slot.offset, slot.bytes, slot.start_ns});
            slot.status = static_cast<uint32_t>(result.status);
            slot.access_token = result.access_token;
            slot.modeled_complete_ns = result.estimated_complete_ns;
            break;
        }
        case ModeledAccessOperation::DataComplete:
            slot.status = static_cast<uint32_t>(service_.dataComplete(
                slot.access_token, static_cast<ModeledAccessCudaStatus>(slot.cuda_status), slot.data_complete_ns));
            break;
        case ModeledAccessOperation::PollAccess: {
            const auto result = service_.pollAccess(slot.access_token, monotonicTimeNs());
            slot.status = static_cast<uint32_t>(result.status);
            slot.terminal_state = static_cast<uint32_t>(result.state);
            slot.error_code = result.error_code;
            slot.queue_ns = result.queue_ns;
            slot.service_ns = result.service_ns;
            slot.modeled_complete_ns = result.modeled_complete_ns;
            break;
        }
        case ModeledAccessOperation::CancelAccess: {
            if (std::memchr(slot.reason, '\0', sizeof(slot.reason)) == nullptr) {
                break;
            }
            const auto result = service_.cancelAccess(slot.access_token, slot.reason, monotonicTimeNs());
            slot.status = static_cast<uint32_t>(result.status);
            slot.terminal_state = static_cast<uint32_t>(result.state);
            break;
        }
        default:
            slot.status = static_cast<uint32_t>(ModeledAccessStatus::Unsupported);
            break;
        }
    } catch (const std::exception &) {
        slot.status = static_cast<uint32_t>(ModeledAccessStatus::InternalError);
    }
}

void ModeledAccessShmServer::shutdown() {
    if (header_ != nullptr) {
        __atomic_store_n(&header_->server_ready, 0U, __ATOMIC_RELEASE);
        for (uint32_t index = 0; index < kModeledAccessMaxSlots; ++index) {
            auto &slot = header_->slots[index];
            const uint32_t state = __atomic_load_n(&slot.state, __ATOMIC_ACQUIRE);
            if (state != static_cast<uint32_t>(ModeledAccessSlotState::Free) &&
                state != static_cast<uint32_t>(ModeledAccessSlotState::Complete)) {
                slot.status = static_cast<uint32_t>(ModeledAccessStatus::ServerStopped);
                __atomic_store_n(&slot.state, static_cast<uint32_t>(ModeledAccessSlotState::Complete),
                                 __ATOMIC_RELEASE);
            }
        }
        munmap(header_, sizeof(*header_));
        header_ = nullptr;
    }
    if (control_fd_ >= 0) {
        close(control_fd_);
        control_fd_ = -1;
    }
    if (owns_control_name_) {
        shm_unlink(config_.control_name.c_str());
        owns_control_name_ = false;
    }
}

} // namespace cxlmemsim
