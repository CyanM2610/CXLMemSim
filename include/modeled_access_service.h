/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */

#ifndef CXLMEMSIM_MODELED_ACCESS_SERVICE_H
#define CXLMEMSIM_MODELED_ACCESS_SERVICE_H

#include "cxl_bulk_model.h"
#include "modeled_access_protocol.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace cxlmemsim {

struct ModeledRegionRequest {
    uint64_t region_id;
    uint64_t capacity;
    uint64_t alignment;
    ModeledAccessProvider provider;
};

struct ModeledRegionResult {
    ModeledAccessStatus status;
    uint64_t server_region_token;
};

struct ModeledAccessRequest {
    uint64_t op_id;
    uint64_t client_id;
    ModeledAccessDirection direction;
    uint64_t server_region_token;
    uint64_t offset;
    uint64_t bytes;
    uint64_t start_ns;
};

struct ModeledAccessBeginResult {
    ModeledAccessStatus status;
    uint64_t access_token;
    uint64_t estimated_complete_ns;
};

struct ModeledAccessPollResult {
    ModeledAccessStatus status;
    ModeledAccessState state;
    uint32_t error_code;
    uint64_t queue_ns;
    uint64_t service_ns;
    uint64_t modeled_complete_ns;
};

struct ModeledAccessRecord {
    uint64_t op_id;
    uint64_t client_id;
    uint64_t access_token;
    uint64_t server_region_token;
    ModeledAccessDirection direction;
    uint64_t offset;
    uint64_t bytes;
    uint64_t start_ns;
    uint64_t service_start_ns;
    uint64_t queue_ns;
    uint64_t service_ns;
    uint64_t modeled_complete_ns;
    uint64_t data_complete_ns;
    ModeledAccessCudaStatus cuda_status;
    ModeledAccessState state;
    uint32_t error_code;
};

struct ModeledAccessStats {
    uint64_t read_operations;
    uint64_t write_operations;
    uint64_t read_bytes;
    uint64_t write_bytes;
    uint64_t queue_depth;
    uint64_t queue_high_watermark;
    uint64_t errors;
    uint64_t cancels;
    uint64_t payload_copy_bytes;
};

class ModeledAccessService {
public:
    struct Config {
        uint64_t provider_capacity;
        uint64_t provider_alignment;
        ModeledAccessProvider provider;
        double read_bandwidth_gbps;
        double write_bandwidth_gbps;
        uint64_t read_latency_ns;
        uint64_t write_latency_ns;
    };

    explicit ModeledAccessService(Config config);

    ModeledRegionResult registerRegion(const ModeledRegionRequest &request);
    ModeledAccessBeginResult beginAccess(const ModeledAccessRequest &request);
    ModeledAccessStatus dataComplete(uint64_t access_token, ModeledAccessCudaStatus cuda_status,
                                     uint64_t data_complete_ns);
    ModeledAccessPollResult pollAccess(uint64_t access_token, uint64_t now_ns);
    ModeledAccessPollResult cancelAccess(uint64_t access_token, const std::string &reason, uint64_t now_ns);
    ModeledAccessStats stats() const;

private:
    void recomputeQueue(ModeledAccessDirection direction);
    void updateState(ModeledAccessRecord &record, uint64_t now_ns);

    Config config_;
    CxlBulkTransferModel timing_model_;
    mutable std::mutex mutex_;
    uint64_t next_region_token_ = 1;
    uint64_t next_access_token_ = 1;
    std::unordered_map<uint64_t, ModeledRegionRequest> regions_;
    std::unordered_map<uint64_t, ModeledAccessRecord> accesses_;
    std::unordered_map<uint64_t, uint64_t> op_tokens_;
    std::vector<uint64_t> read_queue_;
    std::vector<uint64_t> write_queue_;
    ModeledAccessStats stats_{};
};

class ModeledAccessShmServer {
public:
    struct Config {
        std::string control_name;
        ModeledAccessService::Config service;
    };

    explicit ModeledAccessShmServer(Config config);
    ~ModeledAccessShmServer();
    ModeledAccessShmServer(const ModeledAccessShmServer &) = delete;
    ModeledAccessShmServer &operator=(const ModeledAccessShmServer &) = delete;

    bool initialize();
    int pollOnce();
    bool hasPending() const;
    ModeledAccessStats stats() const;
    void shutdown();

private:
    static uint64_t monotonicTimeNs();
    void handle(ModeledAccessSlot &slot);

    Config config_;
    ModeledAccessService service_;
    int control_fd_ = -1;
    bool owns_control_name_ = false;
    ModeledAccessHeader *header_ = nullptr;
};

} // namespace cxlmemsim

#endif
