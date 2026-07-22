/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */

#ifndef CXLMEMSIM_CXL_BULK_SERVER_H
#define CXLMEMSIM_CXL_BULK_SERVER_H

#include "cxl_bulk.h"
#include "cxl_bulk_model.h"

#include <array>
#include <atomic>
#include <cstdint>
#include <string>

struct CxlBulkServerStats {
    uint64_t read_requests;
    uint64_t write_requests;
    uint64_t read_bytes;
    uint64_t write_bytes;
    uint64_t read_cachelines;
    uint64_t write_cachelines;
    uint64_t total_model_latency_ns;
    uint64_t failed_requests;
    uint64_t abandoned_requests;
    uint64_t orphaned_requests;
};

class CxlBulkShmServer {
public:
    struct Config {
        std::string control_name;
        std::string data_name;
        uint64_t data_mapped_size;
        uint64_t data_offset;
        uint64_t data_size;
        double read_bandwidth_gbps;
        double write_bandwidth_gbps;
        uint64_t read_latency_ns;
        uint64_t write_latency_ns;
    };

    explicit CxlBulkShmServer(Config config);
    ~CxlBulkShmServer();

    CxlBulkShmServer(const CxlBulkShmServer &) = delete;
    CxlBulkShmServer &operator=(const CxlBulkShmServer &) = delete;

    bool initialize();
    int poll_once();
    bool has_pending() const;
    CxlBulkServerStats stats() const;
    void shutdown();

private:
    static uint64_t monotonic_time_ns();
    void accept_request(cxl_bulk_slot_t &slot);
    void finish_request(cxl_bulk_slot_t &slot);

    Config config_;
    CxlBulkTransferModel model_;
    int control_fd_ = -1;
    int lock_fd_ = -1;
    cxl_bulk_header_t *header_ = nullptr;
    bool owns_control_name_ = false;
    std::string lock_name_;
    uint64_t server_generation_ = 0;
    std::array<uint64_t, CXL_BULK_MAX_SLOTS> orphan_check_after_ns_{};
    std::atomic<uint64_t> read_requests_{0};
    std::atomic<uint64_t> write_requests_{0};
    std::atomic<uint64_t> read_bytes_{0};
    std::atomic<uint64_t> write_bytes_{0};
    std::atomic<uint64_t> read_cachelines_{0};
    std::atomic<uint64_t> write_cachelines_{0};
    std::atomic<uint64_t> total_model_latency_ns_{0};
    std::atomic<uint64_t> failed_requests_{0};
    std::atomic<uint64_t> abandoned_requests_{0};
    std::atomic<uint64_t> orphaned_requests_{0};
};

#endif
