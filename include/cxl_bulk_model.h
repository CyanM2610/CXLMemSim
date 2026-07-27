/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */

#ifndef CXLMEMSIM_CXL_BULK_MODEL_H
#define CXLMEMSIM_CXL_BULK_MODEL_H

#include <cstdint>
#include <mutex>

enum class CxlBulkDirection { Read, Write };

struct CxlBulkTiming {
    uint64_t service_start_ns;
    uint64_t completion_ns;
    uint64_t model_latency_ns;
    uint64_t serialization_ns;
    uint64_t cacheline_count;
};

struct CxlBulkEstimate {
    uint64_t service_ns;
    uint64_t serialization_ns;
    uint64_t cacheline_count;
};

class CxlBulkTransferModel {
public:
    CxlBulkTransferModel(double read_gbps, double write_gbps, uint64_t read_latency_ns, uint64_t write_latency_ns);

    CxlBulkEstimate estimate(CxlBulkDirection direction, uint64_t bytes, uint64_t byte_offset = 0) const;
    CxlBulkTiming schedule(CxlBulkDirection direction, uint64_t bytes, uint64_t submitted_ns, uint64_t byte_offset = 0);

private:
    double read_gbps_;
    double write_gbps_;
    uint64_t read_latency_ns_;
    uint64_t write_latency_ns_;
    uint64_t next_read_free_ns_ = 0;
    uint64_t next_write_free_ns_ = 0;
    std::mutex mutex_;
};

#endif
