/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */

#include "cxl_bulk_model.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>

namespace {
constexpr uint64_t kCachelineBytes = 64;

uint64_t divide_round_up(uint64_t numerator, uint64_t denominator) {
    return numerator / denominator + (numerator % denominator != 0);
}

uint64_t cacheline_count(uint64_t bytes, uint64_t byte_offset) {
    const uint64_t line_offset = byte_offset % kCachelineBytes;
    const uint64_t full_lines = bytes / kCachelineBytes;
    const uint64_t trailing_bytes = bytes % kCachelineBytes + line_offset;
    return full_lines + divide_round_up(trailing_bytes, kCachelineBytes);
}

uint64_t serialization_time_ns(uint64_t bytes, double bandwidth_gbps) {
    const double duration = std::ceil(static_cast<double>(bytes) / bandwidth_gbps);
    if (!std::isfinite(duration) || duration > static_cast<double>(std::numeric_limits<uint64_t>::max())) {
        throw std::overflow_error("bulk transfer duration overflow");
    }
    return std::max<uint64_t>(1, static_cast<uint64_t>(duration));
}
} // namespace

CxlBulkTransferModel::CxlBulkTransferModel(double read_gbps, double write_gbps, uint64_t read_latency_ns,
                                           uint64_t write_latency_ns)
    : read_gbps_(read_gbps), write_gbps_(write_gbps), read_latency_ns_(read_latency_ns),
      write_latency_ns_(write_latency_ns) {
    if (!std::isfinite(read_gbps_) || read_gbps_ <= 0.0) {
        throw std::invalid_argument("read bandwidth must be positive");
    }
    if (!std::isfinite(write_gbps_) || write_gbps_ <= 0.0) {
        throw std::invalid_argument("write bandwidth must be positive");
    }
}

CxlBulkTiming CxlBulkTransferModel::schedule(CxlBulkDirection direction, uint64_t bytes, uint64_t submitted_ns,
                                             uint64_t byte_offset) {
    std::lock_guard<std::mutex> lock(mutex_);
    const bool is_read = direction == CxlBulkDirection::Read;
    uint64_t &next_free = is_read ? next_read_free_ns_ : next_write_free_ns_;
    const auto value = estimate(direction, bytes, byte_offset);
    const uint64_t service_start = std::max(submitted_ns, next_free);

    const uint64_t max_timestamp = std::numeric_limits<uint64_t>::max();
    if (value.service_ns > max_timestamp - service_start) {
        throw std::overflow_error("bulk completion timestamp overflow");
    }

    next_free = service_start + value.serialization_ns;
    const uint64_t completion = service_start + value.service_ns;
    return {
        .service_start_ns = service_start,
        .completion_ns = completion,
        .model_latency_ns = completion - submitted_ns,
        .serialization_ns = value.serialization_ns,
        .cacheline_count = value.cacheline_count,
    };
}

CxlBulkEstimate CxlBulkTransferModel::estimate(CxlBulkDirection direction, uint64_t bytes, uint64_t byte_offset) const {
    if (bytes == 0) {
        throw std::invalid_argument("bulk transfer size must be positive");
    }
    const bool is_read = direction == CxlBulkDirection::Read;
    const double bandwidth = is_read ? read_gbps_ : write_gbps_;
    const uint64_t latency = is_read ? read_latency_ns_ : write_latency_ns_;
    const uint64_t logical_cachelines = cacheline_count(bytes, byte_offset);
    if (logical_cachelines > std::numeric_limits<uint64_t>::max() / kCachelineBytes) {
        throw std::overflow_error("bulk cache-line footprint overflow");
    }
    const uint64_t wire_bytes = logical_cachelines * kCachelineBytes;
    const uint64_t serialization = serialization_time_ns(wire_bytes, bandwidth);
    if (latency > std::numeric_limits<uint64_t>::max() - serialization) {
        throw std::overflow_error("bulk service duration overflow");
    }
    return {
        .service_ns = serialization + latency,
        .serialization_ns = serialization,
        .cacheline_count = logical_cachelines,
    };
}
