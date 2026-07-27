/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */

#include "modeled_access_service.h"

#include <cassert>

using namespace cxlmemsim;

namespace {
ModeledAccessService makeService(double read_gbps = 1.0, double write_gbps = 2.0, uint64_t read_latency = 100,
                                 uint64_t write_latency = 200) {
    return ModeledAccessService({
        .provider_capacity = 4096,
        .provider_alignment = 64,
        .provider = ModeledAccessProvider::PosixShm,
        .read_bandwidth_gbps = read_gbps,
        .write_bandwidth_gbps = write_gbps,
        .read_latency_ns = read_latency,
        .write_latency_ns = write_latency,
    });
}

uint64_t registerRegion(ModeledAccessService &service) {
    const auto result = service.registerRegion({1, 4096, 64, ModeledAccessProvider::PosixShm});
    assert(result.status == ModeledAccessStatus::Ok);
    assert(result.server_region_token != 0);
    return result.server_region_token;
}
} // namespace

int main() {
    auto service = makeService();
    const uint64_t region = registerRegion(service);

    const auto first = service.beginAccess({1, 7, ModeledAccessDirection::ReadFromCxl, region, 0, 64, 1'000});
    assert(first.status == ModeledAccessStatus::Ok);
    assert(first.estimated_complete_ns == 1'164);

    const auto second = service.beginAccess({2, 8, ModeledAccessDirection::ReadFromCxl, region, 64, 64, 1'000});
    assert(second.status == ModeledAccessStatus::Ok);
    auto second_poll = service.pollAccess(second.access_token, 1'000);
    assert(second_poll.queue_ns == 64);
    assert(second_poll.service_ns == 164);
    assert(second_poll.modeled_complete_ns == 1'228);

    const auto cancelled = service.cancelAccess(first.access_token, "test", 1'001);
    assert(cancelled.state == ModeledAccessState::Cancelled);
    second_poll = service.pollAccess(second.access_token, 1'001);
    assert(second_poll.queue_ns == 0);
    assert(second_poll.modeled_complete_ns == 1'164);

    const auto data_status = service.dataComplete(second.access_token, ModeledAccessCudaStatus::Success, 1'050);
    assert(data_status == ModeledAccessStatus::Ok);
    assert(service.pollAccess(second.access_token, 1'100).state == ModeledAccessState::DataComplete);
    assert(service.pollAccess(second.access_token, 1'164).state == ModeledAccessState::Complete);

    const auto write = service.beginAccess({3, 7, ModeledAccessDirection::WriteToCxl, region, 128, 64, 2'000});
    const auto write_poll = service.pollAccess(write.access_token, 2'000);
    assert(write_poll.queue_ns == 0);
    assert(write_poll.service_ns == 232);

    const auto failed = service.beginAccess({4, 7, ModeledAccessDirection::ReadFromCxl, region, 192, 64, 3'000});
    service.dataComplete(failed.access_token, ModeledAccessCudaStatus::Error, 3'010);
    const auto failed_poll = service.pollAccess(failed.access_token, 4'000);
    assert(failed_poll.state == ModeledAccessState::Error);
    assert(failed_poll.service_ns == 164);

    auto slow = makeService(0.5, 1.0, 200, 300);
    const uint64_t slow_region = registerRegion(slow);
    const auto slow_read = slow.beginAccess({1, 1, ModeledAccessDirection::ReadFromCxl, slow_region, 0, 64, 1'000});
    assert(slow_read.estimated_complete_ns > first.estimated_complete_ns);

    const auto duplicate = service.beginAccess({2, 8, ModeledAccessDirection::ReadFromCxl, region, 64, 64, 1'000});
    assert(duplicate.status == ModeledAccessStatus::Duplicate);
    const auto out_of_range = service.beginAccess({5, 8, ModeledAccessDirection::ReadFromCxl, region, 4090, 64, 1'000});
    assert(out_of_range.status == ModeledAccessStatus::OutOfRange);
    return 0;
}
