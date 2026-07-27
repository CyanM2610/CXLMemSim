/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */

#include "modeled_access_service.h"

#include <array>
#include <cassert>
#include <cstdint>

using namespace cxlmemsim;

int main() {
    std::array<uint8_t, 8192> guarded{};
    for (std::size_t index = 0; index < guarded.size(); ++index) {
        guarded[index] = static_cast<uint8_t>((index * 37 + 11) & 0xff);
    }
    const auto before = guarded;

    ModeledAccessService service({
        .provider_capacity = 4096,
        .provider_alignment = 64,
        .provider = ModeledAccessProvider::PosixShm,
        .read_bandwidth_gbps = 25.0,
        .write_bandwidth_gbps = 25.0,
        .read_latency_ns = 100,
        .write_latency_ns = 150,
    });
    const auto region = service.registerRegion({1, 4096, 64, ModeledAccessProvider::PosixShm});
    const auto access =
        service.beginAccess({1, 1, ModeledAccessDirection::WriteToCxl, region.server_region_token, 512, 2048, 1'000});
    service.dataComplete(access.access_token, ModeledAccessCudaStatus::Success, 2'000);
    service.pollAccess(access.access_token, access.estimated_complete_ns);
    service.cancelAccess(access.access_token, "idempotent terminal", access.estimated_complete_ns);

    assert(guarded == before);
    assert(service.stats().payload_copy_bytes == 0);
    return 0;
}
