#include "cxl_bulk_model.h"

#include <cassert>
#include <cstdint>
#include <stdexcept>

int main() {
    CxlBulkTransferModel model(1.0, 1.0, 100, 150);

    const auto one_byte = model.schedule(CxlBulkDirection::Read, 1, 1'000);
    assert(one_byte.cacheline_count == 1);
    assert(one_byte.serialization_ns == 64);
    assert(one_byte.service_start_ns == 1'000);
    assert(one_byte.completion_ns == 1'164);
    assert(one_byte.model_latency_ns == 164);

    const auto unaligned = model.schedule(CxlBulkDirection::Read, 64, 1'200, 1);
    assert(unaligned.cacheline_count == 2);
    assert(unaligned.serialization_ns == 128);

    const auto first_write = model.schedule(CxlBulkDirection::Write, 128, 2'000);
    assert(first_write.cacheline_count == 2);
    assert(first_write.serialization_ns == 128);
    assert(first_write.service_start_ns == 2'000);
    assert(first_write.completion_ns == 2'278);

    const auto queued_write = model.schedule(CxlBulkDirection::Write, 64, 2'010);
    assert(queued_write.service_start_ns == 2'128);
    assert(queued_write.completion_ns == 2'342);
    assert(queued_write.model_latency_ns == 332);

    const auto independent_read = model.schedule(CxlBulkDirection::Read, 64, 2'010);
    assert(independent_read.service_start_ns == 2'010);
    assert(independent_read.completion_ns == 2'174);

    bool rejected_zero = false;
    try {
        model.schedule(CxlBulkDirection::Read, 0, 3'000);
    } catch (const std::invalid_argument &) {
        rejected_zero = true;
    }
    assert(rejected_zero);

    return 0;
}
