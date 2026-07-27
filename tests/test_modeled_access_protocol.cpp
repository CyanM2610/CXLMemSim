/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */

#include "modeled_access_protocol.h"

#include <cassert>
#include <cstdint>
#include <type_traits>

using namespace cxlmemsim;

int main() {
    static_assert(std::is_same_v<decltype(ModeledAccessSlot::op_id), uint64_t>);
    static_assert(std::is_same_v<decltype(ModeledAccessSlot::offset), uint64_t>);
    static_assert(std::is_same_v<decltype(ModeledAccessSlot::bytes), uint64_t>);
    static_assert(!std::is_pointer_v<decltype(ModeledAccessSlot::offset)>);
    static_assert(!std::is_pointer_v<decltype(ModeledAccessSlot::bytes)>);

    ModeledAccessHeader header{};
    header.magic = kModeledAccessMagic;
    header.version = kModeledAccessVersion;
    header.header_size = sizeof(header);
    header.endian_marker = kModeledAccessEndianMarker;
    header.slot_count = kModeledAccessMaxSlots;
    header.capability_bits = kGpuDirectModeledAccessV1;
    header.server_ready = 1;
    assert(modeledAccessHeaderCompatible(header));

    header.version++;
    assert(!modeledAccessHeaderCompatible(header));
    header.version = kModeledAccessVersion;
    header.capability_bits = 0;
    assert(!modeledAccessHeaderCompatible(header));
    return 0;
}
