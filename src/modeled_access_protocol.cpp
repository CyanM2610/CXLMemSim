/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */

#include "modeled_access_protocol.h"

#include <bit>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

namespace cxlmemsim {

bool modeledAccessHeaderCompatible(const ModeledAccessHeader &header) {
    return std::endian::native == std::endian::little && header.magic == kModeledAccessMagic &&
           header.version == kModeledAccessVersion && header.header_size == sizeof(ModeledAccessHeader) &&
           header.endian_marker == kModeledAccessEndianMarker && header.slot_count == kModeledAccessMaxSlots &&
           (header.capability_bits & kGpuDirectModeledAccessV1) != 0 &&
           __atomic_load_n(&header.server_ready, __ATOMIC_ACQUIRE) != 0;
}

} // namespace cxlmemsim

struct cxl_modeled_client {
    int fd = -1;
    cxlmemsim::ModeledAccessHeader *header = nullptr;
    uint32_t timeout_ms = 0;
    uint64_t server_generation = 0;
    uint64_t next_request_id = 0;
};

namespace {

uint64_t monotonicTimeNs() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

uint64_t deadlineNs(uint32_t timeout_ms) {
    const uint64_t now = monotonicTimeNs();
    const uint64_t duration = static_cast<uint64_t>(timeout_ms) * 1'000'000ULL;
    return duration > UINT64_MAX - now ? UINT64_MAX : now + duration;
}

bool clientAlive(const cxl_modeled_client_t *client) {
    return client != nullptr && client->header != nullptr &&
           __atomic_load_n(&client->header->server_ready, __ATOMIC_ACQUIRE) != 0 &&
           client->header->server_generation == client->server_generation;
}

cxlmemsim::ModeledAccessSlot *reserveSlot(cxl_modeled_client_t *client, uint64_t deadline) {
    using namespace cxlmemsim;
    while (monotonicTimeNs() < deadline) {
        if (!clientAlive(client)) {
            return nullptr;
        }
        for (uint32_t index = 0; index < kModeledAccessMaxSlots; ++index) {
            auto &slot = client->header->slots[index];
            uint32_t expected = static_cast<uint32_t>(ModeledAccessSlotState::Free);
            if (__atomic_compare_exchange_n(&slot.state, &expected,
                                            static_cast<uint32_t>(ModeledAccessSlotState::Reserved), false,
                                            __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
                return &slot;
            }
        }
        std::this_thread::yield();
    }
    return nullptr;
}

int mapStatus(uint32_t status) {
    using cxlmemsim::ModeledAccessStatus;
    switch (static_cast<ModeledAccessStatus>(status)) {
    case ModeledAccessStatus::Ok:
        return CXL_MODELED_SUCCESS;
    case ModeledAccessStatus::OutOfRange:
        return CXL_MODELED_ERROR_RANGE;
    case ModeledAccessStatus::Unsupported:
        return CXL_MODELED_ERROR_UNSUPPORTED;
    default:
        return CXL_MODELED_ERROR_SERVER;
    }
}

void abandonSlot(cxlmemsim::ModeledAccessSlot *slot) {
    using cxlmemsim::ModeledAccessSlotState;
    uint32_t state = __atomic_load_n(&slot->state, __ATOMIC_ACQUIRE);
    while (true) {
        uint32_t replacement;
        if (state == static_cast<uint32_t>(ModeledAccessSlotState::Submitted) ||
            state == static_cast<uint32_t>(ModeledAccessSlotState::Complete)) {
            replacement = static_cast<uint32_t>(ModeledAccessSlotState::Free);
        } else if (state == static_cast<uint32_t>(ModeledAccessSlotState::Reserved)) {
            replacement = static_cast<uint32_t>(ModeledAccessSlotState::Abandoned);
        } else {
            return;
        }
        if (__atomic_compare_exchange_n(&slot->state, &state, replacement, false, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
            return;
        }
    }
}

template <typename Fill, typename Consume>
int submit(cxl_modeled_client_t *client, cxlmemsim::ModeledAccessOperation operation, Fill fill, Consume consume) {
    using namespace cxlmemsim;
    if (!clientAlive(client)) {
        return CXL_MODELED_ERROR_SERVER;
    }
    const uint64_t deadline = deadlineNs(client->timeout_ms);
    ModeledAccessSlot *slot = reserveSlot(client, deadline);
    if (slot == nullptr) {
        return clientAlive(client) ? CXL_MODELED_ERROR_TIMEOUT : CXL_MODELED_ERROR_SERVER;
    }
    std::memset(reinterpret_cast<uint8_t *>(slot) + offsetof(ModeledAccessSlot, operation), 0,
                sizeof(*slot) - offsetof(ModeledAccessSlot, operation));
    slot->operation = static_cast<uint32_t>(operation);
    slot->request_id = ++client->next_request_id;
    slot->submitted_ns = monotonicTimeNs();
    fill(*slot);
    __atomic_store_n(&slot->state, static_cast<uint32_t>(ModeledAccessSlotState::Submitted), __ATOMIC_RELEASE);

    while (monotonicTimeNs() < deadline) {
        if (__atomic_load_n(&slot->state, __ATOMIC_ACQUIRE) ==
            static_cast<uint32_t>(ModeledAccessSlotState::Complete)) {
            const int result = mapStatus(slot->status);
            if (result == CXL_MODELED_SUCCESS) {
                consume(*slot);
            }
            __atomic_store_n(&slot->state, static_cast<uint32_t>(ModeledAccessSlotState::Free), __ATOMIC_RELEASE);
            return result;
        }
        if (!clientAlive(client)) {
            abandonSlot(slot);
            return CXL_MODELED_ERROR_SERVER;
        }
        std::this_thread::yield();
    }
    abandonSlot(slot);
    return CXL_MODELED_ERROR_TIMEOUT;
}

} // namespace

extern "C" {

int cxl_modeled_client_open(const char *control_name, uint32_t timeout_ms, cxl_modeled_client_t **out_client) {
    using namespace cxlmemsim;
    if (control_name == nullptr || control_name[0] != '/' || timeout_ms == 0 || out_client == nullptr) {
        return CXL_MODELED_ERROR_INVALID_ARGUMENT;
    }
    *out_client = nullptr;
    const uint64_t deadline = deadlineNs(timeout_ms);
    int fd = -1;
    while (monotonicTimeNs() < deadline) {
        fd = shm_open(control_name, O_RDWR, 0);
        if (fd >= 0) {
            break;
        }
        if (errno != ENOENT) {
            return CXL_MODELED_ERROR_OPEN;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (fd < 0) {
        return CXL_MODELED_ERROR_NOT_READY;
    }
    void *mapping = mmap(nullptr, sizeof(ModeledAccessHeader), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapping == MAP_FAILED) {
        close(fd);
        return CXL_MODELED_ERROR_OPEN;
    }
    auto *header = static_cast<ModeledAccessHeader *>(mapping);
    while (monotonicTimeNs() < deadline && __atomic_load_n(&header->server_ready, __ATOMIC_ACQUIRE) == 0) {
        std::this_thread::yield();
    }
    if (!modeledAccessHeaderCompatible(*header)) {
        const bool version_matches = header->version == kModeledAccessVersion;
        munmap(header, sizeof(*header));
        close(fd);
        return version_matches ? CXL_MODELED_ERROR_UNSUPPORTED : CXL_MODELED_ERROR_PROTOCOL;
    }
    auto client = std::make_unique<cxl_modeled_client_t>();
    client->fd = fd;
    client->header = header;
    client->timeout_ms = timeout_ms;
    client->server_generation = header->server_generation;
    *out_client = client.release();
    return CXL_MODELED_SUCCESS;
}

void cxl_modeled_client_close(cxl_modeled_client_t *client) {
    if (client == nullptr) {
        return;
    }
    if (client->header != nullptr) {
        munmap(client->header, sizeof(cxlmemsim::ModeledAccessHeader));
    }
    if (client->fd >= 0) {
        close(client->fd);
    }
    delete client;
}

int cxl_modeled_get_capabilities(const cxl_modeled_client_t *client, cxl_modeled_capabilities_t *result) {
    if (!clientAlive(client) || result == nullptr) {
        return CXL_MODELED_ERROR_INVALID_ARGUMENT;
    }
    const auto &header = *client->header;
    *result = {
        .protocol_version = header.version,
        .provider_kind = header.provider_kind,
        .capability_bits = header.capability_bits,
        .server_generation = header.server_generation,
        .data_capacity = header.data_capacity,
        .data_alignment = header.data_alignment,
    };
    return CXL_MODELED_SUCCESS;
}

int cxl_modeled_register_region(cxl_modeled_client_t *client, uint64_t region_id, uint64_t capacity, uint64_t alignment,
                                uint32_t provider_kind, uint64_t *server_region_token) {
    if (client == nullptr || region_id == 0 || capacity == 0 || alignment == 0 || server_region_token == nullptr) {
        return CXL_MODELED_ERROR_INVALID_ARGUMENT;
    }
    return submit(
        client, cxlmemsim::ModeledAccessOperation::RegisterRegion,
        [&](auto &slot) {
            slot.region_id = region_id;
            slot.capacity = capacity;
            slot.alignment = alignment;
            slot.provider_kind = provider_kind;
        },
        [&](const auto &slot) { *server_region_token = slot.server_region_token; });
}

int cxl_modeled_begin_access(cxl_modeled_client_t *client, uint64_t op_id, uint64_t client_id, uint32_t direction,
                             uint64_t server_region_token, uint64_t offset, uint64_t bytes, uint64_t start_ns,
                             cxl_modeled_begin_result_t *result) {
    if (client == nullptr || op_id == 0 || client_id == 0 || server_region_token == 0 || bytes == 0 || start_ns == 0 ||
        result == nullptr) {
        return CXL_MODELED_ERROR_INVALID_ARGUMENT;
    }
    return submit(
        client, cxlmemsim::ModeledAccessOperation::BeginAccess,
        [&](auto &slot) {
            slot.op_id = op_id;
            slot.client_id = client_id;
            slot.direction = direction;
            slot.server_region_token = server_region_token;
            slot.offset = offset;
            slot.bytes = bytes;
            slot.start_ns = start_ns;
        },
        [&](const auto &slot) {
            result->access_token = slot.access_token;
            result->estimated_complete_ns = slot.modeled_complete_ns;
        });
}

int cxl_modeled_data_complete(cxl_modeled_client_t *client, uint64_t access_token, uint32_t cuda_status,
                              uint64_t data_complete_ns) {
    if (client == nullptr || access_token == 0 || data_complete_ns == 0) {
        return CXL_MODELED_ERROR_INVALID_ARGUMENT;
    }
    return submit(
        client, cxlmemsim::ModeledAccessOperation::DataComplete,
        [&](auto &slot) {
            slot.access_token = access_token;
            slot.cuda_status = cuda_status;
            slot.data_complete_ns = data_complete_ns;
        },
        [](const auto &) {});
}

int cxl_modeled_complete_and_poll(cxl_modeled_client_t *client, uint64_t access_token, uint32_t cuda_status,
                                  uint64_t data_complete_ns, cxl_modeled_poll_result_t *result) {
    if (!clientAlive(client) || access_token == 0 || data_complete_ns == 0 || result == nullptr) {
        return CXL_MODELED_ERROR_INVALID_ARGUMENT;
    }
    if ((client->header->capability_bits & cxlmemsim::kModeledCompleteAndPoll) == 0) {
        return CXL_MODELED_ERROR_UNSUPPORTED;
    }
    return submit(
        client, cxlmemsim::ModeledAccessOperation::CompleteAndPoll,
        [&](auto &slot) {
            slot.access_token = access_token;
            slot.cuda_status = cuda_status;
            slot.data_complete_ns = data_complete_ns;
        },
        [&](const auto &slot) {
            result->state = slot.terminal_state;
            result->error_code = slot.error_code;
            result->queue_ns = slot.queue_ns;
            result->service_ns = slot.service_ns;
            result->modeled_complete_ns = slot.modeled_complete_ns;
        });
}

int cxl_modeled_poll_access(cxl_modeled_client_t *client, uint64_t access_token, cxl_modeled_poll_result_t *result) {
    if (client == nullptr || access_token == 0 || result == nullptr) {
        return CXL_MODELED_ERROR_INVALID_ARGUMENT;
    }
    return submit(
        client, cxlmemsim::ModeledAccessOperation::PollAccess, [&](auto &slot) { slot.access_token = access_token; },
        [&](const auto &slot) {
            result->state = slot.terminal_state;
            result->error_code = slot.error_code;
            result->queue_ns = slot.queue_ns;
            result->service_ns = slot.service_ns;
            result->modeled_complete_ns = slot.modeled_complete_ns;
        });
}

int cxl_modeled_begin_access_batch(cxl_modeled_client_t *client, uint64_t client_id, uint32_t direction,
                                   uint64_t region, const cxl_modeled_range_t *ranges, uint32_t count,
                                   cxl_modeled_timed_begin_t *results, uint32_t *completed) {
    if (completed == nullptr) {
        return CXL_MODELED_ERROR_INVALID_ARGUMENT;
    }
    *completed = 0;
    if (client == nullptr || ranges == nullptr || results == nullptr || count == 0) {
        return CXL_MODELED_ERROR_INVALID_ARGUMENT;
    }
    for (uint32_t i = 0; i < count; ++i) {
        auto &out = results[i];
        out.started_ns = monotonicTimeNs();
        const int status = cxl_modeled_begin_access(client, ranges[i].op_id, client_id, direction, region,
                                                    ranges[i].offset, ranges[i].bytes, out.started_ns, &out.result);
        out.observed_ns = monotonicTimeNs();
        if (status != CXL_MODELED_SUCCESS) {
            return status;
        }
        ++*completed;
    }
    return CXL_MODELED_SUCCESS;
}

int cxl_modeled_completion_batch(cxl_modeled_client_t *client, const uint64_t *tokens, uint32_t count,
                                 uint32_t cuda_status, uint64_t data_complete_ns,
                                 cxl_modeled_batch_poll_result_t *results) {
    if (!clientAlive(client) || tokens == nullptr || results == nullptr || count == 0) {
        return CXL_MODELED_ERROR_INVALID_ARGUMENT;
    }
    const bool combined = (client->header->capability_bits & cxlmemsim::kModeledCompleteAndPoll) != 0;
    int status = CXL_MODELED_SUCCESS;
    for (uint32_t i = 0; i < count; ++i) {
        auto &out = results[i];
        out = {};
        if (status == CXL_MODELED_SUCCESS) {
            if (cuda_status == static_cast<uint32_t>(cxlmemsim::ModeledAccessCudaStatus::Pending)) {
                status = cxl_modeled_poll_access(client, tokens[i], &out.result);
            } else if (combined) {
                status = cxl_modeled_complete_and_poll(client, tokens[i], cuda_status, data_complete_ns, &out.result);
            } else {
                status = cxl_modeled_data_complete(client, tokens[i], cuda_status, data_complete_ns);
                if (status == CXL_MODELED_SUCCESS) {
                    status = cxl_modeled_poll_access(client, tokens[i], &out.result);
                }
            }
        }
        out.call_status = status;
    }
    return CXL_MODELED_SUCCESS;
}

int cxl_modeled_cancel_access(cxl_modeled_client_t *client, uint64_t access_token, const char *reason) {
    if (client == nullptr || access_token == 0 || reason == nullptr ||
        std::strlen(reason) >= cxlmemsim::kModeledAccessReasonBytes) {
        return CXL_MODELED_ERROR_INVALID_ARGUMENT;
    }
    return submit(
        client, cxlmemsim::ModeledAccessOperation::CancelAccess,
        [&](auto &slot) {
            slot.access_token = access_token;
            std::memcpy(slot.reason, reason, std::strlen(reason) + 1);
        },
        [](const auto &) {});
}

const char *cxl_modeled_error_string(int error_code) {
    switch (error_code) {
    case CXL_MODELED_SUCCESS:
        return "success";
    case CXL_MODELED_ERROR_INVALID_ARGUMENT:
        return "invalid argument";
    case CXL_MODELED_ERROR_OPEN:
        return "failed to open modeled-access shared memory";
    case CXL_MODELED_ERROR_PROTOCOL:
        return "incompatible modeled-access protocol";
    case CXL_MODELED_ERROR_NOT_READY:
        return "modeled-access server is not ready";
    case CXL_MODELED_ERROR_TIMEOUT:
        return "modeled-access request timed out";
    case CXL_MODELED_ERROR_SERVER:
        return "modeled-access server rejected or stopped the request";
    case CXL_MODELED_ERROR_RANGE:
        return "modeled-access request is out of range";
    case CXL_MODELED_ERROR_UNSUPPORTED:
        return "gpu_direct_modeled_access_v1 is unsupported";
    default:
        return "unknown modeled-access client error";
    }
}

} // extern "C"
