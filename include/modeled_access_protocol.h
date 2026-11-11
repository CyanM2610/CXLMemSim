/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */

#ifndef CXLMEMSIM_MODELED_ACCESS_PROTOCOL_H
#define CXLMEMSIM_MODELED_ACCESS_PROTOCOL_H

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace cxlmemsim {

inline constexpr uint64_t kModeledAccessMagic = UINT64_C(0x43584c4d4f444c31);
inline constexpr uint32_t kModeledAccessVersion = 1;
inline constexpr uint32_t kModeledAccessEndianMarker = UINT32_C(0x01020304);
inline constexpr uint32_t kModeledAccessMaxSlots = 64;
inline constexpr uint64_t kGpuDirectModeledAccessV1 = UINT64_C(1) << 0;
inline constexpr uint64_t kModeledCompleteAndPoll = UINT64_C(1) << 1;
inline constexpr std::size_t kModeledAccessReasonBytes = 64;

enum class ModeledAccessOperation : uint32_t {
    RegisterRegion = 1,
    BeginAccess = 2,
    DataComplete = 3,
    PollAccess = 4,
    CancelAccess = 5,
    CompleteAndPoll = 6,
};

enum class ModeledAccessDirection : uint32_t {
    ReadFromCxl = 1,
    WriteToCxl = 2,
};

enum class ModeledAccessProvider : uint32_t {
    PosixShm = 1,
};

enum class ModeledAccessSlotState : uint32_t {
    Free = 0,
    Reserved = 1,
    Submitted = 2,
    Complete = 3,
    Abandoned = 4,
};

enum class ModeledAccessStatus : uint32_t {
    Ok = 0,
    InvalidRequest = 1,
    Unsupported = 2,
    NotFound = 3,
    OutOfRange = 4,
    Duplicate = 5,
    ServerStopped = 6,
    InternalError = 7,
};

enum class ModeledAccessState : uint32_t {
    Pending = 0,
    ModeledComplete = 1,
    DataComplete = 2,
    Complete = 3,
    Cancelled = 4,
    Error = 5,
};

enum class ModeledAccessCudaStatus : uint32_t {
    Pending = 0,
    Success = 1,
    Error = 2,
};

struct alignas(256) ModeledAccessSlot {
    volatile uint32_t state;
    uint32_t operation;
    uint32_t status;
    uint32_t direction;
    uint32_t cuda_status;
    uint32_t terminal_state;
    uint32_t error_code;
    uint32_t provider_kind;
    uint64_t request_id;
    uint64_t client_id;
    uint64_t op_id;
    uint64_t region_id;
    uint64_t server_region_token;
    uint64_t access_token;
    uint64_t offset;
    uint64_t bytes;
    uint64_t start_ns;
    uint64_t data_complete_ns;
    uint64_t capacity;
    uint64_t alignment;
    uint64_t queue_ns;
    uint64_t service_ns;
    uint64_t modeled_complete_ns;
    uint64_t submitted_ns;
    char reason[kModeledAccessReasonBytes];
    uint8_t reserved[32];
};

struct ModeledAccessHeader {
    uint64_t magic;
    uint32_t version;
    uint32_t header_size;
    uint32_t endian_marker;
    uint32_t slot_count;
    volatile uint32_t server_ready;
    uint32_t flags;
    uint64_t capability_bits;
    uint64_t server_generation;
    uint64_t data_capacity;
    uint64_t data_alignment;
    uint32_t provider_kind;
    uint32_t reserved0;
    uint8_t reserved[56];
    ModeledAccessSlot slots[kModeledAccessMaxSlots];
};

static_assert(sizeof(ModeledAccessSlot) == 256);
static_assert(alignof(ModeledAccessSlot) == 256);
static_assert(offsetof(ModeledAccessHeader, slots) == 256);
static_assert(sizeof(ModeledAccessHeader) == 256 + 256 * kModeledAccessMaxSlots);
static_assert(std::is_standard_layout_v<ModeledAccessSlot>);
static_assert(std::is_trivially_copyable_v<ModeledAccessSlot>);

bool modeledAccessHeaderCompatible(const ModeledAccessHeader &header);

} // namespace cxlmemsim

#ifdef __cplusplus
extern "C" {
#endif

enum cxl_modeled_error {
    CXL_MODELED_SUCCESS = 0,
    CXL_MODELED_ERROR_INVALID_ARGUMENT = 1,
    CXL_MODELED_ERROR_OPEN = 2,
    CXL_MODELED_ERROR_PROTOCOL = 3,
    CXL_MODELED_ERROR_NOT_READY = 4,
    CXL_MODELED_ERROR_TIMEOUT = 5,
    CXL_MODELED_ERROR_SERVER = 6,
    CXL_MODELED_ERROR_RANGE = 7,
    CXL_MODELED_ERROR_UNSUPPORTED = 8,
};

typedef struct cxl_modeled_client cxl_modeled_client_t;

typedef struct cxl_modeled_capabilities {
    uint32_t protocol_version;
    uint32_t provider_kind;
    uint64_t capability_bits;
    uint64_t server_generation;
    uint64_t data_capacity;
    uint64_t data_alignment;
} cxl_modeled_capabilities_t;

typedef struct cxl_modeled_begin_result {
    uint64_t access_token;
    uint64_t estimated_complete_ns;
} cxl_modeled_begin_result_t;

typedef struct cxl_modeled_poll_result {
    uint32_t state;
    uint32_t error_code;
    uint64_t queue_ns;
    uint64_t service_ns;
    uint64_t modeled_complete_ns;
} cxl_modeled_poll_result_t;

int cxl_modeled_client_open(const char *control_name, uint32_t timeout_ms, cxl_modeled_client_t **out_client);
void cxl_modeled_client_close(cxl_modeled_client_t *client);
int cxl_modeled_get_capabilities(const cxl_modeled_client_t *client, cxl_modeled_capabilities_t *result);
int cxl_modeled_register_region(cxl_modeled_client_t *client, uint64_t region_id, uint64_t capacity, uint64_t alignment,
                                uint32_t provider_kind, uint64_t *server_region_token);
int cxl_modeled_begin_access(cxl_modeled_client_t *client, uint64_t op_id, uint64_t client_id, uint32_t direction,
                             uint64_t server_region_token, uint64_t offset, uint64_t bytes, uint64_t start_ns,
                             cxl_modeled_begin_result_t *result);
int cxl_modeled_data_complete(cxl_modeled_client_t *client, uint64_t access_token, uint32_t cuda_status,
                              uint64_t data_complete_ns);
int cxl_modeled_poll_access(cxl_modeled_client_t *client, uint64_t access_token, cxl_modeled_poll_result_t *result);
// Report a real terminal CUDA observation and return the current model state in
// one exchange. COMPLETE still requires the modeled deadline; a pending result
// must be polled later. Requires kModeledCompleteAndPoll, keeps the v1 SHM layout.
int cxl_modeled_complete_and_poll(cxl_modeled_client_t *client, uint64_t access_token, uint32_t cuda_status,
                                  uint64_t data_complete_ns, cxl_modeled_poll_result_t *result);
int cxl_modeled_cancel_access(cxl_modeled_client_t *client, uint64_t access_token, const char *reason);
const char *cxl_modeled_error_string(int error_code);

#ifdef __cplusplus
}
#endif

#endif
