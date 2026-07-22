/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */

#ifndef CXLMEMSIM_CXL_BULK_H
#define CXLMEMSIM_CXL_BULK_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define CXL_BULK_MAGIC UINT64_C(0x43584c42554c4b31)
#define CXL_BULK_VERSION 3U
#define CXL_BULK_MAX_SLOTS 64U
#define CXL_BULK_CACHELINE_SIZE 64U
#define CXL_BULK_DATA_NAME_SIZE 128U

enum cxl_bulk_operation {
    CXL_BULK_OP_READ = 1,
    CXL_BULK_OP_WRITE = 2,
};

enum cxl_bulk_slot_state {
    CXL_BULK_SLOT_FREE = 0,
    CXL_BULK_SLOT_RESERVED = 1,
    CXL_BULK_SLOT_SUBMITTED = 2,
    CXL_BULK_SLOT_IN_PROGRESS = 3,
    CXL_BULK_SLOT_COMPLETE = 4,
};

enum cxl_bulk_status {
    CXL_BULK_STATUS_OK = 0,
    CXL_BULK_STATUS_INVALID_REQUEST = 1,
    CXL_BULK_STATUS_OUT_OF_RANGE = 2,
    CXL_BULK_STATUS_SERVER_STOPPED = 3,
};

enum cxl_bulk_error {
    CXL_BULK_SUCCESS = 0,
    CXL_BULK_ERROR_INVALID_ARGUMENT = 1,
    CXL_BULK_ERROR_OPEN = 2,
    CXL_BULK_ERROR_PROTOCOL = 3,
    CXL_BULK_ERROR_NOT_READY = 4,
    CXL_BULK_ERROR_MAP = 5,
    CXL_BULK_ERROR_TIMEOUT = 6,
    CXL_BULK_ERROR_SERVER = 7,
    CXL_BULK_ERROR_RANGE = 8,
};

typedef struct __attribute__((aligned(128))) cxl_bulk_slot {
    volatile uint32_t state;
    volatile uint32_t copy_done;
    volatile uint32_t abandoned;
    uint32_t operation;
    uint32_t status;
    uint32_t client_pid;
    uint64_t request_id;
    uint64_t offset;
    uint64_t size;
    uint64_t submitted_ns;
    uint64_t host_copy_ns;
    uint64_t service_start_ns;
    uint64_t completion_ns;
    uint64_t model_latency_ns;
    uint64_t serialization_ns;
    uint64_t cacheline_count;
    uint64_t client_start_ticks;
    volatile uint64_t owner_token;
} cxl_bulk_slot_t;

typedef struct cxl_bulk_header {
    uint64_t magic;
    uint32_t version;
    uint32_t header_size;
    uint32_t slot_count;
    uint32_t cacheline_size;
    volatile uint32_t server_ready;
    uint32_t flags;
    uint64_t server_generation;
    uint64_t data_mapped_size;
    uint64_t data_offset;
    uint64_t data_size;
    double read_bandwidth_gbps;
    double write_bandwidth_gbps;
    uint64_t read_latency_ns;
    uint64_t write_latency_ns;
    char data_name[CXL_BULK_DATA_NAME_SIZE];
    cxl_bulk_slot_t slots[CXL_BULK_MAX_SLOTS];
} cxl_bulk_header_t;

typedef struct cxl_bulk_result {
    uint64_t bytes;
    uint64_t host_copy_ns;
    uint64_t model_latency_ns;
    uint64_t serialization_ns;
    uint64_t cacheline_count;
} cxl_bulk_result_t;

#ifdef __cplusplus
static_assert(sizeof(cxl_bulk_slot_t) == 128);
static_assert(offsetof(cxl_bulk_header_t, slots) == 256);
static_assert(sizeof(cxl_bulk_header_t) == 8448);
static_assert(sizeof(cxl_bulk_result_t) == 40);
#else
_Static_assert(sizeof(cxl_bulk_slot_t) == 128, "cxl_bulk_slot_t ABI size changed");
_Static_assert(offsetof(cxl_bulk_header_t, slots) == 256, "cxl_bulk_header_t slot offset changed");
_Static_assert(sizeof(cxl_bulk_header_t) == 8448, "cxl_bulk_header_t ABI size changed");
_Static_assert(sizeof(cxl_bulk_result_t) == 40, "cxl_bulk_result_t ABI size changed");
#endif

typedef struct cxl_bulk_client cxl_bulk_client_t;

int cxl_bulk_client_open(const char *control_name, uint32_t timeout_ms, cxl_bulk_client_t **out_client);
/* Closing a client concurrently with an active transfer is not supported. */
void cxl_bulk_client_close(cxl_bulk_client_t *client);
uint64_t cxl_bulk_client_capacity(const cxl_bulk_client_t *client);
int cxl_bulk_read(cxl_bulk_client_t *client, uint64_t offset, void *dst, uint64_t size, cxl_bulk_result_t *result);
int cxl_bulk_write(cxl_bulk_client_t *client, uint64_t offset, const void *src, uint64_t size,
                   cxl_bulk_result_t *result);
const char *cxl_bulk_error_string(int error_code);

#ifdef __cplusplus
}
#endif

#endif
