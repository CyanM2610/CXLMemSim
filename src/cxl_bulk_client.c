/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */

#include "cxl_bulk.h"

#include <errno.h>
#include <fcntl.h>
#include <sched.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

struct cxl_bulk_client {
    int control_fd;
    int lock_fd;
    int data_fd;
    cxl_bulk_header_t *header;
    void *data_mapping;
    uint8_t *data;
    uint64_t data_mapped_size;
    uint64_t data_size;
    uint32_t timeout_ms;
    uint64_t next_request_id;
    uint64_t process_start_ticks;
    uint64_t owner_token;
    uint64_t server_generation;
};

#define CXL_BULK_LIVENESS_CHECK_NS UINT64_C(10000000)
#define CXL_BULK_OWNER_PID_BITS 24U
#define CXL_BULK_OWNER_PID_MASK UINT64_C(0xffffff)
#define CXL_BULK_OWNER_START_MAX UINT64_C(0xffffffffff)

static uint64_t monotonic_time_ns(void) {
    struct timespec value;
    clock_gettime(CLOCK_MONOTONIC, &value);
    return (uint64_t)value.tv_sec * UINT64_C(1000000000) + (uint64_t)value.tv_nsec;
}

static int deadline_expired(uint64_t deadline_ns) { return monotonic_time_ns() >= deadline_ns; }

static uint64_t timeout_deadline(uint32_t timeout_ms) {
    return monotonic_time_ns() + (uint64_t)timeout_ms * UINT64_C(1000000);
}

static void wait_briefly(void) { sched_yield(); }

static int read_process_start_ticks(uint32_t pid, uint64_t *out_ticks) {
    char path[64];
    if (snprintf(path, sizeof(path), "/proc/%u/stat", pid) < 0) {
        return 0;
    }
    FILE *file = fopen(path, "r");
    if (file == NULL) {
        return 0;
    }
    char line[4096];
    if (fgets(line, sizeof(line), file) == NULL) {
        fclose(file);
        return 0;
    }
    fclose(file);

    char *cursor = strrchr(line, ')');
    if (cursor == NULL || cursor[1] != ' ' || cursor[2] == '\0') {
        return 0;
    }
    cursor += 3;
    for (int field = 4; field <= 22; ++field) {
        while (*cursor == ' ') {
            ++cursor;
        }
        if (*cursor == '\0') {
            return 0;
        }
        errno = 0;
        char *end = NULL;
        const unsigned long long value = strtoull(cursor, &end, 10);
        if (end == cursor || errno == ERANGE) {
            return 0;
        }
        if (field == 22) {
            *out_ticks = (uint64_t)value;
            return *out_ticks != 0;
        }
        cursor = end;
    }
    return 0;
}

static int server_lock_is_held(int control_fd) {
    if (flock(control_fd, LOCK_EX | LOCK_NB) == 0) {
        flock(control_fd, LOCK_UN);
        return 0;
    }
    return errno == EWOULDBLOCK || errno == EAGAIN;
}

static int read_lock_generation(int lock_fd, uint64_t *generation) {
    const ssize_t bytes = pread(lock_fd, generation, sizeof(*generation), 0);
    return bytes == (ssize_t)sizeof(*generation);
}

static int server_identity_is_valid(const cxl_bulk_client_t *client) {
    uint64_t lock_generation = 0;
    return __atomic_load_n(&client->header->server_ready, __ATOMIC_ACQUIRE) != 0 &&
           client->header->server_generation == client->server_generation && server_lock_is_held(client->lock_fd) &&
           read_lock_generation(client->lock_fd, &lock_generation) && lock_generation == client->server_generation;
}

static void close_client_mappings(cxl_bulk_client_t *client) {
    if (client->data_mapping != NULL && client->data_mapping != MAP_FAILED) {
        munmap(client->data_mapping, client->data_mapped_size);
    }
    if (client->header != NULL && client->header != MAP_FAILED) {
        munmap(client->header, sizeof(cxl_bulk_header_t));
    }
    if (client->data_fd >= 0) {
        close(client->data_fd);
    }
    if (client->control_fd >= 0) {
        close(client->control_fd);
    }
    if (client->lock_fd >= 0) {
        close(client->lock_fd);
    }
}

int cxl_bulk_client_open(const char *control_name, uint32_t timeout_ms, cxl_bulk_client_t **out_client) {
    if (control_name == NULL || control_name[0] == '\0' || timeout_ms == 0 || out_client == NULL) {
        return CXL_BULK_ERROR_INVALID_ARGUMENT;
    }
    *out_client = NULL;
    cxl_bulk_client_t *client = calloc(1, sizeof(*client));
    if (client == NULL) {
        return CXL_BULK_ERROR_MAP;
    }
    client->control_fd = -1;
    client->lock_fd = -1;
    client->data_fd = -1;
    client->timeout_ms = timeout_ms;
    const uint64_t deadline = timeout_deadline(timeout_ms);

    const size_t control_name_length = strlen(control_name);
    char *lock_name = malloc(control_name_length + sizeof(".lock"));
    if (lock_name == NULL) {
        free(client);
        return CXL_BULK_ERROR_MAP;
    }
    memcpy(lock_name, control_name, control_name_length);
    memcpy(lock_name + control_name_length, ".lock", sizeof(".lock"));
    client->lock_fd = shm_open(lock_name, O_RDWR, 0);
    free(lock_name);
    if (client->lock_fd < 0) {
        close_client_mappings(client);
        free(client);
        return CXL_BULK_ERROR_NOT_READY;
    }
    while (!server_lock_is_held(client->lock_fd)) {
        if (deadline_expired(deadline)) {
            close_client_mappings(client);
            free(client);
            return CXL_BULK_ERROR_NOT_READY;
        }
        wait_briefly();
    }
    while ((client->control_fd = shm_open(control_name, O_RDWR, 0)) < 0) {
        if (errno != ENOENT || !server_lock_is_held(client->lock_fd) || deadline_expired(deadline)) {
            close_client_mappings(client);
            free(client);
            return CXL_BULK_ERROR_OPEN;
        }
        wait_briefly();
    }
    struct stat control_stat;
    for (;;) {
        if (!server_lock_is_held(client->lock_fd)) {
            close_client_mappings(client);
            free(client);
            return CXL_BULK_ERROR_NOT_READY;
        }
        if (fstat(client->control_fd, &control_stat) == 0 && control_stat.st_size >= (off_t)sizeof(cxl_bulk_header_t)) {
            break;
        }
        if (deadline_expired(deadline)) {
            close_client_mappings(client);
            free(client);
            return CXL_BULK_ERROR_NOT_READY;
        }
        wait_briefly();
    }
    client->header = mmap(NULL, sizeof(cxl_bulk_header_t), PROT_READ | PROT_WRITE, MAP_SHARED, client->control_fd, 0);
    if (client->header == MAP_FAILED) {
        client->header = NULL;
        close_client_mappings(client);
        free(client);
        return CXL_BULK_ERROR_MAP;
    }
    while (__atomic_load_n(&client->header->server_ready, __ATOMIC_ACQUIRE) == 0) {
        if (!server_lock_is_held(client->lock_fd)) {
            close_client_mappings(client);
            free(client);
            return CXL_BULK_ERROR_NOT_READY;
        }
        if (deadline_expired(deadline)) {
            close_client_mappings(client);
            free(client);
            return CXL_BULK_ERROR_NOT_READY;
        }
        wait_briefly();
    }
    if (!server_lock_is_held(client->lock_fd)) {
        close_client_mappings(client);
        free(client);
        return CXL_BULK_ERROR_NOT_READY;
    }
    if (client->header->magic != CXL_BULK_MAGIC || client->header->version != CXL_BULK_VERSION ||
        client->header->header_size != sizeof(cxl_bulk_header_t) || client->header->slot_count != CXL_BULK_MAX_SLOTS ||
        client->header->cacheline_size != CXL_BULK_CACHELINE_SIZE) {
        close_client_mappings(client);
        free(client);
        return CXL_BULK_ERROR_PROTOCOL;
    }
    const uint64_t published_generation = client->header->server_generation;
    uint64_t lock_generation = 0;
    while (published_generation == 0 || !read_lock_generation(client->lock_fd, &lock_generation) ||
           lock_generation != published_generation) {
        if (!server_lock_is_held(client->lock_fd) || deadline_expired(deadline)) {
            close_client_mappings(client);
            free(client);
            return CXL_BULK_ERROR_NOT_READY;
        }
        wait_briefly();
    }
    client->server_generation = published_generation;

    client->data_mapped_size = client->header->data_mapped_size;
    client->data_size = client->header->data_size;
    if (client->data_mapped_size == 0 || client->data_size == 0 ||
        client->header->data_offset > client->data_mapped_size ||
        client->data_size > client->data_mapped_size - client->header->data_offset ||
        memchr(client->header->data_name, '\0', CXL_BULK_DATA_NAME_SIZE) == NULL) {
        close_client_mappings(client);
        free(client);
        return CXL_BULK_ERROR_PROTOCOL;
    }
    client->data_fd = shm_open(client->header->data_name, O_RDWR, 0);
    if (client->data_fd < 0) {
        close_client_mappings(client);
        free(client);
        return CXL_BULK_ERROR_OPEN;
    }
    struct stat data_stat;
    if (fstat(client->data_fd, &data_stat) != 0 || data_stat.st_size < (off_t)client->data_mapped_size) {
        close_client_mappings(client);
        free(client);
        return CXL_BULK_ERROR_PROTOCOL;
    }
    client->data_mapping = mmap(NULL, client->data_mapped_size, PROT_READ | PROT_WRITE, MAP_SHARED, client->data_fd, 0);
    if (client->data_mapping == MAP_FAILED) {
        client->data_mapping = NULL;
        close_client_mappings(client);
        free(client);
        return CXL_BULK_ERROR_MAP;
    }
    client->data = (uint8_t *)client->data_mapping + client->header->data_offset;
    if (!read_process_start_ticks((uint32_t)getpid(), &client->process_start_ticks)) {
        close_client_mappings(client);
        free(client);
        return CXL_BULK_ERROR_PROTOCOL;
    }
    if ((uint64_t)getpid() > CXL_BULK_OWNER_PID_MASK || client->process_start_ticks > CXL_BULK_OWNER_START_MAX) {
        close_client_mappings(client);
        free(client);
        return CXL_BULK_ERROR_PROTOCOL;
    }
    client->owner_token = (client->process_start_ticks << CXL_BULK_OWNER_PID_BITS) | (uint64_t)getpid();
    *out_client = client;
    return CXL_BULK_SUCCESS;
}

void cxl_bulk_client_close(cxl_bulk_client_t *client) {
    if (client == NULL) {
        return;
    }
    close_client_mappings(client);
    free(client);
}

uint64_t cxl_bulk_client_capacity(const cxl_bulk_client_t *client) { return client == NULL ? 0 : client->data_size; }

static cxl_bulk_slot_t *reserve_slot(cxl_bulk_client_t *client, uint64_t deadline_ns) {
    uint64_t next_liveness_check = monotonic_time_ns() + CXL_BULK_LIVENESS_CHECK_NS;
    while (!deadline_expired(deadline_ns)) {
        for (uint32_t index = 0; index < client->header->slot_count; ++index) {
            cxl_bulk_slot_t *slot = &client->header->slots[index];
            uint64_t expected_owner = 0;
            if (!__atomic_compare_exchange_n(&slot->owner_token, &expected_owner, client->owner_token, 0,
                                             __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
                continue;
            }
            uint32_t expected_state = CXL_BULK_SLOT_FREE;
            if (__atomic_compare_exchange_n(&slot->state, &expected_state, CXL_BULK_SLOT_RESERVED, 0, __ATOMIC_ACQ_REL,
                                            __ATOMIC_ACQUIRE)) {
                return slot;
            }
            __atomic_store_n(&slot->owner_token, 0, __ATOMIC_RELEASE);
        }
        if (__atomic_load_n(&client->header->server_ready, __ATOMIC_ACQUIRE) == 0) {
            return NULL;
        }
        const uint64_t now = monotonic_time_ns();
        if (now >= next_liveness_check) {
            if (!server_identity_is_valid(client)) {
                return NULL;
            }
            next_liveness_check = now + CXL_BULK_LIVENESS_CHECK_NS;
        }
        wait_briefly();
    }
    return NULL;
}

static void release_slot_owner(cxl_bulk_slot_t *slot, uint64_t owner_token) {
    uint64_t expected = owner_token;
    __atomic_compare_exchange_n(&slot->owner_token, &expected, 0, 0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE);
}

static void abandon_slot(cxl_bulk_slot_t *slot, uint64_t owner_token) {
    __atomic_store_n(&slot->abandoned, 1U, __ATOMIC_RELEASE);
    uint32_t expected = CXL_BULK_SLOT_COMPLETE;
    if (__atomic_compare_exchange_n(&slot->state, &expected, CXL_BULK_SLOT_FREE, 0, __ATOMIC_ACQ_REL,
                                    __ATOMIC_ACQUIRE)) {
        release_slot_owner(slot, owner_token);
    }
}

static int transfer(cxl_bulk_client_t *client, uint32_t operation, uint64_t offset, void *buffer, uint64_t size,
                    cxl_bulk_result_t *result) {
    if (client == NULL || buffer == NULL || result == NULL || size == 0) {
        return CXL_BULK_ERROR_INVALID_ARGUMENT;
    }
    if (offset > client->data_size || size > client->data_size - offset) {
        return CXL_BULK_ERROR_RANGE;
    }
    memset(result, 0, sizeof(*result));
    const uint64_t deadline = timeout_deadline(client->timeout_ms);
    cxl_bulk_slot_t *slot = reserve_slot(client, deadline);
    if (slot == NULL) {
        return __atomic_load_n(&client->header->server_ready, __ATOMIC_ACQUIRE) == 0 ||
                       !server_identity_is_valid(client)
                   ? CXL_BULK_ERROR_SERVER
                   : CXL_BULK_ERROR_TIMEOUT;
    }

    memset((void *)((uint8_t *)slot + offsetof(cxl_bulk_slot_t, copy_done)), 0,
           offsetof(cxl_bulk_slot_t, owner_token) - offsetof(cxl_bulk_slot_t, copy_done));
    slot->operation = operation;
    slot->client_pid = (uint32_t)getpid();
    slot->request_id = __atomic_add_fetch(&client->next_request_id, 1U, __ATOMIC_RELAXED);
    slot->client_start_ticks = client->process_start_ticks;
    slot->offset = offset;
    slot->size = size;
    slot->submitted_ns = monotonic_time_ns();
    uint32_t expected_state = CXL_BULK_SLOT_RESERVED;
    if (!__atomic_compare_exchange_n(&slot->state, &expected_state, CXL_BULK_SLOT_SUBMITTED, 0, __ATOMIC_ACQ_REL,
                                     __ATOMIC_ACQUIRE)) {
        release_slot_owner(slot, client->owner_token);
        return CXL_BULK_ERROR_SERVER;
    }

    const uint64_t copy_started = monotonic_time_ns();
    if (operation == CXL_BULK_OP_READ) {
        memcpy(buffer, client->data + offset, size);
    } else {
        memcpy(client->data + offset, buffer, size);
    }
    slot->host_copy_ns = monotonic_time_ns() - copy_started;
    __atomic_store_n(&slot->copy_done, 1U, __ATOMIC_RELEASE);

    uint64_t next_liveness_check = monotonic_time_ns() + CXL_BULK_LIVENESS_CHECK_NS;
    for (;;) {
        const uint32_t state = __atomic_load_n(&slot->state, __ATOMIC_ACQUIRE);
        if (state == CXL_BULK_SLOT_COMPLETE) {
            if (slot->status != CXL_BULK_STATUS_OK) {
                __atomic_store_n(&slot->state, CXL_BULK_SLOT_FREE, __ATOMIC_RELEASE);
                release_slot_owner(slot, client->owner_token);
                return CXL_BULK_ERROR_SERVER;
            }
            result->bytes = slot->size;
            result->host_copy_ns = slot->host_copy_ns;
            result->model_latency_ns = slot->model_latency_ns;
            result->serialization_ns = slot->serialization_ns;
            result->cacheline_count = slot->cacheline_count;
            __atomic_store_n(&slot->state, CXL_BULK_SLOT_FREE, __ATOMIC_RELEASE);
            release_slot_owner(slot, client->owner_token);
            return CXL_BULK_SUCCESS;
        }
        if (__atomic_load_n(&client->header->server_ready, __ATOMIC_ACQUIRE) == 0) {
            abandon_slot(slot, client->owner_token);
            return CXL_BULK_ERROR_SERVER;
        }
        const uint64_t now = monotonic_time_ns();
        if (now >= next_liveness_check) {
            if (!server_identity_is_valid(client)) {
                abandon_slot(slot, client->owner_token);
                return CXL_BULK_ERROR_SERVER;
            }
            next_liveness_check = now + CXL_BULK_LIVENESS_CHECK_NS;
        }
        if (deadline_expired(deadline)) {
            abandon_slot(slot, client->owner_token);
            return CXL_BULK_ERROR_TIMEOUT;
        }
        wait_briefly();
    }
}

int cxl_bulk_read(cxl_bulk_client_t *client, uint64_t offset, void *dst, uint64_t size, cxl_bulk_result_t *result) {
    return transfer(client, CXL_BULK_OP_READ, offset, dst, size, result);
}

int cxl_bulk_write(cxl_bulk_client_t *client, uint64_t offset, const void *src, uint64_t size,
                   cxl_bulk_result_t *result) {
    return transfer(client, CXL_BULK_OP_WRITE, offset, (void *)src, size, result);
}

const char *cxl_bulk_error_string(int error_code) {
    switch (error_code) {
    case CXL_BULK_SUCCESS:
        return "success";
    case CXL_BULK_ERROR_INVALID_ARGUMENT:
        return "invalid argument";
    case CXL_BULK_ERROR_OPEN:
        return "failed to open shared memory";
    case CXL_BULK_ERROR_PROTOCOL:
        return "incompatible bulk protocol";
    case CXL_BULK_ERROR_NOT_READY:
        return "bulk server is not ready";
    case CXL_BULK_ERROR_MAP:
        return "failed to map shared memory";
    case CXL_BULK_ERROR_TIMEOUT:
        return "bulk request timed out";
    case CXL_BULK_ERROR_SERVER:
        return "bulk server rejected or stopped the request";
    case CXL_BULK_ERROR_RANGE:
        return "bulk request is out of range";
    default:
        return "unknown bulk client error";
    }
}
