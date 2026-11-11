/* SPDX-License-Identifier: (LGPL-2.1 OR BSD-2-Clause) */

#include "modeled_access_protocol.h"
#include "modeled_access_service.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <string>
#include <sys/mman.h>
#include <thread>
#include <unistd.h>

using namespace cxlmemsim;

namespace {

uint64_t nowNs() {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

void verifyMissingCapabilityFailsClosed(const std::string &name) {
    shm_unlink(name.c_str());
    const int fd = shm_open(name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    assert(fd >= 0);
    assert(ftruncate(fd, sizeof(ModeledAccessHeader)) == 0);
    void *mapping = mmap(nullptr, sizeof(ModeledAccessHeader), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    assert(mapping != MAP_FAILED);
    auto *header = static_cast<ModeledAccessHeader *>(mapping);
    std::memset(header, 0, sizeof(*header));
    header->magic = kModeledAccessMagic;
    header->version = kModeledAccessVersion;
    header->header_size = sizeof(*header);
    header->endian_marker = kModeledAccessEndianMarker;
    header->slot_count = kModeledAccessMaxSlots;
    header->server_generation = 1;
    header->server_ready = 1;

    cxl_modeled_client_t *client = nullptr;
    const int result = cxl_modeled_client_open(name.c_str(), 50, &client);
    assert(result == CXL_MODELED_ERROR_UNSUPPORTED);
    assert(client == nullptr);
    munmap(mapping, sizeof(*header));
    close(fd);
    shm_unlink(name.c_str());
}

void verifyTimedOutRequestReleasesSlot(const std::string &name) {
    ModeledAccessShmServer server({
        .control_name = name,
        .service =
            {
                .provider_capacity = 4096,
                .provider_alignment = 64,
                .provider = ModeledAccessProvider::PosixShm,
                .read_bandwidth_gbps = 1,
                .write_bandwidth_gbps = 1,
                .read_latency_ns = 100,
                .write_latency_ns = 100,
            },
    });
    assert(server.initialize());
    cxl_modeled_client_t *client = nullptr;
    assert(cxl_modeled_client_open(name.c_str(), 5, &client) == CXL_MODELED_SUCCESS);
    uint64_t region = 0;
    assert(cxl_modeled_register_region(client, 1, 4096, 64, static_cast<uint32_t>(ModeledAccessProvider::PosixShm),
                                       &region) == CXL_MODELED_ERROR_TIMEOUT);

    const int fd = shm_open(name.c_str(), O_RDWR, 0);
    assert(fd >= 0);
    void *mapping = mmap(nullptr, sizeof(ModeledAccessHeader), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    assert(mapping != MAP_FAILED);
    const auto *header = static_cast<const ModeledAccessHeader *>(mapping);
    for (const auto &slot : header->slots) {
        assert(__atomic_load_n(&slot.state, __ATOMIC_ACQUIRE) == static_cast<uint32_t>(ModeledAccessSlotState::Free));
    }
    munmap(mapping, sizeof(*header));
    close(fd);
    cxl_modeled_client_close(client);
    server.shutdown();
}

} // namespace

int main() {
    const std::string control_name = "/cxl_modeled_transport_" + std::to_string(getpid());
    verifyMissingCapabilityFailsClosed(control_name + "_unsupported");
    verifyTimedOutRequestReleasesSlot(control_name + "_timeout");

    ModeledAccessShmServer server({
        .control_name = control_name,
        .service =
            {
                .provider_capacity = 4096,
                .provider_alignment = 64,
                .provider = ModeledAccessProvider::PosixShm,
                .read_bandwidth_gbps = 0.01,
                .write_bandwidth_gbps = 0.02,
                .read_latency_ns = 100,
                .write_latency_ns = 150,
            },
    });
    assert(server.initialize());
    std::atomic<bool> running{true};
    std::thread polling([&]() {
        while (running.load(std::memory_order_relaxed)) {
            if (server.pollOnce() == 0) {
                std::this_thread::yield();
            }
        }
    });

    cxl_modeled_client_t *first_client = nullptr;
    cxl_modeled_client_t *second_client = nullptr;
    assert(cxl_modeled_client_open(control_name.c_str(), 1000, &first_client) == CXL_MODELED_SUCCESS);
    assert(cxl_modeled_client_open(control_name.c_str(), 1000, &second_client) == CXL_MODELED_SUCCESS);

    cxl_modeled_capabilities_t capabilities{};
    assert(cxl_modeled_get_capabilities(first_client, &capabilities) == CXL_MODELED_SUCCESS);
    assert(capabilities.protocol_version == kModeledAccessVersion);
    assert(capabilities.capability_bits == (kGpuDirectModeledAccessV1 | kModeledCompleteAndPoll));
    assert(capabilities.data_capacity == 4096);

    uint64_t region = 0;
    assert(cxl_modeled_register_region(first_client, 1, 4096, 64,
                                       static_cast<uint32_t>(ModeledAccessProvider::PosixShm),
                                       &region) == CXL_MODELED_SUCCESS);
    assert(region != 0);
    uint64_t second_region = 0;
    assert(cxl_modeled_register_region(second_client, 1, 4096, 64,
                                       static_cast<uint32_t>(ModeledAccessProvider::PosixShm),
                                       &second_region) == CXL_MODELED_SUCCESS);
    assert(second_region == region);

    const uint64_t start = nowNs();
    cxl_modeled_begin_result_t first{};
    cxl_modeled_begin_result_t second{};
    assert(cxl_modeled_begin_access(first_client, 1, 11, static_cast<uint32_t>(ModeledAccessDirection::WriteToCxl),
                                    region, 0, 1024, start, &first) == CXL_MODELED_SUCCESS);
    assert(cxl_modeled_begin_access(second_client, 2, 22, static_cast<uint32_t>(ModeledAccessDirection::WriteToCxl),
                                    region, 1024, 1024, start, &second) == CXL_MODELED_SUCCESS);
    assert(second.estimated_complete_ns > first.estimated_complete_ns);

    cxl_modeled_poll_result_t second_poll{};
    assert(cxl_modeled_poll_access(second_client, second.access_token, &second_poll) == CXL_MODELED_SUCCESS);
    assert(second_poll.queue_ns > 0);
    assert(second_poll.service_ns > 0);

    assert(cxl_modeled_data_complete(first_client, first.access_token,
                                     static_cast<uint32_t>(ModeledAccessCudaStatus::Success),
                                     nowNs()) == CXL_MODELED_SUCCESS);
    assert(cxl_modeled_data_complete(first_client, first.access_token,
                                     static_cast<uint32_t>(ModeledAccessCudaStatus::Success),
                                     nowNs()) == CXL_MODELED_ERROR_SERVER);
    assert(cxl_modeled_cancel_access(second_client, second.access_token, "transport test") == CXL_MODELED_SUCCESS);
    assert(cxl_modeled_poll_access(second_client, second.access_token, &second_poll) == CXL_MODELED_SUCCESS);
    assert(second_poll.state == static_cast<uint32_t>(ModeledAccessState::Cancelled));

    cxl_modeled_begin_result_t invalid{};
    assert(cxl_modeled_begin_access(first_client, 3, 11, static_cast<uint32_t>(ModeledAccessDirection::ReadFromCxl),
                                    region, 4090, 64, start, &invalid) == CXL_MODELED_ERROR_RANGE);

    // A combined report must still wait for a future model deadline. Using a
    // future start makes the assertion independent of host scheduling delays.
    const uint64_t future_start = nowNs() + 10'000'000'000ULL;
    cxl_modeled_begin_result_t gated{};
    assert(cxl_modeled_begin_access(first_client, 4, 11, static_cast<uint32_t>(ModeledAccessDirection::ReadFromCxl),
                                    region, 0, 1024, future_start, &gated) == CXL_MODELED_SUCCESS);
    cxl_modeled_poll_result_t gated_poll{};
    assert(cxl_modeled_complete_and_poll(first_client, gated.access_token,
                                         static_cast<uint32_t>(ModeledAccessCudaStatus::Success), future_start,
                                         &gated_poll) == CXL_MODELED_SUCCESS);
    assert(gated_poll.state == static_cast<uint32_t>(ModeledAccessState::DataComplete));
    assert(gated_poll.modeled_complete_ns == gated.estimated_complete_ns);
    assert(gated_poll.service_ns == 102'500);
    assert(cxl_modeled_cancel_access(first_client, gated.access_token, "pending test") == CXL_MODELED_SUCCESS);

    cxl_modeled_begin_result_t failure{};
    assert(cxl_modeled_begin_access(first_client, 5, 11, static_cast<uint32_t>(ModeledAccessDirection::ReadFromCxl),
                                    region, 0, 64, nowNs(), &failure) == CXL_MODELED_SUCCESS);
    assert(cxl_modeled_complete_and_poll(first_client, failure.access_token,
                                         static_cast<uint32_t>(ModeledAccessCudaStatus::Error), nowNs(),
                                         &gated_poll) == CXL_MODELED_SUCCESS);
    assert(gated_poll.state == static_cast<uint32_t>(ModeledAccessState::Error));
    cxl_modeled_begin_result_t success{};
    assert(cxl_modeled_begin_access(first_client, 6, 11, static_cast<uint32_t>(ModeledAccessDirection::ReadFromCxl),
                                    region, 0, 64, nowNs(), &success) == CXL_MODELED_SUCCESS);
    while (nowNs() < success.estimated_complete_ns) {
        std::this_thread::yield();
    }
    assert(cxl_modeled_complete_and_poll(first_client, success.access_token,
                                         static_cast<uint32_t>(ModeledAccessCudaStatus::Success), nowNs(),
                                         &gated_poll) == CXL_MODELED_SUCCESS);
    assert(gated_poll.state == static_cast<uint32_t>(ModeledAccessState::Complete));

    const cxl_modeled_range_t ranges[] = {{100, 0, 64}, {101, 4090, 64}, {102, 0, 64}};
    cxl_modeled_timed_begin_t began[3]{};
    uint32_t completed = 99;
    assert(cxl_modeled_begin_access_batch(first_client, 11, static_cast<uint32_t>(ModeledAccessDirection::ReadFromCxl),
                                          region, ranges, 3, began, &completed) == CXL_MODELED_ERROR_RANGE);
    assert(completed == 1 && began[0].result.access_token != 0);
    assert(began[0].observed_ns >= began[0].started_ns);
    const uint64_t batch_tokens[] = {began[0].result.access_token, UINT64_MAX};
    cxl_modeled_batch_poll_result_t batch_results[2]{};
    assert(cxl_modeled_completion_batch(first_client, batch_tokens, 2,
                                        static_cast<uint32_t>(ModeledAccessCudaStatus::Pending), 0,
                                        batch_results) == CXL_MODELED_SUCCESS);
    assert(batch_results[0].call_status == CXL_MODELED_SUCCESS);
    assert(batch_results[0].result.state != static_cast<uint32_t>(ModeledAccessState::Complete));
    assert(batch_results[1].call_status == CXL_MODELED_ERROR_SERVER);
    assert(cxl_modeled_completion_batch(first_client, batch_tokens, 2,
                                        static_cast<uint32_t>(ModeledAccessCudaStatus::Error), nowNs(),
                                        batch_results) == CXL_MODELED_SUCCESS);
    assert(batch_results[0].result.state == static_cast<uint32_t>(ModeledAccessState::Error));
    assert(batch_results[1].call_status == CXL_MODELED_ERROR_SERVER);

    cxl_modeled_begin_result_t pending[2]{};
    const uint64_t pending_start = nowNs() + 10'000'000'000ULL;
    for (uint64_t i = 0; i < 2; ++i) {
        assert(cxl_modeled_begin_access(first_client, 120 + i, 11,
                                        static_cast<uint32_t>(ModeledAccessDirection::ReadFromCxl), region, i * 64, 64,
                                        pending_start, &pending[i]) == CXL_MODELED_SUCCESS);
    }
    const uint64_t pending_tokens[] = {pending[0].access_token, pending[1].access_token};
    assert(cxl_modeled_completion_batch(first_client, pending_tokens, 2,
                                        static_cast<uint32_t>(ModeledAccessCudaStatus::Success), pending_start,
                                        batch_results) == CXL_MODELED_SUCCESS);
    for (uint32_t i = 0; i < 2; ++i) {
        assert(batch_results[i].call_status == CXL_MODELED_SUCCESS);
        assert(batch_results[i].result.state == static_cast<uint32_t>(ModeledAccessState::DataComplete));
        assert(cxl_modeled_cancel_access(first_client, pending_tokens[i], "batch test") == CXL_MODELED_SUCCESS);
    }
    assert(cxl_modeled_completion_batch(first_client, pending_tokens, 2,
                                        static_cast<uint32_t>(ModeledAccessCudaStatus::Pending), 0,
                                        batch_results) == CXL_MODELED_SUCCESS);
    assert(batch_results[0].result.state == static_cast<uint32_t>(ModeledAccessState::Cancelled));
    assert(batch_results[1].result.state == static_cast<uint32_t>(ModeledAccessState::Cancelled));

    cxl_modeled_client_close(first_client);
    cxl_modeled_client_close(second_client);
    running.store(false, std::memory_order_relaxed);
    polling.join();
    const auto stats = server.stats();
    assert(stats.write_operations == 2);
    assert(stats.payload_copy_bytes == 0);
    server.shutdown();
    return 0;
}
