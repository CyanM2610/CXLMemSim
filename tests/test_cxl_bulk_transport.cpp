#include "cxl_bulk.h"
#include "cxl_bulk_server.h"

#include <atomic>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <functional>
#include <sstream>
#include <string>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {
std::string unique_name(const char *prefix) { return std::string("/") + prefix + "_" + std::to_string(getpid()); }

uint64_t wait_for_zombie_start_ticks(pid_t pid) {
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (std::chrono::steady_clock::now() < deadline) {
        std::ifstream stat("/proc/" + std::to_string(pid) + "/stat");
        std::string line;
        if (stat && std::getline(stat, line)) {
            const auto close_paren = line.rfind(')');
            if (close_paren != std::string::npos) {
                std::istringstream fields(line.substr(close_paren + 2));
                char state = '\0';
                fields >> state;
                std::string value;
                for (int field = 4; field <= 22 && fields >> value; ++field) {
                    if (field == 22 && state == 'Z') {
                        return std::stoull(value);
                    }
                }
            }
        }
        std::this_thread::yield();
    }
    return 0;
}

void test_concurrent_server_start_has_single_owner() {
    const std::string control_name = unique_name("cxl_bulk_owner_control");
    const std::string lock_name = control_name + ".lock";
    const std::string data_name = unique_name("cxl_bulk_owner_data");
    shm_unlink(control_name.c_str());
    shm_unlink(lock_name.c_str());
    shm_unlink(data_name.c_str());
    const int data_fd = shm_open(data_name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    assert(data_fd >= 0);
    assert(ftruncate(data_fd, 4096) == 0);

    const CxlBulkShmServer::Config config{
        .control_name = control_name,
        .data_name = data_name,
        .data_mapped_size = 4096,
        .data_offset = 0,
        .data_size = 4096,
        .read_bandwidth_gbps = 1.0,
        .write_bandwidth_gbps = 1.0,
        .read_latency_ns = 100,
        .write_latency_ns = 150,
    };
    CxlBulkShmServer first(config);
    CxlBulkShmServer second(config);
    std::atomic<int> waiting{0};
    std::atomic<bool> start{false};
    bool first_result = false;
    bool second_result = false;
    auto initialize = [&](CxlBulkShmServer &server, bool &result) {
        waiting.fetch_add(1, std::memory_order_release);
        while (!start.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
        result = server.initialize();
    };
    std::thread first_thread(initialize, std::ref(first), std::ref(first_result));
    std::thread second_thread(initialize, std::ref(second), std::ref(second_result));
    while (waiting.load(std::memory_order_acquire) != 2) {
        std::this_thread::yield();
    }
    start.store(true, std::memory_order_release);
    first_thread.join();
    second_thread.join();
    assert(first_result != second_result);

    first.shutdown();
    second.shutdown();
    close(data_fd);
    shm_unlink(control_name.c_str());
    shm_unlink(lock_name.c_str());
    shm_unlink(data_name.c_str());
}

void test_client_waits_for_header_publication() {
    const std::string control_name = unique_name("cxl_bulk_publish_control");
    const std::string lock_name = control_name + ".lock";
    const std::string data_name = unique_name("cxl_bulk_publish_data");
    shm_unlink(control_name.c_str());
    shm_unlink(lock_name.c_str());
    shm_unlink(data_name.c_str());

    const int data_fd = shm_open(data_name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    assert(data_fd >= 0);
    assert(ftruncate(data_fd, 4096) == 0);
    const int control_fd = shm_open(control_name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    assert(control_fd >= 0);
    const int lock_fd = shm_open(lock_name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    assert(lock_fd >= 0);
    assert(ftruncate(lock_fd, sizeof(uint64_t)) == 0);
    assert(flock(lock_fd, LOCK_EX | LOCK_NB) == 0);
    assert(ftruncate(control_fd, sizeof(cxl_bulk_header_t)) == 0);
    void *mapping = mmap(nullptr, sizeof(cxl_bulk_header_t), PROT_READ | PROT_WRITE, MAP_SHARED, control_fd, 0);
    assert(mapping != MAP_FAILED);
    auto *header = static_cast<cxl_bulk_header_t *>(mapping);
    std::memset(header, 0, sizeof(*header));

    std::thread publisher([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        constexpr uint64_t generation = 42;
        header->magic = CXL_BULK_MAGIC;
        header->version = CXL_BULK_VERSION;
        header->header_size = sizeof(*header);
        header->slot_count = CXL_BULK_MAX_SLOTS;
        header->cacheline_size = CXL_BULK_CACHELINE_SIZE;
        header->server_generation = generation;
        header->data_mapped_size = 4096;
        header->data_size = 4096;
        std::memcpy(header->data_name, data_name.c_str(), data_name.size() + 1);
        __atomic_store_n(&header->server_ready, 1U, __ATOMIC_RELEASE);
        assert(pwrite(lock_fd, &generation, sizeof(generation), 0) == sizeof(generation));
    });

    cxl_bulk_client_t *client = nullptr;
    const int open_result = cxl_bulk_client_open(control_name.c_str(), 1000, &client);
    publisher.join();
    assert(open_result == CXL_BULK_SUCCESS);
    assert(client != nullptr);
    cxl_bulk_client_close(client);

    munmap(mapping, sizeof(cxl_bulk_header_t));
    flock(lock_fd, LOCK_UN);
    close(lock_fd);
    close(control_fd);
    close(data_fd);
    shm_unlink(control_name.c_str());
    shm_unlink(lock_name.c_str());
    shm_unlink(data_name.c_str());
}

void test_stale_control_object_is_rejected() {
    const std::string control_name = unique_name("cxl_bulk_stale_control");
    const std::string data_name = unique_name("cxl_bulk_stale_data");
    shm_unlink(control_name.c_str());
    shm_unlink(data_name.c_str());

    const int data_fd = shm_open(data_name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    assert(data_fd >= 0);
    assert(ftruncate(data_fd, 4096) == 0);
    const int control_fd = shm_open(control_name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    assert(control_fd >= 0);
    assert(ftruncate(control_fd, sizeof(cxl_bulk_header_t)) == 0);
    void *mapping = mmap(nullptr, sizeof(cxl_bulk_header_t), PROT_READ | PROT_WRITE, MAP_SHARED, control_fd, 0);
    assert(mapping != MAP_FAILED);
    auto *header = static_cast<cxl_bulk_header_t *>(mapping);
    std::memset(header, 0, sizeof(*header));
    header->magic = CXL_BULK_MAGIC;
    header->version = CXL_BULK_VERSION;
    header->header_size = sizeof(*header);
    header->slot_count = CXL_BULK_MAX_SLOTS;
    header->cacheline_size = CXL_BULK_CACHELINE_SIZE;
    header->data_mapped_size = 4096;
    header->data_size = 4096;
    std::memcpy(header->data_name, data_name.c_str(), data_name.size() + 1);
    __atomic_store_n(&header->server_ready, 1U, __ATOMIC_RELEASE);

    cxl_bulk_client_t *client = nullptr;
    assert(cxl_bulk_client_open(control_name.c_str(), 100, &client) == CXL_BULK_ERROR_NOT_READY);
    assert(client == nullptr);

    munmap(mapping, sizeof(cxl_bulk_header_t));
    close(control_fd);
    close(data_fd);
    shm_unlink(control_name.c_str());
    shm_unlink(data_name.c_str());
}
} // namespace

int main() {
    test_concurrent_server_start_has_single_owner();
    test_client_waits_for_header_publication();
    test_stale_control_object_is_rejected();

    const std::string control_name = unique_name("cxl_bulk_control");
    const std::string data_name = unique_name("cxl_bulk_data");
    constexpr uint64_t mapped_size = 4096;
    constexpr uint64_t data_offset = 128;
    constexpr uint64_t data_size = 2048;

    shm_unlink(control_name.c_str());
    shm_unlink(data_name.c_str());
    const int data_fd = shm_open(data_name.c_str(), O_CREAT | O_EXCL | O_RDWR, 0600);
    assert(data_fd >= 0);
    assert(ftruncate(data_fd, mapped_size) == 0);
    void *data_mapping = mmap(nullptr, mapped_size, PROT_READ | PROT_WRITE, MAP_SHARED, data_fd, 0);
    assert(data_mapping != MAP_FAILED);

    CxlBulkShmServer server({
        .control_name = control_name,
        .data_name = data_name,
        .data_mapped_size = mapped_size,
        .data_offset = data_offset,
        .data_size = data_size,
        .read_bandwidth_gbps = 1.0,
        .write_bandwidth_gbps = 1.0,
        .read_latency_ns = 100,
        .write_latency_ns = 150,
    });
    assert(server.initialize());

    std::atomic<bool> running{true};
    std::thread polling([&]() {
        while (running.load(std::memory_order_relaxed)) {
            if (server.poll_once() == 0) {
                std::this_thread::yield();
            }
        }
    });

    cxl_bulk_client_t *client = nullptr;
    assert(cxl_bulk_client_open(control_name.c_str(), 1000, &client) == 0);
    assert(client != nullptr);
    assert(cxl_bulk_client_capacity(client) == data_size);

    std::vector<uint8_t> source(257);
    for (size_t index = 0; index < source.size(); ++index) {
        source[index] = static_cast<uint8_t>((index * 17) & 0xff);
    }
    cxl_bulk_result_t write_result{};
    assert(cxl_bulk_write(client, 17, source.data(), source.size(), &write_result) == 0);
    assert(write_result.bytes == source.size());
    assert(write_result.cacheline_count == 5);
    assert(write_result.serialization_ns == 5 * CXL_BULK_CACHELINE_SIZE);
    assert(write_result.model_latency_ns >= 150 + 5 * CXL_BULK_CACHELINE_SIZE);

    std::vector<uint8_t> destination(source.size(), 0);
    cxl_bulk_result_t read_result{};
    assert(cxl_bulk_read(client, 17, destination.data(), destination.size(), &read_result) == 0);
    assert(destination == source);
    assert(read_result.cacheline_count == 5);
    assert(read_result.serialization_ns == 5 * CXL_BULK_CACHELINE_SIZE);
    assert(read_result.model_latency_ns >= 100 + 5 * CXL_BULK_CACHELINE_SIZE);

    const auto stats = server.stats();
    assert(stats.read_requests == 1);
    assert(stats.write_requests == 1);
    assert(stats.read_bytes == source.size());
    assert(stats.write_bytes == source.size());
    assert(stats.read_cachelines == 5);
    assert(stats.write_cachelines == 5);

    CxlBulkShmServer duplicate_server({
        .control_name = control_name,
        .data_name = data_name,
        .data_mapped_size = mapped_size,
        .data_offset = data_offset,
        .data_size = data_size,
        .read_bandwidth_gbps = 1.0,
        .write_bandwidth_gbps = 1.0,
        .read_latency_ns = 100,
        .write_latency_ns = 150,
    });
    assert(!duplicate_server.initialize());

    const int control_fd = shm_open(control_name.c_str(), O_RDWR, 0);
    assert(control_fd >= 0);
    void *control_mapping = mmap(nullptr, sizeof(cxl_bulk_header_t), PROT_READ | PROT_WRITE, MAP_SHARED, control_fd, 0);
    assert(control_mapping != MAP_FAILED);
    auto *control_header = static_cast<cxl_bulk_header_t *>(control_mapping);
    cxl_bulk_slot_t &orphan = control_header->slots[CXL_BULK_MAX_SLOTS - 1];
    const pid_t zombie_pid = fork();
    assert(zombie_pid >= 0);
    if (zombie_pid == 0) {
        _exit(0);
    }
    const uint64_t zombie_start_ticks = wait_for_zombie_start_ticks(zombie_pid);
    assert(zombie_start_ticks != 0);
    assert(static_cast<uint64_t>(zombie_pid) <= 0xffffff);
    assert(zombie_start_ticks <= 0xffffffffff);
    orphan.client_pid = static_cast<uint32_t>(zombie_pid);
    orphan.client_start_ticks = zombie_start_ticks;
    orphan.owner_token = (zombie_start_ticks << 24) | static_cast<uint64_t>(zombie_pid);
    orphan.status = CXL_BULK_STATUS_OK;
    __atomic_store_n(&orphan.state, CXL_BULK_SLOT_RESERVED, __ATOMIC_RELEASE);
    const auto orphan_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (__atomic_load_n(&orphan.state, __ATOMIC_ACQUIRE) != CXL_BULK_SLOT_FREE &&
           std::chrono::steady_clock::now() < orphan_deadline) {
        std::this_thread::yield();
    }
    assert(__atomic_load_n(&orphan.state, __ATOMIC_ACQUIRE) == CXL_BULK_SLOT_FREE);
    const auto stats_with_orphan = server.stats();
    assert(stats_with_orphan.orphaned_requests == 1);
    assert(stats_with_orphan.failed_requests == 1);
    assert(waitpid(zombie_pid, nullptr, 0) == zombie_pid);
    munmap(control_mapping, sizeof(cxl_bulk_header_t));
    close(control_fd);

    cxl_bulk_client_close(client);
    running.store(false, std::memory_order_relaxed);
    polling.join();
    server.shutdown();
    shm_unlink((control_name + ".lock").c_str());

    munmap(data_mapping, mapped_size);
    close(data_fd);
    shm_unlink(data_name.c_str());
    return 0;
}
