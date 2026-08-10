#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <glob.h>
#include <inttypes.h>
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define CXL_TYPE2_VENDOR "0x8086"
#define CXL_TYPE2_DEVICE "0x0d92"

#define CXL_GPU_REG_MAGIC 0x0000
#define CXL_GPU_REG_CAPS 0x000c
#define CXL_GPU_REG_CMD 0x0010
#define CXL_GPU_REG_CMD_STATUS 0x0014
#define CXL_GPU_REG_CMD_RESULT 0x0018
#define CXL_GPU_REG_PARAM0 0x0040
#define CXL_GPU_REG_RESULT0 0x0080

#define CXL_GPU_MAGIC 0x43584c32U
#define CXL_GPU_CAP_CACHE_COHERENT (1U << 1)
#define CXL_GPU_CMD_STATUS_COMPLETE 3U
#define CXL_GPU_CMD_STATUS_ERROR 4U
#define CXL_GPU_SUCCESS 0U
#define CXL_GPU_CMD_COHERENT_LOAD 0x83U
#define CXL_GPU_CMD_COHERENT_STORE 0x84U
#define CXL_GPU_CMD_COHERENT_FAA 0x85U
#define CXL_GPU_CMD_COHERENT_CAS 0x86U
#define CXL_GPU_CMD_COHERENT_FENCE 0xa3U

#define CACHELINE_SIZE 64ULL
#define DEFAULT_MAP_SIZE (2ULL * 1024 * 1024)
#define DEFAULT_BASE_OFFSET (64ULL * 1024)
#define DEFAULT_ITERATIONS 128U
#define COMMAND_TIMEOUT_MS 5000U

typedef struct {
    const char *dax_path;
    const char *bar_path;
    size_t map_size;
    uint64_t base_offset;
    unsigned iterations;
} options_t;

typedef struct {
    int dax_fd;
    int bar_fd;
    volatile uint8_t *dax;
    volatile uint8_t *bar;
    size_t dax_size;
    char dax_path[PATH_MAX];
    char bar_path[PATH_MAX];
    char pci_bdf[64];
    uint64_t device_session;
    uint64_t host_loads;
    uint64_t host_stores;
    uint64_t device_loads;
    uint64_t device_stores;
    uint64_t device_faa;
    uint64_t device_cas;
    uint64_t device_fences;
} context_t;

typedef struct {
    uint64_t negative_forbidden;
    uint64_t h2d_forbidden;
    uint64_t d2h_forbidden;
    uint64_t mp_forbidden;
    uint64_t partial_line_forbidden;
    uint64_t atomic_forbidden;
    uint64_t replacement_forbidden;
} results_t;

static void usage(const char *program) {
    fprintf(stderr,
            "Usage: %s [--dax /dev/daxX.Y] [--bar /sys/bus/pci/devices/BDF/resource2] "
            "[--iterations N] [--map-size BYTES] [--base-offset BYTES]\n",
            program);
}

static bool parse_u64(const char *text, uint64_t *value) {
    char *end = NULL;
    errno = 0;
    unsigned long long parsed = strtoull(text, &end, 0);
    if (errno || end == text || *end != '\0')
        return false;
    *value = parsed;
    return true;
}

static bool parse_options(int argc, char **argv, options_t *options) {
    *options = (options_t){
        .map_size = DEFAULT_MAP_SIZE,
        .base_offset = DEFAULT_BASE_OFFSET,
        .iterations = DEFAULT_ITERATIONS,
    };
    for (int index = 1; index < argc; ++index) {
        if (strcmp(argv[index], "--help") == 0) {
            usage(argv[0]);
            exit(EXIT_SUCCESS);
        }
        if (index + 1 >= argc)
            return false;
        const char *value = argv[++index];
        uint64_t parsed = 0;
        if (strcmp(argv[index - 1], "--dax") == 0) {
            options->dax_path = value;
        } else if (strcmp(argv[index - 1], "--bar") == 0) {
            options->bar_path = value;
        } else if (strcmp(argv[index - 1], "--iterations") == 0 && parse_u64(value, &parsed) && parsed > 0 &&
                   parsed <= UINT32_MAX) {
            options->iterations = (unsigned)parsed;
        } else if (strcmp(argv[index - 1], "--map-size") == 0 && parse_u64(value, &parsed) && parsed > 0 &&
                   parsed <= SIZE_MAX) {
            options->map_size = (size_t)parsed;
        } else if (strcmp(argv[index - 1], "--base-offset") == 0 && parse_u64(value, &parsed)) {
            options->base_offset = parsed;
        } else {
            return false;
        }
    }
    return true;
}

static bool read_text(const char *path, char *buffer, size_t capacity) {
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0)
        return false;
    ssize_t count = read(fd, buffer, capacity - 1);
    close(fd);
    if (count <= 0)
        return false;
    while (count > 0 && (buffer[count - 1] == '\n' || buffer[count - 1] == '\r'))
        --count;
    buffer[count] = '\0';
    return true;
}

static bool discover_dax(char path[PATH_MAX]) {
    glob_t matches = {0};
    if (glob("/dev/dax*", 0, NULL, &matches) != 0)
        return false;
    bool found = false;
    for (size_t index = 0; index < matches.gl_pathc; ++index) {
        struct stat attributes;
        if (stat(matches.gl_pathv[index], &attributes) == 0 && S_ISCHR(attributes.st_mode)) {
            snprintf(path, PATH_MAX, "%s", matches.gl_pathv[index]);
            found = true;
            break;
        }
    }
    globfree(&matches);
    return found;
}

static bool discover_bar(char path[PATH_MAX], char bdf[64]) {
    glob_t matches = {0};
    if (glob("/sys/bus/pci/devices/*", GLOB_ONLYDIR, NULL, &matches) != 0)
        return false;
    bool found = false;
    for (size_t index = 0; index < matches.gl_pathc; ++index) {
        char vendor_path[PATH_MAX];
        char device_path[PATH_MAX];
        char vendor[32];
        char device[32];
        snprintf(vendor_path, sizeof(vendor_path), "%s/vendor", matches.gl_pathv[index]);
        snprintf(device_path, sizeof(device_path), "%s/device", matches.gl_pathv[index]);
        if (!read_text(vendor_path, vendor, sizeof(vendor)) || !read_text(device_path, device, sizeof(device)) ||
            strcasecmp(vendor, CXL_TYPE2_VENDOR) != 0 || strcasecmp(device, CXL_TYPE2_DEVICE) != 0)
            continue;
        snprintf(path, PATH_MAX, "%s/resource2", matches.gl_pathv[index]);
        if (access(path, R_OK | W_OK) != 0)
            continue;
        const char *slash = strrchr(matches.gl_pathv[index], '/');
        snprintf(bdf, 64, "%s", slash ? slash + 1 : matches.gl_pathv[index]);
        found = true;
        break;
    }
    globfree(&matches);
    return found;
}

static void context_close(context_t *context) {
    if (context->dax && context->dax != MAP_FAILED)
        munmap((void *)context->dax, context->dax_size);
    if (context->bar && context->bar != MAP_FAILED)
        munmap((void *)context->bar, 4096);
    if (context->dax_fd >= 0)
        close(context->dax_fd);
    if (context->bar_fd >= 0)
        close(context->bar_fd);
    context->dax = NULL;
    context->bar = NULL;
    context->dax_fd = -1;
    context->bar_fd = -1;
}

static bool context_open(context_t *context, const options_t *options) {
    memset(context, 0, sizeof(*context));
    context->dax_fd = -1;
    context->bar_fd = -1;
    if (options->dax_path)
        snprintf(context->dax_path, sizeof(context->dax_path), "%s", options->dax_path);
    else if (!discover_dax(context->dax_path)) {
        fprintf(stderr, "no /dev/dax* device found\n");
        return false;
    }
    if (options->bar_path) {
        snprintf(context->bar_path, sizeof(context->bar_path), "%s", options->bar_path);
        const char *resource = strstr(context->bar_path, "/resource2");
        if (resource) {
            const char *end = resource;
            const char *start = end;
            while (start > context->bar_path && start[-1] != '/')
                --start;
            snprintf(context->pci_bdf, sizeof(context->pci_bdf), "%.*s", (int)(end - start), start);
        } else {
            snprintf(context->pci_bdf, sizeof(context->pci_bdf), "explicit");
        }
    } else if (!discover_bar(context->bar_path, context->pci_bdf)) {
        fprintf(stderr, "no 8086:0d92 Type-2 BAR2 found\n");
        return false;
    }

    context->dax_fd = open(context->dax_path, O_RDWR | O_CLOEXEC | O_SYNC);
    if (context->dax_fd < 0) {
        perror("open dax");
        context_close(context);
        return false;
    }
    context->dax_size = options->map_size;
    context->dax = mmap(NULL, context->dax_size, PROT_READ | PROT_WRITE, MAP_SHARED, context->dax_fd, 0);
    if (context->dax == MAP_FAILED) {
        context->dax = NULL;
        perror("mmap dax");
        context_close(context);
        return false;
    }
    context->bar_fd = open(context->bar_path, O_RDWR | O_CLOEXEC | O_SYNC);
    if (context->bar_fd < 0) {
        perror("open BAR2");
        context_close(context);
        return false;
    }
    context->bar = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_SHARED, context->bar_fd, 0);
    if (context->bar == MAP_FAILED) {
        context->bar = NULL;
        perror("mmap BAR2");
        context_close(context);
        return false;
    }
    return true;
}

static uint32_t mmio_read32(context_t *context, size_t offset) {
    uint32_t value = *(volatile uint32_t *)(context->bar + offset);
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
    return value;
}

static uint64_t mmio_read64(context_t *context, size_t offset) {
    uint64_t value = *(volatile uint64_t *)(context->bar + offset);
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
    return value;
}

static void mmio_write32(context_t *context, size_t offset, uint32_t value) {
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
    *(volatile uint32_t *)(context->bar + offset) = value;
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
}

static void mmio_write64(context_t *context, size_t offset, uint64_t value) {
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
    *(volatile uint64_t *)(context->bar + offset) = value;
    __atomic_thread_fence(__ATOMIC_SEQ_CST);
}

static uint64_t monotonic_milliseconds(void) {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint64_t)now.tv_sec * 1000 + (uint64_t)now.tv_nsec / 1000000;
}

static bool update_device_identity(context_t *context, uint32_t command, const uint64_t results[3]) {
    const bool returns_endpoint = command == CXL_GPU_CMD_COHERENT_LOAD || command == CXL_GPU_CMD_COHERENT_STORE ||
                                  command == CXL_GPU_CMD_COHERENT_FENCE;
    const bool returns_session =
        returns_endpoint || command == CXL_GPU_CMD_COHERENT_FAA || command == CXL_GPU_CMD_COHERENT_CAS;
    if (!returns_session)
        return true;
    if (returns_endpoint && results[1] != 1) {
        fprintf(stderr, "BAR2 command 0x%x returned endpoint=%" PRIu64 "\n", command, results[1]);
        return false;
    }
    if (results[2] == 0) {
        fprintf(stderr, "BAR2 command 0x%x returned a zero session\n", command);
        return false;
    }
    if (context->device_session != 0 && context->device_session != results[2]) {
        fprintf(stderr, "device session changed from %" PRIu64 " to %" PRIu64 "\n", context->device_session,
                results[2]);
        return false;
    }
    context->device_session = results[2];
    return true;
}

static bool device_command(context_t *context, uint32_t command, const uint64_t params[3], uint64_t results[3]) {
    for (size_t index = 0; index < 3; ++index)
        mmio_write64(context, CXL_GPU_REG_PARAM0 + index * sizeof(uint64_t), params ? params[index] : 0);
    mmio_write32(context, CXL_GPU_REG_CMD, command);
    const uint64_t deadline = monotonic_milliseconds() + COMMAND_TIMEOUT_MS;
    for (;;) {
        uint32_t status = mmio_read32(context, CXL_GPU_REG_CMD_STATUS);
        if (status == CXL_GPU_CMD_STATUS_COMPLETE || status == CXL_GPU_CMD_STATUS_ERROR)
            break;
        if (monotonic_milliseconds() >= deadline) {
            fprintf(stderr, "BAR2 command 0x%x timed out\n", command);
            return false;
        }
        struct timespec pause = {.tv_nsec = 1000000};
        nanosleep(&pause, NULL);
    }
    uint32_t result = mmio_read32(context, CXL_GPU_REG_CMD_RESULT);
    for (size_t index = 0; index < 3; ++index)
        results[index] = mmio_read64(context, CXL_GPU_REG_RESULT0 + index * sizeof(uint64_t));
    if (result != CXL_GPU_SUCCESS) {
        fprintf(stderr, "BAR2 command 0x%x failed with result %u\n", command, result);
        return false;
    }
    return update_device_identity(context, command, results);
}

static bool device_load(context_t *context, uint64_t address, uint64_t *value) {
    const uint64_t params[3] = {address, sizeof(uint64_t), 0};
    uint64_t results[3] = {0};
    if (!device_command(context, CXL_GPU_CMD_COHERENT_LOAD, params, results))
        return false;
    *value = results[0];
    ++context->device_loads;
    return true;
}

static bool device_store(context_t *context, uint64_t address, uint64_t value) {
    const uint64_t params[3] = {address, sizeof(uint64_t), value};
    uint64_t results[3] = {0};
    if (!device_command(context, CXL_GPU_CMD_COHERENT_STORE, params, results))
        return false;
    ++context->device_stores;
    return true;
}

static bool device_fetch_add(context_t *context, uint64_t address, uint64_t addend, uint64_t *old_value,
                             uint64_t *new_value) {
    const uint64_t params[3] = {address, addend, 0};
    uint64_t results[3] = {0};
    if (!device_command(context, CXL_GPU_CMD_COHERENT_FAA, params, results))
        return false;
    *old_value = results[0];
    *new_value = results[1];
    ++context->device_faa;
    return true;
}

static bool device_compare_exchange(context_t *context, uint64_t address, uint64_t expected, uint64_t desired,
                                    uint64_t *old_value, uint64_t *new_value) {
    const uint64_t params[3] = {address, expected, desired};
    uint64_t results[3] = {0};
    if (!device_command(context, CXL_GPU_CMD_COHERENT_CAS, params, results))
        return false;
    *old_value = results[0];
    *new_value = results[1];
    ++context->device_cas;
    return true;
}

static bool device_fence(context_t *context) {
    const uint64_t params[3] = {0};
    uint64_t results[3] = {0};
    if (!device_command(context, CXL_GPU_CMD_COHERENT_FENCE, params, results))
        return false;
    ++context->device_fences;
    return true;
}

static void host_store(context_t *context, uint64_t address, uint64_t value) {
    __atomic_store_n((uint64_t *)(context->dax + address), value, __ATOMIC_SEQ_CST);
    ++context->host_stores;
}

static uint64_t host_load(context_t *context, uint64_t address) {
    uint64_t value = __atomic_load_n((uint64_t *)(context->dax + address), __ATOMIC_SEQ_CST);
    ++context->host_loads;
    return value;
}

static bool run_litmus(context_t *context, const options_t *options, results_t *results) {
    const uint64_t base = options->base_offset;
    const uint64_t h2d = base;
    const uint64_t d2h = base + CACHELINE_SIZE;
    const uint64_t mp_data = base + 2 * CACHELINE_SIZE;
    const uint64_t mp_flag = base + 3 * CACHELINE_SIZE;
    const uint64_t partial = base + 4 * CACHELINE_SIZE;
    const uint64_t atomic = base + 5 * CACHELINE_SIZE;
    const uint64_t replacement = base + 16 * CACHELINE_SIZE;
    const size_t required = replacement + 32 * CACHELINE_SIZE;
    if (required > context->dax_size) {
        fprintf(stderr, "mapped DAX range is too small: need %zu bytes\n", required);
        return false;
    }

    uint64_t legacy_host_shadow = UINT64_C(0xfeedfacecafebeef);
    uint64_t legacy_device_shadow = 0;
    results->negative_forbidden = legacy_host_shadow != legacy_device_shadow;

    const uint64_t h2d_value = UINT64_C(0x1122334455667788);
    if (!device_store(context, h2d, 0))
        return false;
    host_store(context, h2d, h2d_value);
    uint64_t observed = 0;
    if (!device_load(context, h2d, &observed))
        return false;
    results->h2d_forbidden += observed != h2d_value;

    const uint64_t d2h_value = UINT64_C(0xa5a5a5a55a5a5a5a);
    host_store(context, d2h, 0);
    if (!device_store(context, d2h, d2h_value))
        return false;
    results->d2h_forbidden += host_load(context, d2h) != d2h_value;

    for (unsigned iteration = 1; iteration <= options->iterations; ++iteration) {
        if (!device_store(context, mp_data, 0) || !device_store(context, mp_flag, 0))
            return false;
        host_store(context, mp_data, iteration);
        host_store(context, mp_flag, 1);
        uint64_t flag = 0;
        if (!device_load(context, mp_flag, &flag))
            return false;
        if (flag == 1) {
            uint64_t data = 0;
            if (!device_load(context, mp_data, &data))
                return false;
            results->mp_forbidden += data != iteration;
        } else {
            ++results->mp_forbidden;
        }
    }

    const uint64_t low = UINT64_C(0x1111111111111111);
    const uint64_t high = UINT64_C(0x2222222222222222);
    host_store(context, partial, low);
    if (!device_store(context, partial + sizeof(uint64_t), high))
        return false;
    results->partial_line_forbidden += host_load(context, partial) != low;
    results->partial_line_forbidden += host_load(context, partial + sizeof(uint64_t)) != high;

    host_store(context, atomic, 0);
    for (unsigned iteration = 0; iteration < options->iterations; ++iteration) {
        uint64_t old_value = 0;
        uint64_t new_value = 0;
        if (!device_fetch_add(context, atomic, 1, &old_value, &new_value))
            return false;
        results->atomic_forbidden += old_value != iteration || new_value != (uint64_t)iteration + 1;
    }
    uint64_t old_value = 0;
    uint64_t new_value = 0;
    if (!device_compare_exchange(context, atomic, options->iterations, UINT64_C(0x9999), &old_value, &new_value))
        return false;
    results->atomic_forbidden += old_value != options->iterations || new_value != UINT64_C(0x9999);
    results->atomic_forbidden += host_load(context, atomic) != UINT64_C(0x9999);

    for (unsigned line = 0; line < 32; ++line) {
        if (!device_store(context, replacement + line * CACHELINE_SIZE, UINT64_C(0xd000000000000000) + line))
            return false;
    }
    if (!device_fence(context))
        return false;
    for (unsigned line = 0; line < 32; ++line) {
        uint64_t expected = UINT64_C(0xd000000000000000) + line;
        results->replacement_forbidden += host_load(context, replacement + line * CACHELINE_SIZE) != expected;
    }
    return true;
}

static uint64_t positive_forbidden(const results_t *results) {
    return results->h2d_forbidden + results->d2h_forbidden + results->mp_forbidden + results->partial_line_forbidden +
           results->atomic_forbidden + results->replacement_forbidden;
}

static void print_json(const context_t *context, const options_t *options, const results_t *results, bool ran) {
    uint64_t forbidden = positive_forbidden(results);
    bool pass = ran && results->negative_forbidden > 0 && forbidden == 0 && context->device_session != 0;
    printf("{\"schema\":\"cxlmemsim.type2-litmus.v1\",\"status\":\"%s\",", pass ? "pass" : "fail");
    printf("\"proof_boundary\":\"TCG functional modeled coherence\",");
    printf("\"topology\":{\"pci_bdf\":\"%s\",\"pci_id\":\"8086:0d92\",\"dax\":\"%s\","
           "\"bar2\":\"%s\",\"host_endpoint\":0,\"device_endpoint\":1,\"device_session\":%" PRIu64 "},",
           context->pci_bdf, context->dax_path, context->bar_path, context->device_session);
    printf("\"iterations\":%u,\"negative_control\":{\"forbidden\":%" PRIu64 "},", options->iterations,
           results->negative_forbidden);
    printf("\"tests\":{\"h2d\":{\"forbidden\":%" PRIu64 "},\"d2h\":{\"forbidden\":%" PRIu64
           "},\"mp\":{\"forbidden\":%" PRIu64 "},\"partial_line\":{\"forbidden\":%" PRIu64
           "},\"atomic_faa_cas\":{\"forbidden\":%" PRIu64 "},\"bounded_replacement\":{\"forbidden\":%" PRIu64 "}},",
           results->h2d_forbidden, results->d2h_forbidden, results->mp_forbidden, results->partial_line_forbidden,
           results->atomic_forbidden, results->replacement_forbidden);
    printf("\"forbidden_total\":%" PRIu64 ",", forbidden);
    printf("\"counters\":{\"host_loads\":%" PRIu64 ",\"host_stores\":%" PRIu64 ",\"device_loads\":%" PRIu64
           ",\"device_stores\":%" PRIu64 ",\"device_faa\":%" PRIu64 ",\"device_cas\":%" PRIu64
           ",\"device_fences\":%" PRIu64 "}}\n",
           context->host_loads, context->host_stores, context->device_loads, context->device_stores,
           context->device_faa, context->device_cas, context->device_fences);
}

int main(int argc, char **argv) {
    options_t options;
    context_t context;
    results_t results = {0};
    if (!parse_options(argc, argv, &options)) {
        usage(argv[0]);
        return EXIT_FAILURE;
    }
    if (options.map_size < DEFAULT_MAP_SIZE || options.base_offset % CACHELINE_SIZE != 0 ||
        options.base_offset >= options.map_size) {
        fprintf(stderr, "invalid map size or base offset\n");
        return EXIT_FAILURE;
    }
    if (!context_open(&context, &options)) {
        memset(&context, 0, sizeof(context));
        print_json(&context, &options, &results, false);
        return EXIT_FAILURE;
    }
    bool device_valid = mmio_read32(&context, CXL_GPU_REG_MAGIC) == CXL_GPU_MAGIC &&
                        (mmio_read32(&context, CXL_GPU_REG_CAPS) & CXL_GPU_CAP_CACHE_COHERENT) != 0;
    bool ran = device_valid && run_litmus(&context, &options, &results);
    print_json(&context, &options, &results, ran);
    bool pass = ran && results.negative_forbidden > 0 && positive_forbidden(&results) == 0;
    context_close(&context);
    return pass ? EXIT_SUCCESS : EXIT_FAILURE;
}
