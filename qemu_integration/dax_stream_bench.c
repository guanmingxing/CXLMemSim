#define _GNU_SOURCE

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

enum BenchMode {
    MODE_UNSET = 0,
    MODE_WRITE,
    MODE_VERIFY,
};

enum AccessMode {
    ACCESS_MMAP = 0,
    ACCESS_DIRECT,
};

struct BenchOptions {
    enum BenchMode mode;
    enum AccessMode access;
    const char *device;
    uint64_t offset;
    uint64_t bytes;
    uint64_t seed;
    int have_bytes;
};

static void usage(const char *program) {
    fprintf(stderr,
            "usage: %s --mode write|verify [--access mmap|direct] --device PATH --offset BYTES --bytes BYTES "
            "--seed N\n"
            "defaults: --access mmap --device /dev/dax0.0 --offset 0 --seed 1\n",
            program);
}

static int parse_u64(const char *text, uint64_t *out) {
    char *end = NULL;
    unsigned long long value = 0;

    errno = 0;
    value = strtoull(text, &end, 0);
    if (errno != 0 || end == text || *end != '\0') {
        return -1;
    }

    *out = (uint64_t)value;
    return 0;
}

static const char *mode_name(enum BenchMode mode) { return mode == MODE_WRITE ? "write" : "verify"; }

static const char *access_name(enum AccessMode access) { return access == ACCESS_DIRECT ? "direct" : "mmap"; }

static uint64_t splitmix64(uint64_t value) {
    value += UINT64_C(0x9e3779b97f4a7c15);
    value = (value ^ (value >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)) * UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31);
}

static uint64_t pattern_word(uint64_t seed, uint64_t word_index) {
    return splitmix64(seed ^ (word_index * UINT64_C(0x9e3779b97f4a7c15)));
}

static uint64_t checksum_update(uint64_t checksum, uint64_t value) {
    checksum ^= value + UINT64_C(0x9e3779b97f4a7c15) + (checksum << 6) + (checksum >> 2);
    return checksum;
}

static double elapsed_seconds(const struct timespec *start, const struct timespec *end) {
    return (double)(end->tv_sec - start->tv_sec) + (double)(end->tv_nsec - start->tv_nsec) / 1000000000.0;
}

static void print_json_string(FILE *out, const char *text) {
    fputc('"', out);
    for (; *text != '\0'; ++text) {
        unsigned char ch = (unsigned char)*text;

        if (ch == '"' || ch == '\\') {
            fputc('\\', out);
            fputc((int)ch, out);
        } else if (ch == '\b') {
            fputs("\\b", out);
        } else if (ch == '\f') {
            fputs("\\f", out);
        } else if (ch == '\n') {
            fputs("\\n", out);
        } else if (ch == '\r') {
            fputs("\\r", out);
        } else if (ch == '\t') {
            fputs("\\t", out);
        } else if (ch < 0x20) {
            fprintf(out, "\\u%04x", (unsigned int)ch);
        } else {
            fputc((int)ch, out);
        }
    }
    fputc('"', out);
}

static void print_result(const struct BenchOptions *opts, double elapsed_sec, uint64_t errors, uint64_t checksum) {
    double mib_per_sec = 0.0;

    if (elapsed_sec > 0.0) {
        mib_per_sec = ((double)opts->bytes / (1024.0 * 1024.0)) / elapsed_sec;
    }

    fputs("{\"mode\":", stdout);
    print_json_string(stdout, mode_name(opts->mode));
    fputs(",\"access\":", stdout);
    print_json_string(stdout, access_name(opts->access));
    fputs(",\"device\":", stdout);
    print_json_string(stdout, opts->device);
    printf(",\"offset\":%" PRIu64 ",\"bytes\":%" PRIu64 ",\"seed\":%" PRIu64
           ",\"elapsed_sec\":%.9f,\"mib_per_sec\":%.6f,\"errors\":%" PRIu64 ",\"checksum\":%" PRIu64 "}\n",
           opts->offset, opts->bytes, opts->seed, elapsed_sec, mib_per_sec, errors, checksum);
}

static int parse_args(int argc, char **argv, struct BenchOptions *opts) {
    int i = 1;

    opts->mode = MODE_UNSET;
    opts->access = ACCESS_MMAP;
    opts->device = "/dev/dax0.0";
    opts->offset = 0;
    opts->bytes = 0;
    opts->seed = 1;
    opts->have_bytes = 0;

    while (i < argc) {
        const char *arg = argv[i];
        const char *value = NULL;

        if (strcmp(arg, "--mode") == 0 || strcmp(arg, "--access") == 0 || strcmp(arg, "--device") == 0 ||
            strcmp(arg, "--offset") == 0 || strcmp(arg, "--bytes") == 0 || strcmp(arg, "--seed") == 0) {
            if (i + 1 >= argc) {
                fprintf(stderr, "missing value for %s\n", arg);
                return -1;
            }
            value = argv[i + 1];
            i += 2;
        } else {
            fprintf(stderr, "unknown argument: %s\n", arg);
            return -1;
        }

        if (strcmp(arg, "--mode") == 0) {
            if (strcmp(value, "write") == 0) {
                opts->mode = MODE_WRITE;
            } else if (strcmp(value, "verify") == 0) {
                opts->mode = MODE_VERIFY;
            } else {
                fprintf(stderr, "invalid mode: %s\n", value);
                return -1;
            }
        } else if (strcmp(arg, "--access") == 0) {
            if (strcmp(value, "mmap") == 0) {
                opts->access = ACCESS_MMAP;
            } else if (strcmp(value, "direct") == 0) {
                opts->access = ACCESS_DIRECT;
            } else {
                fprintf(stderr, "invalid access: %s\n", value);
                return -1;
            }
        } else if (strcmp(arg, "--device") == 0) {
            opts->device = value;
        } else if (strcmp(arg, "--offset") == 0) {
            if (parse_u64(value, &opts->offset) != 0) {
                fprintf(stderr, "invalid offset: %s\n", value);
                return -1;
            }
        } else if (strcmp(arg, "--bytes") == 0) {
            if (parse_u64(value, &opts->bytes) != 0) {
                fprintf(stderr, "invalid bytes: %s\n", value);
                return -1;
            }
            opts->have_bytes = 1;
        } else if (strcmp(arg, "--seed") == 0) {
            if (parse_u64(value, &opts->seed) != 0) {
                fprintf(stderr, "invalid seed: %s\n", value);
                return -1;
            }
        }
    }

    if (opts->mode == MODE_UNSET) {
        fputs("missing --mode write|verify\n", stderr);
        return -1;
    }
    if (!opts->have_bytes) {
        fputs("missing --bytes\n", stderr);
        return -1;
    }
    if (opts->offset % 4096 != 0) {
        fputs("offset must be aligned to 4096 bytes\n", stderr);
        return -1;
    }
    if (opts->access == ACCESS_DIRECT && opts->bytes % 4096 != 0) {
        fputs("direct access bytes must be aligned to 4096 bytes\n", stderr);
        return -1;
    }
    if (opts->bytes == 0 || opts->bytes % sizeof(uint64_t) != 0) {
        fputs("bytes must be nonzero and divisible by 8\n", stderr);
        return -1;
    }
    if (UINT64_MAX - opts->offset < opts->bytes) {
        fputs("offset plus bytes overflows uint64_t\n", stderr);
        return -1;
    }
    if (opts->offset + opts->bytes > (uint64_t)SIZE_MAX) {
        fputs("mapping length exceeds size_t\n", stderr);
        return -1;
    }

    return 0;
}

static int validate_file_range(int fd, const struct BenchOptions *opts) {
    struct stat st;
    uint64_t end = opts->offset + opts->bytes;

    if (fstat(fd, &st) != 0) {
        perror("fstat");
        return -1;
    }
    if (st.st_size > 0 && end > (uint64_t)st.st_size) {
        fprintf(stderr, "range [%" PRIu64 ", %" PRIu64 ") exceeds file size %" PRIu64 "\n", opts->offset, end,
                (uint64_t)st.st_size);
        return -1;
    }

    return 0;
}

static void run_write(volatile uint64_t *words, uint64_t base_word, uint64_t word_count, uint64_t seed,
                      uint64_t *checksum) {
    uint64_t i = 0;
    uint64_t local_checksum = 0;

    for (i = 0; i < word_count; ++i) {
        uint64_t value = pattern_word(seed, base_word + i);

        words[i] = value;
        local_checksum = checksum_update(local_checksum, value);
    }

    *checksum = local_checksum;
}

static uint64_t run_verify(volatile uint64_t *words, uint64_t base_word, uint64_t word_count, uint64_t seed,
                           uint64_t *checksum) {
    uint64_t i = 0;
    uint64_t errors = 0;
    uint64_t local_checksum = 0;

    for (i = 0; i < word_count; ++i) {
        uint64_t actual = words[i];
        uint64_t expected = pattern_word(seed, base_word + i);

        local_checksum = checksum_update(local_checksum, actual);
        if (actual != expected) {
            if (errors < 8) {
                fprintf(stderr,
                        "mismatch word=%" PRIu64 " byte_offset=%" PRIu64 " expected=0x%016" PRIx64
                        " actual=0x%016" PRIx64 "\n",
                        i, (base_word + i) * (uint64_t)sizeof(uint64_t), expected, actual);
            }
            if (errors < UINT64_MAX) {
                ++errors;
            }
        }
    }

    *checksum = local_checksum;
    return errors;
}

static int write_all(int fd, const void *buffer, size_t bytes, uint64_t offset) {
    const char *cursor = (const char *)buffer;
    size_t done = 0;

    while (done < bytes) {
        ssize_t rc = pwrite(fd, cursor + done, bytes - done, (off_t)(offset + done));

        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("pwrite");
            return -1;
        }
        if (rc == 0) {
            fputs("pwrite returned 0 before completing request\n", stderr);
            return -1;
        }
        done += (size_t)rc;
    }

    return 0;
}

static int read_all(int fd, void *buffer, size_t bytes, uint64_t offset) {
    char *cursor = (char *)buffer;
    size_t done = 0;

    while (done < bytes) {
        ssize_t rc = pread(fd, cursor + done, bytes - done, (off_t)(offset + done));

        if (rc < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("pread");
            return -1;
        }
        if (rc == 0) {
            fputs("pread reached EOF before completing request\n", stderr);
            return -1;
        }
        done += (size_t)rc;
    }

    return 0;
}

static int run_direct(const struct BenchOptions *opts, int fd, uint64_t *errors, uint64_t *checksum) {
    const size_t chunk_size = 1024 * 1024;
    void *buffer = NULL;
    uint64_t done = 0;
    uint64_t local_checksum = 0;
    uint64_t local_errors = 0;

    if (posix_memalign(&buffer, 4096, chunk_size) != 0) {
        fputs("posix_memalign failed\n", stderr);
        return -1;
    }

    while (done < opts->bytes) {
        size_t this_chunk = chunk_size;
        uint64_t base_word = (opts->offset + done) / sizeof(uint64_t);
        uint64_t word_count = 0;
        uint64_t i = 0;
        uint64_t *words = (uint64_t *)buffer;

        if (opts->bytes - done < (uint64_t)this_chunk) {
            this_chunk = (size_t)(opts->bytes - done);
        }
        word_count = this_chunk / sizeof(uint64_t);

        if (opts->mode == MODE_WRITE) {
            for (i = 0; i < word_count; ++i) {
                uint64_t value = pattern_word(opts->seed, base_word + i);

                words[i] = value;
                local_checksum = checksum_update(local_checksum, value);
            }
            if (write_all(fd, buffer, this_chunk, opts->offset + done) != 0) {
                free(buffer);
                return -1;
            }
        } else {
            if (read_all(fd, buffer, this_chunk, opts->offset + done) != 0) {
                free(buffer);
                return -1;
            }
            for (i = 0; i < word_count; ++i) {
                uint64_t actual = words[i];
                uint64_t expected = pattern_word(opts->seed, base_word + i);

                local_checksum = checksum_update(local_checksum, actual);
                if (actual != expected) {
                    if (local_errors < 8) {
                        fprintf(stderr,
                                "mismatch word=%" PRIu64 " byte_offset=%" PRIu64 " expected=0x%016" PRIx64
                                " actual=0x%016" PRIx64 "\n",
                                i, (base_word + i) * (uint64_t)sizeof(uint64_t), expected, actual);
                    }
                    if (local_errors < UINT64_MAX) {
                        ++local_errors;
                    }
                }
            }
        }

        done += this_chunk;
    }

    if (opts->mode == MODE_WRITE && fsync(fd) != 0) {
        perror("fsync");
        local_errors = local_errors == 0 ? 1 : local_errors;
    }

    free(buffer);
    *errors = local_errors;
    *checksum = local_checksum;
    return 0;
}

int main(int argc, char **argv) {
    struct BenchOptions opts;
    struct timespec start;
    struct timespec end;
    void *mapping = MAP_FAILED;
    volatile uint64_t *words = NULL;
    uint64_t checksum = 0;
    uint64_t errors = 0;
    uint64_t word_count = 0;
    uint64_t base_word = 0;
    size_t map_len = 0;
    int open_flags = O_RDWR;
    int fd = -1;
    int rc = 1;

    if (parse_args(argc, argv, &opts) != 0) {
        usage(argv[0]);
        return 2;
    }

#ifdef O_DIRECT
    if (opts.access == ACCESS_DIRECT) {
        open_flags |= O_DIRECT;
    }
#endif
    fd = open(opts.device, open_flags);
    if (fd < 0) {
        perror("open");
        return 1;
    }

    if (validate_file_range(fd, &opts) != 0) {
        close(fd);
        return 1;
    }

    if (opts.access == ACCESS_DIRECT) {
        if (clock_gettime(CLOCK_MONOTONIC, &start) != 0) {
            perror("clock_gettime");
            close(fd);
            return 1;
        }
        if (run_direct(&opts, fd, &errors, &checksum) != 0) {
            close(fd);
            return 1;
        }
        if (clock_gettime(CLOCK_MONOTONIC, &end) != 0) {
            perror("clock_gettime");
            errors = errors == 0 ? 1 : errors;
            end = start;
        }
        if (close(fd) != 0) {
            perror("close");
            errors = errors == 0 ? 1 : errors;
        }
        print_result(&opts, elapsed_seconds(&start, &end), errors, checksum);
        return errors == 0 ? 0 : 1;
    }

    map_len = (size_t)(opts.offset + opts.bytes);
    mapping = mmap(NULL, map_len, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (mapping == MAP_FAILED) {
        perror("mmap");
        close(fd);
        return 1;
    }

    words = (volatile uint64_t *)((char *)mapping + (size_t)opts.offset);
    word_count = opts.bytes / sizeof(uint64_t);
    base_word = opts.offset / sizeof(uint64_t);

    if (clock_gettime(CLOCK_MONOTONIC, &start) != 0) {
        perror("clock_gettime");
        munmap(mapping, map_len);
        close(fd);
        return 1;
    }

    if (opts.mode == MODE_WRITE) {
        run_write(words, base_word, word_count, opts.seed, &checksum);
        if (msync((void *)((char *)mapping + (size_t)opts.offset), (size_t)opts.bytes, MS_SYNC) != 0) {
            perror("msync");
            errors = 1;
        }
    } else {
        errors = run_verify(words, base_word, word_count, opts.seed, &checksum);
    }

    if (clock_gettime(CLOCK_MONOTONIC, &end) != 0) {
        perror("clock_gettime");
        errors = errors == 0 ? 1 : errors;
        end = start;
    }

    if (munmap(mapping, map_len) != 0) {
        perror("munmap");
        errors = errors == 0 ? 1 : errors;
    }
    if (close(fd) != 0) {
        perror("close");
        errors = errors == 0 ? 1 : errors;
    }

    print_result(&opts, elapsed_seconds(&start, &end), errors, checksum);
    rc = errors == 0 ? 0 : 1;
    return rc;
}
