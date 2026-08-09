#include "ssd_streaming_backend.h"
#include <chrono>
#include <cstdio>
#include <vector>

int main(int argc, char **argv) {
    SsdStreamingConfig cfg;
    cfg.backing_path = "/home/heke/temp/qemu-camp/scratch/cxl_bench.img";
    cfg.page_size = 4096;
    cfg.capacity_bytes = 256ull * 1024 * 1024;
    cfg.cache_pages = 512;
    cfg.read_ahead_pages = 0;
    cfg.use_io_uring = false;
    cfg.use_odirect = false;
    SsdStreamingBackend be(cfg);
    if (!be.initialize()) { fprintf(stderr, "init failed\n"); return 1; }

    const uint64_t PS = cfg.page_size;
    const uint64_t hot_pages = 200;      /* reused KV/hot weights */
    const uint64_t weight_base = 4096;   /* streamed ternary weights */
    const uint64_t weight_pages = 3000;
    const int tokens = 10;
    std::vector<uint8_t> buf(PS);

    auto t0 = std::chrono::steady_clock::now();
    for (int tok = 0; tok < tokens; tok++) {
        for (int r = 0; r < 3; r++)
            for (uint64_t p = 0; p < hot_pages; p++) be.read(p * PS, buf.data(), PS);
        for (uint64_t i = 0; i < weight_pages; i++) {
            uint64_t addr = (weight_base + i) * PS;
            be.set_streaming(addr, PS);
            be.read(addr, buf.data(), PS);
        }
    }
    auto t1 = std::chrono::steady_clock::now();
    auto st = be.get_stats();
    double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    printf("reads=%lu cache_hits=%lu page_faults=%lu evictions=%lu bytes_read=%lu hit_rate=%.1f%% time_ms=%.0f\n",
           (unsigned long)st.reads, (unsigned long)st.cache_hits, (unsigned long)st.page_faults,
           (unsigned long)st.evictions, (unsigned long)st.bytes_read,
           100.0 * st.cache_hits / (st.cache_hits + st.page_faults), ms);
    be.shutdown();
    return 0;
}
