/*
 * Look-ahead prefetch pipeline benchmark for the SSD streaming backend.
 *
 * Models the CXL data-supply path of a ternary-weight LLM decode loop: every
 * token streams a run of use-once weight tiles; each tile costs a modelled
 * backing read latency plus a modelled compute time. It compares demand-only
 * loading against a look-ahead prefetcher that stays a fixed depth ahead over
 * several parallel channels, so the backing latency overlaps the compute of
 * earlier tiles. The latency is a model parameter (backing_latency_ns), not a
 * measured device latency, so the result is deterministic on any host.
 * Args: [lat_ns] [comp_ns] [depth] [threads].
 */
#include "../include/ssd_streaming_backend.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <vector>

using clk = std::chrono::steady_clock;

static void spin_ns(uint64_t ns)
{
    const auto deadline = clk::now() + std::chrono::nanoseconds(ns);
    while (clk::now() < deadline) {
    }
}

int main(int argc, char **argv)
{
    const uint32_t ps = 4096;
    const uint64_t weight_pages = 512;
    const int tokens = 8;
    const uint64_t lat = argc > 1 ? strtoull(argv[1], nullptr, 10) : 3000;
    const uint64_t comp = argc > 2 ? strtoull(argv[2], nullptr, 10) : 2000;
    const int depth = argc > 3 ? atoi(argv[3]) : 8;
    const int pt = argc > 4 ? atoi(argv[4]) : 1;

    std::vector<uint64_t> seq;
    seq.reserve((size_t)tokens * weight_pages);
    for (int t = 0; t < tokens; t++) {
        for (uint64_t p = 0; p < weight_pages; p++) {
            seq.push_back(p);
        }
    }
    const size_t n = seq.size();

    SsdStreamingConfig cfg;
    cfg.backing_path = "/home/heke/temp/qemu-camp/scratch/cxl_pipe.img";
    cfg.capacity_bytes = (weight_pages + 16) * ps;
    cfg.page_size = ps;
    cfg.cache_pages = 256;
    cfg.read_ahead_pages = 0;
    cfg.backing_latency_ns = lat;

    auto run = [&](bool pipelined) {
        SsdStreamingBackend be(cfg);
        be.initialize();
        std::vector<uint8_t> buf(ps);
        std::atomic<size_t> consumed{0};
        std::atomic<size_t> next{0};
        std::atomic<bool> done{false};

        std::vector<std::thread> pf;
        if (pipelined) {
            for (int k = 0; k < pt; k++) {
                pf.emplace_back([&] {
                    for (;;) {
                        size_t i = next.fetch_add(1);
                        if (i >= n || done.load()) {
                            break;
                        }
                        while (i > consumed.load() + depth && !done.load()) {
                            std::this_thread::yield();
                        }
                        be.prefetch(seq[i] * ps, ps);
                    }
                });
            }
        }

        auto t0 = clk::now();
        for (size_t i = 0; i < n; i++) {
            uint64_t addr = seq[i] * ps;
            be.set_streaming(addr, ps);
            be.read(addr, buf.data(), ps);
            spin_ns(comp);
            consumed.store(i + 1);
        }
        auto t1 = clk::now();
        done.store(true);
        for (auto &t : pf) {
            t.join();
        }

        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        SsdStreamingStats st = be.get_stats();
        be.shutdown();
        return std::make_pair(ms, st);
    };

    /* warm the backing file so both arms fault the same way */
    run(false);

    auto d = run(false);
    auto p = run(true);
    double ms_demand = d.first, ms_pipe = p.first;
    double bw_demand = (double)n * ps / (ms_demand / 1e3) / (1024 * 1024);
    double bw_pipe = (double)n * ps / (ms_pipe / 1e3) / (1024 * 1024);

    printf("pages=%zu lat=%lu ns comp=%lu ns depth=%d threads=%d\n",
           n, (unsigned long)lat, (unsigned long)comp, depth, pt);
    printf("demand-only : %8.2f ms  faults=%lu hits=%lu  supply=%.1f MiB/s\n",
           ms_demand, (unsigned long)d.second.page_faults,
           (unsigned long)d.second.cache_hits, bw_demand);
    printf("pipelined   : %8.2f ms  faults=%lu hits=%lu  supply=%.1f MiB/s\n",
           ms_pipe, (unsigned long)p.second.page_faults,
           (unsigned long)p.second.cache_hits, bw_pipe);
    printf("speedup     : %.2fx   supply gain: %.2fx\n",
           ms_demand / ms_pipe, bw_pipe / bw_demand);
    return 0;
}
