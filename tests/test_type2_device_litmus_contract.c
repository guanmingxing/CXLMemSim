#define main type2_device_litmus_program_main
#include "../microbench/type2_device_litmus.c"
#undef main

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition))                                                                                              \
            return __LINE__;                                                                                           \
    } while (0)

int main(void) {
    context_t context = {0};
    uint64_t atomic_results[3] = {41, 42, 77};
    uint64_t load_results[3] = {42, 1, 77};
    uint64_t fence_results[3] = {0, 1, 77};

    CHECK(update_device_identity(&context, CXL_GPU_CMD_COHERENT_FAA, atomic_results));
    CHECK(context.device_session == 77);
    CHECK(update_device_identity(&context, CXL_GPU_CMD_COHERENT_CAS, atomic_results));
    CHECK(update_device_identity(&context, CXL_GPU_CMD_COHERENT_LOAD, load_results));
    CHECK(update_device_identity(&context, CXL_GPU_CMD_COHERENT_FENCE, fence_results));

    load_results[1] = 0;
    CHECK(!update_device_identity(&context, CXL_GPU_CMD_COHERENT_LOAD, load_results));
    atomic_results[2] = 0;
    CHECK(!update_device_identity(&context, CXL_GPU_CMD_COHERENT_CAS, atomic_results));
    atomic_results[2] = 78;
    CHECK(!update_device_identity(&context, CXL_GPU_CMD_COHERENT_CAS, atomic_results));
    return 0;
}
