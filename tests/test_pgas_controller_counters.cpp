#include "cxlendpoint.h"
#include "policy.h"

#include <array>
#include <cstdlib>
#include <iostream>
#include <memory>

#define REQUIRE(condition)                                                                                             \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            std::cerr << "Requirement failed: " #condition << "\n";                                                    \
            std::exit(1);                                                                                              \
        }                                                                                                              \
    } while (0)

namespace {

std::unique_ptr<CXLController> make_controller() {
    std::array<Policy *, 4> policies = {
        new AllocationPolicy(),
        new MigrationPolicy(),
        new PagingPolicy(),
        new CachingPolicy(),
    };

    auto controller = std::make_unique<CXLController>(policies, 256, PAGE, 10, 100);
    controller->insert_end_point(new CXLMemExpander(25, 25, 100, 150, 0, 256));
    controller->construct_topo("(1);");
    return controller;
}

} // namespace

int main() {
    auto controller = make_controller();

    controller->record_cxl_access(100, 7, 0x1000, false);
    controller->record_cxl_access(200, 7, 0x1040, true);

    REQUIRE(controller->counter.remote.get() == 2);
    REQUIRE(controller->counter.local.get() == 0);
    REQUIRE(controller->counter.hitm.get() == 0);
    REQUIRE(controller->counter.backinv.get() == 0);
    REQUIRE(controller->counter.local_hit_ratio() == 0.0);

    REQUIRE(controller->thread_map.size() == 1);

    REQUIRE(controller->CXLSwitch::counter.load.get() == 1);
    REQUIRE(controller->CXLSwitch::counter.store.get() == 1);

    REQUIRE(controller->cur_expanders.size() == 1);
    auto *endpoint = controller->cur_expanders.front();
    REQUIRE(endpoint != nullptr);
    REQUIRE(endpoint->counter.load.get() == 1);
    REQUIRE(endpoint->counter.store.get() == 1);

    std::cout << "PGAS controller counters: remote=" << controller->counter.remote.get()
              << " switch_load=" << controller->CXLSwitch::counter.load.get()
              << " switch_store=" << controller->CXLSwitch::counter.store.get()
              << " endpoint_load=" << endpoint->counter.load.get()
              << " endpoint_store=" << endpoint->counter.store.get() << " threads=" << controller->thread_map.size()
              << "\n";

    return 0;
}
