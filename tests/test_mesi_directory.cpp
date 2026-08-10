#ifndef CXLMEMSIM_MESI_DIRECTORY_STANDALONE
#include "coherency_engine.h"
#endif
#include "mesi_directory.h"

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

namespace cxlmemsim::mesi_v2 {

struct PendingTransaction {
    int marker{};
};

} // namespace cxlmemsim::mesi_v2

using namespace cxlmemsim::mesi_v2;

namespace {

int failures = 0;

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            std::cerr << __func__ << ':' << __LINE__ << ": CHECK failed: " #condition << '\n';                         \
            ++failures;                                                                                                \
        }                                                                                                              \
    } while (false)

constexpr std::uint64_t kLineA = 0x1000;
constexpr std::uint64_t kLineB = 0x2000;
constexpr std::uint64_t kLineC = 0x3000;
constexpr std::uint64_t kLineD = 0x4000;

std::uint64_t holder(std::uint16_t host_id) { return std::uint64_t{1} << host_id; }

void checkInvalid(const DirectorySnapshot &snapshot) {
    CHECK(snapshot.state == MesiState::I);
    CHECK(!snapshot.owner.has_value());
    CHECK(snapshot.sharers == 0);
    CHECK(snapshot.epoch == 0);
    CHECK(snapshot.server_copy_current);
    CHECK(isValidSnapshot(snapshot));
}

void checkExclusive(const DirectorySnapshot &snapshot, std::uint16_t owner, std::uint64_t epoch) {
    CHECK(snapshot.state == MesiState::E);
    CHECK(snapshot.owner == owner);
    CHECK(snapshot.sharers == 0);
    CHECK(snapshot.epoch == epoch);
    CHECK(snapshot.server_copy_current);
    CHECK(isValidSnapshot(snapshot));
}

void checkShared(const DirectorySnapshot &snapshot, std::uint64_t sharers, std::uint64_t epoch) {
    CHECK(snapshot.state == MesiState::S);
    CHECK(!snapshot.owner.has_value());
    CHECK(snapshot.sharers == sharers);
    CHECK(snapshot.epoch == epoch);
    CHECK(snapshot.server_copy_current);
    CHECK(isValidSnapshot(snapshot));
}

void checkModified(const DirectorySnapshot &snapshot, std::uint16_t owner, std::uint64_t epoch) {
    CHECK(snapshot.state == MesiState::M);
    CHECK(snapshot.owner == owner);
    CHECK(snapshot.sharers == 0);
    CHECK(snapshot.epoch == epoch);
    CHECK(!snapshot.server_copy_current);
    CHECK(isValidSnapshot(snapshot));
}

void checkSnapshotEqual(const DirectorySnapshot &actual, const DirectorySnapshot &expected) {
    CHECK(actual.state == expected.state);
    CHECK(actual.owner == expected.owner);
    CHECK(actual.sharers == expected.sharers);
    CHECK(actual.epoch == expected.epoch);
    CHECK(actual.server_copy_current == expected.server_copy_current);
}

void checkCountersEqual(const TransitionCounters &actual, const TransitionCounters &expected) {
    CHECK(actual.gets == expected.gets);
    CHECK(actual.getm == expected.getm);
    CHECK(actual.upgrade == expected.upgrade);
    CHECK(actual.puts == expected.puts);
    CHECK(actual.putm == expected.putm);
}

void checkDiagnosticsEqual(const EntryDiagnostics &actual, const EntryDiagnostics &expected) {
    CHECK(actual.gets == expected.gets);
    CHECK(actual.getm == expected.getm);
    CHECK(actual.upgrade == expected.upgrade);
    CHECK(actual.puts == expected.puts);
    CHECK(actual.putm == expected.putm);
}

void checkRejectedWithoutMutation(MesiDirectory &directory, MesiDirectory::LockedLine &locked,
                                  const TransitionResult &result, const DirectorySnapshot &expected,
                                  const TransitionCounters &counters_before,
                                  const EntryDiagnostics &diagnostics_before) {
    CHECK(result.status == TransitionStatus::InvalidState);
    checkSnapshotEqual(result.snapshot, expected);
    checkSnapshotEqual(locked.snapshot(), expected);
    checkCountersEqual(directory.transitionCounters(), counters_before);
    checkDiagnosticsEqual(locked.diagnostics(), diagnostics_before);
}

void testUntouchedLinesAreImplicitInvalidAndSparse() {
    MesiDirectory directory;

    CHECK(directory.allocatedLineCount() == 0);
    const auto untouched = directory.inspect(kLineA);
    CHECK(untouched.has_value());
    if (untouched)
        checkInvalid(*untouched);
    CHECK(directory.allocatedLineCount() == 0);

    const auto entry = directory.getOrCreate(kLineA);
    CHECK(entry != nullptr);
    CHECK(entry->lineAddress() == kLineA);
    CHECK(directory.allocatedLineCount() == 1);
    const auto allocated = directory.inspect(kLineA);
    CHECK(allocated.has_value());
    if (allocated)
        checkInvalid(*allocated);
}

void testLockedLineIsMoveOnlyAndOwnsPendingReference() {
    static_assert(!std::is_copy_constructible_v<MesiDirectory::LockedLine>);
    static_assert(!std::is_copy_assignable_v<MesiDirectory::LockedLine>);
    static_assert(std::is_move_constructible_v<MesiDirectory::LockedLine>);
    static_assert(std::is_nothrow_move_assignable_v<MesiDirectory::LockedLine>);

    MesiDirectory directory;
    auto locked = directory.lockLine(kLineA);
    CHECK(locked.has_value());
    if (!locked)
        return;

    CHECK(locked->lineAddress() == kLineA);
    CHECK(locked->pendingTransaction() == nullptr);
    auto pending = std::make_shared<PendingTransaction>();
    pending->marker = 17;
    locked->setPendingTransaction(pending);
    CHECK(locked->pendingTransaction() == pending);
    CHECK(locked->pendingTransaction()->marker == 17);
    locked->setPendingTransaction(nullptr);
    CHECK(locked->pendingTransaction() == nullptr);
}

void testLockedLineMoveAssignmentIsSafeAfterDirectoryLifetimeEnds() {
    std::optional<MesiDirectory::LockedLine> first;
    std::optional<MesiDirectory::LockedLine> second;

    {
        auto directory = std::make_unique<MesiDirectory>();
        CHECK(directory->gets(kLineA, 1).status == TransitionStatus::Committed);
        CHECK(directory->gets(kLineB, 2).status == TransitionStatus::Committed);
        first = directory->lockLine(kLineA);
        second = directory->lockLine(kLineB);
        CHECK(first.has_value());
        CHECK(second.has_value());
        if (!first || !second)
            return;
    }

    *first = std::move(*second);
    CHECK(first->lineAddress() == kLineB);
    const auto expected = first->snapshot();
    checkExclusive(expected, 2, 1);

    bool moved_from_rejected = false;
    try {
        (void)second->snapshot();
    } catch (const std::logic_error &) {
        moved_from_rejected = true;
    }
    CHECK(moved_from_rejected);

    const DirectorySnapshot modified{MesiState::M, 2, 0, expected.epoch, false};
    const auto result = first->commitUpgrade(2, expected, modified);
    CHECK(result.status == TransitionStatus::Committed);
    checkModified(result.snapshot, 2, 2);
    CHECK(first->diagnostics().upgrade == 1);
}

void testLockedLineSelfMoveAssignmentIsANoOp() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 3).status == TransitionStatus::Committed);
    auto locked = directory.lockLine(kLineA);
    CHECK(locked.has_value());
    if (!locked)
        return;

    auto &guard = *locked;
    guard = std::move(guard);
    CHECK(guard.lineAddress() == kLineA);
    const auto expected = guard.snapshot();
    checkExclusive(expected, 3, 1);

    const DirectorySnapshot modified{MesiState::M, 3, 0, expected.epoch, false};
    const auto result = guard.commitUpgrade(3, expected, modified);
    CHECK(result.status == TransitionStatus::Committed);
    checkModified(result.snapshot, 3, 2);
}

void testLockedLineRemainsUsableAfterDirectoryLifetimeEnds() {
    std::optional<MesiDirectory::LockedLine> escaped;
    DirectorySnapshot expected;

    {
        auto directory = std::make_unique<MesiDirectory>();
        CHECK(directory->gets(kLineA, 7).status == TransitionStatus::Committed);
        escaped = directory->lockLine(kLineA);
        CHECK(escaped.has_value());
        if (!escaped)
            return;
        expected = escaped->snapshot();
    }

    CHECK(escaped.has_value());
    if (!escaped)
        return;

    const DirectorySnapshot modified{MesiState::M, 7, 0, expected.epoch, false};
    const auto result = escaped->commitUpgrade(7, expected, modified);
    CHECK(result.status == TransitionStatus::Committed);
    checkModified(result.snapshot, 7, 2);
    checkModified(escaped->snapshot(), 7, 2);

    const auto diagnostics = escaped->diagnostics();
    CHECK(diagnostics.gets == 1);
    CHECK(diagnostics.getm == 0);
    CHECK(diagnostics.upgrade == 1);
    CHECK(diagnostics.puts == 0);
    CHECK(diagnostics.putm == 0);
}

void testLockedCommitFinalizesTask4PostSnoopTransitions() {
    MesiDirectory directory;

    CHECK(directory.getm(kLineA, 1).status == TransitionStatus::Committed);
    {
        auto locked = directory.lockLine(kLineA);
        CHECK(locked.has_value());
        if (!locked)
            return;
        const auto expected = locked->snapshot();
        const DirectorySnapshot next{MesiState::S, std::nullopt, holder(1) | holder(2), expected.epoch, true};
        const auto result = locked->commitGets(2, expected, next);
        CHECK(result.status == TransitionStatus::Committed);
        checkShared(result.snapshot, holder(1) | holder(2), 2);
        const auto diagnostics = locked->diagnostics();
        CHECK(diagnostics.gets == 1);
        CHECK(diagnostics.getm == 1);
    }

    CHECK(directory.getm(kLineB, 3).status == TransitionStatus::Committed);
    {
        auto locked = directory.lockLine(kLineB);
        CHECK(locked.has_value());
        if (!locked)
            return;
        const auto expected = locked->snapshot();
        const DirectorySnapshot next{MesiState::M, 4, 0, expected.epoch, false};
        const auto result = locked->commitGetm(4, expected, next);
        CHECK(result.status == TransitionStatus::Committed);
        checkModified(result.snapshot, 4, 2);
        const auto diagnostics = locked->diagnostics();
        CHECK(diagnostics.getm == 2);
    }

    CHECK(directory.gets(kLineC, 5).status == TransitionStatus::Committed);
    CHECK(directory.gets(kLineC, 6).status == TransitionStatus::Committed);
    {
        auto locked = directory.lockLine(kLineC);
        CHECK(locked.has_value());
        if (!locked)
            return;
        const auto expected = locked->snapshot();
        const DirectorySnapshot next{MesiState::M, 5, 0, expected.epoch, false};
        const auto result = locked->commitUpgrade(5, expected, next);
        CHECK(result.status == TransitionStatus::Committed);
        checkModified(result.snapshot, 5, 3);
        const auto diagnostics = locked->diagnostics();
        CHECK(diagnostics.gets == 2);
        CHECK(diagnostics.upgrade == 1);
    }

    const auto counters = directory.transitionCounters();
    CHECK(counters.gets == 3);
    CHECK(counters.getm == 3);
    CHECK(counters.upgrade == 1);
    CHECK(counters.puts == 0);
    CHECK(counters.putm == 0);
}

void testLockedCommitRejectsSameOwnerExclusiveToModifiedAsGetm() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 7).status == TransitionStatus::Committed);

    auto locked = directory.lockLine(kLineA);
    CHECK(locked.has_value());
    if (!locked)
        return;

    const auto expected = locked->snapshot();
    const auto counters_before = directory.transitionCounters();
    const auto diagnostics_before = locked->diagnostics();
    const DirectorySnapshot next{MesiState::M, 7, 0, expected.epoch, false};
    const auto result = locked->commitGetm(7, expected, next);

    CHECK(result.status == TransitionStatus::InvalidState);
    checkExclusive(result.snapshot, 7, 1);
    checkExclusive(locked->snapshot(), 7, 1);

    const auto counters_after = directory.transitionCounters();
    CHECK(counters_after.gets == counters_before.gets);
    CHECK(counters_after.getm == counters_before.getm);
    CHECK(counters_after.upgrade == counters_before.upgrade);
    CHECK(counters_after.puts == counters_before.puts);
    CHECK(counters_after.putm == counters_before.putm);
    const auto diagnostics_after = locked->diagnostics();
    CHECK(diagnostics_after.gets == diagnostics_before.gets);
    CHECK(diagnostics_after.getm == diagnostics_before.getm);
    CHECK(diagnostics_after.upgrade == diagnostics_before.upgrade);
    CHECK(diagnostics_after.puts == diagnostics_before.puts);
    CHECK(diagnostics_after.putm == diagnostics_before.putm);
}

void testLockedUpgradeCommitsSameOwnerExclusiveToModified() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 7).status == TransitionStatus::Committed);

    auto locked = directory.lockLine(kLineA);
    CHECK(locked.has_value());
    if (!locked)
        return;

    const auto expected = locked->snapshot();
    const DirectorySnapshot next{MesiState::M, 7, 0, expected.epoch, false};
    const auto result = locked->commitUpgrade(7, expected, next);
    CHECK(result.status == TransitionStatus::Committed);
    checkModified(result.snapshot, 7, 2);
    checkModified(locked->snapshot(), 7, 2);

    const auto counters = directory.transitionCounters();
    CHECK(counters.gets == 1);
    CHECK(counters.getm == 0);
    CHECK(counters.upgrade == 1);
    CHECK(counters.puts == 0);
    CHECK(counters.putm == 0);
    const auto diagnostics = locked->diagnostics();
    CHECK(diagnostics.gets == 1);
    CHECK(diagnostics.getm == 0);
    CHECK(diagnostics.upgrade == 1);
    CHECK(diagnostics.puts == 0);
    CHECK(diagnostics.putm == 0);
}

void testLockedGetmRejectsSharedRequesterPromotionWithoutMutation() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).status == TransitionStatus::Committed);
    CHECK(directory.gets(kLineA, 2).status == TransitionStatus::Committed);

    auto locked = directory.lockLine(kLineA);
    CHECK(locked.has_value());
    if (!locked)
        return;

    const auto expected = locked->snapshot();
    const auto counters_before = directory.transitionCounters();
    const auto diagnostics_before = locked->diagnostics();
    const DirectorySnapshot modified{MesiState::M, 1, 0, expected.epoch, false};
    const auto result = locked->commitGetm(1, expected, modified);

    checkRejectedWithoutMutation(directory, *locked, result, expected, counters_before, diagnostics_before);
}

void testLockedGetmRejectsSharedRequesterPartialReconciliationWithoutMutation() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).status == TransitionStatus::Committed);
    CHECK(directory.gets(kLineA, 2).status == TransitionStatus::Committed);
    CHECK(directory.gets(kLineA, 3).status == TransitionStatus::Committed);

    auto locked = directory.lockLine(kLineA);
    CHECK(locked.has_value());
    if (!locked)
        return;

    const auto expected = locked->snapshot();
    const auto counters_before = directory.transitionCounters();
    const auto diagnostics_before = locked->diagnostics();
    const DirectorySnapshot partially_invalidated{MesiState::S, std::nullopt, holder(1) | holder(3), expected.epoch,
                                                  true};
    const auto result = locked->commitGetm(1, expected, partially_invalidated);

    checkRejectedWithoutMutation(directory, *locked, result, expected, counters_before, diagnostics_before);
}

void testLockedGetmCommitsNonSharerPromotionFromShared() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 1).status == TransitionStatus::Committed);
    CHECK(directory.gets(kLineA, 2).status == TransitionStatus::Committed);

    auto locked = directory.lockLine(kLineA);
    CHECK(locked.has_value());
    if (!locked)
        return;

    const auto expected = locked->snapshot();
    const DirectorySnapshot modified{MesiState::M, 3, 0, expected.epoch, false};
    const auto result = locked->commitGetm(3, expected, modified);

    CHECK(result.status == TransitionStatus::Committed);
    checkModified(result.snapshot, 3, 3);
    CHECK(directory.transitionCounters().getm == 1);
    CHECK(locked->diagnostics().getm == 1);
}

void testLockedFinalizersCommitLegalPartialAckReconciliation() {
    {
        MesiDirectory directory;
        CHECK(directory.getm(kLineA, 1).status == TransitionStatus::Committed);
        auto locked = directory.lockLine(kLineA);
        CHECK(locked.has_value());
        if (!locked)
            return;

        const auto expected = locked->snapshot();
        const DirectorySnapshot downgraded_without_grant{MesiState::S, std::nullopt, holder(1), expected.epoch, true};
        const auto result = locked->commitGets(2, expected, downgraded_without_grant);
        CHECK(result.status == TransitionStatus::Committed);
        checkShared(result.snapshot, holder(1), 2);
        CHECK(directory.transitionCounters().gets == 1);
        CHECK(locked->diagnostics().gets == 1);
    }

    {
        MesiDirectory directory;
        CHECK(directory.gets(kLineA, 3).status == TransitionStatus::Committed);
        CHECK(directory.gets(kLineA, 4).status == TransitionStatus::Committed);
        CHECK(directory.gets(kLineA, 5).status == TransitionStatus::Committed);
        auto locked = directory.lockLine(kLineA);
        CHECK(locked.has_value());
        if (!locked)
            return;

        const auto expected = locked->snapshot();
        const DirectorySnapshot partially_invalidated{MesiState::S, std::nullopt, holder(4) | holder(5), expected.epoch,
                                                      true};
        const auto result = locked->commitGetm(6, expected, partially_invalidated);
        CHECK(result.status == TransitionStatus::Committed);
        checkShared(result.snapshot, holder(4) | holder(5), 4);
        CHECK(directory.transitionCounters().getm == 1);
        CHECK(locked->diagnostics().getm == 1);
    }

    {
        MesiDirectory directory;
        CHECK(directory.gets(kLineA, 7).status == TransitionStatus::Committed);
        CHECK(directory.gets(kLineA, 8).status == TransitionStatus::Committed);
        CHECK(directory.gets(kLineA, 9).status == TransitionStatus::Committed);
        auto locked = directory.lockLine(kLineA);
        CHECK(locked.has_value());
        if (!locked)
            return;

        const auto expected = locked->snapshot();
        const DirectorySnapshot partially_invalidated{MesiState::S, std::nullopt, holder(7) | holder(9), expected.epoch,
                                                      true};
        const auto result = locked->commitUpgrade(7, expected, partially_invalidated);
        CHECK(result.status == TransitionStatus::Committed);
        checkShared(result.snapshot, holder(7) | holder(9), 4);
        CHECK(directory.transitionCounters().upgrade == 1);
        CHECK(locked->diagnostics().upgrade == 1);
    }

    {
        MesiDirectory directory;
        CHECK(directory.gets(kLineA, 10).status == TransitionStatus::Committed);
        auto locked = directory.lockLine(kLineA);
        CHECK(locked.has_value());
        if (!locked)
            return;

        const auto expected = locked->snapshot();
        const DirectorySnapshot invalidated_without_grant{MesiState::I, std::nullopt, 0, expected.epoch, true};
        const auto result = locked->commitGetm(11, expected, invalidated_without_grant);
        CHECK(result.status == TransitionStatus::Committed);
        CHECK(result.snapshot.state == MesiState::I);
        CHECK(result.snapshot.epoch == 2);
        CHECK(directory.transitionCounters().getm == 1);
        CHECK(locked->diagnostics().getm == 1);
    }

    {
        MesiDirectory directory;
        CHECK(directory.getm(kLineA, 12).status == TransitionStatus::Committed);
        auto locked = directory.lockLine(kLineA);
        CHECK(locked.has_value());
        if (!locked)
            return;

        const auto expected = locked->snapshot();
        const DirectorySnapshot data_invalidated_without_grant{MesiState::I, std::nullopt, 0, expected.epoch, true};
        const auto result = locked->commitGetm(13, expected, data_invalidated_without_grant);
        CHECK(result.status == TransitionStatus::Committed);
        CHECK(result.snapshot.state == MesiState::I);
        CHECK(result.snapshot.epoch == 2);
        CHECK(directory.transitionCounters().getm == 2);
        CHECK(locked->diagnostics().getm == 2);
    }
}

void testLockedFinalizersRejectRelabelingAndInvalidRelationships() {
    {
        MesiDirectory directory;
        auto locked = directory.lockLine(kLineA);
        CHECK(locked.has_value());
        if (!locked)
            return;
        const auto expected = locked->snapshot();
        const auto counters_before = directory.transitionCounters();
        const auto diagnostics_before = locked->diagnostics();
        const DirectorySnapshot modified{MesiState::M, 1, 0, expected.epoch, false};
        const auto result = locked->commitGets(1, expected, modified);
        checkRejectedWithoutMutation(directory, *locked, result, expected, counters_before, diagnostics_before);
    }

    {
        MesiDirectory directory;
        CHECK(directory.gets(kLineA, 1).status == TransitionStatus::Committed);
        auto locked = directory.lockLine(kLineA);
        CHECK(locked.has_value());
        if (!locked)
            return;
        const auto expected = locked->snapshot();
        const auto counters_before = directory.transitionCounters();
        const auto diagnostics_before = locked->diagnostics();
        const DirectorySnapshot shared{MesiState::S, std::nullopt, holder(1) | holder(2), expected.epoch, true};
        const auto result = locked->commitPutm(1, expected, shared);
        checkRejectedWithoutMutation(directory, *locked, result, expected, counters_before, diagnostics_before);
    }

    {
        MesiDirectory directory;
        CHECK(directory.getm(kLineA, 1).status == TransitionStatus::Committed);
        auto locked = directory.lockLine(kLineA);
        CHECK(locked.has_value());
        if (!locked)
            return;
        const auto expected = locked->snapshot();
        const auto counters_before = directory.transitionCounters();
        const auto diagnostics_before = locked->diagnostics();
        const DirectorySnapshot shared_without_old_owner{MesiState::S, std::nullopt, holder(2), expected.epoch, true};
        const auto result = locked->commitGets(2, expected, shared_without_old_owner);
        checkRejectedWithoutMutation(directory, *locked, result, expected, counters_before, diagnostics_before);
    }

    {
        MesiDirectory directory;
        CHECK(directory.gets(kLineA, 1).status == TransitionStatus::Committed);
        CHECK(directory.gets(kLineA, 2).status == TransitionStatus::Committed);
        auto locked = directory.lockLine(kLineA);
        CHECK(locked.has_value());
        if (!locked)
            return;
        const auto expected = locked->snapshot();
        const auto counters_before = directory.transitionCounters();
        const auto diagnostics_before = locked->diagnostics();
        const DirectorySnapshot requester_removed{MesiState::S, std::nullopt, holder(2), expected.epoch, true};
        const auto getm_result = locked->commitGetm(1, expected, requester_removed);
        checkRejectedWithoutMutation(directory, *locked, getm_result, expected, counters_before, diagnostics_before);
        const auto upgrade_result = locked->commitUpgrade(1, expected, requester_removed);
        checkRejectedWithoutMutation(directory, *locked, upgrade_result, expected, counters_before, diagnostics_before);
    }
}

void testLockedCommitRejectsStaleOrInvalidMetadataWithoutMutation() {
    MesiDirectory directory;
    CHECK(directory.getm(kLineA, 7).status == TransitionStatus::Committed);

    auto locked = directory.lockLine(kLineA);
    CHECK(locked.has_value());
    if (!locked)
        return;

    const auto original = locked->snapshot();
    const auto counters_before = directory.transitionCounters();
    const auto diagnostics_before = locked->diagnostics();
    const DirectorySnapshot desired{MesiState::M, 8, 0, original.epoch, false};

    auto stale_epoch = original;
    --stale_epoch.epoch;
    auto stale_next = desired;
    stale_next.epoch = stale_epoch.epoch;
    const auto epoch_rejection = locked->commitGetm(8, stale_epoch, stale_next);
    CHECK(epoch_rejection.status == TransitionStatus::StaleMetadata);
    CHECK(epoch_rejection.snapshot.epoch == original.epoch);
    CHECK(epoch_rejection.snapshot.owner == original.owner);

    auto stale_state = original;
    stale_state.state = MesiState::E;
    stale_state.server_copy_current = true;
    const auto state_rejection = locked->commitGetm(8, stale_state, desired);
    CHECK(state_rejection.status == TransitionStatus::StaleMetadata);

    auto invalid_next = desired;
    invalid_next.state = MesiState::S;
    invalid_next.sharers = holder(8);
    const auto invalid_rejection = locked->commitGets(8, original, invalid_next);
    CHECK(invalid_rejection.status == TransitionStatus::InvalidState);

    const auto after = locked->snapshot();
    CHECK(after.state == original.state);
    CHECK(after.owner == original.owner);
    CHECK(after.sharers == original.sharers);
    CHECK(after.epoch == original.epoch);
    CHECK(after.server_copy_current == original.server_copy_current);

    const auto counters_after = directory.transitionCounters();
    CHECK(counters_after.gets == counters_before.gets);
    CHECK(counters_after.getm == counters_before.getm);
    CHECK(counters_after.upgrade == counters_before.upgrade);
    CHECK(counters_after.puts == counters_before.puts);
    CHECK(counters_after.putm == counters_before.putm);
    const auto diagnostics_after = locked->diagnostics();
    CHECK(diagnostics_after.gets == diagnostics_before.gets);
    CHECK(diagnostics_after.getm == diagnostics_before.getm);
    CHECK(diagnostics_after.upgrade == diagnostics_before.upgrade);
    CHECK(diagnostics_after.puts == diagnostics_before.puts);
    CHECK(diagnostics_after.putm == diagnostics_before.putm);
}

void testConcurrentGetsSerializeThroughLockedCommitPath() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 0).status == TransitionStatus::Committed);
    CHECK(directory.gets(kLineA, 1).status == TransitionStatus::Committed);

    std::atomic<int> unexpected_results{};
    std::vector<std::thread> readers;
    for (std::uint16_t host = 0; host < 8; ++host) {
        readers.emplace_back([&directory, &unexpected_results, host] {
            for (int repeat = 0; repeat < 100; ++repeat) {
                const auto result = directory.gets(kLineA, host);
                if (!result.succeeded())
                    unexpected_results.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto &reader : readers)
        reader.join();

    CHECK(unexpected_results.load(std::memory_order_relaxed) == 0);
    const auto final = directory.inspect(kLineA);
    CHECK(final.has_value());
    if (final)
        checkShared(*final, 0xff, 8);
    CHECK(directory.transitionCounters().gets == 8);

    auto locked = directory.lockLine(kLineA);
    CHECK(locked.has_value());
    if (locked)
        CHECK(locked->diagnostics().gets == 8);
}

void testGetsStableTransitionsAndNoOpEpoch() {
    MesiDirectory directory;

    const auto first = directory.gets(kLineA, 3);
    CHECK(first.status == TransitionStatus::Committed);
    checkExclusive(first.snapshot, 3, 1);

    const auto second = directory.gets(kLineA, 7);
    CHECK(second.status == TransitionStatus::Committed);
    checkShared(second.snapshot, holder(3) | holder(7), 2);

    const auto repeated = directory.gets(kLineA, 7);
    CHECK(repeated.status == TransitionStatus::NoChange);
    checkShared(repeated.snapshot, holder(3) | holder(7), 2);

    const auto third = directory.gets(kLineA, 9);
    CHECK(third.status == TransitionStatus::Committed);
    checkShared(third.snapshot, holder(3) | holder(7) | holder(9), 3);

    const auto counters = directory.transitionCounters();
    CHECK(counters.gets == 3);
    CHECK(counters.getm == 0);
    CHECK(counters.upgrade == 0);
    CHECK(counters.puts == 0);
    CHECK(counters.putm == 0);

    auto locked = directory.lockLine(kLineA);
    CHECK(locked.has_value());
    if (locked)
        CHECK(locked->diagnostics().gets == 3);
}

void testGetmFromInvalidExclusiveAndShared() {
    MesiDirectory directory;

    const auto from_invalid = directory.getm(kLineA, 1);
    CHECK(from_invalid.status == TransitionStatus::Committed);
    checkModified(from_invalid.snapshot, 1, 1);

    CHECK(directory.gets(kLineB, 2).status == TransitionStatus::Committed);
    const auto same_owner_getm = directory.getm(kLineB, 2);
    CHECK(same_owner_getm.status == TransitionStatus::InvalidState);
    checkExclusive(same_owner_getm.snapshot, 2, 1);

    const auto from_exclusive = directory.getm(kLineB, 4);
    CHECK(from_exclusive.status == TransitionStatus::Committed);
    checkModified(from_exclusive.snapshot, 4, 2);

    CHECK(directory.gets(kLineC, 5).status == TransitionStatus::Committed);
    CHECK(directory.gets(kLineC, 6).status == TransitionStatus::Committed);
    const auto from_shared = directory.getm(kLineC, 7);
    CHECK(from_shared.status == TransitionStatus::Committed);
    checkModified(from_shared.snapshot, 7, 3);

    const auto counters = directory.transitionCounters();
    CHECK(counters.gets == 3);
    CHECK(counters.getm == 3);
}

void testUpgradeRequiresTheExclusiveOwner() {
    MesiDirectory directory;
    CHECK(directory.gets(kLineA, 11).status == TransitionStatus::Committed);

    const auto wrong_owner = directory.upgrade(kLineA, 12);
    CHECK(wrong_owner.status == TransitionStatus::InvalidState);
    checkExclusive(wrong_owner.snapshot, 11, 1);

    const auto upgrade = directory.upgrade(kLineA, 11);
    CHECK(upgrade.status == TransitionStatus::Committed);
    checkModified(upgrade.snapshot, 11, 2);

    const auto repeated = directory.upgrade(kLineA, 11);
    CHECK(repeated.status == TransitionStatus::InvalidState);
    checkModified(repeated.snapshot, 11, 2);
    CHECK(directory.transitionCounters().upgrade == 1);
}

void testPutsAndPutmPreserveStableInvariants() {
    MesiDirectory directory;

    CHECK(directory.gets(kLineA, 1).status == TransitionStatus::Committed);
    CHECK(directory.gets(kLineA, 2).status == TransitionStatus::Committed);
    const auto retain_one = directory.puts(kLineA, 1);
    CHECK(retain_one.status == TransitionStatus::Committed);
    checkShared(retain_one.snapshot, holder(2), 3);

    const auto repeated_puts = directory.puts(kLineA, 1);
    CHECK(repeated_puts.status == TransitionStatus::InvalidState);
    checkShared(repeated_puts.snapshot, holder(2), 3);

    const auto remove_last = directory.puts(kLineA, 2);
    CHECK(remove_last.status == TransitionStatus::Committed);
    CHECK(remove_last.snapshot.state == MesiState::I);
    CHECK(!remove_last.snapshot.owner.has_value());
    CHECK(remove_last.snapshot.sharers == 0);
    CHECK(remove_last.snapshot.epoch == 4);
    CHECK(remove_last.snapshot.server_copy_current);
    CHECK(isValidSnapshot(remove_last.snapshot));

    CHECK(directory.gets(kLineB, 3).status == TransitionStatus::Committed);
    const auto put_exclusive = directory.puts(kLineB, 3);
    CHECK(put_exclusive.status == TransitionStatus::Committed);
    CHECK(put_exclusive.snapshot.state == MesiState::I);
    CHECK(put_exclusive.snapshot.epoch == 2);
    CHECK(isValidSnapshot(put_exclusive.snapshot));

    CHECK(directory.getm(kLineC, 4).status == TransitionStatus::Committed);
    const auto wrong_putm = directory.putm(kLineC, 5);
    CHECK(wrong_putm.status == TransitionStatus::InvalidState);
    checkModified(wrong_putm.snapshot, 4, 1);
    const auto puts_modified = directory.puts(kLineC, 4);
    CHECK(puts_modified.status == TransitionStatus::InvalidState);
    checkModified(puts_modified.snapshot, 4, 1);

    const auto put_modified = directory.putm(kLineC, 4);
    CHECK(put_modified.status == TransitionStatus::Committed);
    CHECK(put_modified.snapshot.state == MesiState::I);
    CHECK(!put_modified.snapshot.owner.has_value());
    CHECK(put_modified.snapshot.sharers == 0);
    CHECK(put_modified.snapshot.epoch == 2);
    CHECK(put_modified.snapshot.server_copy_current);
    CHECK(isValidSnapshot(put_modified.snapshot));

    const auto counters = directory.transitionCounters();
    CHECK(counters.puts == 3);
    CHECK(counters.putm == 1);
}

void testPerEntryDiagnosticsCoverAllOperations() {
    MesiDirectory directory;

    CHECK(directory.getm(kLineD, 1).status == TransitionStatus::Committed);
    CHECK(directory.putm(kLineD, 1).status == TransitionStatus::Committed);
    CHECK(directory.gets(kLineD, 1).status == TransitionStatus::Committed);
    CHECK(directory.upgrade(kLineD, 1).status == TransitionStatus::Committed);
    CHECK(directory.putm(kLineD, 1).status == TransitionStatus::Committed);
    CHECK(directory.gets(kLineD, 1).status == TransitionStatus::Committed);
    CHECK(directory.gets(kLineD, 2).status == TransitionStatus::Committed);
    CHECK(directory.puts(kLineD, 2).status == TransitionStatus::Committed);
    CHECK(directory.puts(kLineD, 1).status == TransitionStatus::Committed);

    auto locked = directory.lockLine(kLineD);
    CHECK(locked.has_value());
    if (!locked)
        return;
    const auto diagnostics = locked->diagnostics();
    CHECK(diagnostics.gets == 3);
    CHECK(diagnostics.getm == 1);
    CHECK(diagnostics.upgrade == 1);
    CHECK(diagnostics.puts == 2);
    CHECK(diagnostics.putm == 2);
}

void testInvalidInputsDoNotAllocateOrMutateCounters() {
    MesiDirectory directory;

    CHECK(directory.getOrCreate(kLineA + 1) == nullptr);
    CHECK(!directory.inspect(kLineA + 1).has_value());
    CHECK(!directory.lockLine(kLineA + 1).has_value());
    CHECK(!directory.shardIndexFor(kLineA + 1).has_value());
    CHECK(directory.gets(kLineA + 1, 1).status == TransitionStatus::UnalignedAddress);
    CHECK(directory.getm(kLineA, 64).status == TransitionStatus::InvalidHost);
    CHECK(directory.upgrade(kLineA, 64).status == TransitionStatus::InvalidHost);
    CHECK(directory.puts(kLineA, 64).status == TransitionStatus::InvalidHost);
    CHECK(directory.putm(kLineA, 64).status == TransitionStatus::InvalidHost);
    CHECK(directory.allocatedLineCount() == 0);

    const auto counters = directory.transitionCounters();
    CHECK(counters.gets == 0);
    CHECK(counters.getm == 0);
    CHECK(counters.upgrade == 0);
    CHECK(counters.puts == 0);
    CHECK(counters.putm == 0);
}

void testSparseAllocationAndDeterministicSharding() {
    bool zero_shards_rejected = false;
    try {
        MesiDirectory invalid(0);
    } catch (const std::invalid_argument &) {
        zero_shards_rejected = true;
    }
    CHECK(zero_shards_rejected);

    MesiDirectory directory(7);
    for (std::uint64_t line_number = 0; line_number < 28; ++line_number) {
        const auto address = line_number * MesiDirectory::kLineSize;
        const auto shard = directory.shardIndexFor(address);
        CHECK(shard.has_value());
        if (shard)
            CHECK(*shard == line_number % 7);
    }
    CHECK(directory.allocatedLineCount() == 0);

    const auto first = directory.getOrCreate(0);
    const auto same = directory.getOrCreate(0);
    const auto second = directory.getOrCreate(7 * MesiDirectory::kLineSize);
    CHECK(first != nullptr);
    CHECK(first == same);
    CHECK(second != nullptr);
    CHECK(second != first);
    CHECK(directory.allocatedLineCount() == 2);
}

void testControlledCommitRejectsOwnerSharerCoexistence() {
    MesiDirectory directory;
    auto locked = directory.lockLine(kLineA);
    CHECK(locked.has_value());
    if (!locked)
        return;

    const auto expected = locked->snapshot();
    const DirectorySnapshot invalid{MesiState::S, 1, holder(2), expected.epoch, true};
    const auto result = locked->commitGets(1, expected, invalid);
    CHECK(result.status == TransitionStatus::InvalidState);
    checkInvalid(result.snapshot);
    checkInvalid(locked->snapshot());
    CHECK(locked->diagnostics().gets == 0);
    CHECK(directory.transitionCounters().gets == 0);
}

#ifndef CXLMEMSIM_MESI_DIRECTORY_STANDALONE
void testCoherencyEngineOwnsIndependentStrictV2Directory() {
    CoherencyEngine engine(0, nullptr, nullptr);

    const auto transition = engine.strictV2Gets(kLineA, 6);
    CHECK(transition.status == TransitionStatus::Committed);
    checkExclusive(transition.snapshot, 6, 1);
    CHECK(engine.strictV2Directory().allocatedLineCount() == 1);
    CHECK(engine.strictV2TransitionCounters().gets == 1);

    const auto legacy_stats = engine.get_stats();
    CHECK(legacy_stats.coherency_messages == 0);
    CHECK(legacy_stats.invalidations == 0);
    CHECK(legacy_stats.downgrades == 0);
    CHECK(legacy_stats.writebacks == 0);
    CHECK(legacy_stats.remote_ops == 0);
}
#endif

} // namespace

int main() {
    testUntouchedLinesAreImplicitInvalidAndSparse();
    testLockedLineIsMoveOnlyAndOwnsPendingReference();
    testLockedLineMoveAssignmentIsSafeAfterDirectoryLifetimeEnds();
    testLockedLineSelfMoveAssignmentIsANoOp();
    testLockedLineRemainsUsableAfterDirectoryLifetimeEnds();
    testLockedCommitFinalizesTask4PostSnoopTransitions();
    testLockedCommitRejectsSameOwnerExclusiveToModifiedAsGetm();
    testLockedUpgradeCommitsSameOwnerExclusiveToModified();
    testLockedGetmRejectsSharedRequesterPromotionWithoutMutation();
    testLockedGetmRejectsSharedRequesterPartialReconciliationWithoutMutation();
    testLockedGetmCommitsNonSharerPromotionFromShared();
    testLockedFinalizersCommitLegalPartialAckReconciliation();
    testLockedFinalizersRejectRelabelingAndInvalidRelationships();
    testLockedCommitRejectsStaleOrInvalidMetadataWithoutMutation();
    testConcurrentGetsSerializeThroughLockedCommitPath();
    testGetsStableTransitionsAndNoOpEpoch();
    testGetmFromInvalidExclusiveAndShared();
    testUpgradeRequiresTheExclusiveOwner();
    testPutsAndPutmPreserveStableInvariants();
    testPerEntryDiagnosticsCoverAllOperations();
    testInvalidInputsDoNotAllocateOrMutateCounters();
    testSparseAllocationAndDeterministicSharding();
    testControlledCommitRejectsOwnerSharerCoexistence();
#ifndef CXLMEMSIM_MESI_DIRECTORY_STANDALONE
    testCoherencyEngineOwnsIndependentStrictV2Directory();
#endif
    if (failures != 0) {
        std::cerr << failures << " checks failed\n";
        return EXIT_FAILURE;
    }
    std::cout << "MESI directory tests passed\n";
    return EXIT_SUCCESS;
}
