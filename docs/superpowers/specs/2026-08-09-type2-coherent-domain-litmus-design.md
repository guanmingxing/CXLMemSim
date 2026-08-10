# Type-2 Coherent Domain Litmus Design

## Proof boundary

The Type-2 HDM range is one server-authoritative MESI domain. Endpoint 0 models the guest/host path that QEMU invokes
for CFMWS accesses. Endpoint 1 models the Type-2 device path invoked through BAR2 commands. Both endpoints use
protocol v2 and distinct registered sessions. Protocol v1 remains legacy and is not accepted as coherence proof.

This is a functional full-system proof under TCG. It proves that every modeled host/device access reaches the same
directory and obeys its ownership transfers. It does not claim physical CPU LLC snoops under KVM.

## Endpoint contract

Each endpoint has a bounded 64-byte write-back cache and a selectable write-back or write-through policy. A read miss
issues GETS. A write miss issues GETM; an S/E hit issues UPGRADE before mutation. Dirty replacement issues PUTM and
clean replacement issues PUTS. Endpoint code must release its cache mutex before calling the server because the server
may synchronously reenter the endpoint with a snoop.

SNP_INV and SNP_DOWNGRADE acknowledge only after the local state change. SNP_DATA_INV and SNP_DATA_DOWNGRADE include
the complete dirty line in the ACK. The server persists returned dirty data before committing the requester grant.
Timeout, malformed ACK, send failure, and partial ACK fail closed. A partial transaction may commit only endpoint
effects that already acknowledged; it never grants ownership to the requester.

## Litmus oracle

Every positive run reports zero forbidden outcomes and nonzero directory, snoop, ACK, and writeback evidence:

1. H2D: host writes in M; device reads the new value after SNP_DATA_DOWNGRADE.
2. D2H: device writes in M; host reads the new value after SNP_DATA_DOWNGRADE.
3. MP: after observing `flag=1`, the reader must not observe an older data value.
4. Partial-line merge: ownership transfer preserves writes to both halves of one cache line.
5. Atomic FAA/CAS: returned old values form a legal total order and the final scalar is exact.
6. Bounded replacement: dirty LRU eviction writes back before the line leaves the cache.
7. Partial ACK: the requester times out without a grant; only acknowledged invalidations change the directory.

The negative control intentionally uses independent legacy shadows and must exhibit at least one forbidden stale read.
This prevents a test that passes without traversing the coherent domain.

## Full-system mapping

QEMU CFMWS callbacks use the host endpoint. BAR2 coherent load/store/FAA/CAS commands use the device endpoint and the
same DPA offsets. A guest program maps the Type-2 devdax range and BAR2, runs the same litmus cases, and emits JSON with
topology, iteration counts, forbidden outcomes, and protocol counters. QEMU qtests cover command layout and routing;
only QEMU plus the real CXLMemSim server plus the guest program counts as end-to-end proof.
