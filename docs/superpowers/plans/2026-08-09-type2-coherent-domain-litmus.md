# Type-2 Coherent Domain Litmus Implementation Plan

1. Add a server-side Type-2 litmus target before adding endpoint-cache production code. Preserve the compile failure as
   the TDD red gate.
2. Implement the bounded endpoint cache and in-process transport adapter. Make H2D, D2H, MP, partial-line, atomic,
   replacement, WT, and partial-ACK cases pass.
3. Add protocol-v2 server request dispatch and transport adapters for TCP, SHM, and RDMA without changing v1 behavior.
4. Add QEMU host-agent routing for Type-2 CFMWS and device-agent BAR2 coherent load/store/FAA/CAS commands.
5. Add qtests for command ABI, DPA bounds, endpoint identity, and forbidden legacy fallback.
6. Add a guest devdax/BAR2 litmus runner with machine-readable JSON and a TCG launch script.
7. Run focused tests, full Release/Debug server tests, QEMU CXL qtests, and the full guest experiment. Archive logs and
   counter snapshots, then obtain independent spec and quality reviews before committing and pushing both branches.
