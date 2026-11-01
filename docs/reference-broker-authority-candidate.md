# Reference broker authority candidate

This is an isolated, disabled image-only init, upload and commit experiment. It
is not a complete worker recovery protocol and must not be enabled or deployed.
The daemon/worker runtime does not invoke this path. Mixed inputs including
auxiliary files, GCPs and LiDAR are unsupported; do not treat image-only
inventory as a general input contract. Crash-recovery reconciliation and
production release qualification remain unfinished. Uncertain external
mutations retain their intent and require inspection; they are not replayed
automatically.

The witness checks the active reference claim and its manifest and pipeline
identity, persists a coordinator intent before forwarding init, and retains a
separate completion proof for replay. Missing or mismatched records hold the
request. This is cooperative fencing: it does not prevent privileged local
access, direct engine calls, or uncooperative processes.

The reference path applies a shared deadline to broker and lifecycle locks,
SQLite busy waits, and the adapter. Filesystem operations, SQLite commit and
fsync, and OS scheduling may exceed it. It is not a hard real-time guarantee.

The synthetic POSIX suites are `python3 -m unittest
tests.test_reference_broker_authority_candidate
tests.test_reference_adapter_loopback_candidate`. They check claim identity,
upload and commit evidence, failed forwarding, partial restore, response
mismatch, lock contention, a mocked-forward process crash, and loopback HTTP.
These tests do not establish power-loss durability, real NodeODM behavior, or a
real worker-to-Viewer end-to-end handoff.
