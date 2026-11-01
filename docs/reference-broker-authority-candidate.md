# Reference broker authority candidate

This is an isolated, disabled image-only init, upload and commit experiment. It
is not a complete worker recovery protocol and must not be enabled or deployed.
The daemon runtime does not invoke the separate, hard-disabled signed-plan
worker entry. Its authority witness requires the coordinator and broker in the
same process and cannot be used as cross-PC authority. Mixed inputs including
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
The integrated synthetic worker test uses a real scratch ledger, outbox and
coordinator delivery receiver, but mocks the NodeODM adapter and archive
download and constructs the worker without its production constructor. It
simulates a lost receipt and retains all synthetic source and result files.
These tests do not establish power-loss durability, real NodeODM behavior,
real Viewer custody, restart recovery, or a real end-to-end handoff. The
offline schema preparation requires empty intent state; migration of nonempty
or previously running systems remains unqualified. Poll and result publication
hold the same-process authority through I/O and have a ten-second authority
deadline; long archive transfers can therefore hold and require inspection.
