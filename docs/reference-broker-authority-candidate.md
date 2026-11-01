# Reference broker authority candidate

This is an isolated, disabled init-only experiment. It is not a complete
worker recovery protocol and must not be enabled or deployed. Upload, commit,
worker integration, crash-recovery reconciliation, and production release
qualification remain unfinished. Uncertain external mutations retain their
intent and require inspection; they are not replayed automatically.

The witness checks the active reference claim and its manifest and pipeline
identity, persists a coordinator intent before forwarding init, and retains a
separate completion proof for replay. Missing or mismatched records hold the
request. This is cooperative fencing: it does not prevent privileged local
access, direct engine calls, or uncooperative processes.

The reference path applies a shared deadline to broker and lifecycle locks,
SQLite busy waits, and the adapter. Filesystem operations, SQLite commit and
fsync, and OS scheduling may exceed it. It is not a hard real-time guarantee.

The synthetic POSIX suite is `python3 -m unittest
tests.test_reference_broker_authority_candidate`. It checks claim identity,
failed forwarding, replay, partial restore, response mismatch, and lock
contention. These tests do not establish power-loss durability or prove a
real worker-to-Viewer end-to-end handoff.
