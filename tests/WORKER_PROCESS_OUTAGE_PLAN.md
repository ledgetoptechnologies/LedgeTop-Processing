# Pending owned worker-process interruption rehearsal

This plan is not implemented or approved for execution. Independent review of
both the eventual fixture and its exact signal/ownership logic is required
before creating or terminating any test process.

Current staged implementation uses the actual `WorkerService`/`ReferenceDriver` execution loop in a
separately owned child, using only synthetic worker-local archive bytes and
private temporary state. The coordinator is a real HTTP server on a thread in
the fixture supervisor process, not a separate process; stopping its listener
does not test coordinator process death or restart. A separately controlled,
synthetic NodeODM-compatible engine produces a small fixed ZIP and tracks
initialize/commit/download counts. No existing service or client data is used.
The coordinator is not mocked, but the supervisor and coordinator share a process.

1. Create and print one task-owned retained root from the outset. Pin and record
   the exact source commit, fixture hashes and synthetic TLS trust. Start the
   loopback coordinator thread and engine and worker as individually owned direct
   child handles under a process-group watchdog. A separate container runner
   and Windows Job Object outer watchdog are staged but not reviewed or run.
   Record every PID/handle; no guessed PID or process-name actions.
2. Allow exactly one synthetic task to reach a local durable archive/outbox.
   Confirm its byte size and SHA-256, immutable outbox identity, retry state,
   and one engine initialization/commit/download. Temporarily stop the
   coordinator's listener with a fixture-controlled outage mechanism while
   keeping the supervisor process and its SQLite state alive. Require the worker to
   record a retry and retain its local bytes; don't treat a timeout as evidence
   that no result commit happened.
3. Only after the worker emits the explicit persisted-state marker and the
   supervisor verifies its still-live direct child handle, terminate that exact
   worker child abruptly, wait with a bounded timeout and verify/reap it. Never
   signal a shared/user process or enumerate-and-kill. On termination/reap
   uncertainty, latch the test as failed, retain its handles/IDs and report for
   inspection; do not launch a replacement worker while ownership is uncertain.
4. Restore the fixture coordinator listener. Start one new owned worker process
   on the same retained state root only after confirming the first is gone.
   Assert it reopens the same archive/ledger/outbox, retries the identical
   completion identity, receives an exact coordinator metadata receipt, and
   exposes the same archive bytes through the actual worker result endpoint.
   Check no second engine execution and no byte deletion. Metadata acknowledgement
   is not coordinator custody or Viewer acceptance.
5. Stop only the recorded owned synthetic processes in the reviewed order;
   wait and verify each exit. Any cleanup failure is an explicit uncertainty.
   Preserve root, SQLite journals, synthetic ZIP and output logs; no recursive
   deletion. A bounded outer watchdog must handle partial setup and ensure no
   spawned child survives. The current process-group watchdog cannot guarantee
   this if its group leader exits early, and server.shutdown may block; do not
   execute until independent review confirms the container and outer watchdog close
   those gaps. The fixture emits success only after local cleanup and the group
   watchdog verifies group exit; container exit must also be independently
   verified before reporting a passing rehearsal. No real data or live services.

An operating-system process kill does not prove host power-loss durability.
A separate test is necessary to verify actual Viewer retrieval and acceptance,
TLS/certificate handling across hosts, storage device flush semantics and
production deployment. The existing `0b13bad` test has transport mocks and
reopens Python objects in one process; it is not a substitute for this test.

## Owned synthetic runs

On 2026-11-01, using Processing `9a7fe81f39f6b4479e67f7eea617a4db13aee233`
and local image `sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159`:

* Initial run `f5dfd48f7adc466781aa691cffe4632b` failed before input
  request. Its fixture passed an unsupported `--odm-only` argument; stderr was
  discarded. Root `C:\Users\fstor\AppData\Local\Temp\ledgetop-acceptance-outer-s3wleae0`
  and stopped container `bde5203513573b23607553378ba27ddaa7909ad3ab400b4d444ddefe95615057`
  were retained. Container exit 1; outer job active count 0; inner group exited.
* After removing that argument and retaining worker diagnostics, run
  `948b613a05334fe8aa7d635686ebcf1d` passed. Retained root
  `C:\Users\fstor\AppData\Local\Temp\ledgetop-acceptance-outer-pllfgm9z`;
  stopped container `a0d1605eeebd3df3d77b8c8bb38954d7785fff1867d19feb7bbca2d23c79deb4`
  exited 0, exact run label verified, inner group exited, outer job active 0.
  Fixture SHA-256: `18d2b0e6013a6c8ce7156ecb1a6c4145ff817ad1abefa396840711d76566c07f`;
  engine fixture SHA-256: `f7f3fbc6f3ff0af14c1ef4fad1c6343e4500a09681636e627e028c5e0035903e`;
  inner watchdog SHA-256: `401c6c8295e66ad52b0d2c6348b1b0625da4200510d0895ff977e1e17a096e82`.
  Exact outbox payload and coordinator/worker metadata receipt matched after
  killing and reaping the first worker and starting another. Proxy bytes matched
  the worker archive; synthetic engine initialize/upload/commit/download counts
  were each one. The coordinator listener was stopped and restarted in the same
  supervisor process. This did not exercise a coordinator process crash, real
  ODM, Viewer acceptance, device power loss, or production deployment.
