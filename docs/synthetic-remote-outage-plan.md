# Synthetic remote-authority interruption rehearsal — independent review required

Status: draft only. No source snapshot, certificates, Docker volumes or containers have been created for this draft; no process or container has been signaled. The successful clean remote-authority rehearsal in `synthetic-remote-broker-evidence.md` remains unchanged.

## Intended observation

Using the actual pinned Processing `RemoteAuthoritySession`, `NodeMutationBroker`, Queue, authority lease, TaskStore, broker intent fence and synthetic NodeODM-like engine, interrupt a separately owned coordinator container **after** its prepare challenge and the engine's receipt of the init but **before** the engine responds. The engine fsyncs an event marker and remains paused behind a separate test-only release marker. The launcher must see the engine's forwarded-event log, verify the exact coordinator's full ID, name, run label and running state, issue `docker kill --signal KILL` **only to that owned coordinator ID**, and verify the same ID/name/label is exited with code 137. Only then may the launcher write a release marker via `docker exec` into the still-verified owned broker container and allow the synthetic engine to respond.

The broker must observe `SessionHeld`, retain an operation in `started`, retain the matching NodeGate reservation, refuse the same-session retry and observe exactly one synthetic engine request. A new, separately named verifier container then acquires the coordinator authority lease **before** opening the same synthetic coordinator Queue state. It must observe the original coordinator broker intent still `started`, the job held for `coordinator_restart`, and admission blocked. This is a restart/reopen of the actual synthetic Queue state, not a restart of an operational application deployment or a successful reconciliation/release. No test automatically clears or resolves the intent.

A timeout, engine forwarding not observed, unknown Docker create/kill/exec outcome, unexpected coordinator exit status, changed ID/name/label, failed broker or verifier, missing evidence, resource or process uncertainty, or any failed stop must be INCONCLUSIVE. A forwarded engine call cannot be undone by a disconnected authority. The test MUST NOT infer completion, retry the init via a fresh authority session, or let a later session adopt the node.

## Staged code and owned resources

- `scripts/synthetic-remote-outage/harness.py` — synthetic engine, actual remote coordinator session, node broker and restart verifier roles; uses only synthetic input and generated test credentials.
- `scripts/synthetic-remote-outage-snapshot.py` — prepares only the pinned `processing/` source tree plus this harness under a new run-owned directory; no deletion.
- `scripts/synthetic-broker-namespace-certs.py` — previously reviewed fresh-directory synthetic-only CA and distinct client/server directories. The issuer key is not mounted. No production credentials.
- `scripts/synthetic-remote-outage-runner.py` — five original labelled containers plus a distinct verifier (six total); four per-run labelled volumes (`node-state`, `coordinator-state`, `control`, `events`). The coordinator alone mounts its state; the broker alone mounts node state; broker and engine share only the synthetic `events` volume. No Docker socket, published ports, host network, user dataset or external service. Exact full-ID/name/run-label verification is required before the targeted kill, the marker write, and each task-owned stop. No deletion.
- `scripts/synthetic-remote-outage-watchdog-windows.py` — reviewed-pattern Windows Job Object around the new runner, with exact run-name/full-ID/label reconciliation for six containers and four volumes. A timeout can still leave uncertain late Docker daemon creates or unverified processes; it must report INCONCLUSIVE and retain all resources.

The source commit to review is `ad960902e1871a787fb5ad44037bf70be12d78ff`; a fresh snapshot must be content-digested after staging. Synthetic mTLS credentials must be freshly generated in a new retained root. The real Docker engine/backend identity probes and broker qualification switch are patched only inside the isolated test process. The Unix-socket bootstrap is test-only and the test does not establish host-root/Docker-admin fencing, all-node admission, real NodeODM, upload/commit, result delivery, power-loss durability, or production readiness.

## Review gate

Before staging inputs, launching the watchdog, creating/starting/stopping Docker resources or sending the targeted KILL signal, independently review exact code, synthetic event ordering and timeouts, resource ownership/label reconciliation, Job Object termination and late-create handling, source/cert provenance and permissions, and verifier restart behavior. Task-owned volumes, stopped containers, snapshot, keys and logs remain retained for later review; no image/container/volume/file deletion or broad cleanup is permitted.
