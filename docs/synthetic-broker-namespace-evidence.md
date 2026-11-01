# Synthetic broker namespace rehearsal evidence

Run: `c8587ae7ae25b3374f1c016d`. Outcome: PASS for the scoped synthetic assertions below. No real client data, production services, or production credentials were used. All task-owned containers, volumes, the synthetic source snapshot and synthetic TLS materials were retained; no deletion was performed.

- Broker source commit: `ad960902e1871a787fb5ad44037bf70be12d78ff`.
- Staged source snapshot SHA-256: `4cdee8159b8c2e1f3af65e20a1dae68581535191ecaecb2aef3cc7ae7ee13ab1`.
- Existing local Python image ID: `sha256:673719e071ecf27434fd2635bbd8d00f49f4fdd81f243143438a42ced8bedf60`.
- Synthetic source root: `C:/Users/fstor/.codex/artifacts/synthetic-broker-source-a2bf6bf3c5fff9b6`.
- Synthetic credential root: `C:/Users/fstor/.codex/artifacts/synthetic-broker-certs-a2bf6bf3c5fff9b6`. The issuer private key and synthetic leaf keys are test-only; no credential content is included here.
- All container and volume labels: `ledgetop.rehearsal=c8587ae7ae25b3374f1c016d`.

| Role | Exact container ID | Independently verified state |
|---|---|---|
| Initializer | `aa8a32a5c363639f43fbb03606d9cbff210709d16c51dd3bc193fc682b994edf` | exited |
| Broker | `77ead9bc0c748f9c8b3a8b49fe22a6e15e03d879512886ad06653c48521f14ae` | exited |
| Synthetic engine | `548e008c11bcce57fd0d25d7c5bf5da3a60b31bfcbc6096ffc01a249d0c5ea34` | exited |
| Isolated probe | `90fe34b332f004cdc1d3da8ccfcd201f14a530042a45377e4053ecf9354da86e` | exited |
| Test coordinator | `45d509eb45bcc5f90850fa8c038abbe0a76921b9aa6a0437156ac0815c343cc5` | exited |

Retained volumes: `lt-broker-c8587ae7ae25b3374f1c016d-state` and `lt-broker-c8587ae7ae25b3374f1c016d-control`. A separate label-filtered Docker query confirmed these two volume names and the five exited containers. The Windows watchdog reported zero active owned Job Object processes. No host or production container was stopped.

The real synthetic-process `NodeMutationBroker.mutate`, `LocalCoordinatorAuthority`, Queue, authority lease, TaskStore, and loopback adapter accepted a signed test init through a test-only mTLS Unix-socket handler. A client that did not present a certificate was denied. Replaying the same signed init returned the same result, the synthetic engine recorded exactly one request, and the broker's operation journal reported `complete`. The launcher checked the broker and synthetic engine shared their network namespace, the unrelated probe used a distinct namespace without the control socket, the synthetic engine's only observed listener on the test port was IPv4 loopback, and the owned containers had no host-published ports.

This test intentionally patched the broker qualification flag and Docker engine/backend identity probes inside the synthetic broker process. It used `LocalCoordinatorAuthority` inside that same process; it did **not** use `RemoteAuthoritySession`, actual remote coordinator authority, real NodeODM, upload, commit, result handoff, node crash or recovery release. The control handler is test-only, not a production listener. The test does not prove host-root, Docker-administrator, attached-container or direct backend isolation. The previous namespace test's Windows-host probe was inconclusive. No claim is made that all-node scheduling, durable reconciliation or production exclusion is qualified.
