# Remote authority synthetic namespace rehearsal evidence

Run: `c0a3749ebf781611eac079af`. Scoped result: PASS for a successful synthetic init, exact replay and no-certificate TLS denial. No live processing service, customer data or production credential was used; nothing was deleted.

- Pinned Processing source commit: `ad960902e1871a787fb5ad44037bf70be12d78ff`.
- Synthetic snapshot SHA-256: `1662e41d8280a209bfbcd38ad1ca93335e317e6e644682f42e750f3b5bfe6534`.
- Existing image ID: `sha256:673719e071ecf27434fd2635bbd8d00f49f4fdd81f243143438a42ced8bedf60`.
- Retained source root: `C:/Users/fstor/.codex/artifacts/synthetic-remote-source-a18bd0f14852d171`.
- Retained synthetic-only certificate root: `C:/Users/fstor/.codex/artifacts/synthetic-remote-certs-a18bd0f14852d171`. Its key content is not recorded here.
- All listed containers and volumes carry label `ledgetop.rehearsal=c0a3749ebf781611eac079af`.

| Role | Exact container ID | Verified state |
|---|---|---|
| Initializer | `adb7537a7a22d00b75c17bda008d249bdff50ba81c1bb12136dfa88956a0c8ad` | exited |
| Broker | `2b063cea9e28a53f62ea2e209a8dff077e39e10020ff0c5e2f71fbf92427fd8d` | exited |
| Synthetic engine | `951ee2f391101b7524dbbe4cd993b5f45e50c3c03cd9ff982ab467472ceca787` | exited |
| Isolated probe | `db0ca8d8c2de5238217f9c9f8465dc3bebc2cbf116ebc26114df7b764c767edd` | exited |
| Coordinator | `52a0192d8d0fd2af1a6ab1d5003b46da459620f9e0bdb4c9007c03550b9df43d` | exited |

Retained labelled volumes: `lt-remote-c0a3749ebf781611eac079af-node-state`, `lt-remote-c0a3749ebf781611eac079af-coordinator-state`, and `lt-remote-c0a3749ebf781611eac079af-control`. Independent label-filtered Docker queries confirmed these volumes and five exited containers. The Windows watchdog reported zero active owned Job Object processes and verified exact container IDs, names and labels before stopping; no resources were deleted.

The separate synthetic coordinator used an actual Queue, AuthorityLease and broker-intent fence, and the broker used the actual `RemoteAuthoritySession` and `NodeMutationBroker` over a mutually authenticated TLS stream carried by a Unix socket. The coordinator denied an anonymous TLS connection. After the test-only authenticated bootstrap, a signed synthetic init succeeded; exact replay was identical and the engine received one request. Logs recorded a complete broker operation journal and complete coordinator intent.

**Limits:** the broker qualification flag and Docker engine/backend identity probes were patched inside the test processes. The bootstrap and Unix-socket listener are test harness code, not production deployment/enrollment. This was one clean run; it did not test disconnect, lost acknowledgements, coordinator death, restart, durable reconciliation, upload, commit, result return, two-node admission, actual NodeODM or physical exclusivity. It does not fence host root, Docker administrators, direct backend access or older processes. A pass is not authorization or evidence of production readiness.
