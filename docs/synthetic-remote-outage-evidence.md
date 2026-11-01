# Synthetic remote-authority interruption evidence

Run `4bf0cc0812fe28d49502e330` passed the scoped, reviewed synthetic interruption scenario. Source commit `ad960902e1871a787fb5ad44037bf70be12d78ff`; staged snapshot SHA-256 `db8756a130fd29f1e09d8e0bd1d028260cf39299f0b33a3f416cf08adee8dc35`; local image ID `sha256:673719e071ecf27434fd2635bbd8d00f49f4fdd81f243143438a42ced8bedf60`. Retained synthetic source root `C:/Users/fstor/.codex/artifacts/synthetic-outage-source-e68e0389728b75ed` and synthetic-only certificate root `C:/Users/fstor/.codex/artifacts/synthetic-outage-certs-e68e0389728b75ed`. No credential content is recorded here.

After the synthetic engine had received the init and paused its response, the launcher verified the full ID, name and run label of the owned coordinator and issued KILL only to that container. Independent inspection confirmed coordinator ID `83fb734e81e34158eef9e447317c1ea048bc437539e08a0e4c7b920ba5b3cd4d` exited with status 137. The engine was then released. The actual synthetic broker reported an unresolved started node intent, a held NodeGate reservation, a rejected same-session retry and exactly one engine request. A separate verifier acquired the synthetic authority lease before reopening the coordinator Queue; it observed the original broker intent still started, the job held for `coordinator_restart`, and admission closed. No reconciliation or release was performed.

All following containers carry `ledgetop.rehearsal=4bf0cc0812fe28d49502e330` and were independently verified exited:

| Role | Full container ID |
|---|---|
| Initializer | `4d45a405b98bd6982c023c043f29de76851de6f33eeeddf97b1a111a92ba505e` |
| Broker | `7ec29d3b89734ba82ef8f1aaf4bc1a4f7952754308dcd35a18838667bc5528d4` |
| Synthetic engine | `33f2384a8b922efa965b025e0b8d413782f5d7ec8e042c879ff42a0618b65372` |
| Isolated probe | `7c81679a197c75598ed63df7be6f4cd1ac758047ba67196c3afa2b8caa3d885e` |
| Coordinator | `83fb734e81e34158eef9e447317c1ea048bc437539e08a0e4c7b920ba5b3cd4d` |
| Restart verifier | `d1c96d92a4c14f5c5ae5d2d6dee2c0b33b083c7cc6c704342376027314da9e7d` |

Four run-labelled volumes (`node-state`, `coordinator-state`, `control`, `events`) remain retained under prefix `lt-outage-4bf0cc0812fe28d49502e330-`. The Windows watchdog reported zero active owned Job Object processes. A separate label-filtered query confirmed six exited containers and four retained volumes. No resources were deleted.

This was one controlled synthetic process interruption. Broker qualification and Docker engine/backend identity probes were patched only in test processes; the bootstrap and listener are test-only. It does not establish power-loss safety, automatic reconciliation or delivery, no-duplicate behavior after a *new* authority session, real NodeODM behavior, upload/commit, two-node admission, production host isolation, or readiness for client jobs. Host root and Docker administrators remain trusted.
