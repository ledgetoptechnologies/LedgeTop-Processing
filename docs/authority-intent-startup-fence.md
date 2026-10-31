# Unresolved handoff blocks newer coordinator startup

When either the authority-scoped handoff intent or its ACK exists, the newer
`AuthorityLease` refuses ordinary acquisition before opening the lease and
checks again after obtaining the flock. It never removes or interprets either
marker. This holds after a supervisor crash or a lost GO until an independently
qualified reconciliation procedure resolves the operation.

The guard applies only to binaries containing this change. Existing PR197 and
other older coordinators ignore these marker files; host-level old-writer and
Docker-administrator fencing is still required. The experimental FD receiver
is not wired into application startup, and this marker is not itself a safe
candidate handoff, a host-wide writer fence or an automatic rollback policy.

Synthetic tests include actual supervisor SIGKILL after durable intent,
lost ACK followed by normal startup, an orphan ACK, and a marker created
between the first check and flock. They do not prove filesystem power-loss
durability or protect against privileged removal of the marker.
