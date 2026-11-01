# Local backend identity check in guarded dispatch

This inactive, source-only candidate refuses guarded mutation and management
reads without an installer-supplied `BackendPin`: the full Docker container ID,
immutable image ID and expected host port. The fixed local Unix Docker daemon
is inspected and the exact container must be running with only the expected
loopback mapping for port 3000. Before and after each bounded exchange the
guard checks the pinned daemon and backend. A changed backend leaves the
existing reservation/intents uncertain; it is not evidence to retry.

The probe is strictly local. It cannot inspect the remote main machine from the
coordinator host: a node-local broker must perform this check on each node and
must be authenticated and bound to the coordinator's actual owner and claim.
The installer must independently select and protect the pins. Inspecting a
container before and after a connection is not atomic with connecting to it,
does not prevent replacement and return to the original state, does not identify
an arbitrary socket peer, and does not restrict root, Docker administrators or
other network writers. Network isolation, daemon-management fencing, result
download, durable recovery, safe cancellation and node-broker transport remain
required. Both existing qualification flags remain false. This is not a
production rollout or authorization to enable dispatch.

The checks use the existing overall operation deadline. Slow Docker inspection
can therefore refuse even a healthy node; this fails closed and retains work.
