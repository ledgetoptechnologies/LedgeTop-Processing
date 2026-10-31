# Reference worker image

`Dockerfile.reference-worker` packages the optional reference worker separately
from the coordinator. Building the image does not provision or start a worker.
Run it with `--config` pointing at a pre-existing owner-private configuration.
Without a valid configuration and physical gate, the daemon fails closed.

The default image user is unprivileged. A real deployment must use the same OS
identity and **same underlying canonical NodeGate directory** as every other
cooperating writer for that engine; a fresh gate inside the container does not
provide mutual exclusion. Mount a previously provisioned gate location and set
`XDG_STATE_HOME` consistently with the host. Independently provision persistent,
owner-private worker state and scratch mounts, a verified coordinator/broker CA,
worker server certificate and key, and distinct scoped credentials. Configure
the network so the fixed loopback `engineOrigin` actually reaches the intended
engine; an ordinary bridge container's loopback is not the host loopback.

Image packaging does not verify those identities, protect against direct engine
clients, establish a NAS broker or Viewer acceptance, or enable TreeDar. Do not
start a second worker against an active engine until the ownership, maintenance,
retention and recovery procedures have been qualified for the target host.
