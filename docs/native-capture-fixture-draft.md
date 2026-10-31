# Loopback-only NativeEngine capture fixture

This source-only experiment invokes the existing `NativeEngine.download()` GET
against a literal loopback HTTP address, feeds its response to the preallocated
slot's durable streaming journal, and exposes the locally sealed bytes through
the existing locked reader. It is not registered with the coordinator, a node
agent, a production endpoint, Docker startup, or any deployment configuration.
The explicit loopback restriction is part of this test seam, not a node security
boundary. The binding is supplied by the test fixture; neither the binding nor
the synthetic token proves a real coordinator identity, NodeGate reservation,
engine ownership, or authenticated client. The coordinator is absent from the
exercise, but no real coordinator-outage recovery was tested.

Tests use an isolated network-none Linux container with a loopback synthetic
HTTP server that implements the `/task/<uuid>/download/all.zip` route. They
exercise successful capture and locked replay, wrong task identity, duplicate
length headers, truncated and disconnected response bodies, plus killing the
capture process **after the capture call returned and its descriptors closed**,
then replaying in a new process. That test does not kill an active capture or
prove process-death recovery at an in-progress network boundary. Network-none
is supplied by the test invocation, not asserted or enforced by the test code.
Files remain after
success or incomplete capture; no engine or local cleanup and no acknowledgement
are sent. A test also demonstrates that a different valid ZIP supplied by the
server is locally sealed: the source API does not provide a trusted archive
hash, and a syntactically valid shorter or wrong archive cannot be identified
from a self-computed hash. ZIP checks remain structural only.

`NativeEngine.download()` supplies an idle socket timeout, not a total transfer
deadline. This adapter does not add one and must not be treated as a bounded
production transfer service. Tested payloads and process waits are synthetic and
bounded; tmpfs does not establish real persistent storage, power-loss recovery,
filesystem reservation guarantees or safe cleanup. A future implementation must
first integrate real authenticated node enrollment, a shared physical NodeGate,
qualified allocation and output accounting, process supervision, durable
coordinator receipt and independent Viewer custody. Engine retention during a
long outage remains unresolved. See `direct-slot-capture-draft.md` for journal
and locked-reader trust assumptions.
