# Experimental authority FD handoff: disabled

The source contains unqualified one-shot SCM_RIGHTS primitives only. They are
not wired to `processing.app`, any packaged startup option, updater or runtime.
They must not be used for production cutover. The supervisor holds the original
operation fence after every outcome, including a successful send of `GO`, which
is explicitly reported as ambiguous rather than as a completed handoff.

The host attestor inspects an exact Docker container/image, compares peer PID,
process start time and cgroup, and holds a pidfd while transferring. The
candidate checks peer UID, exact authority and metadata, lease inode and flock;
the broker fsyncs a one-shot authority-scoped intent before transfer and an ACK
record before sending `GO`. Missing or ambiguous responses hold; no automatic
replay or cleanup is implemented. The protected listener, root-owned Docker
access, trusted intent directory and external container lifecycle are operator
prerequisites, not established by these functions.

Unit tests use actual Unix sockets and descriptor transfer between local
processes, including truncated ancillary data and split/lost ACK. They **mock**
Docker identity and cgroup attestation and do not prove that a real Docker
container's PID/user namespaces are mapped correctly. Socket and filesystem
operations have no complete absolute wall-clock deadline. Candidate startup,
supervisor death, unknown `GO`, queue admission, persistent old-writer
exclusion, administrator privileges and safe candidate handoff are unqualified.
Retaining the original lease is not a proof that no other writer can bypass it.
