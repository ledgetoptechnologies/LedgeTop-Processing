# Direct V2 supervised recovery: deployment design (not enabled)

This is a design for recovering an interrupted source upload without guessing
that its reader has stopped. It is not an installation guide, and it does not
authorize deleting partial files or retiring existing worker pins. The current
runtime does not yet satisfy these requirements.

## Current evidence and boundary

On 2026-11-06, read-only inspection showed both processing hosts use cgroup v2
with the systemd driver; the `bkoltz` login cannot write `/sys/fs/cgroup`.
The cluster coordinator container runs unprivileged as uid 10001 with
no-new-privileges. The main NodeODX container runs as uid 0 with Docker
privileged mode. Neither observation proves isolated V2 writer ownership or
prevents another privileged host process from accessing a worker descriptor.
Do not use the existing NodeODX container as the source-writer isolation
boundary.

The test-only `tests/test_direct_v2_supervisor_recovery.py` exercises a
confined child and synthetic journal in an isolated Docker test container. It
does **not** supervise a deployed worker, atomically retire a real
ScratchLedger pin, exclude host peers, or survive supervisor-process restart.
The live worker continues to retain uncertain pins.

## Minimal target architecture

1. A protected host supervisor, started by a root-managed systemd service,
   owns the worker lifecycle, durable recovery ledger and a private control
   endpoint. The coordinator authenticates to that endpoint using independently
   provisioned identities. An untrusted request cannot choose a UID, cgroup,
   executable, mount, signer, operation identity, or recovery decision.
2. One confined writer process handles a job or bounded batch, not one process
   per image. A trusted bootstrap starts it in a unique systemd transient unit
   and cgroup with a dedicated worker UID and a private mount namespace. The
   supervisor owns the TLS/source connection and passes only explicit pipe
   endpoints to the writer. No spool descriptor, Docker socket, systemd
   control socket, or writable shared host directory is inherited by the
   writer. The writer may access only its operation-specific private scratch.
3. Under the same durable node gate and ScratchLedger reservation, the
   supervisor journals authority, node and worker instance, job, generation,
   source/derived inventory digest, object ID, expected size/hash, operation ID,
   readiness challenge, writer incarnation, cgroup identity, and exact pin ID
   before any byte is admitted. The current `direct_writer_pins` table alone
   does not contain enough identity for this proof.
4. A worker may not fork/clone, create sockets or pass descriptors, ptrace or
   acquire another process's FDs, use io_uring to bypass the filter, change
   namespaces/cgroups, or execute a different program after its bootstrap.
   Apply a reviewed seccomp allowlist or verified systemd syscall filter;
   verify actual filters at runtime and test the denied operations. Use
   `NoNewPrivileges=yes`, an empty capability bounding set, native syscall
   architecture only, protected cgroups and a private proc view. Bind only
   operation-owned storage; other services, including privileged containers,
   must not share or modify it.
5. The worker UID must not be shared with other host services. The supervisor
   must verify the unit's configured UID, mount namespace, process identity,
   cgroup and sandbox policy before the first byte, and must fence new work
   while recovery is being decided. Root, the systemd manager, the kernel and
   the supervisor are trusted; compromise of that host authority is outside
   this boundary. Yama `ptrace_scope=1` by itself is not this proof.

Systemd's [execution sandbox documentation](https://www.freedesktop.org/software/systemd/man/256/systemd.exec.html)
describes syscall/address-family and proc controls. Its [kill semantics](https://www.freedesktop.org/software/systemd/man/262/systemd.kill.html)
describe `KillMode=control-group`. Directive support must be checked on each
installed systemd version; in particular, the main host reports version 249
and the cluster host 259. The example design is not a claim that either host
currently has these controls or that a proposed unit has been validated.

## Recovery state machine

* **Running:** the original V2 writer pin and schema-2 node hold remain in
  place. A timeout, closed network connection, dead PID, or expired proof is
  not sufficient to release either.
* **Quiescing:** freeze the exact operation and refuse replacement writers.
  Stop the exact systemd invocation with control-group kill semantics, verify
  the manager's boot/invocation identity and that the corresponding cgroup is
  empty while creation/migration is blocked. Verify the independently enforced
  no-delegation and exclusive-storage conditions. If a cgroup disappears,
  identities cannot be matched, the supervisor restarts without recoverable
  evidence, or any check is ambiguous, retain the pin and hold the node.
* **Recovery intent:** durably record an exact, authenticated quiescence
  attestation tied to the operation, reservation, pin, inventory and file
  identity. An arbitrary request flag or caller-provided PID is not an
  attestation. This step requires an actual protected supervisor and pinned
  trust configuration, neither of which is implemented yet.
* **Filesystem reconciliation:** under exclusive ownership, use directory FDs,
  no-follow checks and the recorded device/inode to inspect the exact partial.
  A complete published file must be hash-verified and file/directory-synced.
  An incomplete partial may be removed only under the proven exclusive writer
  boundary; sync its parent afterward. Unknown files, changed identities,
  unverified absence, mount changes or sync errors stay held.
* **Commit:** in one FULL-synchronous ScratchLedger transaction, recheck the
  original pin, gate, operation and attestation, retire only that pin and
  persist an immutable recovery receipt. A crash after filesystem sync but
  before this transaction must safely repeat the exact recovery; a crash after
  commit must replay the exact receipt without repeating deletion. Do not
  infer completion from a missing row, and never turn this local recovery into
  `sourceCustodyAccepted` or Viewer `fullyAccepted`.
* **Retry:** only after the exact retirement receipt is reconciled with the
  coordinator may it request the same object again from the still-retained,
  authenticated Viewer source. Any changed generation, manifest, worker
  instance, source hold or authority requires a new validated handoff.

## Work needed before use

Implement the protected supervisor and worker bootstrap, exact invocation and
FD-containment proof, durable operation/recovery schema, atomic ScratchLedger
pin retirement, authenticated coordinator integration, and source retention
checks. Qualify process death during real partial writes, delegated-FD attempts,
unit/cgroup recreation, supervisor reboot, coordinator outage, changed worker
instance, crash before/after fsync and DB commit, duplicate recovery and full
Viewer retry. Test disk/WAL overhead and batch performance separately; no
10,000-photo throughput or cleanup safety is established by the fixture.

The owner performs any required privileged setup on the main machine. This
design makes no host changes or live deployment.
