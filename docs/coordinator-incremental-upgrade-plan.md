# Incremental coordinator upgrade: 671→6bb

This is a review plan and source-only validation component, not an activation
command. No remote Docker, maintenance, configuration, systemd or NAS mutation
has occurred. The old strict updater and gate bridge remain unchanged.

## Read-only observed predecessor

October28 observations confirmed coordinator container
`450217abc4942e6152d5462be7d2f99a356ccf2a5b393800772070fd5fd88278`,
source `671fd2a6bde40088bdfad91ccf67eab45ee3d8d7`, image
`sha256:649f53f2b5dc32cd9eb229c42b6bbd7b479249fba5a8d65c48d8afc4f157b249`,
healthy with zero restarts, started `2026-10-28T15:04:54.798609898Z`.
Authority remains `7131882e-38ce-4d3e-99cc-4ba5a94d2ee8`.

The existing state volume is
`ledgetop-processing-prototype-c3fcb3a-state` at `/prototype-state`; private TLS
files remain a read-only bind from the existing `reference-10a3848/coordinator-tls`
directory. Native endpoints remain loopback18080 and `192.168.50.89:3005`;
reference TLS remains `192.168.50.89:18443`. User remains10001:10001 and restart
policy remains unless-stopped. No environment values or credentials are recorded
here. Private configuration is schema1 with no recoveryBindings; its SHA256 is
`b46754c8e02fa95ca1f5696bddd4f0a5b5ee591d52a899ff5c877dab329b059d`.

The database quick check passes:12 terminal historical jobs,10 succeeded and2
failed, no active jobs, all Viewer/reference tables empty, and **two** existing
owned maintenance schedule rows. Public history SHA256 remains
`2ad24fe66eaab476299534c64ba89acbf236bd33d959489c1bcf4c7ecc6fcf9f`.
Exact logical jobs-row SHA256 is
`c44eafa43d067160a06bace84961613fa4a04cbb2177ec20491a83e40d105759`;
owned-row SHA256 is
`eae2ceaeaec56669e764e5b68101593d8b696c751c35a1e00c49ff92b403ad91`;
schema SHA256 is
`1f4bc730fb0242e380f6eaeef1a2b197bd883c17377c082a87a18238c4f2b80e`.
Those hashes describe the bounded read-only census, not an execution grant.

Both nodes were active (cluster revision9, main18), generation1 physical gates
unreserved, with zero backend queued/retained tasks, no upload activity or
processing children. Current cluster backend is container `7f4723957ad215a52b5f69cdbdc32b323b914e48d62783914974f67b97d28611`,
image `sha256:5ce31be03711d280e7f97ee3820dd982b68227181165b3f430b037447c3d13b6`,
ODM3.6.2. Main is container `a817f90f25de309569b07fad98a4ee36907c1e374c702bf2cf2c96d3e6086e69`,
image `sha256:b9d9a1a4603db9f092b790a6e58586fcf944758b7ca1149daefb2bd55dc6bdde`,
ODX3.8.5. These identities must be refreshed before any mutation. Both image
updaters still use source020c15c; their directories are not repointed. Legacy
ClusterODX remains stopped.

## Actual blockers and bounded component

`update_reference_coordinator.py` binds predecessor7ebe/e2cc and its census
requires the owned maintenance table empty. `coordinator_gate_bridge.py` binds
obsolete main container78be. Replaying those recipes or patching their globals
is refused. Neither an empty-table exception nor ignoring the entire owned
table is an acceptable migration.

`ops.incremental_coordinator_binding` supplies explicit immutable predecessor,
backend, source, environment/config and logical-history identities. Its
`checkpoint_identity()` exposes data for a future explicit profile in the
existing updater; it contains no Docker/network/activation entry point.
The bounded census uses read-only files/SQL, validates historical request and
receipt bytes/checksums, and preserves complete prior owned rows. It rejects
unknown schema, active jobs, nonempty native directories, unexpected files,
oversized state and unqualified history. Before any SQLite open it requires the
physical4096-byte-page DELETE header (read/write versions1/1); a clean closed WAL
header is refused without permitting SQLite mode=ro to create WAL/SHM sidecars.
The exact two-row initial history is
bound separately from nodes and future operation rows.

`compare_transition` accepts at most two exact operation-owned schedule rows,
each matching the fixed node image and original revision CAS. Existing rows must
remain byte-identical. A cancel/reopen needs the exact saved node/plan/revision
intent; a target-image match or active row alone is insufficient. All other
schema, history and state remain compared. Read-only observations are not live
gate possession, durable mutation intents or authorization to replay requests.

The new component is not yet wired to an incremental executor or bridge with
current immutable backend identities. That source integration, independent
review and qualification are required before remote preparation/activation.
No NAS dependency has been identified for this coordinator-only upgrade.

## Required reviewed execution sequence

1. Create a new private operation/checkpoint ABI that explicitly selects the
   incremental binding. Refresh exact coordinator, backend, source/image/module,
   environment, TLS/config, complete history, nodes, volume ownership and gate
   identities. Require the same sole running volume owner and both idle native
   backends. Keep schema1 configuration, credentials, ports/mounts and updater
   directories unchanged. No jobs, grants, worker activation or fake readiness.
2. Acquire both real host gate leases through the reviewed challenge bridge
   bound to the current immutable backend containers/images. Before each
   distinct mutation recheck the physical proof, original history and actual
   node revisions. FULL-save each stable owned schedule request before POST to
   the installed `/schedule-owned` API. A lost acknowledgement permits exact
   receipt lookup and full request/response/CAS reconciliation, never a new
   request or guessed ownership. Unknown/conflicting state holds.
3. Once both own plans are acknowledged draining, prove no queue, upload,
   process, Viewer transfer or reference work remains. Save the exact drained
   census with original owned rows plus only the two operation rows. Stop only
   the original coordinator with graceful shutdown. Confirm its process exited
   and **no** running container owns the volume before taking an offline backup.
4. Copy/hash/fsync every existing state file into a new private backup, verifying
   raw file hashes plus full logical/schema/owned-history census and unchanged
   TLS/config. Do not restore, delete, reset or purge anything. Retain the exact
   predecessor container/image stopped under a unique rollback name. Each
   stop/rename/create/start intent must be saved before execution; an unknown
   response requires identity inspection, not blind replay.
5. Create/start the reviewed6bb image with the exact existing recipe, state
   volume, read-only TLS mount, environment/credentials and ports. Re-read the
   actual loaded image/module bytes and actual sole owner. Local Docker Desktop
   image identity must not be assumed portable after export/load: qualify the
   actual remote immutable image and source bytes before use. Keep reference
   workers, schema2 recovery bindings and constructor-only combined execution
   disabled. Do not change main-node compute limits or backend containers.
6. Verify real native/reference listener health, endpoint identity, dynamic
   options, authenticated role boundaries, unchanged12-job history, exact
   schema1 state and operation-only draining transitions. Verify private config,
   TLS and environment digests, both native image/gate identities, and stopped
   legacy ClusterODX. A health200 alone is insufficient.
7. FULL-save each exact own cancel intent before reopening that node. Compare
   current plan/revision/image and both live gates again. Reopen only the two
   acknowledged operation plans; peer changes or uncertain ownership hold.
   Verify exact active revision increments, unchanged prior owned receipts,
   sole coordinator, endpoints, original history and unchanged updater paths.

Rollback is separately invoked and guarded. Before reopening admission, stop a
failed candidate only after proving its exact operation identity; retain it for
inspection and restart only the exact predecessor over the same verified volume
and configuration. The old671 Queue cold-opens the qualified schema1 state,
including preserved/new owned receipts, in local tests. This is not permission
to restore a stale backup. After new work/schema/ownership changes, rollback
requires fresh drain and compatibility review. Worker recovery-control ledger
ABI/downgrade rules remain separate and are not qualified by this coordinator
plan. No automatic old-process concurrency or database repair is allowed.

## Local candidate and qualification

The exact frozen6bb archive (`5b36d3f1dc9bd05475a3e9b5b7440f53a89f3e6e92e89a76d307bb8dbe7b7b75`)
built locally with the unchanged Dockerfile and exact public base
`python:3.13-slim-bookworm@sha256:e32be0e55814a08977501ca863271d9bd02443710db8f2afaf8372e2c4508530`.
The local candidate is
`sha256:e96d957f3eff3f6a33fa673fa9f8ce211d82969c3724d16ba24721f6b376f42b`,
Linuxamd64/user10001:10001. All63 in-image Python files match the frozen archive;
their canonical map digest is
`7d3a0ad233f1e5fc65fdc059de0df89842c98078096e6205e336b33f3cce3fce`.
Critical imports resolve under `/app/processing`, not a mounted source tree.
Root independently verified this image without bind mounts or live config.

The actual candidate image passed22 reference-runtime/startup/schema1/shutdown
tests with read-only frozen tests, networknone and no live configuration. The new
incremental tests use exact SHA-pinned671 Queue/Maintenance source fixtures,
representative synthetic12-row terminal history plus two real owned schedule
API receipts, current Queue reopen, exact drain/cancel transitions, predecessor
cold rollback reopen and tamper/refusal cases. They create no remote jobs or
containers. The representative schema digest is explicitly asserted equal to
the read-only installed schema digest above. These are Queue/Maintenance
reopen tests, not a full old-image/app/private-config migration qualification.
Full application startup over an isolated qualified state copy and rollback
closure remain execution-profile gates. None of this is installed acceptance
or permission to activate.
