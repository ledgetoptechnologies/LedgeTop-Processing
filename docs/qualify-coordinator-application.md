# Isolated application qualification on the coordinator host

`ops.qualify_coordinator_application` is a separate qualifier, not an updater,
activator, backup restorer or NAS tool. It has no SSH interface. The implementation
has only been exercised with synthetic local state/configuration. Running it
against the installed private configuration remains a separately reviewed action.

It accepts a closed `QualificationBinding`: the reviewed `IncrementalBinding`
fields plus `candidate_image`, an exact expected local Docker `engine_id`, and
the exact existing private recipe paths below.
All identities/hashes must be refreshed and reviewed; matching a container name
or a health200 is insufficient.

- Environment file:
  `/home/bkoltz/.local/share/ledgetop-processing-prototype/viewer-ba01f37/runtime.env`
- Read-only TLS/config directory:
  `/home/bkoltz/.local/share/ledgetop-processing-reference/reference-10a3848/coordinator-tls`
- Container private config: `/run/processing-reference/runtime.json`
- Live state volume: `ledgetop-processing-prototype-c3fcb3a-state`

The source pair is explicitly671→6bb. Actual `/app/processing` raw image bytes
are verified before importing application-dependent helper code. The exact
49-file predecessor map is
`c5d141da519e4787e26fb2c01dfc6506d4834f730885c802a940c36d87ddbb77`;
the63-file candidate map is
`04a14d73f7f1652b82a61428be51f9dd751b9bc957094500f8eb776458387add`.
Flat file counts, regular files and per-file bytes are bounded; unexpected
directories/bytecode or code changes hold. Transported immutable image IDs must
be locally qualified, rather than assumed portable from Docker Desktop.

## Execution and retention

The on-host operator supplies a binding JSON file and a **new** operation directory
directly under `/home/bkoltz/.local/share/ledgetop-processing-qualifications`.
That dedicated parent must already be owned by the invoking user and mode0700,
with no symlink ancestors. The qualifier creates only its new child and private
0600 records; it does not provision or repair existing directories. The new child
entry and each intent/receipt are fsynced before progressing.

```text
python -m ops.qualify_coordinator_application --binding <reviewed-binding.json> --operation-dir <new-private-operation-path> --execute-isolated-qualification
```

This is a command shape, not approval to run the installed-config qualification.
Without the explicit execution flag it holds. An existing operation directory
also holds; no implicit resume, intent overwrite, retry, cleanup or volume reuse
is provided. Before each Docker mutation, a stable named intent is saved. A lost
reply/timeout leaves the named copy/container and evidence for exact review; it
does not authorize running the phase again. Source/configuration/ownership drift
holds. The final state is `qualified_copy_retained`; the copy is intentionally
retained for separate reviewed cleanup, never restored over live state.

The qualifier verifies the exact running predecessor container/image/user,
existing state/TLS mount recipe, declared application environment digest and
image defaults. Environment values remain in memory on that host. Docker reads
the existing environment file through `--env-file`; tokens are never put in
arguments, records or output. Docker's automatic per-container HOSTNAME is not
part of the declared application environment comparison. The helper receives
only the bounded application key names and independently hashes its actual
environment against the bound digest before accessing config/state. Host-file
inode/mode/size/mtime/ctime also remain exact, catching change-and-restore races.

Production Docker calls use only root-owned `/usr/bin/docker` with explicit
`--host unix:///run/docker.sock`. Binary/socket identities and the expected
engine ID are checked; context/host/TLS redirect environment overrides are
removed. A different engine, daemon restart or changed local endpoint holds.

Exactly two bounded reviewed helper sources are transferred over Docker stdin,
together with binding/baseline metadata. Their SHA256 values are pinned in the
launcher and operation intent and rechecked before each phase. No source tree,
operation directory or source file is bind-mounted. This works when the staged
source belongs to UID1000 with0700 directories/0600 files while the actual
application runs as10001:10001. No permissions are relaxed and no private
configuration/certificate/environment bytes are included in that stdin payload.

One uniquely labelled local disposable volume is initialized for UID10001.
The returned name is insufficient: an inspect receipt must prove the exact
ownership label, local driver/scope, absence of special options, mount identity
and creation identity. Those immutable fields are persisted and rechecked before
initialization and every later write phase. Foreign/replaced volumes hold.
The initializer also verifies the exact writable mount, a root-owned ordinary
non-symlink directory, and a bounded empty-directory observation before **any**
chmod/chown. Its held directory descriptor and visible identity must remain
exact; nonempty or already changed roots are retained without permission repair.
Only the snapshot helper sees the live volume, mounted **read-only** at `/source`.
It validates the exact source census (DELETE4096 header, SQLite integrity/schema,
12 terminal jobs, two exact owned receipts, active nodes, no Viewer/reference
work, bounded empty native directories). A real read-only SQLite backup creates
the disposable database; bounded auxiliary files and modes are copied to that
new volume. All new files/directories are fsynced. Exact source before/after
censuses must match. The copied logical state must match through the reviewed
row/CAS comparator; SQLite page-layout differences are not treated as history
changes. No writable live-volume mount exists.

The qualifier then runs actual predecessor, candidate and predecessor again
against only that copy. Each helper has networknone, no published ports,
read-only root,2CPU/2GiB/128PID **qualifier** limits, dropped capabilities and
bounded private `/tmp`. No main processing limits or backend containers change.
Docker logging is disabled for these helpers; bounded stdout is captured locally
as closed metadata receipts. Raw subprocess errors are never echoed.

Existing config/TLS are mounted at their exact original paths read-only. The
actual environment must still enable Viewer, disable photo smoke and use
schema1 without recovery bindings. Native/TLS binds must be wildcard or loopback;
the configuration is never rewritten to make a specific LAN address bind.
The TLS client validates the existing CA and127.0.0.1 certificate identity.
No TLS verification bypass is available. Different SAN/bind policy holds.

Real CoordinatorService, native/TLS listeners and the empty Viewer dispatcher
start. Read-only loopback requests check native health/authority and reference
authentication, correct Viewer status access and worker/Viewer role isolation.
They do not request discovery, submit/claim work, create grants, access a backend
or assert backend readiness. Optional recovery stays absent. Graceful shutdown
joins all listeners/dispatcher, closes sockets and proves the actual Queue lock
can be reacquired. Full logical state and config/TLS content/identity metadata
must match the original copy baseline after every lifecycle. Private bytes never
leave the host. Only approved hashes, booleans and operation identities are
returned by the outer CLI.

## Qualification evidence and remaining boundary

`tests.test_qualify_coordinator_application.QualifierTests` covers the command
recipe with a synthetic transport, durable intent ordering, strict helper result
types, unknown outcomes/no replay, path/mount/environment drift and private FD
bounds. Its SQLite cases perform an actual consistent backup and detect a real
concurrent synthetic source-node revision change; they are not backup mocks.

The separate opt-in `SyntheticPrivateHelperImageTests` drives the **actual stdin
launcher** in the two exact cached images. Its seed fixture is also streamed with
a separate bounded digest check: no test phase mounts a source tree or helper
file, and no source permissions are changed. It creates only new local synthetic
volumes/env/TLS, snapshots a read-only synthetic source and runs old→candidate→old
through the actual helper with no mounted source code. A second case rejects
altered helper bytes and oversized stdin before any private mounts, and a wrong
actual-environment digest before snapshot writes. A third actual-volume case
places a sentinel in an owned labelled root and proves initializer refusal leaves
root UID/GID/mode and sentinel content unchanged. Test volumes
are removed after completion. These tests do not read installed private paths.

Run the focused Linux tests normally. Run the separate image oracle explicitly
on the local Docker host with `PRIVATE_QUALIFIER_IMAGE_ORACLE=1`; its three cases
otherwise skip and must not be counted as default-suite coverage. Record exact
source pins, image IDs, actual commands and results separately.

Installed private-config/state-copy acceptance, any remote command, updater
activation, gate possession, backend health, scientific processing, sustained
history retirement and rollback after new production work remain unqualified
until their own reviewed operations. This component grants none of them.
