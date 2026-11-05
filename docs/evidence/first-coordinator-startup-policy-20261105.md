# Independently generated first-cutover startup policy

The first-cutover policy now comes from `build_startup_policy` in
`ops/coordinator_startup_policy.py`, not a scan of candidate SQLite. The builder
checks an independently reviewed source-hash map against exact candidate files,
extracts only known literal CREATE statements through AST, and applies the known
constructor groups to the pinned693 schema in private in-memory SQLite. It never
constructs Queue, SourceReaders, SourceSpool, or a runtime and never opens a
candidate/state database. The resulting policy pins image/source, source hashes,
full private configuration hashes, and selected non-secret inventory/recovery
configuration. Bearers and other private config values are not included.

The supported first profile is the exact two-node enrolled693 upgrade: ordinary
schema2 bootstrap and schema6 final config, unchanged workers/recovery binding,
empty legacy push targets and source release disabled. Enabling SourceRelease is
explicitly refused rather than permitting unreviewed background/schema effects.
Bootstrap declares31 exact tables; final declares35 and the one reader index.
The only added legacy column is nullable nodes.target_engine_version. Unknown
schema objects, changed original columns/rows/files/directories, and any new
source/execution/release/pin payload remain held. This is not the generic future
V2-to-V2 updater profile.

The exact runtime context is closed to these10 fields:

- bootstrapStartupId, currentStartupId, planId, ownerSha256, requestIds
- operationBinding, sourceReaderBinding, sourceCustody
- bootstrapWindow, finalWindow

Gate startup/plan/request identities come from authenticated operator calls and
the durable plan. Operation/source physical bindings come from independent
original descriptor/path/marker metadata, not stored database rows. The new
operator `/v1/prototype/coordinator-custody` response correlates the actual
existing runtime source-reader epoch and binding digest with its real Queue,
process, registered session and held lease. The verifier requires that digest
match the independently reconstructed physical binding. A stored epoch alone is
never accepted. The transport must also verify actual PID/start and lease
ownership and bind its private observation artifact to the pending intent.

Expected freeze rows are reconstructed from the actual transition protocol:
three exact request/receipt bodies, same owner/plan, bootstrap epoch closed when
appropriate and final epoch active/closed for its exact phase. Reserved control
bytes are computed from the reviewed startup charge, not observed as an expected
value. A conservative independently measured metadata margin proves SourceReader
startup does not need the optional full-quota loan in this first profile; if the
margin or reserved remainder is unproven, verification refuses.

Inventory/recovery bytes and hashes are generated from configuration. The one
SQL-generated inventory timestamp is treated only as bounded metadata: canonical
SQLite UTC seconds within the independently recorded bootstrap start/readiness
window, at most120 seconds. All other row fields remain exact. No timestamp
selects an owner or authorizes work. Final reader binding/epoch rows are generated
from independent physical proof plus the authenticated live runtime observation;
pins/fences must be empty. The new local reader lease bytes/mode and directory
mode are checked against the independently read marker and exact keyed digest.

`verify_projection` uses the generated phase rather than arbitrary row
placeholders when the closed startup specification is present. The immutable
policy file SHA must match the journal plan before activation; its configuration
hashes/image/source must independently match the concrete approved recipes. The
comparator does not itself authenticate a caller or prove writer exclusion.
Default absence of a policy or required evidence remains a hold.

Validation:51/51 Linux tests passed, no skips, in2.751 seconds (journal31,
policy-generation5, projection14, actual normal-service quarantine1). The actual
normal `python -m processing.app` child uses real TLS/operator RPCs and original
state/external leases. A full baseline is captured before startup; the policy is
generated before candidate launch. After clean bootstrap stop it validates both
bootstrap and rollback-bootstrap projections. The same exact source/image
boundary restarts frozen with schema6 and real task-owned tmpfs source config;
authenticated runtime custody plus independent local marker stat/hash qualifies
final-frozen, and clean stop qualifies rollback-final. All legacy row/file bytes
remain unchanged, no source jobs are admitted, and the source spool is empty.
No backend processing, installed PC handover, NAS operation, publication or
actual Docker recreation is claimed by this process-level qualification.

The single container used1CPU/768MiB, read-only source/image, no network,
UID/GID10001, --init and128MiB tmpfs. It exited0, auto-removed, and exact task-label
inventory was empty. Two real journal child-death cases and dispatch-rejection
replay checks are included in that run; concrete daemon transport proof remains
owned by the separate driver qualification.


## Remaining full-goal enablement

SourceRelease-disabled first cutover is a staging boundary, not a permanently
narrower release. After original custody/quarantine/final activation is qualified,
the full goal still requires a separately controlled configuration/schema
transition enabling SourceRelease, its actual installed runtime acceptance,
signed Viewer-to-backend delivery/retention tests and cleanup observation against
the operator-qualified NAS boundary. That later transition must declare the three
reviewed release tables and prove its timers/IO honor the same central freeze;
this first-profile builder deliberately cannot silently authorize it.
