# First-cutover exact projection comparator

`ops/coordinator_candidate_projection.py` is a read-only comparator for a full
693 custody baseline and an independently reviewed candidate transformation
policy. It never constructs Queue/SourceSpool, acquires any copy's lease, repairs
SQLite, restores a database, or authorizes a cutover by itself.

The exact callable is `verify_projection(root, *, baseline, policy_bytes,
expected_policy_sha256, expected_baseline_sha256, key, phase, gate_binding, uid,
max_metadata_bytes=64*1024*1024, max_artifact_bytes=1024**4,
max_duration_seconds=300, reviewed_schema_bytes=None)`. Phases are `bootstrap-frozen`, `final-frozen`, `rollback-bootstrap`, and `rollback-final`. It returns bounded counts/digests and explicitly reports
`writerExclusionProven=false`. The concrete transport must separately establish
original external/main lock ownership and authenticate the gate observations.

The baseline must be full retained custody, with its immutable digest. A metadata
probe is refused. Full predecessor census now also records every retained
relative directory as a keyed path ID and exact mode; no paths are disclosed in
that inventory. Candidate verification preserves all 19 original table columns,
rows, callbacks and history, all retained files (excluding changed SQLite bytes),
and every directory mode. Only the exact additive nullable
`nodes.target_engine_version` column is supported. Added payload/operation pins
must remain empty before first admission, even if a policy attempts to declare
nonempty rows. Exact new schema objects, all additional startup/control rows,
files and directory modes must match the reviewed declaration.

The policy is closed JSON with fields `schemaVersion`, `profile`,
`candidateImageId`, `candidateSourceCommit`, `oldColumns`,
`additionalColumns`, `bindingNames`, and `phases`. `objects` is the exact ordered
`sqlite_master` representation, including indexes and triggers (therefore an
undeclared object is refused). Each phase declares exactly `rows`, `files`, and
`directories`, plus the exact `objects` for that phase. Bootstrap and final
constructors add different reviewed tables; rollback selects its exact journal
proven last-started phase, never an arbitrary schema union. Every added table is represented, including empty tables.
Templates support only explicit named binding replacement, canonical JSON, and
SHA-256 of canonical JSON. Bindings must come from independent authenticated
configuration/physical/gate evidence, not the rows under comparison.

No production declaration or policy generator is qualified in this component.
A caller cannot turn an observed database into its own expected policy. That
would only prove self-consistency, and is specifically outside this contract.
The concrete transport remains held until a separately reviewed declaration
binds exact candidate source/schema, immutable deployment configuration, genuine
freeze receipts, startup epochs and original physical authority identities.
The helper deliberately offers no default policy or schema discovery mode.

Bounds precede live database opening and materialization: coarse physical limit,
SQLite LENGTH/schema limits, scalar row/cell-length scans and aggregate row/byte
budgets, then full projected values/integrity checks. File hashing uses owned,
no-follow descriptors with before/after physical identity checks, cooperative
duration/byte bounds and a bounded path count. No SQLite sidecars are recovered
or removed. Kernel I/O may still block beyond cooperative deadlines; the
transport must use an outer owned-process deadline and retain unknown outcomes.

`freeze_expectations` independently reconstructs the three exact canonical
freeze requests/receipts and four control-table row sets from the original
transport/configuration identities. It verifies UUIDs, owner/authority binding,
three distinct requests, the closed-original/current epoch relationship and the
bounded reviewed remaining reservation. It never reads a database. The normal
application quarantine fixture now compares actual persisted rows to this
reconstruction, using authenticated RPC identities and independently statted
original state/lease identities.

Validation:77/77 Linux tests passed, no skips, in3.976 seconds:41 predecessor
census,14 projection/reconstruction,21 journal and1 actual normal application
quarantine/restart. The latter uses real TLS and operator RPCs, clean child
process stops, genuine original leases and independently reconstructed control
rows. The comparator fault cases use synthetic exact declarations. This proves
neither an independently reviewed production transformation artifact nor actual
Docker recreation/backup/installed handover. The first run's one stale rollback
test index was corrected after adding the holder phase; imported fixture test
discovery was also deduplicated. The final run was green and its owned container
was auto-removed, with exact label inventory empty.

Command: `docker run --rm --name ltds-first-cutover-projection-20261105
--label ltds.task=first-cutover-projection --network none --cpus 1 --memory 768m
--read-only --user 10001:10001 --init --tmpfs /tmp:rw,nosuid,nodev,mode=1777,size=128m
-e PYTHONDONTWRITEBYTECODE=1 -v <isolated-worktree>:/workspace:ro -w /workspace
--entrypoint python sha256:57f01267c88439d872f6782b54511e314614a76145e32cfe2fad003e5ca0fac8
-m unittest tests.test_coordinator_predecessor_census
tests.test_coordinator_candidate_projection tests.test_enrolled_coordinator_cutover
tests.test_first_coordinator_quarantine -q`.


The optional in-memory `reviewed_schema_bytes` is still pinned to the canonical
693 schema hash and capped at64 KiB. It supports serialized helper execution
through existing container stdin without installing ops/contracts files into
that image or writing any tool into live state. Absence uses the reviewed
repository artifact; supplying bytes does not relax schema validation.
