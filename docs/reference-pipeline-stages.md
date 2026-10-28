# Sequential ODM and original-LiDAR stage foundation

`processing/reference_pipeline.py` supplies the **unintegrated**, Linux-only
`reference-sequential-v1` stage journal. It is not a dispatcher, engine adapter,
completion sender, capability flag or installed service. The current production
driver and router still reject combined jobs. TreeDar and Gaussian capability
advertisement are unchanged; Gaussian splats remain disabled.

The intended combined job has one existing parent submission, Queue claim,
scratch reservation and physical node fence. ODM consumes its original image
objects first. TreeDar then consumes the one separately authenticated **original
LiDAR object** from that same submission. ODM reconstruction output cannot be
relabelled LiDAR or passed as acquisition evidence. No child job, rewritten
manifest or replacement pipeline digest is created.

## Binding and trusted interfaces

`PipelinePolicy` fixes the main node, worker and instance, coordinator authority
and origin, Docker Engine, gate generation, both engine versions and runtime
digests, provenance-policy digest, allowed environments and per-stage byte
budgets. This first foundation accepts image objects plus exactly one LiDAR
object, with both ODM and TreeDar requested. The complete normalized submission,
plan and parent identity persist before stage work. The claim token is hashed;
its plaintext is not written into this journal.

`bind_parent()` also requires the existing claim-request ID, scratch reservation
ID and physical-gate receipt ID. A trusted `ParentGuard` must have the exact
policy/physical binding and implement `check(parent_binding, mutation=...)`.
That check must independently verify the actual parent claim, exact reservation
and physical fence; it returns `None` or raises. A boolean is not accepted as
authorization. Read-only recovery may inspect a coordinator-restart-held parent,
but a new stage needs a currently authorized running claim.

Each trusted `IntermediateAdapter` has the journal's exact stage/version/runtime
binding and three methods:

- `start(intent)` makes at most one mutation attempt for that exact intent.
- `lookup(intent)` only reads durable evidence for that attempt. It must never
  launch, resume, finalize or remove an engine task as a side effect.
- `verify(intent, proof)` verifies the actual terminal engine identity and
  durable, reservation-owned output bytes; it returns `None` or raises.

These interfaces are trusted host code, not submission-controlled objects.
Binding dictionaries and proof hashes are consistency checks, not authentication.
The installed factory must bind the configured provenance verifier and its
complete policy to `provenance_policy_sha256`; the hash alone proves neither
acquisition nor verifier identity. The foundation reuses the existing exact
provenance validator with synthetic acquisition disabled. It authenticates the
original LiDAR receipt immediately before the TreeDar intent and again before
the start callback. The real TreeDar adapter must still validate LAS/LAZ geometry,
CRS, units, ground coverage and source-count assurance independently.

Guard checks bracket callbacks but do not hold a distributed execution lease
themselves. The real adapters must enforce the same live physical fence at the
mutation boundary, retain the reserved inputs and outputs, and never publish an
intermediate parent completion, release the global slot, settle scratch or
release the parent gate. Existing standalone ODM/TreeDar delivery adapters cannot
be substituted unchanged because they currently perform final parent delivery.

## Durable sequence and recovery

The parent states are `bound`, `executing`, `waiting_treedar`, `ready_final`,
`failed` and `cancelled`. Each fixed stage is `new`, `issued`, `succeeded`,
`failed` or `cancelled`. TreeDar cannot leave `new` until the ODM success proof
is durable. The second intent includes the exact preceding proof digest.

`advance()` commits a FULL SQLite `issued` intent before calling `start()` once.
The intent includes its UUID, complete parent binding, exact stage inputs and
runtime, provenance where applicable, plan hash and preceding proof hash.
An exception or missing reply does not authorize another start. Calling
`advance()` again on an issued stage fails closed; `reconcile()` calls only the
adapter's read-only lookup. A missing proof remains issued. An intent committed
before a crash but never sent remains held for explicit recovery too; the
foundation does not infer that a missing task permits a safe retry.

Terminal proof validation checks the exact JSON-typed parent/stage identities,
input IDs, runtime/version, terminal outcome, digest and existing result-object
schema. The stage boundary additionally enforces the actual Viewer v1 wire roles:
`pointcloud`, `mesh`, `orthophoto`, `dem`, `report`, `tree_inventory` and
`odm_archive`. ODM success must include exactly one positive-byte `odm_archive`;
TreeDar success must include none. Only `report` may have zero bytes. These checks
are stricter than the legacy general Python result validator and apply before a
stage proof can be committed. The final manifest therefore has the one archive
required by Viewer's `requestedEngines: ['odm', 'treedar']` binding.
Success objects use disjoint `odm-` and `treedar-` namespaces and must fit
their stage's data budget. Each intent additionally reserves half the parent
object-count allowance and an explicit UTF-8 metadata allowance, including space
for the other stage and final-manifest header. Thus a successful first stage
cannot consume the final closure's metadata capacity. Failed/cancelled proofs
contain no output manifest and stop the sequence.

Only verified proofs are committed. Stage and parent closure updates share one
transaction; losing its acknowledgement permits readback without rerunning the
engine. Reopening the journal preserves immutable policy, parent and intent
bindings. Private owner/mode/inode checks, an exclusive OS lease, process/fork
checks and an exact dedicated SQLite schema prevent accidental mixed ownership.
Foreign databases are refused without changing their bytes. Symlink paths are
rejected before opening journal files. The journal **never creates directories**:
its real UID-owned mode-0700 state root must already exist. The worker factory
must durably provision that root and all newly created ancestor entries, and
retain the state-root owner lease through construction and every stage callback.
Missing nested roots are rejected before any database, lock or directory is
created. Construction fsyncs the existing state root and its parent before it
returns, publishing its database/lock entries; this does not replace the factory's
durable ancestor-provisioning responsibility. Factory root publication remains a
separate integration qualification gate.

## Final closure is not downstream acceptance

After both stages succeed, `final_result()` returns one existing v1 result
manifest with the original parent job/generation/environment/pipeline digest,
both engine versions and both sets of namespaced objects. Before that point it
raises. On failure, `terminal_failure()` returns the existing bound failure
record; it never fabricates partial success.

`ready_final` proves an internal metadata closure only. This module has no Queue
completion, outbox, reader-pin, ACK, gate-release or cleanup API. The outer worker
must retain the global slot through final engine completion registration, offer
the final manifest once through the existing durable outbox, verify the exact
coordinator metadata receipt, and follow the existing Viewer acceptance and
source-retention lifecycles. Downloads, stage success and this closure never
authorize file deletion or release of retained originals.

## Synthetic qualification and remaining integration

The focused suite uses actual Queue/ReferenceStore, ScratchLedger, NodeGate and
SQLite journals, with synthetic engine callbacks and synthetic metadata labelled
as acquired only by a test verifier. It exercises ordering and immutable parent
selection, one final wire closure, global exclusivity, lost mutation replies and
commit acknowledgements, restart-held reconciliation, failures/cancellation,
provenance revocation/relabel refusal, guard loss, strict proof types, combined
metadata budgets, required archive/known-role/zero-byte rules, filesystem
ownership, foreign databases and fork/lease refusal. The maximum-object combined
fixture contains one positive ODM archive and only valid report objects.
It also checks missing-root refusal without directory or file creation, and real
fsync calls for the preexisting root and parent before constructor return.
It does not establish genuine acquisition, LiDAR geometry, real processing,
installed runtime readiness or NAS hardware durability.

Remaining work includes real intermediate ODM/TreeDar adapters, driver/router
composition and physical-fence handoff, authentic NAS provenance issuance,
enforced source retention, final result streaming/ACK integration and installed
failure/recovery qualification. Combined processing must remain unadvertised
until those paths are implemented and accepted.

## Paired Viewer wire probe

The Python tests alone cannot establish compatibility with Viewer's stricter
result policy. Emit an actual journal closure in the isolated Linux test
environment with `python -B -m tests.emit_reference_pipeline_result`. Save its
synthetic JSON output and validate it on a Node 24 machine:

```text
node tests/reference_pipeline_wire_probe.cjs combined-fixture.json /trusted/referenceProtocol.cjs 17d505c268b53ff5e17c61bd6f89fff8081b9089d35ca00cef6d96906d934159
```

The trusted module is the actual Viewer `server/referenceProtocol.js` from
Reference commit `867dd3f26c16203b5fdceec3af1671846ffa208c`, saved as CommonJS.
The probe checks its canonical LF source hash before loading it; it does not
download code or contact Viewer. Its expected identity comes from the original
synthetic Queue claim, with both engines requested. It verifies the real combined
manifest, then rehashes five invalid variants and requires rejection at the
specific archive, byte-size, role or engine-version boundary. It changes neither
repository nor runtime and proves no archive geometry or ingestion acceptance.
