# Native TreeDar attempt controller

`processing.reference_treedar_stage.TreeDarNativeStage` is an inactive,
source-only engine lifecycle foundation. It exposes `begin`, `tick` and
`read_evidence`. It deliberately has no `start`, `lookup` or `verify` methods
matching the PipelineJournal v1 adapter contract. It registers no capability,
factory, daemon, container deployment or endpoint.

## Preserved parent and original inputs

Construction requires the actual PipelineJournal, ScratchLedger,
DaemonStateLock and concrete NativeODMStage for the same parent/state owner.
The durable private state root must already exist. Dedicated rollback SQLite
state and a private OS lock bind root/file identities, exact schema, policy and
runtime SHA. A process change, released owner, state replacement or changed
dependency holds the controller. Construction never provisions a parent gate.

`begin(intent)` accepts only the exact already-issued TreeDar intent in the real
parent journal, with successful preceding ODM proof and original combined
submission. It reconstructs the original plan and verifies the preceding proof
through the concrete NativeODMStage, including its real runtime and retained
archive. Neither a configured hash nor a persisted success label substitutes for
that verification. No child submission or replacement manifest is produced.

The bound input resolver selects exactly the original LiDAR object, verifies its
owned path and original byte count/SHA, and retains original parent identifiers,
generation, environment, manifest SHA and pipeline digest in the bootstrap task.
The bound trusted provenance reader must read the current receipt, which must
match the issued receipt and independently pass the journal's authenticated
provenance verifier. Reader equality alone does not authorize provenance.
Synthetic acquisition is disabled in the task. The caller must supply qualified
readers from a future trusted factory; arbitrary submission callbacks are not a
supported configuration source.

All parent input bytes, the retained ODM archive and the already-started parent
scratch reservation remain allocated. TreeDar adopts the same physical gate
receipt. It never reserves another slot, registers parent results, prepares
completion, creates an outbox, settles a parent, releases a gate or deletes
artifacts. ParentGuard runs outside NodeGate so a real guard can take that OS
lease itself. Immediately before each Docker mutation, owner, physical receipt,
reservation, Engine/image and installed sources are checked again under the
physical gate lease. Provenance and fresh authorization are repeated after
potentially lengthy input/prior verification and before either mutation.

## One attempt and retained native evidence

The private journal FULL-commits preparation before filesystem publication,
`create_pending` before Docker create and `start_pending` before Docker start.
Preparation, create and start are never replayed, including after a missing reply
or checkpoint. `begin` refuses every existing attempt. `tick` can inspect a
container discovered by its exact deterministic name, but never starts a
discovered created container. Unknown outcomes remain retained for explicit
reconciliation. No stop, remove, pull or cancellation operation exists here.

The cached immutable image launches the existing fixed
`processing.treedar_engine --inside` bootstrap. Every executed processing import
in its SOURCE_FILES list is SHA-pinned and individually mounted read-only; the
container receives only original input, output directory, control file and those
fixed sources. It receives no Docker socket, worker credentials or network.
The old standalone engine's start/poll/collection/settlement paths are unused.

Inspection checks actual Engine, CID, image ID, user, entrypoint, command,
environment, working directory, disabled network, read-only root, dropped
capabilities, no privilege/device/socket additions, no restart policy, exact
input/output/control/source mounts, security option and temporary filesystem.
The binding label alone is insufficient. Image base environment must be pinned
explicitly in TreeStagePolicy; an unexpected inherited image environment holds.
Base image environment keys must be valid and unique. Expected container
environment uses Docker's key-based merge: the fixed Python settings override
matching base keys while all other pinned values remain. Duplicate actual keys
are rejected, including duplicates whose values happen to agree.
Inherited image labels are pinned separately as immutable validated key/value
pairs. The forced stage label overlays any matching base key; the full resulting
label map is checked exactly, so OCI source/revision labels remain pinned and
unconfigured additions or changed inherited values hold the controller.
No CPU, memory or process count limit is added to the processing container.
Existing LiDAR admission checks and file/disk bounds remain necessary.

Every tick counts growing native raw files, control and bootstrap proof against
the same parent reservation, even before exit. Accounting only increases and
never discards the retained ODM baseline. Exited containers allow bounded file
hashing and a sealed, versioned local execution-evidence checkpoint. Native exit
zero is still marked `exited_unvalidated`, not scientific success. Failed and
oversized outputs remain retained; over-budget observations hold rather than
delete or restart work. Observation is not a filesystem quota: trusted image
behavior, headroom and timely ticks must still be qualified before deployment.

`max_files` means native raw files under `run/` only. An optional single
`proof.json` has its own `native_proof_budget` allowance; it does not consume a
raw-file slot. The run directory itself is an enumeration entry, not an artifact.
Both pre-seal and SQL evidence validation use the same raw-count-plus-one-proof
bound, so 16 or 64 raw files plus their proof remain readable after restart.
The complete closed evidence validator runs before terminal publication and on
every SQL read. Docker exit codes must be integer values from 0 through 255;
booleans, negative and oversized codes cannot create a terminal checkpoint.
Raw file bytes are summed against `max_raw_bytes`. Both filesystem collection
and shared evidence validation apply `native_proof_budget` separately to the
proof, including when a valid proof is larger than the entire raw-byte cap.

`read_evidence(intent)` acquires only the local owner/operation lease and reads
sealed SQLite evidence. It performs no Docker command, guard call, ledger
transaction/write, pipeline advance or artifact cleanup. It does not authorize
execution or certify current files. A subsequent tick rechecks runtime and
artifact hashes; SQL evidence alone is historical evidence.

## Explicit integration gate

The current outer v1 protocol accepts a small legacy role set and cannot express
the native scientific TreeDar files. This controller therefore cannot produce a
successful v1 stage proof or final combined manifest. Its local evidence uses
`ltds-treedar-native-execution-v1` and `held_pending_outer_v2`. No reports are
fabricated to hide this mismatch.

The separately owned bundle builder is not called by this foundation. No partial
ZIP, archive publication, scientific bundle validation or Viewer acceptance is
claimed. Before that integration, add explicit durable archive/publication
intents and account for simultaneous retained ODM, original inputs, raw native
files, control/native evidence, partial ZIP, validation disk and final ZIP under
the same parent allocation. Never recycle raw bytes while a ZIP coexists or
delete them because an ACK is missing. Viewer owns the agreed outer-v2 delivery
contract and acceptance. Factory integration also needs a real intent-issuance
entry point that preserves this execution-only gate, qualified provenance/input
readers and image/source/environment pins.

## Synthetic verification

`tests.test_reference_treedar_stage` exercises real prior NativeODMStage over its
isolated HTTP fixture plus actual Queue, parent journal, ScratchLedger, NodeGate
and DaemonStateLock. Docker lifecycle/inspect responses are synthetic. Tiny
invented inputs and native file placeholders verify lifecycle and accounting;
they do not validate LiDAR geometry, actual TreeDar execution or acquisition.
Tests cover lost create/start replies, no replay, prior archive corruption,
strict intent/runtime/mount drift, provenance and owner changes, ongoing byte
accounting, retained failures and SQL-only evidence reads. No live node or client
dataset is involved.
