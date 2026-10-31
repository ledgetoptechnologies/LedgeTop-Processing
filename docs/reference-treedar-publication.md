# Local native TreeDar bundle publication

`reference_treedar_publication.NativeBundlePublication` is an inactive local
publication component. It publishes the already executed TreeDar native outputs
as the versioned draft bundle, under the original combined parent. It does not
select an outer transport role, create a final pipeline result, settle a parent,
release its global execution fence, acknowledge Viewer delivery, clean up source
data, or start an engine. Installation and routing remain separate work.

The constructor requires the actual `TreeDarNativeStage` and its concrete
`ProductionParentGuard`, original `PipelineJournal`, `ScratchLedger` and
`DaemonStateLock`. A configuration hash or caller boolean cannot substitute for
those objects. Configuration, SQL schema, private root/file identities, process
identity and the daemon lease are checked throughout. The publication journal
and its nonblocking OS flock are dedicated to this component. The real daemon
lease and publication flock exclude the old writer before any recovery copy.

## Native success qualification

`begin(intent)` verifies the exact original issued TreeDar intent, preceding ODM
proof, live original parent claim and physical fence. It rechecks all original
photo inputs and original LiDAR bytes, authenticates the original LiDAR receipt,
and uses the actual ODM controller to verify its retained terminal archive.
It freshly inspects the original TreeDar container's full pinned configuration,
mounted bootstrap source digest and exact exited/zero state. Generic zero exit
status and the stage's `exited_unvalidated` observation alone are insufficient.

The retained `proof.json` must match the stage's sealed evidence, closed bootstrap
schema, original job/generation/environment/manifest/pipeline identity and engine
version. The exact pinned control task must still match its original inputs,
provenance, policy and runtime configuration. Admission must be the genuine
original LiDAR acquisition, with matching receipt/file binding, metre units,
approved declared datum, bounded point/grid counts, ground count/coverage and
CRS/count assurance fields. Synthetic acquisition and `genuineAcquisition=false`
are rejected. Every native filename, role, format, size and SHA is checked against
the real source bytes; required outputs and both directory closures must match.

The scientific checks are performed by the pinned executed bootstrap: LAS
admission, finite raster windows, shared raster geometry/CRS and summary/native
manifest consistency. This host component verifies that execution's exact proof
and bytes; its synthetic unit tests do not constitute genuine sensor evidence.
Current pinned bootstrap outputs are flat `run/` files. Optional nested classify
output support is a separate native-stage/bootstrap integration gate.

## Durable file protocol and budgets

The SQLite rollback journal uses `synchronous=FULL`. The original publication
intent and numbered copy plan commit before creation of the private output/state
directories or partial. The exclusively created 0600 partial has a recorded
device/inode. The existing bundle writer's exact durable intent is persisted
before streaming. Every copy has new deterministic paths and identity under the
same original parent and manifest; existing files are never truncated or replaced.

Whole-parent allocation retains original inputs, preceding ODM bytes, native
RAW, native proof/control/evidence and prior copy bytes. Each copy additionally
charges a conservative immutable control allowance covering twice the entire
shared publication DB size (including rollback coexistence). The database page
count is bounded before commit. The bundle writer then checks its separate
archive/manifest/intent/control coexistence allowance against the reservation's
actual remaining bytes. Observed partial/control sizes monotonically increase
allocation after success or failure. A second hardlink names the same inode and
does not consume a second archive's byte allowance.

The first control allowance is charged before the original source/publication
row is inserted; later copies charge another allowance. A lost acknowledgment
after that row but before a copy plan can resume its first copy. Every retained
copy is censused on recovery; an abandoned incomplete partial is pinned by size
and SHA before a new copy and rechecked on subsequent recoveries. Recovery ZIP
parsing first checks bounded EOCD/ZIP64 records and the exact known central
directory names/count/size, before `ZipFile` may allocate its member list.

After ZIP closure, the local receipt commits before a `link_pending` checkpoint.
Publication uses an exclusive hardlink to the final name, file/directory fsync,
then `linked` and `unlink_pending` checkpoints before removing only the redundant
partial name. The final directory is fsynced before `published` commits. RAW,
native proof, original inputs and final archive remain retained independently of
any downstream availability or acknowledgment.

## Recovery and bounded preparation quarantine

`reconcile(job)` requalifies original native evidence and obtains fresh parent
authorization. A complete partial is verified against the original native
manifest, ZIP metadata/CRC/object bytes and recorded inode; it can recover a lost
receipt without copying. An existing link or completed unlink is reconciled by
that same inode and durable phase, with no repeated engine action.

A structurally unfinished local ZIP in a recorded `streaming` attempt may be
retained and followed by a new numbered copy. The old writer must be inactive
under the actual daemon/publication leases. All prior artifacts remain accounted
and retained. The original terminal RAW/proof is verified again, and a fresh
copy must fit the remaining whole-parent reservation. A fixed cap of at most
three attempts holds repeated I/O failures; insufficient capacity holds too.
A complete archive with corrupt metadata, body or CRC is held rather than
treated as a transient interrupted copy. Unknown files, changed inodes, lost
authority or changed source evidence also hold.

Early `planned`/`owned` crashes use a separate retained quarantine protocol. Each
new durable plan pins the real private reservation-parent inode and verifies
initial absence of both dedicated preparation paths. The empty partial and its
directory are fsynced before the `owned` inode checkpoint. Recovery holds both
real writer leases and freshly verifies parent/native authority. It accepts only
the authorized private directories and single-link owned 0600 filenames; the
partial must be empty. A recorded partial inode must still match. A bounded
interrupted intent may exist only after the owned checkpoint; its bytes are
opaque retained metadata, never publication authority.

Recovery fsyncs and commits an `abandoned_preparation` receipt containing exact
present/absent paths, directory/file identities, sizes and SHA hashes. It charges
the whole old copy allowance conservatively, then starts a new numbered copy.
It never adopts the unrecorded partial as a result, overwrites it, deletes it, or
restarts the engine. Every future recovery rechecks the immutable abandoned
census, including prior absences. Unknown entries, unsafe links/modes, changed
recorded inodes, later appearance/disappearance, nonempty early partials and
legacy plans lacking the initial-absence/parent binding hold. Repeated creation
failures and insufficient remaining capacity are bounded by the same attempt
cap and retained-capacity rules.

`read_receipt(job)` is historical SQL-only observation. It performs no Docker,
network authorization, source revalidation or publication mutation. A historical
receipt does not authorize resumed execution or assert current Viewer acceptance.

## Tests

Run `python -B -W error::ResourceWarning -m unittest
tests.test_reference_treedar_publication` on Linux. Tests use the actual TLS worker
API, Queue/claims, private driver token source, ProductionParentGuard, ledger,
pipeline journal, physical fence/daemon/publication OS leases and bundle ZIP APIs.
Docker transport and native admission/output payloads are synthetic fixtures.
Tests cover checkpoint/reply loss, retained interrupted copies, caps/capacity,
archive corruption, unknown artifacts, owner/PID drift and SQL-only readback.
Preparation crash tests inject loss before/after each directory, after actual
partial creation, after the owned checkpoint, during intent writing and before
the streaming checkpoint; they assert new copy identity and retained old bytes.
No production container, NAS dataset or real acquisition is modified.
