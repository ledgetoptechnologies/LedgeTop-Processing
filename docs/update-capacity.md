# Shared image-staging capacity

`processing.update_capacity` supplies a cooperative, Linux-only image-staging
budget for one physical Docker Engine and OS user. It performs bounded read-only
Docker/storage probes and private accounting writes. It never pulls, deletes,
prunes, deploys, clears a processing gate, or expires a reservation.

## API and fixed policy

```python
policy = CapacityPolicy(
    docker_executable="/usr/bin/docker",
    docker_endpoint="unix:///run/docker.sock",
    engine_id=reviewed_engine_id,
    docker_root_dir="/var/lib/docker",
    legacy_capacity_journals=tuple(sorted(reviewed_legacy_paths)),
)
capacity = ImageStagingCapacity(policy)
```

The reviewed Docker root must be verified for that actual engine before this
composition is enabled. The policy requires a free-space floor of at least
100 GiB and an exact 64 GiB staging reservation; the default probe deadline is
ten seconds. Paths, local socket, engine, floor, reservation and explicit legacy
inventory are immutable journal bindings. Optional `docker_probe` and
`disk_probe` constructor hooks are trusted test seams; production defaults use
the actual fixed Docker CLI and filesystem.

`canonical_capacity_path(engine_id)` is the passwd OS-user home followed by
`.local/state/LedgeTop-Processing/image-staging/sha256(engine_id)`. There is no
caller-selected journal, supervisor/channel namespace, XDG override or expiry.
Every cooperating process for that engine must use this book. Its root is 0700;
SQLite and the dedicated flock file are single-link, UID-owned 0600 files.
PID, path, directory/file device/inode, schema, mode and symlink checks fail
closed. Fork children reject inherited objects before acquiring their parent's
mutex. SQLite transactions are FULL, and new journal-directory entries are
fsynced. A separate OS flock serializes admission across independent processes.

The public methods are:

- `capacity(candidate, bytes) -> bool`: the existing registry admission callback.
- `release_staged(candidate, registry) -> bool`: verified transient release;
  `False` means no canonical reservation existed, after cached proof succeeds.
- `verify_legacy() -> None`: normal startup/tick verification, even when discovery
  is not due; it also checks fresh physical storage identity.
- `reconcile_legacy(path, registry) -> True`: explicit bootstrap closure only.

## Admission and release

Admission first obtains fresh bounded `docker info` fields for EngineID and
DockerRootDir. The actual root must be a real, unchanged directory. `statvfs`
available-to-user bytes and filesystem/device/inode identity are checked on that
path, rather than on a caller-selected nearby mount. The frozen identity rejects
a remount, replaced root, engine change, or new Docker storage location.

Inside the shared FULL transaction, admission requires:

`freeBytes >= floor + sum(all active reservations) + newReservation`

An already-active immutable target has `newReservation=0`, so retries do not
double-charge it. Fresh free-space loss can still deny an active retry. A denial
returns exact `False`; unavailable/invalid evidence raises a bounded error and
never authorizes a pull. Reservations are keyed by repository, immutable manifest,
config, platform and physical engine, not tag, candidate observation or service
instance. A changed index observation cannot duplicate the budget or bypass an
unknown pull's retained reservation.

An interrupted/partial/unknown pull retains the full reservation across restart.
Allocated bytes already reduce actual filesystem free space; retaining the full
transient budget during uncertainty is deliberately conservative. It is not a
hard extraction quota: the reviewed worst-case reservation and free floor do not
sandbox Docker or another writer.

Release requires an actual `DockerRegistry` with the exact engine, executable,
endpoint and staging bound. `inspect_staged_artifact` must prove the exact cached
artifact, including manifest/config, platform, local image ID/kind and size. The
reservation changes atomically to released with a durable immutable proof. Actual
free space now accounts for the stored image, so the transient reservation can
be removed from the sum. Unknown, missing, changed, held, wrong-engine or forged
proof never frees it. Repeating an exact release is idempotent, including a lost
commit response. No file or image is deleted.

A released target cannot silently acquire a new reservation through the boolean
callback. This prevents a delayed release from freeing a newer pull's budget.
Cached reuse uses the registry's definite staged record. External cache loss
requires explicit operator inspection/recovery; there is no automatic repull or
reservation generation reset.

## Read-only cached-artifact proof

`DockerRegistry.inspect_staged_artifact(candidate)` is additive; existing
`inspect_staged`, `stage` and `reconcile` contracts are unchanged. It validates
the requested stored closed candidate, the existing STAGED pull row, and that
row's own stored candidate ID/hash bound to the same immutable pull. It freshly
inspects the requested artifact and compares every stable metadata field,
excluding only the candidate observation ID. It returns typed observed metadata
without rewriting the staging record or reconciling an unknown outcome.

Only a missing target row with no unresolved global pull raises the exported
`RegistryNotStaged(RegistryHeld)`. A pre-admitter may catch exactly that type to
request capacity. Unknown/held/mismatched evidence raises another error and must
remain held. A successful cached proof may return `True` without reserving a new
pull budget because `Registry.stage` then only verifies its prior cached record.

Pre-admission belongs before the supervisor records a staging intent, so ordinary
insufficient capacity can wait normally. The registry callback repeats the same
idempotent admission before a pull. If space changes between those two checks,
the later denial remains a conservative held operation; it never triggers an
unsafe pull retry. Recurring scheduling/service installation is separate work.

## Explicit legacy closure

Every known historical manual book must be listed in the policy. An unclosed
listed book blocks all normal admission/release/startup checks. This module does
not search for, silently import, erase, or change old manual budgets.

The only supported legacy schema is exactly
`reservations(candidate TEXT PRIMARY KEY,bytes INTEGER NOT NULL)`, with its
implicit primary-key index. The file and real parent must be private and owned;
links, sidecars, extra schema, unknown rows and changed reservation sizes are
refused. Each row must reserve exactly 64 GiB. Closure records the exact path,
device/inode/mode, size/mtime, raw file digest and logical/schema digests.

Legacy candidate IDs may be version 1 observations of a version 2 canonical
staged artifact. Bootstrap validates the legacy candidate's own closed schema
and hash, locates its immutable pull row, requires STAGED, then validates the
metadata's actual stored version 2 candidate and identical repository/manifest/
config/platform/engine binding. It uses `inspect_staged(v2)` for fresh proof;
it never calls `reconcile(v1)` or guesses runtime identity from the config ID.
The canonical book seals the exact alias and artifact proof. Old registry/manual
files remain byte-identical. Missing, orphaned, conflicting or possibly pulling
records stop the entire closure.

Normal ticks only verify the sealed closure against the unchanged legacy book.
New/changed rows, a replaced inode, changed schema or an altered proof block
future admission. Closures are immutable and have no TTL. Fully cached historical
images remain counted by actual disk usage, while old manual rows stay intact.

## Verification scope

`tests.test_update_capacity` uses real temporary SQLite, actual process/flock
competition, fork ownership guards, and the real registry with synthetic Docker
responses. It tests exact floor boundaries, aggregate/idempotent admission,
free-space/engine/filesystem changes, unknown pulls, lost reserve/release commit
replies, definite release, observation aliases, legacy v1→v2 closure and refusal
paths. Registry tests separately cover the typed read-only artifact API. Default
`statvfs` runs only on temporary test paths; no actual Docker command, image pull,
host cleanup, SSH, deployment or recurring service installation occurs.
