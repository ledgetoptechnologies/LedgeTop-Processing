# Small real-node rehearsal: existing coordinator

This is a preparation record, not a go decision or a new coordinator. The
user authorized a small copied-input test. The 2026-11-04 read-only checks
found the staging credential could reach the existing Viewer facade, which
advertised one slot and an empty queue; its live dispatch code sends a job of
25 photos to the cluster when eligible and otherwise waits rather than falling
back to main. These observations are snapshots, not a node reservation.

The user-selected mapping folder contained more than 25 JPEGs. Its first 25
sorted entries totaled about 172 MiB. Names, source paths, photo metadata and
credentials are intentionally omitted from this shared record. Do not infer
geographic overlap, product quality, or engine success from this count.

## Blocker: result custody before submission

The installed coordinator fetches a successful NodeODM archive lazily on a
Viewer download. The cluster NodeODM image has a default 2,880-minute terminal
task cleanup. There is no qualified worker-side durable result outbox or
verified coordinator-held archive before that first download. Therefore a
prolonged Viewer/coordinator outage can outlast the engine retention window.
The current exploratory test must remain **unsubmitted** until the task owner
establishes durable result custody for this run or the release implements and
qualifies that custody. Do not alter live engine cleanup settings to evade this
hold. Always preserve the originals regardless of test outcome.

## Preconditions after custody is qualified

1. Re-read the live coordinator version, exact staging credential binding,
   processing flags, node assignment policy, free space, active/held jobs,
   NodeODM queue and external writers immediately before admission. Verify a
   small job is cluster-only under the actual deployed code; a prior status
   check does not reserve a node. Production has priority over waiting staging
   work and a running task is not interrupted.
2. Create a task-owned private Viewer database and persistent storage using the
   exact reviewed Viewer source and a separate environment; do not reuse or
   modify the production Viewer or the existing staging Viewer's volume. Keep
   the staging credential out of arguments, logs, source and public artifacts.
   Verify database locking, available space and restart behavior rather than
   overriding storage headroom as synthetic fixtures do.
3. Copy only the selected small set into an owned scratch directory; verify
   filenames, byte counts and SHA-256 against the read-only originals. Record
   the ownership marker, exact task/attempt/engine IDs and copy manifest in a
   private receipt. Do not edit, move or delete originals or unrelated tasks.
4. Submit exactly one job through the existing coordinator with the staging
   credential. Monitor its actual assigned node, state and progress without
   changing its options or deleting evidence. A status observation is not proof
   of task/process liveness or restart-safe custody.
5. Verify a persistent, task-bound coordinator or worker result copy before
   any engine cleanup; independently verify the downloaded archive, Viewer
   receipt and generated output through the real Viewer path. Preserve failed,
   incomplete or ambiguous work for inspection. A completed synthetic model
   does not establish real photogrammetry quality.
6. Only after durable downstream acceptance and explicit identification of
   every test-owned resource, remove the test's copies according to the
   existing retention rules. Never recursively clean a shared volume, remove
   another task, or treat disconnection as cleanup authorization.

This single-node exploratory run would exercise the installed coordinator and
cluster NodeODM, not qualify the unreleased parallel scheduler, main-node
fallback, staging Viewer deployment, large datasets, or reboot/power-loss
recovery. No runtime change or job submission was made for this record.
