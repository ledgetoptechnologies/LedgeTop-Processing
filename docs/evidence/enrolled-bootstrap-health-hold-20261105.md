# Actual forward attempt: bootstrap health observation held

This incomplete qualification reached the first real candidate start, then
held. It is not a completed cutover or proof of a constructor defect. No
installed PC, TrueNAS dataset, client input or real engine processing was used.

## Exact scope and boundary

The one-shot prepared operation was `cf8dc840-f288-493f-9746-cd0863a21b94`,
using frozen harness `951ef9a` (archive SHA256
`fcbe8edbdb1decfbf969f95c967c27da32af396bb8e904b6e9051be19fcdae04`).
Packaged candidate image:
`sha256:378559e42456017b965d1e2e38b56eb627274734aeb317a758d84c0bc99bb5ab`,
processing source `18750d4d599026c18e569a9c67135e6c9fdc7d1a`.
The real693 predecessor image remained distinct. Controller and worker
fixture ops/processing loaded the frozen read-only archive, while both
coordinator apps used their immutable packaged sources.

The parent controller used UID10001 with only supplemental Docker-socket GID0,
one CPU,768MiB, read-only root/source/CLI, bounded tmpfs and private bridge with
no published ports. Its qualification-only budget was900seconds; production
command deadlines and startup timestamp windows were unchanged. A reviewed
quiet window was coordinated. The full inventory fence stayed enabled.

Eight phases completed through `bootstrap-created`: real693 stop, original
dual-kernel-lease holder, full census, accepted immutable TAR backup, verified
original lease release, and exact quarantine candidate creation. The journal
then retained `start-bootstrap` pending at dispatch attempt8 with
`admissionOpened=false`. No freeze intent/drain/seal or final app was reached.

The controller ran from23:21:03.691864711Z to23:22:08.024155495Z on
November5,2026:64.332seconds. Neither inventory churn nor the900-second
alarm caused this hold.

## Failure evidence and limits

The production path was `_observe(start-bootstrap) -> _health ->
BoundaryProbe.fresh -> TrackedExec`, ending in
`remote_script_failed_after_retirement`. The health body did not succeed, and
its exact remote process had retired. This differs from a failed Docker start
command or an unresolved remote process.

Static source shows `_health` performs one authenticated HTTP GET/JSON decode
immediately after start, with no startup-readiness wait. A slow listener could
therefore explain this result. It is **not proved**: constructor failure or
another health-body exception remains possible.

The qualification's known-safe finally cleanup removed the inner bootstrap
and state before private app state/logs were captured. That is a diagnostic
limitation and must be corrected before another failure-producing attempt.
No missing logs are represented as a green constructor or a known race.

## Current disposition

Outer controller `ltds-phase-forward-cf8dc840f288493f`, exact ID
`9d12304e5d4524ea940b68bd84fb803aaabb0ac3b82439f4cada0891a4dc85fb`,
is stopped: PID0, exit1, no OOM and exact operation label independently checked.
Its private artifact volume
`ltds-phase-forward-artifacts-cf8dc840f288493f` remains, referenced only by
that stopped controller. Exact inner task-label container and volume
inventories were empty. Unowned containers and the shared builder were left
untouched.

A cold read-only journal copy is retained privately. Full cold backup-copy
verification and outer/artifact removal require the root agent's review;
they have not been performed at this checkpoint. This operation is not
replayable because the disposable originals were cleaned up. No retry or
rollback ran.

## Required diagnostic follow-up

Persist the reviewed plan/bundle and exact configuration copies in private
controller artifacts before dispatch. On any later failure, retain the owned
failed app/state and bounded private state/log evidence before cleanup.
Distinguish a delayed listener from a genuine constructor refusal. Only if
supported, add a bounded read-only startup health wait with the original
unchanged startup window and container incarnation. Do not retry a mutation,
silence a permanent error, invent an expected row from candidate data or
claim the22-phase sequence passed.
