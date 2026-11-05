# Installed authority lease compatibility checkpoint

The installed 693cb7e coordinator is enrolled in the separate host authority
lease at `/run/ledgetop-authority-lease`. Its required SQLite marker and actual
external mount must survive replacement. A main `coordinator.lock` inside a
copied volume is not equivalent to this host-wide fence.

This patch restores `PROCESSING_AUTHORITY_LEASE_DIR` and requires the existing
original external flock before Queue can write enrolled state. Direct Queue
construction also rejects a required marker without the actual matching lease.
Every coordinator DB scope rechecks its pinned descriptor/directory/state
identity. Queue retains the external lease through operation/source drain and
main-epoch sealing, releasing it only after the main lease closes. The main
epoch pin binding includes the enrolled lease's physical identity.

The old marker DDL, authority UUID and lease filename remain compatible.
Lease diagnostic contents are preserved and never treated as ownership proof.
The descriptor is non-inheritable; paths, ownership, single links and exact
private original identity are checked. Missing, changed, copied, malformed or
unenrolled states remain held. New enrollment is deliberately not automatic;
it requires a separately reviewed migration. No host or dataset permissions
were changed, and no live lease was acquired by this development task.

Native checks passed (Linux-only flock/factory/process-death cases skipped).
Tests use synthetic old-marker fixtures and cover missing configuration before
any state/lockfile writes, malformed marker refusal, original external lease
excluding a copied state actor, wrong/fake state binding, missing lease file
refusal, actual make_handler configuration and real process death. The latter
Linux cases are present and pending the serialized Linux test slot.

This is not a qualified installed cutover. Current-V2 census/backup/rollback
still must preserve the actual 22 terminal jobs, seven legacy Viewer tasks,
file reservations/option evidence and the exact external mount before a
replacement can be considered. Disabled deployed admission drain was verified
by the parent task; no legacy barrier was enabled or bypassed here.
