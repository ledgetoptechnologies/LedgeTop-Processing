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

Fresh parent read-only metadata identified the actual enrolled directory as
UID/GID10001 mode0711 and its file as UID/GID10001 mode0600/single-link. The
external directory check therefore permits exactly owner0700 or owner0711:
0711 allows search, but no foreign writes or directory listing. The original
file must remain0600. Exact0711 positive and0770/0777 negative cases are included; no
installed permission was changed or normalized. Deployment compatibility still
requires checking the original mounted state directory, not just this lease.

The subsequent parent metadata check found the full actual layout: enrolled
state root0755, database0600, main lock0644, external directory0711 and external
file0600, all UID/GID10001 and single-link files. Exact state0755 is accepted
only after reading the valid existing required marker/private database and
then acquiring the actual enrolled external descriptor before any Queue write.
An unenrolled0755 or writable0770/0777 state root is refused unchanged. New or
unenrolled state still requires0700; NAS/source roots and source-reader private
directories were not relaxed. A full-layout fixture checks actual make_handler
and ReferenceRuntime construction (synthetic TLS loading), descriptor exclusion,
unchanged modes/lease+lock bytes and the old authority/marker rows.

Final bounded Linux check passed **84/84 tests, no skips, in3.397s** across
`test_authority_lease_current`, `test_reference_runtime`,
`test_coordinator_freeze` and `test_node_roles`. Existing dependency image,
one CPU/1GiB, read-only source, no network and private tmpfs were used; the
owned `ltds-enrolled-actual-layout` container exited and auto-removed. No actual
host permissions, containers, original data, NAS or deployment changed.

This is not a qualified installed cutover. Current-V2 census/backup/rollback
still must preserve the actual 22 terminal jobs, seven legacy Viewer tasks,
file reservations/option evidence and the exact external mount before a
replacement can be considered. Disabled deployed admission drain was verified
by the parent task; no legacy barrier was enabled or bypassed here.
