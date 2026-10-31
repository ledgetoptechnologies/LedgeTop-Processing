# Installing the next reference coordinator

## Unreviewed current-predecessor adaptation

The exact current predecessor is supplied explicitly by full container ID,
image ID and source revision and is bound to the private checkpoint. Every
prepare/activate/rollback invocation must supply the same values. The previously
listed predecessor below is historical, not a current live pin. The current
adaptation remains activation-disabled until independently reviewed and tested
against the intended live gate and backup environment.

The census retains and hashes the bounded `pilot-journals` tree and accepts
existing owned-maintenance receipt history only with the exact reviewed table
schema; its complete rows are hashed and compared across the backup. No receipt
or journal is deleted. Unknown paths, symlinks, active jobs, altered history
or exceeded bounds hold. The old predecessor must not already have a lease bind:
this path is for the current unleased-to-leased transition, not a general
lease-aware predecessor upgrade.

The new candidate requires an existing canonical authority lease directory
owned by numeric UID/GID 10001 and mode 0711, containing the authority's existing
regular single-link 0600 lock file owned by the same UID/GID. The executor binds
directory and file device/inode into the checkpoint and rechecks both before
mutations; it never creates or replaces the lock. All authorized coordinators
for this authority must use the same qualified filesystem and lock inode. An
operator must establish and verify effective cross-process `flock` semantics and
cooperative non-replacement throughout the window. The candidate's lease marker
is accepted only for forward verification. If that new marker exists, the
automatic old-binary rollback path holds and retains the candidate; it does not
silently restart an older binary that ignores the lease. Backup captures file
bytes, numeric ownership and modes; it is not a claim of xattr/ACL/timestamp
equivalence.

The owned scheduling API is not installed in the current sole coordinator.
Current observed container `7ebe34636518961ddb4398b545a6a73d0254a47a093b198cf5d2d8411345cd07`
uses image `sha256:48651a4f4b0ab88f9c4969ed2536344842ff974d262e1aa9c5bdff5e27b38de9`
and source `e2ccf489ab41160acff3f90e2b4bcd568f72cf24`. Revalidate these facts before
any operation. Source readiness does not prove installation.

The historical `ops/install_reference_coordinator.py` cannot be reused unchanged.
Its prepare and activation validators expect a native-only predecessor, reject
the existing reference-config override, and its rollback selects native facts.
Its backup/rollback guards also assume no reference jobs and only a narrow
history census. Preserve its frozen prior-operation artifacts.

## Required executor behavior

1. Bind the exact current reference container, image, source, authority, state
   volume and recipe. Preserve native ports 3005/18080, reference TLS port 18443,
   the existing read-only TLS mount, native environment file and reference-config
   path, UID 10001, security settings, resource settings and logging/network
   configuration. Compare image defaults separately from unchanged application
   overrides; never print environment values or regenerate credentials/PKI.
2. Acquire one operation lock, record baseline public-history hash and a complete
   logical database/file census, and drain both nodes using only exact owned
   maintenance plans. The old API cannot look up owned scheduling requests:
   persist its single legacy POST intent before sending and hold on a lost ACK,
   without adopting a matching target or issuing another schedule.
3. Hold the canonical physical gates on both processing hosts through live
   contexts. A saved JSON attestation is insufficient. Verify their authority,
   generation, engine and idle reservations and recheck live ownership before
   mutations. Drain jobs, uploads, readers and both coordinator listeners.
4. Stop the exact current coordinator gracefully; verify it stopped and no
   running container owns the state volume. Compare the stopped database/file
   census with the baseline, catching admissions that arrived while draining.
   Make an offline, hashed, fsynced backup with verified numeric UID/modes and
   SQLite integrity. Do not restore or delete state during update.
5. Retain only this operation's exact predecessor under a unique name; leave the
   older legacy container `6300…` untouched. Create/start one candidate once,
   reconciling unknown responses by full identity, operation label and image.
   Verify sole ownership, authority/history preservation, native/TLS health,
   configuration roles and new API support without issuing a new live plan.
   Reopen only this operation's exact maintenance plans after all checks.
6. A rollback must first stop the exact candidate and prove no volume owner and
   a compatible stopped-state census. New jobs, owned-schedule receipt rows,
   claims or unknown schema/progress changes block downgrade. An empty additive
   owned-schedule table can be explicitly allowlisted because the old reference
   binary ignores it. If compatibility fails, retain/restart the exact new
   candidate and keep nodes fenced; never restore the backup over new work.

The Viewer census accepts the reservation and option-evidence tables only as
the exact reviewed pair with their exact autoindexes and no rows. Candidate
verification may explicitly accept their empty addition to an older baseline;
the offline rollback gate does not. If the candidate added these tables, an
unqualified old-binary restart is refused and the exact candidate is retained.
Nonempty Viewer tables or a partial or changed schema still hold the updater.
The comparison tests use synthetic state; they do not establish production
rollback or cross-host exclusion.

The executor and its live cross-host gate transport still need implementation
and runtime acceptance. The current main/cluster worker images are not
coordinator rollback targets.

## Acceptance needed

Use temporary SQLite and synthetic Docker/HTTP boundaries to test reference
recipe and environment preservation, immutable predecessor selection, both-plan
ownership, lost schedule/cancellation/stop/create/start responses, lost or busy
gate holders, late admissions during drain, backup bytes and ownership, one
volume owner, TLS-preserving rollback, and refusal to downgrade after new work
or owned receipts. Include both online and stopped-state compatibility checks.
Only then prepare an immutable source/image and an exact runtime operation.
