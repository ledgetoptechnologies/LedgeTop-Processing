# Closed predecessor census component qualification

This component is a read-only custody check for the exact installed
`693cb7eca32295da959a4f42399f317577ef8a8a` predecessor. It does not activate the
historical updater or replace its checkpoint ABI. The historical controller is
unchanged. Current-V2-to-V2 updates and automatic hot-journal restart remain open.

`contracts/coordinator-693cb7e-schema.json` was derived from the literal DDL in
seven frozen source modules using an in-memory SQLite database only. It contains
the exact 19-table schema, including the existing required authority marker,
Viewer option evidence, reservations, and the exact ALTER-produced legacy job and
Viewer schemas. The canonical artifact hash is pinned in the implementation.
No Queue, source-spool reconciliation, process lease, network, or original state
constructor runs during the census.

The component:

- Requires the existing required marker, exact configured authority and nodes,
  closed terminal jobs, complete succeeded Viewer tasks, zero unresolved
  reservations and reference executions. Unknown schema objects, tables, paths,
  partials, journals, WAL, symlinks, and hard links are refused.
- Checks task/job/request/generation/environment/node/claim relationships, option
  evidence digests, every input's size/hash/name binding, archive receipt identity
  and bytes/hash, and historical incarnation ownership. Archive checks prove local
  receipt custody only; they do not establish signed Viewer acceptance or permit
  removing an engine's retained task.
- Hashes every column in every retained table under the private per-run key.
  Callback flags, result metadata, removed flags, logs, timestamps, and histories
  are not ignored. Client paths, input hashes, row values, and credentials are not
  exposed in the returned snapshot. Files use keyed content and path digests.
- Uses separate explicit artifact and metadata budgets. The SQLite physical bound
  is derived from the reviewed metadata ceiling, bounded row count, per-row/index
  page allowance and free-page headroom, rather than a blind 64 MiB file ceiling.
- Verifies descriptor/path identity and before/after timestamps during file reads,
  plus final database/root/retained-directory stability. It never deletes or
  recovers a SQLite journal.

The optional reviewed-count checkpoint uses plan inputs, not a hardcoded job
count. The synthetic fixture matches the reported aggregate 22 jobs, 7 Viewer
tasks, 0 reservations, 7 option records, and 155 input records; its identifiers,
photos, engine output, ZIPs, and receipts are entirely synthetic.

The first bounded Linux runner passed 19/19 tests (16 census cases plus 3
optimized-Python safety cases), no skips, in 0.530 seconds. It used one CPU,
768 MiB RAM, no network, read-only source/root, and a 128 MiB temporary filesystem.
The named task container was auto-removed and its filtered inventory was empty.
Final qualification passed 21/21 Linux tests (18 census plus 3 optimized-safety),
no skips, in 0.556 seconds with the same resource limits. This includes historical
incarnation custody and refusal of a receipt symlink before open. The final owned
container was removed; filtered task inventory was empty.

Open integration boundaries:

- The controller must establish final writer exclusion with the original enrolled
  external lease and original main state lease after the old writer stops, then
  obtain an offline baseline and verified backup. This function explicitly returns
  `writerExclusionProven=false` and does not manufacture that proof.
- Actual mounts, image/revision/config pins, backup/headroom qualification,
  candidate additive startup transforms, rollback without restoring over new
  writes, and an installed end-to-end cutover remain unqualified here.
- Owned node-maintenance history is preserved and checked against the exact old
  canonical request/digest/receipt reconstruction. An active residual plan must
  match its latest retained receipt and a later closed revision, with cancellation,
  explicit healthy operator-resume, or successful update metadata. Pending states,
  foreign/unbound plans, tampered old receipts and target/date mismatches hold.
- The new current-V2/source-release tables remain unsupported by this predecessor
  profile. NAS custody, signed Viewer acceptance, and hot-journal recovery are
  separate proof boundaries, not claims made by this census.

Maintenance-history followup: 26/26 Linux tests passed, no skips, in 0.654 seconds
(23 census cases plus 3 optimized-safety cases), with the same resource limits.
The fixture now retains 16 owned historical schedule receipts and both active
residual plans, matching the reported aggregate shape. A malformed historical
request, current target mismatch, unclosed plan, and unknown current plan all
refuse. The task container was removed and filtered inventory was empty. This is
synthetic shape qualification; the actual installed rows/files have not yet been
read through this component.
