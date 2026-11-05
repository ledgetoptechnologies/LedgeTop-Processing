# NAS source spool preparation for the coordinated path

Preparation only. Beau performs TrueNAS changes; this document is not a record
of provisioning, tested NAS custody or installed Processing workers.

## Intended layout

Use the agreed separate subfolder in the existing dataset:
`/mnt/Plugins/App_Data/Model-Viewer/Storage/processing-spool`.
Keep the Viewer originals, models and database in their current locations.
Do not recursively change ownership or permissions on the existing Storage tree.

Only the coordinator needs the NAS spool mount for this execution path. Workers
on both PCs fetch their claimed inputs through the authenticated HTTPS source
broker into local reserved scratch. Do not mount the share into NodeODM/TreeDar,
and do not require an additional NAS mount on main just to transfer source files.

The current coordinator runs as container UID/GID `10001:10001`. A dedicated
NAS identity/share should permit only the required spool access; Linux mount UID
mapping is distinct from that NAS account's actual identity and server ACL.
Credentials stay in a private host-managed credential file, outside app config,
source control, logs and chat. Provision the reviewed read/write mount through
the host, then bind it into the single coordinator container.

## Evidence needed before activation

1. Inspect the actual parent and proposed path ACL and share scope. Preserve
   administrative recovery access. A private directory mode or share ACL alone
   does not prove isolation from Viewer, whose existing parent mount is writable.
   Include effective parent rename/delete authority in that review. If an app
   mount mask or narrowed bind is needed, Beau applies it in the TrueNAS app
   configuration; do not infer enforcement from a directory listing alone.
2. Verify the coordinator sees the intended mounted filesystem and approved
   root, with correct effective ownership and private modes. Require the mount
   rather than falling back to an ordinary directory on the cluster SSD.
3. Create the configured spool identity marker only in the new approved spool
   location. Match its exact content and mount/root paths to runtime schema 6;
   missing/replaced mounts and marker mismatches must hold admission.
4. Qualify write, file fsync, same-directory hard-link publication, unlink,
   directory fsync and reopen/hash verification on that actual filesystem with
   bounded synthetic bytes. A filesystem that rejects these durability barriers
   is not qualified merely because ordinary copies work. Test the known
   link-before-partial-unlink crash and repeated directory-fsync failure too.
5. Verify configured source byte reservations, free-space floor, maximum object
   size and actual available space. Logical reservations and `statvfs` checks
   are cooperative safeguards, not a physical filesystem quota or backup.
6. Keep source/result cleanup disabled for the initial installed test. Prove
   the complete acceptance/retry chain and inspect retained bytes before enabling
   the separately reviewed release policy. An elapsed timeout is not acceptance.

Snapshots/backups are separate operator controls. This implementation must not
claim it has created a ZFS snapshot or hold when it has only fsynced a file and
recorded a spool receipt. Per-job middleware snapshot management is not required
to demonstrate the initial synthetic intake/download path and should not become
a new unreviewed release dependency.

## Current external prerequisites

The last read-only SMB check found no `processing-spool` subfolder, and the last
SSH check found no NAS spool bind in the coordinator container. No NAS folder,
share, ACL, marker, persistent mount or app bind was created by those checks.
After Beau supplies the actual ACL/share configuration, produce exact scoped
operator steps and perform mount qualification before enabling Viewer admission.
