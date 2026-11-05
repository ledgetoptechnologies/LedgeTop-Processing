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
If the selected transport uses credentials, keep them in a private host-managed
credential file, outside app config, source control, logs and chat. Provision the reviewed read/write mount through
the host, then bind it into the single coordinator container.

Mount the approved `processing-spool` subfolder itself as the configured mount
root. Set the spool root equal to that mount root, so the identity marker stays
inside the approved folder. Mounting all of `Storage` with a nested spool root
would place the mount identity marker outside the approved subtree.

## Transport durability contract

Keep the agreed ordinary subfolder in the existing dataset. A child dataset or
`sync=always` is not required just to establish this contract. For NFS, qualify
an explicitly synchronous server export backed by ZFS `sync=standard` (or a
stronger verified policy), with a hard client mount. An asynchronous export or
ZFS `sync=disabled` cannot support the intended acknowledgement guarantee.

A successful client directory `fsync` alone is insufficient evidence: Linux
NFS directory fsync is a no-op, and the CIFS implementation also relies on
synchronous namespace operations. For NFS, the relevant server-side evidence is
that synchronous nfsd create/link/unlink/rename operations commit the affected
metadata, and the actual ZFS export implements the metadata commit with
`zfs_fsync`. Verify the installed server versions and actual export policy;
do not infer server durability from a successful copy or client API return.
SMB remains an option only with equivalent verified server namespace durability;
client UID masks or relaxed caching/permission options do not supply that proof.

Primary implementation and policy references:

- [Linux nfsd metadata commit](https://raw.githubusercontent.com/gregkh/linux/v7.0.14/fs/nfsd/vfs.c)
- [Linux NFS directory operations](https://raw.githubusercontent.com/torvalds/linux/v7.0/fs/nfs/dir.c)
- [Linux CIFS filesystem operations](https://raw.githubusercontent.com/gregkh/linux/v7.0.14/fs/smb/client/cifsfs.c)
- [OpenZFS export metadata commit](https://raw.githubusercontent.com/openzfs/zfs/zfs-2.3.4/module/os/linux/zfs/zpl_export.c)
- [ZFS sync property](https://openzfs.github.io/openzfs-docs/man/master/7/zfsprops.7.html#sync)
- [NFS export sync policy](https://man7.org/linux/man-pages/man5/exports.5.html)

These references explain the required contract; they do not prove the installed
NAS configuration. Client reopen/restart tests likewise are not a physical
power-loss test. No such NAS outage test has been performed.

## Evidence needed before activation

The first operator check is read-only, in the TrueNAS system shell:

```sh
sudo midclt call filesystem.getacl /mnt/Plugins/App_Data/Model-Viewer/Storage false true
sudo zfs get -H -o name,property,value,source sync,acltype,aclmode,aclinherit,mountpoint Plugins/App_Data/Model-Viewer
```

This requests the advanced ACL with resolved owner/group IDs; it does not set
permissions. The parameter order is documented in the
[TrueNAS 25.10 getacl API](https://api.truenas.com/v25.10/api_methods_filesystem.getacl.html).

1. Inspect the actual parent and proposed path ACL and share scope. Preserve
   administrative recovery access. A private directory mode or share ACL alone
   does not prove isolation from Viewer, whose existing parent mount is writable.
   Include effective parent rename/delete authority in that review. If an app
   mount mask or narrowed bind is needed, Beau applies it in the TrueNAS app
   configuration; do not infer enforcement from a directory listing alone.
   One candidate is a read-only child mount masking
   `/app/storage/processing-spool` in both Viewer containers. Qualify it with no
   mount-management capability or alternate unmasked bind, failed create and
   parent rename/delete operations, and an unchanged NAS canary. It is not
   enforced until Beau applies and verifies the actual app configuration.
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
   For NFS, also record the installed `uname -r`, `zfs --version` and relevant
   `exportfs -v` output after the scoped export exists. Inspect only the intended
   export; keep unrelated share details out of review records.
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
