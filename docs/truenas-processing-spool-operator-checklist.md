# Processing queue storage decision

## Current first-release design

The photo queue spool lives on the cluster PC's local SSD. Viewer submits the
job and then displays status, progress, logs and returned outputs. The cluster
coordinator durably accepts the upload, queues it when required, and dispatches
it to the first eligible worker in the configured order. The TrueNAS Viewer
storage dataset is not the Processing spool.

No TrueNAS SMB/NFS share, child dataset, ACL change, mount, snapshot or custody
service is required for this first release. Do not follow earlier NAS-spool
mount or permission steps for this workflow. That design was superseded by the
spool-first local-SSD decision recorded in Engineering Hub scope note v36 and
photo-queue plan v13.

## Retention during the pilot

- Keep source photos, coordinator spool files and completed result archives.
- Automatic cleanup remains disabled.
- Later redundant-spool cleanup may be considered only after Viewer outputs
  are verified Ready, at least 24 hours have passed, and no reader or upload is
  active. Preserve Viewer originals and accepted outputs.
- If cluster SSD capacity is insufficient, reject or defer new work before
  acknowledging durable acceptance. Never silently discard an accepted job.

## TrueNAS boundary

Beau controls TrueNAS administration. Any Viewer service inspection or restart
must be performed by Beau in a planned test window. Do not change the Viewer
storage dataset ACL or create a NAS share for the Processing spool.
