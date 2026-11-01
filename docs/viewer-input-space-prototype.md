# Inactive Viewer input-space reservation prototype

The default Viewer cumulative task input limit remains 20 GiB. Each HTTP upload
request remains limited to 2 GiB and 64 multipart parts; a larger dataset must
be split into compatible requests. The existing scheduler prefers the cluster
node at or below its 1,000-photo limit and routes a 10,000-photo job to main.
These facts do not establish that a real 50 GB dataset can complete safely.

The disabled experimental input-space guard reserves physical bytes using
`posix_fallocate`: first for the remaining multipart request while one part is
spooled, then for the exact destination file before copying it. It checks a
configurable free-space floor on the corresponding filesystem before each
reservation and fails the upload with a bounded 503 if space or allocation is
unavailable. Runtime enablement requires Viewer, a configured authority lease,
POSIX allocation support and an intentionally false qualification constant.
There is no automatic deployment or limit increase.

This is a per-part/spool-and-copy reservation, not a reservation of the entire
dataset, NodeODM workspace, remote-node storage, intermediate outputs, or final
archives. The free-space check and allocation are not one atomic transaction:
other writers can cross the floor concurrently. Filesystems, quotas, snapshots,
and copy-on-write behavior need qualification on the actual hosts. A crash can
leave an existing owned incoming file for later inspection; no new cleanup is
introduced. A multipart request can have earlier parts committed before a later
part fails, so the client must reconcile or retry exact files. The tests use only
small synthetic bytes and include a local HTTP request, not a real 50 GB job.
