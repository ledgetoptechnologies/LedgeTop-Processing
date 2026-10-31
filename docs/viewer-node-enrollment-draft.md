# Draft node-side Viewer enrollment

This isolated prototype composes the draft coordinator Viewer-worker claim,
actual `DaemonStateLock`, a preprovisioned `NodeGate`, and the draft same-inode
output slot. It adds no listener, worker daemon, engine call, dispatcher,
result handoff, acknowledgement, cleanup, or deployment configuration. It is
not connected to the installed systems.

An operator-provisioned private worker policy fixes the coordinator HTTPS
origin, pinned CA hash, bearer, node/worker/instance, authority, independently
provisioned gate identity, output root/mount identity, allocation budget and
environment allowlist. The worker writes the job, request and generated gate
receipt intent to its private FULL-synchronous SQLite journal *before* sending
the authenticated claim request. It checks the hostname-verified TLS response
against its fixed identity and stores the exact response. Retrying after a lost
response uses the same durable request; a different response or request holds.

Under the actual local NodeGate lock, the worker compares the configured gate
lock's device/inode/mount identity and full stored node/authority/origin/epoch,
reserves or rechecks the same receipt and binds the claimed job. It records
the immutable slot binding, creates or reopens the preallocated same-inode slot,
then rechecks gate and slot while both are held. Interrupted gate reservation
or binding can be reconciled only with the exact same journal and claim. An
uncommitted, partial or conflicting slot is retained and held. No automatic
release or cleanup exists. The sole success label is `enrolled_no_dispatch`.
On a coordinator-restart hold, the coordinator draft may replay its original
claim; this prototype may reserve local resources, but that replay and the
enrollment result never authorize an engine start or override a held job.

The TLS claim is authenticated to the pinned coordinator and uses the worker
bearer, but NodeGate is cooperative: this prototype trusts its local provisioner,
state root, configuration, stable ancestors and gate file. The gate's
`engineId` is configured and stored, not independently attested against Docker.
Old coordinators, direct NodeODM clients, privileged or same-UID writers,
unrecorded state copies and different gate paths can bypass or race it. The
claim's input digest is only upload-time inventory metadata, not a current-byte
verification. Allocation is the local filesystem's fallocate/st_blocks report,
not proof of physical backing, thin storage, quota or power-loss safety. Mount
IDs can change across reboot and intentionally hold until reviewed. The private
journal contains the claim token and requires appropriate operator protection.
There is no qualified host installer, schema upgrade/rollback, total execution
pipeline or Viewer custody. A future authenticated enrollment acknowledgement
and actual dispatch must validate current job authority, input bytes and an
enforced physical ingress fence separately.

Bounded tests use a synthetic TLS coordinator, synthetic task files and a real
local NodeGate and allocation on tmpfs. They exercise exact retries, conflicting
identities, disk failure and process kills at claim, local journal, gate and
allocation boundaries. They do not test live systems, a real engine, a real
persistent disk, Docker-daemon/host restart, power loss or a full processing
pipeline. Network-none is supplied by the test invocation, not asserted by the
test code. Interrupted ambiguous files and reservations are intentionally kept.
Once a binding is recorded, a later missing gate reservation or missing bound
job is a hold; the worker will not silently recreate either. Before that binding
commit, an interrupted reservation or bind can be reconciled only if the gate
still carries the same durable receipt and job identity.
