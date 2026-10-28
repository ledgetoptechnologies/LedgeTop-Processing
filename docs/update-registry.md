# Immutable image discovery and staging

`processing.update_registry` implements registry discovery and image staging for
an explicitly configured local Docker engine. It does not qualify an image,
schedule maintenance, replace a container, or install a background service.

Construct `RegistryPolicy` with an absolute Docker executable, fixed local Docker
endpoint and engine ID, a tuple of `RegistryChannel` records, and a trusted
`staging_reservation_bytes` bound. Each channel names an exact fully qualified
repository and tag. Existing Docker credentials are used without exporting them.
No live release channels have been selected by this implementation.

`DockerRegistry(path, policy, capacity_admit=callback)` binds its private SQLite
journal to that complete policy. `discover(channel)` reads the tag, verifies the
selected Linux/amd64 child manifest by its immutable digest and raw bytes, then
reads the tag again to detect changed descriptor mappings. The returned candidate
must remain exactly equal to the persisted discovery record.

The registry manifest digest and Docker config/image ID have different meanings.
Only `repository@manifestDigest` is pulled; `configDigest` is checked against the
local image ID after staging. The immutable reference, platform, repository
digest and local engine identity must all match.

Before `stage(candidate)` pulls, the capacity callback must durably reserve the
configured amount, idempotently for that candidate. A pull intent is committed
before Docker runs. An uncertain response holds further staging; `reconcile`
only inspects the exact immutable reference and never repeats the pull. Neither
method prunes images or releases another component's capacity reservation.

The reservation is not an extraction quota. Compressed layer bytes cannot bound
unpacked storage; a post-pull size check cannot prevent disk exhaustion during
extraction. The operator must supply a sufficient trusted reservation before
enabling staging. Docker's verbose manifest output also does not prove the
top-level index digest, transient ABA changes, or index-only annotation changes.
The selected immutable child manifest remains the staging identity.

Subprocess output and execution time are bounded. Tests exercise malformed
metadata, digest/config confusion, tag changes, capacity refusal, concurrent
intent fencing, uncertain pulls and inspection-only reconciliation. Synthetic
tests do not establish that a release is safe for production adoption.
