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

The registry manifest digest, configuration-blob digest and local runtime image
ID have different meanings. Only `repository@manifestDigest` is pulled.
Discovery schema 2 saves the exact selected child's descriptor (media type,
digest and size) and configuration digest; it does not guess a local image ID.
Staging reads the actual Docker ID. A classic-store ID must equal the config
digest; a manifest-backed ID must equal the selected manifest digest and have
an exactly matching local descriptor. Platform, immutable repository digest and
local engine identity must also match. Staged metadata carries `configDigest`,
actual `imageId`, and explicit `imageIdKind` (`config` or `manifest`).
Docker's [containerd image-store implementation](https://github.com/moby/moby/blob/master/daemon/containerd/image_inspect.go)
reports the target descriptor digest as its image ID; this behavior was also
observed on the main processing host during the first real staging attempt.

`matches_local_image(candidate, image_id)` can prove an already adopted image
only through this verified local mapping, never by comparing an artifact digest
to a runtime pin. Its unavailable or invalid cached proof returns `False`; it
does not stage or qualify anything.

Legacy discovery schema 1 stored a guessed `localImageId` config alias. Exact
old records remain readable for inspection-only reconciliation, but that alias
is never runtime proof. Recover their missing descriptor only from the same
immutable child with verified raw hash, config and platform. The pull identity
does not change across this correction, so an uncertain pull cannot be replayed
by converting its candidate record. Existing supervisor/qualifier receipts
without the new required runtime identity must not be reinterpreted as success.

Before `stage(candidate)` pulls, the capacity callback must durably reserve the
configured amount, idempotently for that candidate. A pull intent is committed
before Docker runs. An uncertain response holds further staging; `reconcile`
only inspects the exact immutable reference and never repeats the pull. Neither
method prunes images or releases another component's capacity reservation.

`inspect_staged(candidate)` is a read-only qualification prerequisite. It
requires a previously definite staged record, checks its unchanged metadata
against the exact local image and engine, and neither promotes an unknown pull
nor changes the journal. Only explicit `reconcile` can resolve a staging intent.

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
