# Local Git builds on the processing PCs

Package publication is not required for local synthetic testing. Push the reviewed
source branch, transfer its exact Git history over the existing SSH connection,
clone it privately on the target PC, and build a local image from a pinned commit.
Do not replace a running service merely because the build passes.

## Source and access

Both processing PCs currently lack working direct private-repository clone
authentication. Their GHCR image-pull credentials do not grant repository access.
The verified alternative is a Git bundle made from the already-pushed branch refs,
transferred over SSH, checked by SHA-256, and cloned on the PC. This installs no
new credential. Verify the bundle refs, detached checkout commit, clean status,
and source hashes before building. Never include untracked data or credentials.

Use an exclusive operation directory owned by `bkoltz`, mode `0700`. Inside that
private parent, clone with `umask 022` so tracked source files and directories
have their normal Git permissions. A `077` cloning umask produced an image whose
root-owned source was unreadable by the worker's UID 1000. The first tests caught
this; only the new owned clones were corrected. TrueNAS permissions were untouched.

## Images and builders

Use commit-specific local tags, not `latest`, for example:

```sh
docker build --pull=false --network=none \
  -t ledgetop-processing-local:coordinator-COMMIT .

DOCKER_BUILDKIT=1 docker build --pull=false --network=none \
  -f Dockerfile.reference-worker \
  -t ledgetop-processing-local:worker-COMMIT .
```

Run against the verified local Docker engine. The worker requires the existing
BuildKit path because its Dockerfile-specific ignore file includes `ops/`; the
legacy builder applies the coordinator's root ignore file instead and fails.
No new builder service is needed. The Dockerfile pins its public Python base by
digest; obtain that exact base if absent. Building and pulling that public base
use no GitHub Actions minutes or GitHub package publication.

Inspect the actual image ID, source inventory, UID, command and layers. Run
synthetic tests against the image's own `/app/processing` modules under its normal
UID. Mount only tests/contracts read-only; never shadow the application source.
Use a disposable container with no network, published ports, engine socket,
credentials or live state. Keep failed build/test receipts and compare the live
container census before and after.

## Deployment is a separate step

A clean Linux Git checkout has LF source bytes. Older qualified images contain
different raw newline layouts even when the code normalizes identically. Bind
qualification to the actual image and source inventory; do not reuse another
image's acceptance evidence. Local and remote deployment helpers must likewise
use identical raw bytes for their helper digest.

Activation uses the existing owned drain, fresh backup, health verification and
rollback workflow. Keep one coordinator, the existing service identity and the
original persistent state. The local build path does not enable the newer worker
pipeline, automatic cleanup, TreeDar or the large client batch by itself.
