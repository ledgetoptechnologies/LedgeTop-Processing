# Reference worker image

`Dockerfile.reference-worker` packages the optional **ODM-only** reference worker
separately from the coordinator. Its entrypoint rejects TreeDar routing before
opening worker state; this image has no Docker CLI. Building the image does not
provision or start a worker.
Run it with `--config` pointing at a pre-existing owner-private configuration.
Without a valid configuration and physical gate, the daemon fails closed.

The default image user is unprivileged. A real deployment must use the same OS
identity and **same underlying canonical NodeGate directory** as every other
cooperating writer for that engine; a fresh gate inside the container does not
provide mutual exclusion. On the current Linux nodes the host gate is owned by
`bkoltz` UID/GID `1000:1000`. For that specific layout, use `--user 1000:1000`,
set `XDG_STATE_HOME=/node-state`, and bind the host's existing
`/home/bkoltz/.local/state/LedgeTop-Processing/node-gates` directory read-write
to `/node-state/LedgeTop-Processing/node-gates`. Verify the host's real gate
binding and UID before use; if its state home differs, mount the actual directory.
Do not bind an empty replacement or use the image's default UID for that gate.
An isolated synthetic lock check on the main host verified that a container run
as `1000:1000` with this mapping observes a host-held gate lock. This does not
validate the live gate's binding, other writers, or deployment.
Independently provision persistent,
owner-private worker state and scratch mounts, a verified coordinator/broker CA,
worker server certificate and key, and distinct scoped credentials. Configure
the network so the fixed loopback `engineOrigin` actually reaches the intended
engine; an ordinary bridge container's loopback is not the host loopback.

Image packaging does not verify those identities, protect against direct engine
clients, establish a NAS broker or Viewer acceptance, or enable TreeDar. Do not
start a second worker against an active engine until the ownership, maintenance,
retention and recovery procedures have been qualified for the target host.
The draft reference v2 protocol is not wired or supported by this image; its
candidate contract resources are intentionally not included.

## Opt-in synthetic image test

`tests.test_reference_worker_image_e2e` is skipped unless explicitly enabled.
Build the image from the source revision under test, with its full Git commit
in the `org.opencontainers.image.revision` label. Set
`LT_REFERENCE_WORKER_TEST_IMAGE` to the resulting local `sha256:...` image ID
and `LT_REFERENCE_WORKER_SOURCE_REVISION` to that same 40-character commit;
then run `python -m unittest tests.test_reference_worker_image_e2e -v`.
The test verifies the image ID and revision label before running. Docker must
be available, and the pinned base/image and `openssl` inside the image must
already be available. It does not pull an image.

The test uses generated credentials and three bytes of synthetic input. It
starts the packaged daemon CLI in a nonroot, network-disabled, read-only
container explicitly run as UID/GID `10001:10001`, with temporary state, a synthetic HTTPS broker, a synthetic engine,
and the real local coordinator API. The coordinator listener is stopped before
the transfer grant is made available; the test confirms the engine has received
no work, kills the worker process, restarts both, and verifies a single engine
submission, retained input, archive digest and coordinator receipt. Container
and test state are removed afterward.

The coordinator runs as a thread in the same container and the engine and
broker are fixtures. This test does not cover a worker restart during engine
processing, real NodeODM, Viewer acceptance, NAS storage, or power loss. The
revision label is supplied by the builder and is not itself a provenance
attestation. The fixture script, synthetic-engine implementation and contract
JSON are mounted from the test checkout; their bytes are not pinned to the
image's revision label. Review the checkout alongside the image when using this
test as release evidence.
