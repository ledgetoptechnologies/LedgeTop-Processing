# Disabled exact-image NodeODX retention fixture

This is an unexecuted plan, not an authorized live test or a qualified
retention result. `ops/nodeodx_retention_fixture_candidate.py` implements
read-only local image/Engine preflight and a partial inspect-shape validator.
Its `--run` path is unconditionally disabled; it
does not create, start, stop or remove anything. No image is pulled.

`ops/nodeodx_retention_lifecycle_candidate.py` contains an **unreviewed,
unexecuted and hard-disabled draft** for explicit labelled volume/container
creation and exact-owner start, stop and removal. A fixed in-container Node
probe and orchestration draft cover loopback-only synthetic init/upload,
count, volume marker, restart and remove. Failure retains resources after an
attempted exact-owner stop. These are untested: effective device detection,
Docker inspect shapes, timeout/daemon ambiguity, API compatibility and cleanup
have not been validated. Do not enable or call its mutation methods.
The draft now compares an independently supplied private environment policy
without printing it, refuses capability additions and extra security options,
requires a private cgroup namespace and an exact labelled-volume mountpoint,
and does not retry an ambiguous Docker exec timeout. Its allowed default
`UsernsMode` is **not** proof of user-namespace isolation; daemon policy,
effective device access and host ownership remain unqualified. The draft
additionally compares in-container user and cgroup namespace identities with
the launcher's PID-1 namespace view, checks common GPU device names and
effective capabilities, and compares the mounted data directory identity to
an open host volume descriptor before task probes. Those checks do not prove
that PID 1 is the physical host or exhaustively detect every device; the
image's default startup executes before the in-container check. The expected
environment and digests supplied by a caller are not themselves authority.
An ambiguous mutation latches a hold against replay and removal; an exact
verified container stop is permitted as a one-shot safety action.

The draft requires the same task UUID and API-reported input count before and
after restart for its retention flag; the marker alone cannot set it. That
count does not prove exact input bytes or a completed result.

The two permitted image IDs are the freshly reported cluster and main image
IDs. The preflight fixes the local Docker Unix socket, requires a separately
verified exact Engine ID, inspects only selected image metadata, does not
print command/user contents, and refuses an unknown local image ID, platform,
working directory or declared volume. It deliberately fails if the daemon
represents a manifest and config digest differently; that mapping must be
independently proved rather than guessed. The output size check is after
capture, not a hard memory bound. Preflight must not be run against a remote
Docker context or interpreted as proof that the image is currently deployed.

## Candidate owned lifecycle, not yet implemented

1. Establish the target host/Engine identity read-only and compare its current
   image ID with the desired exact image. Check resource availability and other
   active work. Generate a private random owner ID. Persist owner, exact image,
   host/Engine and create intent in an exclusively created private journal; do
   not log environment, credentials, private inspect fields or API query tokens.
2. Create a unique labelled Docker volume and a unique labelled container from
   the already-local immutable image with `--pull never`, `--network none`, no
   published ports, bind mounts, production data, GPU, devices, Docker socket,
   restart policy, or privileged mode. The proposed policy limits the
   container to one CPU, 1 GiB, 128 PIDs, drops all capabilities, prevents new
   privileges, disables logs, mounts only its labelled volume at `/var/www/data`
   with `volume-nocopy`, and bounds tmpfs for scratch/logs. The draft checks
   that Docker reports `NoCopy`; it has not been verified against actual
   Docker inspect output. Vendor startup may require the isolated
   writable container layer; this must be confirmed against the exact image,
   with no production config or credentials. Network-none and limits must be
   inspected after creation, before start or any test API. An image that needs
   unavailable privileges, configuration or network must fail closed.
3. Verify the exact returned container ID, unique name, owner labels, image ID,
   Engine ID, volume identity, all mounts, network, restart policy and limits.
   Use only a bounded non-detached `docker exec` in this owned container to
   reach its loopback API. Its probe code must use fixed methods/routes and
   bounded reads and timeouts. A CLI timeout does not prove an exec or daemon
   operation stopped: persist uncertainty and inspect the exact owned object;
   never replay a mutation on a lost acknowledgement.
4. Only after separately reviewing the actual image's NodeODX API and private
   task layout, initialize one uniquely assigned synthetic task and upload a
   tiny generated test input. Do not commit, run reconstruction, access a GPU,
   or use real client photos. Inspect the exact synthetic task; disconnect the
   test client and verify status, then stop and restart only the owned
   container with the same labelled data volume and repeat the inspection.
   A file marker or an uploaded input demonstrates only its measured
   persistence; `imagesCount` does not prove exact input identity. If exact
   file hashes cannot be read safely via a reviewed API/layout, report that
   gap rather than asserting it.
5. Remove only the exact synthetic task if a reviewed API and state allow it;
   ambiguous remove or unexpected task behavior is a hold. After all task and
   client processes exit, stop the exact owned container and re-inspect its
   identity and mount set. Remove it only if ownership is still proven; remove
   the exact labelled volume only after proving no container mounts it and the
   expected synthetic task/data are no longer needed. Verify both names absent.
   Preserve and report resources on ambiguous create, stop, identity, deletion
   or filesystem outcomes. Never run broad prune or delete a computed host
   directory; never stop or remove an existing service.

The plan cannot prove survival of a **completed real NodeODX result** without
first producing or obtaining one. A fabricated marker, uncommitted input, or
manually injected result file is not that proof. A separate, explicitly
reviewed real processing test with suitable authorized synthetic images and
resources would be required. A container restart is not a host reboot, Docker
daemon restart, power-loss test, or production data durability guarantee.

On 2026-11-04, eight mocked preflight/inspection tests passed. They invoked no
Docker command and did not test the lifecycle, Node probe, real inspect shapes,
API, user namespace, resource cleanup, or retention.
