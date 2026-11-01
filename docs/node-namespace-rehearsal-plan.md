# Isolated node network namespace rehearsal — review required

Status: proposed only. No container, network, image pull, firewall rule, or live service has been changed. This rehearsal cannot establish the current production nodes' safety.

## Goal and boundary

Test whether an engine bound only to loopback inside a dedicated Docker network namespace is reachable by a broker sharing that namespace, while an unrelated container and an unprivileged host process cannot reach that same listener through the ordinary host and container network paths. No engine ports will be published. The engine and broker will use synthetic HTTP payloads and a unique per-run response marker; no client images, credentials, datasets, or production paths are used.

Trust host root, Docker administrators, processes with namespace-entry capabilities, and anyone allowed to attach containers to the broker's network namespace. Those principals can bypass or reconfigure this topology. Namespace isolation is not a security boundary against them. A Docker administrator can create another container with `--network container:<broker>`. This test does not prove exclusive authority, durable all-node admission, restart recovery, result delivery, network isolation against host administrators, or safety of existing direct NodeODM routes.

## Owned resources and limits

Before execution, inspect Docker availability and existing resource names read-only; do not print environment variables. Select an already-local image with a verified immutable image ID containing the required runtime. If none exists, stop for review of a specific immutable image and acquisition; do not pull a floating tag. Limit the rehearsal to one broker namespace holder, one synthetic engine sharing it, and one unrelated probe, all with no restart policy, no bind mounts, no access to Docker socket, no privileged mode, no added capabilities, no published ports, bounded memory/CPU/pids, and non-root users where the image supports them. Do not modify existing networks, containers, images, firewall, or host services.

Assign a random run identifier; use it in resource names and a dedicated Docker label. Record IDs and labels immediately after each successful create, along with image ID, owner process and start time. Start each owned container only after verifying its ID, expected label and exact configuration. Track any task-owned subprocesses and wait for their exit. Keep the test under a short bounded deadline; stop on setup, identity, timeout, or policy mismatch.

## Proposed topology

1. Create a minimal broker namespace-holder with `network=none`. Keep its process alive only for the bounded test.
2. Create a synthetic engine with network mode `container:<exact broker container ID>`. It binds a randomly selected high TCP port on `127.0.0.1` within the shared namespace and serves only a short unique marker. The process must not bind `0.0.0.0` or IPv6. No port is published.
3. Create a separate, unrelated probe with `network=none`, also without capabilities or mounts. It must not share the broker/engine namespace. It probes its own loopback port with a strict deadline; it must not receive the marker.
4. Run the positive broker-side probe inside the broker namespace: it must retrieve exactly the run marker. Inspect the owned engine and holder by exact IDs to verify the expected network mode and absence of published ports. Inspect the unrelated probe by exact ID to verify its distinct namespace.
5. Run a bounded host-side unprivileged loopback probe to the same port. If a host listener already occupies it or yields an unrelated response, classify the host check as inconclusive; do not interfere with it. A host probe must never receive the marker. Account explicitly for IPv4 and IPv6 when determining that no host port was published.

The broker's `network=none` namespace cannot accept remote coordinator traffic by itself. A production design would need a separately authenticated, narrowly scoped control channel, such as an explicitly owned Unix socket or a carefully designed relay; that channel and its durability are not tested here. The existing broker adapter expects a Docker-published host-loopback port, so this topology is incompatible with the current adapter until separately changed and reviewed. Do not flip a qualification flag.

## Test outcomes and cleanup

Pass only if the positive probe returns the exact marker, unrelated probe does not, engine has no host-published port, and inspected namespace modes match. Report the host result separately, and classify timeouts, unavailable images, host port conflicts, missing isolation primitives, or identity differences as inconclusive/failure rather than a pass. Never assert that an unprivileged container probe proves host or Docker-admin isolation.

Before stopping or removing anything, verify each exact recorded resource ID still carries this run's label and is not shared or user-owned. After independent review of this plan and cleanup method, stop/remove only verified task-owned containers by their exact IDs and verify they exited. Never use broad Docker prune, wildcard removal, recursive filesystem cleanup, or remove an image. If ownership or cleanup authorization is uncertain, leave the resource untouched and report it. Do not stop existing production or user containers.

## Review amendment: routed probe and namespace evidence

The unrelated `network=none` loopback check alone is insufficient. Add a fourth, separately owned probe on a newly created, task-labelled **internal bridge**. Inspect its network identity, non-loopback interface, and namespace link; compare `/proc/self/ns/net` from all four containers. Broker and engine must share exactly one namespace; the bridge probe and none-network probe must each differ from it and from one another. Inspect `/sys/class/net` in the broker namespace and require only `lo`; inspect `/proc/net/tcp` and `/proc/net/tcp6` there to require exactly the expected synthetic IPv4 loopback listener and no wildcard or IPv6 listener on that port.

Run the bridge probe against the bridge gateway and against its own non-loopback address at the chosen port. The broker namespace has no non-loopback address to route to; therefore negative gateway and probe-address checks are supporting evidence only, not a routed packet sent to the broker. Mark the bridge result inconclusive if its address or gateway cannot be established. Host loopback must also be tested separately; local port conflicts make the host result inconclusive. Network namespace and socket observations are point-in-time, and Docker administrators can change the topology immediately after inspection.

For each created container and the bridge, capture the full returned ID, verify its exact name and run label through an ID-specific inspect, and verify its expected mode before starting or using it. Reconcile each create even if the CLI times out: inspect by exact unique name and expected label before deciding whether a resource exists. Never assume a missing CLI response proves creation failed. Do not stop, remove, or alter a resource if its exact ID, name or run label does not match. The proposed runner retains stopped task-owned containers and network for review and performs no deletion. Review the script and the precise stopping operation independently before any Docker action.

## Runner and watchdog amendment: awaiting independent review

The staged runner uses four owned containers (namespace holder, shared-namespace engine, isolated none-network probe, and bridge probe) and one labelled internal bridge. It waits for the engine's actual HTTP response before reading the listener table. A test pass is printed only after every recorded container is re-inspected by exact ID/name/label and confirmed stopped and the bridge identity is rechecked; a stop or verification failure returns a nonzero/inconclusive result. It never removes containers, the bridge, or images.

The separate POSIX outer watchdog starts only this runner in a new process group with a 90-second deadline. On timeout it signals that owned group while its leader remains alive, waits for exit, then checks only the four exact per-run names, verifies their full ID/name/label before any stop, and retains resources. The Docker daemon may finish a timed-out create after that reconciliation, so the watchdog always reports an uncertain outcome and never claims successful cleanup. The run identifier and names must be retained for any later inspection. Its timeout handling and signals require independent review before execution; no action has been taken yet.

The watchdog does not treat the child leader's exit as proof that its descendants exited. After the leader is reaped, it neither probes nor signals that numeric process group again, because the identifier could have been reused. A descendant may remain or may have changed session; descendant cleanup is unverified, so watchdog timeout remains inconclusive and requires ownership-aware inspection.

## Windows Job Object watchdog amendment: awaiting review

The staged Windows watchdog uses the previously reviewed suspended-child pattern: create the runner suspended, assign its process handle to an owned kill-on-close Job Object, then resume. It applies a 90-second timeout, terminates only its owned Job on timeout, checks Job accounting, and reconciles only the exact run names/full IDs/labels before stopping task-owned containers. It withholds the inner runner's PASS output until Job and resource verification finish. Failure or timeout remains inconclusive because Docker daemon creates can complete later and processes outside the Job are not accounted for. It does not delete containers, networks, images, or files. The Windows adaptation has not been executed and needs independent review of API bindings, Job accounting, timeout paths, ownership checks, and reconciliation before any test.
