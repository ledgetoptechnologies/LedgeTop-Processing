# Startup/API/options-only candidate qualification

`processing/update_qualifier.py` implements the existing supervisor `Qualifier`
protocol for Linux hosts. It is source only: no adapter is installed or enabled,
no channel is approved, and no candidate has been started by this change.

The policy is deliberately `startup-api-options-only-v1`. Success proves that a
definitely staged linux/amd64 image started with its reviewed defaults, exposed
the expected actual `odm` or `odx` `/info` identity and a valid fresh `/options`
catalog, then its exact private candidate container was stopped and removed.
It does **not** qualify photogrammetry/reconstruction, GPU initialization or
kernels, real imagery, accuracy, throughput, or production resource use. The
evidence explicitly marks these as unqualified. A supervisor may accept this
receipt only under an explicitly approved startup-only policy; it is not a
substitute for a later processing acceptance policy.

## Composition

Construct `StartupPolicy(qualifier_id, engine_id, node_id, authority_id,
expected_engine, startup_spec_sha256, supervisor_state_root, run_user, ...)`
and `StartupQualifier(path, policy, registry=actual_registry, runner=None)`.
The object exposes the exact supervisor `.binding`, `.qualify(request)`, and
read-only-Docker `.lookup(request)` contract. Its binding `policySha256` covers
the frozen policy, registry policy, exact fixed probe source, and startup-only
scope. Use that value in `SupervisorPolicy.qualification_policy_sha256`.

`startup_spec(image)` hashes the reviewed image metadata map with exactly
`User`, `WorkingDir`, `Entrypoint`, `Cmd`, and `Volumes`. There is no automatic
learning of this approved hash. Review it separately for the intended candidate
policy. Current production metadata is only a baseline, not approval of a new
candidate. The default Node layout is `/usr/bin/node`, working directory
`/var/www`, with no declared image volumes; unknown defaults, credential-bearing
arguments, remote origins, or unexpected layouts fail closed. Raw arguments are
not printed or stored; only their approved digest is journaled.

The caller supplies no command, code, URL, mount, production configuration,
environment credentials, or token. The default image startup remains unchanged.
There is no synthetic config override or production config reuse. An image that
cannot expose these GET routes with its isolated defaults fails this policy;
supporting another layout requires a separate explicit review.

## Isolation and lifecycle

The actual `DockerRegistry.inspect_staged` proves previously definite staging
and the live local config ID, immutable repository manifest digest, platform and
physical Docker Engine identity. Qualification never pulls an image, promotes
an unknown pull, or confuses a manifest digest with a local image ID.

A private OS-exclusive lease and FULL SQLite journal protect one local
qualification at a time. The exact supervisor request, policy, candidate,
physical node/engine/authority, host generation, and own receipt directory are
bound before Docker mutation. Every create, start, exec, stop and removal has a
durable intent first. Repeated calls never replay a nonterminal mutation.

The candidate uses its exact config image ID with `--pull never`, network
`none`, no ports, no mounts, no Docker socket, no production credentials, no GPU
device requests, read-only root, all capabilities dropped, and no new
privileges. Its only writable paths are private tmpfs at `/var/www/data`,
`/var/www/tmp`, `/var/www/logs`, and `/tmp`. Its reviewed user is explicit. Docker
logs are disabled. No CPU, RAM, PID, or thread limits are introduced, including
on main. Startup is bounded by command deadlines; that is not a processing
resource qualification.

The only exec command runs a fixed Node.js constant. It makes internal
`GET http://127.0.0.1:3000/info` and `GET /options` requests. It cannot submit,
upload, commit, cancel or remove a processing task. There are byte bounds,
request idle timeouts, and an independent 65-second in-process hard watchdog
inside a 70-second host exec deadline. The watchdog covers trickling responses
that would defeat idle timeouts. This startup lane launches no heavy processing
job and does not implement production execution admission.

The host validates actual container ID/image/ownership label, default arguments,
user, working directory, network, ports, mounts, tmpfs, privilege settings and
resource settings before start and after terminal stop. It also rechecks that
the exact server remains running after the probe and before its own stop intent;
an already exited/crashed server cannot pass. This is not a soak-test claim.
A stopped container
must report non-running and PID zero. Only then is removal attempted. Successful
exact-ID absence queries, bracketed by the same physical Engine identity, prove
removal; names and generic inspect errors never prove absence. With no writable
host mounts or persistent image volumes, removing the own stopped container
disposes its private tmpfs output. The sanitized evidence and receipt remain in
the journal. No raw API body, default command, Docker stdout/stderr, credential,
task data, or image is stored in the receipt.

## Recovery and remaining acceptance work

`.lookup` performs only engine identity checks and successful read-only exact-ID
container listing. It never creates, starts, execs, stops or removes a container.
It can reconcile a lost removal response only when a matching container ID,
sanitized API evidence and verified terminal-stop proof were already durable.
Missing a container before those facts were durable cannot manufacture success.
Unknown create/start/exec/stop outcomes remain held for separate operator
inspection; a still-running candidate is not automatically stopped on restart.
No unknown or foreign container is adopted or removed.

The existing photo-smoke tests contain marker-only JPEG blobs and supplied
reconstruction JSON, not a real multiview processing fixture. The previous real
photo trial cannot be reused under the no-real-photos requirement. A future
processing qualifier needs a pinned deterministic textured multiview synthetic
scene, actual reconstructed-artifact acceptance, and the same physical/global
execution admission as production (or an explicitly approved isolated off-host
lane). Network isolation alone does not prevent concurrent resource use. These
remain required before claiming processing or GPU qualification.

Tests use real SQLite, the actual registry/supervisor components, fake Docker
lifecycle/response transport, and actual fixed Node probe loopback-wire tests
when an existing Node.js executable is available. They do not launch a candidate,
pull an image, contact production, or access NAS data.
