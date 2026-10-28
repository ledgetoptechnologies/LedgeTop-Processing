# Startup/API/options-only candidate qualification

## Actual main-PC acceptance — October 28, 2026

Committed source `65b7015` passed the real isolated startup check on
`192.168.50.88`. The candidate returned NodeODX API 2.4.0, engine 3.8.5 and 80
fresh options. The exact candidate was verified running after the probe, then
stopped and removed, including its temporary files and writable layer. An
independent read-only review verified all 68 staged source files, receipt and
SQLite hashes, container absence, and the unchanged installed main service.
See [actual acceptance evidence](evidence/main-nodeodx-startup-20261028.json).

The supervisor is `qualified` at generation zero with zero schedule/run
intents. The installed node still uses its original image and start time.
This is startup/API/options acceptance; reconstruction and GPU processing remain
unqualified. The candidate build reports the same engine version as the
installed node. No processing data was submitted.

The first read-only-root attempt failed because the vendor logger needs a
writable application directory. A second attempt served the API but exposed the
Node launcher's version banner before JSON. Both failed test containers were
removed after exact ownership and terminal-state proof; their held journals and
failure closures remain separate from the successful receipt. These real
findings led to the explicitly bound disposable-layer and launcher-output
policies described below.

## Source and scope

`processing/update_qualifier.py` implements the existing supervisor `Qualifier`
protocol for Linux hosts. Its one-shot main-PC startup acceptance does not
install an unattended updater, approve processing/GPU acceptance, or replace the
running node.

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

The actual `DockerRegistry.inspect_staged` proves previously definite staging,
the immutable repository manifest/config descriptors, platform, physical Docker
Engine identity, and the **actual** local Docker image ID. Classic stores may
report the config digest as `.Id`; Docker's containerd store can report the
manifest digest instead, requiring its exact matching manifest `Descriptor`.
The registry proves this mapping; the qualifier never guesses a local ID from a
config or manifest digest. Staged metadata records both `configDigest` and
`imageIdKind: config|manifest` beside actual `imageId`.

Supervisor requests and qualification receipts must include `localImageId` from
that actual staged `imageId`, separately from artifact `configDigest`. Creation,
default-image inspection, container-image checks and terminal-stop proof all use
`localImageId`. Evidence retains all three identities and the proven image-ID
kind. A config digest is never silently substituted when the runtime reports a
manifest ID. Qualification never pulls or promotes an unknown pull.

A private OS-exclusive lease and FULL SQLite journal protect one local
qualification at a time. The exact supervisor request, policy, candidate,
physical node/engine/authority, host generation, and own receipt directory are
bound before Docker mutation. Every create, start, exec, stop and removal has a
durable intent first. Repeated calls never replay a nonterminal mutation.

The candidate uses its exact proven local image ID with `--pull never`, network
`none`, no ports, no mounts, no Docker socket, no production credentials, no GPU
device requests, all capabilities dropped, and no new privileges. The frozen
`filesystem_mode` accepts only `ephemeral-node-defaults-v1`: the candidate has
its own **ephemeral writable root layer** and must report
`ReadonlyRootfs:false`. No `--read-only` flag is passed. Private tmpfs remains at
`/var/www/data`, `/var/www/tmp`, `/var/www/logs`, and `/tmp`. Its reviewed user is explicit. Docker
logs are disabled. No CPU, RAM, PID, or thread limits are introduced, including
on main. Startup is bounded by command deadlines; that is not a processing
resource qualification.

This filesystem profile accommodates the vendor's default startup behavior:
the Node logger checks write access to its default log path, which can be the
`/var/www` application directory. The default command and configuration remain
untouched. The writable layer is not a host bind or production volume and does
not grant additional Docker privilege. Its explicit policy field changes the
qualification policy hash; an earlier read-only-profile journal is not replayed
or silently converted. A fresh reviewed context is allowed only after separate
proof closes the prior failed attempt. Its held journal and evidence remain.

The separately frozen `launcher_stdout_mode` accepts only
`node-or-nvm-use-banner-v1`. The probe output must be raw JSON or exactly one
anchored ASCII NVM launcher line, `Now using node v<semver> (npm v<semver>)`,
ending in LF or CRLF and at most 96 bytes, immediately followed by the JSON
object. Node/npm versions are individually bounded. ANSI output, diagnostics,
unknown prefixes, multiple banners, extra blank lines after a banner, malformed
versions, and trailing output are rejected. No arbitrary lines are searched,
trimmed, or discarded. The fixed GET probe itself is unchanged.

Evidence records `launcherStdoutMode`, `launcherPrefixKind`, typed
`launcherNodeVersion`/`launcherNpmVersion` (null for raw JSON), and the exact
`launcherPrefixSha256`; the raw banner/stdout is not retained. These launcher
versions are distinct from the actual engine version reported by `/info`.
The new mode changes the policy hash. An earlier held launcher attempt is not
replayed or declared successful; it needs separate owned-container closure and
a fresh explicit policy context before another attempt. This compatibility
change is not evidence that newer engine software was installed.

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
disposes its private tmpfs output and writable container layer. Evidence records
`filesystemMode` and sets `containerWritableLayerDisposed:true` only after that
exact-ID absence proof. This is container-resource disposal, not a claim of
physical secure erasure. The sanitized evidence and receipt remain in
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
