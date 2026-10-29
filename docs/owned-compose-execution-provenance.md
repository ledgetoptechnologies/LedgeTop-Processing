# Owned Compose execution provenance and private ingress

This source candidate fixes two blockers found before an actual empty-worker
Docker acceptance run. It is inactive until independent source/ownership/crash
review and actual Linux qualification. It does not change worker recovery or
indexed schema/policy, publish packages, execute updates, or migrate old receipts.

## Private ingress binding

An operator-owned `gateway.json` may explicitly set `listenAddress` to a
canonical fixed IP. `127.0.0.1` produces only its IPv4 listener, and `::1`
produces only its IPv6 listener. No wildcard listener from the other family is
silently added. Hostnames, zone identifiers, aliases, whitespace and nginx
syntax are refused. Omitting the field preserves the existing dual wildcard
behavior exactly. The proposed lab explicitly selects IPv4 loopback for both
initial configuration and subsequent close/reopen reloads.

## Original source and actual executed recipe

Docker Compose records the file it actually executes in
`com.docker.compose.project.config_files`. The updater previously executed a
receipt's `resolved-compose.json`, whereas worker verification required the
original `compose.json` label. Unit FakeCLI fixtures did not establish this real
Docker behavior. The worker-aware adapter now requires provenance for the
actual immutable resolved artifact, rather than pretending that its label is
the original source file.

`WorkerComposeNode` hardcodes mandatory provenance; there is no job, HTTP or
configuration switch to disable it. Generic non-worker `ComposeNode` retains
its prior behavior for compatibility. That compatibility is not a safe way to
bypass worker maintenance. Installed legacy update services/entrypoints must
be explicitly migrated or disabled before production worker activation.

The worker path independently resolves the original source through the real
Compose CLI and pins its raw bytes/identity. It then resolves the exact image
override, checks existing hook/scaling rules and requires the result to equal
the original resolved model with only image and the fixed `pull_policy=never`
override. Source bytes must remain unchanged across that observation.

The private source directory contains `.compose-executions/` with an immutable
registry binding, reservation records, content-addressed artifacts and separate
registration records. The registry binds the actual source-parent/registry
inodes, canonical source path, project, service and source model hash excluding
only the selected image. A reservation is durable before its artifact is
created. A registration pins the exact artifact bytes/hash/path/inode and its
reservation digest before a Docker command can execute it.

The actual `up` command uses that registered artifact. Per-operation
`compose-execution.json` separately retains the current original-source
resolution and registry proof. Original image pinning may legitimately change
the source file during the existing owned CAS; the non-image source model
remains fixed. Artifact-creation source identity remains preserved separately.

Later operations and rollback reuse exact registered artifacts from this same
source registry, retaining all older artifacts. They do not accept arbitrary
prior receipt paths, labels, same-byte replacement inodes, foreign source roots,
project/service/image substitutions, unknown registry files or orphan blobs.
Previously generated legacy `resolved-compose.json` paths without this
provenance are held. They need a separately reviewed migration or an explicitly
drained re-establishment from authoritative source; no automatic adoption exists.

## Bounds and interruption behavior

At most 64 recipes of at most 1 MiB each are retained, with bounded 16 KiB control
records. Reusing an existing exact recipe, including rollback at the cap, does
not allocate another entry. Exhaustion holds without TTL deletion or pruning.
Only complete canonical reservations/registrations are accepted. Unknown or
partial registry creation/control records hold for investigation. A known
reservation interrupted before artifact creation, or after a complete artifact
fsync before registration, may finish only in the same original operation with
unchanged source resolution. The durable reservation binds that operation
directory's device and inode as well as its canonical path; recreating the
same path after moving the original directory is held. Partial artifacts are retained and held rather
than overwritten. A lost Docker execution response leaves exact recipe/proof
material intact; the existing update receipt and no-replay protocol still own
the outcome. A recipe hash alone is not evidence that execution happened.

Host-only tests cover configuration, artifact identities, sequential image
transitions, same-version/new-image distinction, retained rollback, wrong
source/path/project/service/image/content, bounded history, interruption windows
and unknown execution outcome. They exercise no real Docker service. POSIX
ownership/fsync and actual Compose namespace/update acceptance remain separate
Linux gates. Windows path/descriptor ctime use different OS meanings; only that
descriptor-field comparison is platform-specific, while full path snapshots
remain stable before/after reads and POSIX comparisons remain strict.
