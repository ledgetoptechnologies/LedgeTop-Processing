# Private deployment inputs for the first enrolled cutover

`ops.enrolled_coordinator_deployment` validates one closed configuration bundle
against the existing reviewed cutover plan. It does not activate a controller,
qualify a worker, change a container or modify NAS files. The example under
`docs/examples/enrolled-cutover-private-bundle.example.json` is synthetic: its
image/source/host/file pins are placeholders and all qualification gates are
absent. It cannot authorize activation.

Create actual bundles only as operator-owned private mode-0600 files. Do not put
them in Git. Tokens, private keys, environment contents, client files and census
keys are forbidden bundle fields; only exact existing private paths and hashes
are referenced. The file reader refuses duplicate keys, nonfinite JSON, unknown
fields, symlinks, shared files, foreign owners and oversized input.

Application configs are different from operator-local bundle/environment/key
files: they remain UID/GID10001 mode0600 for the existing container identity.
The operator does not need permission to read them directly. Their references
include host `path`, candidate `containerPath` and SHA-256.
`read_config_from_predecessor` revalidates the exact original immutable recipe
and environment, then reads through its existing read-only config mount using
Docker's bounded non-following TAR readback. Direct files use the single-file
reader. An operation directory named `first-cutover-<operationId>` uses a closed
directory archive: UID/GID10001 mode0700 directory and exactly two mode0600
regular files, `bootstrap.json` and `final.json`, with the reviewed hashes. A
symlink directory, member, unknown entry or incomplete set is refused. Candidate
recipes may bind that operation directory to `/run/processing-reference`; the
original readback path is derived independently from its original mount.

Private environment validation explicitly disables the native dispatcher in
both application recipes, enables the legacy read-only adapter only in final,
preserves original state/authority-lease paths and node IDs, and resolves the
reference-config path through its reviewed read-only bind to the pinned private
host file. Loader/path overrides are refused. These checks prevent a correctly
hashed environment from selecting a different execution mode or source path.

| Field | Binding |
| --- | --- |
| `profile`, `operationId`, `authorityId` | Exact existing first-cutover mode and reviewed operation |
| `host` | Independently observed Docker Engine/host-boot identities, fixed Unix socket, CLI path and SHA-256 |
| `predecessor` | Exact enrolled container/image/source/name, original state volume and external lease mount |
| `candidateSourceCommit`, `reviewedSourceRoot`, `sourceHashes` | Immutable candidate source pin and the existing startup-policy builder's exact reviewed source inventory |
| `recipes` | Separate closed bootstrap/final/holder recipes; application recipes match the plan hashes and same candidate image |
| `holderSettings` | Operation/authority and original physical paths; the full plan digest is derived after loading the plan |
| `artifacts` | Private config/environment/policy/schema/key/counts file references, cross-bound to existing plan and recipe pins |
| `nodeEndpoints` | Existing startup-policy engine origins and reviewed order for the two enrolled nodes |
| `workers` | Existing SSH host/actor UID, worker-handover settings and current public readiness expectations; credentials remain in the private worker config |
| `qualifications` | References to the existing reviewed gate artifacts; `null` stays unqualified |

The canonical bundle SHA-256 must equal
`plan.bindings.deploymentSha256`. The bundle cannot contain its own digest or the
full plan digest: the latter already includes the deployment digest. The holder
receives `planSha256=digest(reviewed_plan)` only after validation, avoiding a hash
fixed point. Its actual command/ownership label also binds the serialized helper
source and these derived settings.

`verify_private_artifacts(bundle, plan)` first revalidates the plan binding,
checks the existing private file hashes, then reconstructs the startup policy
using the existing `build_startup_policy` from reviewed source/config/schema
bytes. The regenerated policy must match the pinned file exactly. It never
learns expected schema or rows from a candidate database. A caller requiring all
qualification gates refuses missing gates before reading private files.

Hash presence is not fresh runtime readiness. The phase driver must still check
actual Engine/boot/container/mount/source identities, current authenticated
worker readiness, original kernel flocks, stopped writers, authenticated frozen
Gate ownership and live source custody. Old recovered observer results establish
historical evidence and descriptor retirement; a new control effect requires a
new current read-only boundary challenge. The bundle supplies inputs to those
existing authorities and does not replace them with a ready boolean.

This first-cutover profile deliberately follows the already reviewed policy's
exact cluster/main inventory, bootstrap-v2/final-v6 settings and disabled source
release. Later inventory changes and controlled source-release enablement remain
separate work in the full workflow goal. Ordinary runtime modes are unchanged.

## Authorized staging preparation ABI (not executed)

Missing staged files are work to prepare on the processing PC, not a request to
change TrueNAS or original file permissions. A later qualified preparation
action can use the same immutable image and UID10001 with network disabled,
read-only root filesystem, no state/lease/NAS mounts and only the exact existing
config directory bound writable. It must verify the original directory identity,
create its operation subdirectory exclusively, then create only the two known
config files with no-follow/exclusive mode0600, complete writes, file fsyncs and
both directory fsyncs. It must never overwrite current configs or alter existing
ownership/modes. Private payload bytes travel over stdin, not argv or images.

The preparation receipt must bind operation/plan/image/config-root identity and
both content digests. Helper creation and start are separately durable intents;
unknown outcomes inspect the exact owned helper/subdirectory and never blindly
retry, delete partial evidence or promote bytes without closed readback and
actual helper retirement. The subsequent ordinary cutover preflight still
validates all configuration hashes and the independently generated policy.
The separate `enrolled_config_stage` module implements this preparation. It is
not invoked by bundle validation and still requires actual Docker qualification.

`TrackedWorkerProbe` reuses the private remote-exec lifecycle with explicit
reviewed actor UID and a host-bound envelope, separate from Docker container
identity. SSH uses the existing `bkoltz` key and strict known-host verification;
the fixed reviewed interpreter is POSIX-quoted, isolated and bytecode-disabled.
Recovered results prove historical body completion and process retirement,
never fresh worker readiness. The fresh authenticated readiness probe and phase
driver remain responsible for current settings/source/unit/engine identity.
