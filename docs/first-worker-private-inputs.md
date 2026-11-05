# First worker: remaining private inputs and activation binding

This is a static input map, not evidence that the installed coordinator is
bootstrap-frozen or that either absent worker is ready. Existing installed
metadata and staged material must be refreshed by the same cutover owner.
There are no generated credentials or executable activation effects here.

## Authoritative worker configuration inputs

`ops/reference_worker_daemon.py:load_config` is the authoritative closed schema.
Populate each exact field from the following sources; do not copy test fixtures
or infer a worker instance from a node name.

| Exact worker fields | Authoritative input and necessary comparison |
| --- | --- |
| `nodeId`, `workerId`, `workerInstanceId` | Exact node key and `workers[node].workerId` / `.instanceId` in the private coordinator reference runtime selected by the cutover plan. Match the private bundle's expected worker identity. |
| `workerBearer`, `resultReadBearer` | Existing `workers[node].bearer` / `.resultBearer` from that same runtime, handled only in memory/private files. Preserve separation; never log or place in argv. |
| `authorityId`, `engineId`, `gateGeneration`, `gateCoordinatorOrigin` | Existing runtime `recoveryBindings[node]` compared with original canonical NodeGate JSON and physical Docker Engine identity. All nodes share the actual enrolled authority; do not mint or increment it. The Gate origin can differ from the new reference TLS origin: preserve its original binding. |
| `coordinatorOrigin`, `brokerOrigin` | Actual configured reference TLS worker API/source-broker endpoint, correlated with the selected coordinator listener/mount/private CA and tracked private bundle. The reference coordinator is normally `https://192.168.50.89:18443`; verify the actual candidate mapping rather than assigning this from the Gate origin or a fixture. |
| `engineOrigin`, `engineToken`, `engineVersion` | Exact current private native endpoint/credential and `/info` version. Main currently loopback port 13004, engine 3.8.5; cluster port 13001, engine 3.6.2. Recheck physical container/image/catalog pins. Never put native credentials into result/catalog responses or use the public ingress as the worker engine. |
| `caFile`, `serverCertFile`, `serverKeyFile` | Existing per-host Processing TLS files, validated owner/mode/nofollow, chain, actual IP/loopback SAN, key match and validity. Coordinator trust must match the worker CA and vice versa. Existing certificate records are a lead, not current expiry proof. No new CA/key issuance is needed if the originals pass. |
| `listenHost`, `listenPort` | Actual allowed worker listener matching `workers[node].resultOrigin`, normally that host's IP with port 18445. Confirm no conflicting listener and the existing host firewall policy. This is the existing worker endpoint, not a Docker daemon exposure. |
| `stateRoot`, `scratchRoot` | NEW operation-specific absent paths beneath the actual existing service user's home, matched by installer checks. No old state, scratch databases, receipts or lease adoption. |
| `outputBudget` | Exact runtime `recoveryBindings[node].outputBudget`, matching worker recovery and scheduling expectations. This is a disk/output reservation, not a photo limit or CPU cap. |
| `safetyFloor`, `transferTtl`, `cleanupGrace`, `pollSeconds` | Explicit reviewed deployment policy passed to existing ScratchLedger/Driver. Derive from the approved private settings; do not invent fixture defaults or infer from hardware alone. |
| `shareResults` | Explicit `true` required by current worker schema. |
| `allowedEnvironments` | Exact approved production/staging sharing policy. Preserve origin-specific credentials and production priority; the field does not create another scheduler. |
| `cleanupEnabled`, `engineCleanupEnabled`, `viewerReceiptKeys` | Existing reviewed cleanup choice and actual Viewer signed-copy trust map, identical where required to coordinator trust. Enabling either cleanup requires nonempty validated signed-copy pins. Do not use an ephemeral test issuer. NAS source-release remains a separate coordinator feature/role and first-profile policy. |
| `sourceRangeV2Enabled` | Explicit reviewed V2 whole/range policy compatible with the selected source-grant renewal/fetch recovery code. A staged old source archive is not proof that the latest recovery fix is present. |
| `allowedOdmOptions` | Omit or `[]`; dynamic definitions come from the existing authenticated worker catalog relay. Static option allowlists are rejected. |
| `executionRouting` | Existing per-job route: `{schemaVersion:1,mode:"per-job-v1",treedar:null}` for ODM-only enrollment. A main TreeDar object requires its actual installed image/source/version and separate qualification; do not fabricate it or treat an omitted route as TreeDar readiness. |
| `maintenanceReadiness` | Exact `{schemaVersion:1,bearer,sourceRoot,sourceSha256,activation:null}`. Reuse an already explicitly staged separate readiness credential if available; otherwise prepare one NEW per-host private read-only-role credential, distinct from all claim/result/native roles. Source root is the manifest's new `/opt/.../<operationId>/source`; hash is its exact installed Python footprint. No existing maintainer activation may be borrowed. |

The first-enrollment manifest also needs actual hostname/boot ID/Docker Engine
ID, service account UID/GID/home, exact original Gate path/hash and bindings,
physical native container ID/image/version/loopback port/catalog hash, the NEW
source/dependency/config descriptors and the raw rendered unit SHA-256. Source
and dependency descriptors are now tangible in the dated artifact receipts.
Private config descriptor and final refreshed physical pins are still missing.
Changing source requires recomputing its archive/footprint, config digest,
rendered unit expectations and private bundle pins together.

## Safe generation path and existing tooling boundary

Do not rerun `ops/provision_reference_config.py` to generate worker configs. It
creates a legacy staging-only coordinator credential skeleton with fixed example
worker IDs/instances; it has no WorkerDaemon configuration writer, maintenance
readiness credential, recovery/source/Viewer trust inputs or broker contract.
Its historical receipt explicitly says worker intake is not configured.

A narrowly scoped generator should read the exact selected private runtime,
the per-host native credential file and TLS descriptors using owner-private
bounded nofollow reads. It should compare all public identity/recovery/bundle
pins before composing bytes, reject any conflicting roles, and write only a NEW
operation-owned 0600 staging config via exclusive create. No stdout values,
credential argv, environment dumps, native mutation, lock creation or service
calls are needed. Emit the config SHA/length/owner and field-validation result
only. It must not instantiate `build_worker`, SourceSpool, Driver or NodeGate.

The existing installer subsequently runs real `load_config` as the service user
against the installed immutable source/dependency environment. This validates
schema/private-file access without starting a worker. Actual TLS nonce,
NodeGate/Driver/source/process readiness is a distinct post-start proof.

The package versions were already approved repository pins; the recorded actual
binary wheel hashes supply reproducibility, not an additional user approval
gate. They do not claim a full upstream supply-chain audit or hermetic OS image.

## Exact bootstrap-frozen activation prerequisite

The tracked existing cutover action `seal-bootstrap-gate` ends at
`bootstrap-frozen`. Before starting a first worker, the same operation owner
must authenticate `/v1/prototype/coordinator-freeze` and compare its current
authority, startup ID, plan ID/owner and phase against the committed
`start-bootstrap` / `seal-bootstrap-gate` journal evidence. Do not substitute a
saved DB row, a fabricated frozen boolean, final-startup ID, or a fresh unrelated
plan. Independent physical source/lease custody and old writer retirement still
apply. No such live observation has been asserted by this static document.

Then verify the original per-host idle NodeGate/kernel lease/engine/task census,
unit/config/source/interpreter pins and actual systemd manager startup
environment. Journal one exact start intent before the existing
`SystemdWorkerControl.command('start')`; uncertain start outcomes are inspection
only. Reuse `ReferenceWorkerHandover.ready` without `__enter__`/`stop_old`, or
`enrolled_worker_readiness_probe` plus its tracked SSH retirement envelope.
Require full current nonce/config/source/authority/instance/engine/generation
proof, with activation `null` for this initial enrollment, before the same
cutover owner advances final-readiness and reopens claims.

The current installer intentionally does not implement this activation action.
An authenticated initial-start adapter/checkpoint in the existing cutover
workflow is the remaining executable integration seam; it must not become a
second controller. Main's exact root installation invocation additionally needs
administrator execution. No other user input has been identified by this audit.
