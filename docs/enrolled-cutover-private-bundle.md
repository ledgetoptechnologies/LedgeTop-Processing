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
| `workers` | Existing worker-handover settings plus current public readiness expectations; credentials remain in the private worker config |
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
