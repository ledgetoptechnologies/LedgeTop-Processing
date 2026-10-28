# Original combined-parent preparation

`CombinedPreparationPolicy` is an explicit, default-off constructor profile for
the actual `ReferenceDriver`. No daemon configuration, installed capability or
runtime is enabled by this source change. Its only successful outcome is
`combined_prepared_held`, with reason
`combined_metadata_and_final_transport_unqualified`. That state grants no engine,
scientific validation, metadata custody or final delivery permission.

The profile admits only an original main-node combined ODM→TreeDar submission.
It uses the existing authenticated worker claim and broker download paths,
actual driver journal, ScratchLedger and NodeGate. It retains the original
submission, claim request/token binding, generation, transfer reservation and
input paths. It creates no child claim, rewritten manifest or second scratch
reservation. Driver startup rejects conflicting standalone execution/delivery
history and incompatible profiles; it does not migrate that history.

## Preparation order and uncertain outcomes

Before claiming, the driver persists the immutable profile and original claim
request ID. After the response it persists the bounded original claim binding
and reservation request before reserving the whole original input/output budget.
If saving the larger claim record then fails, the same request ID and exact
original reservation are reconciled. A changed response binding holds.

After verified download and the coordinator's processing acknowledgement, the
preparer commits a bounded `reserve_pending` identity in the existing driver
database. It includes the full parent binding and planned receipt, plus digests
of the original transfer and input paths. Fresh authenticated running claim,
immutable backend runtime and idle-engine checks precede physical reserve.
`bind_pending` is committed before bind, with another fresh admission check.
The actual driver OS lease and private owner bind the entire operation.

A lost local reply does not authorize blind reserve/bind replay. A read-only
NodeGate snapshot can prove the exact original receipt, physical generation,
authority, engine, origin and null completion digest. An exact unbound receipt
can reconcile committed reserve before a distinct freshly admitted bind; an
exact original job/worker binding can reconcile committed bind. Missing,
conflicting, malformed, orphaned or advanced state holds with the original
intent retained. In particular, an unknown bind whose receipt remains unbound
is not automatically retried. No gate release or cleanup occurs here.

The preparer constructs the concrete PipelineJournal, DriverClaimTokens,
ProductionParentGuard and OriginalClaimRecovery together. A failed constructor
does not cache partially attached components. Both stages stay unissued. Recovery
preparation publishes the actual typed control charge and bounded metadata under
the original parent and same receipt. A fully prepared coordinator restart
reconstructs that existing parent and resumes the same original claim using the
narrow preexecution recovery proof, followed by fresh normal admission. A
restart before sufficient original local preparation evidence exists can remain
held; it cannot fabricate missing charge/publication or gate history.

The metadata verifier is explicitly deny-only. There is no issuer/custody
authority, scientific-input acceptance or outer-v2 result transport supplied by
this module. The internal recovery observation is not returned as a combined
execution grant. Native/TreeDar controllers, final manifests, outboxes, result
delivery, settlement and retained-file retirement are not invoked.

## Bounded storage and policy identity

RecoveryPolicy's explicit `preparation_allowance` must equal
`3 × (driver_page_cap + pipeline_page_cap) × 4096 + 65536` bytes. This covers
the complete driver and pipeline databases, their bounded rollback coexistence
and publication overhead, rather than assuming unused recovery slack. It is
added to the existing recovery allowance and then to the original parent's
output budget, preserving both engine-stage maxima. Before typed control
publication those bytes remain in the actual whole-parent scratch reservation;
publication charges and retains them through the existing typed control hold.

Positive database caps require 4096-byte DELETE-mode databases. WAL/SHM are
refused; rollback files must be private, single-link regular files within twice
the corresponding database cap. The pipeline cap is part of its binding and the
recovery publication scope. Driver policy/cap changes are rejected before any
database/background action. This profile skips the legacy background executor,
and pins the actual owner, components, endpoint/configuration and transport
methods used by the driver, broker, claim client and native backend. These
checks detect accidental live dependency replacement; they are not a sandbox
against arbitrary code executing inside the process.

Preparation history has explicit row/count/serialization and whole-file caps.
No automatic purging is implemented. The existing recovery scope's default
64-charge cap limits prepared jobs even without a coordinator restart. At any
cap, preserve evidence and hold. Qualified final acknowledgement, retirement,
history export and migration remain prerequisites for sustained production use.

Adding `preparation_allowance` changes RecoveryPolicy serialization/digests even
when zero. The publication scope also gains `pipelineStoragePageCap`. Previously
published recovery descriptors fail closed; no automatic in-place migration or
old-code rollback is qualified. An absent driver profile retains the legacy
standalone path, but that does not make old published control metadata compatible
with the new descriptor ABI.

## Synthetic qualification

`tests.test_reference_combined_preparation` drives actual ReferenceDriver.step,
Queue/ReferenceStore, TLS WorkerAPI, BrokerClient, ScratchLedger, NodeGate,
DaemonStateLock and recovery preparation without seeded claim/gate rows. It
covers local lost replies, failed full-claim persistence after reservation,
fresh operator holds, prepared coordinator restart, partial construction,
profile/cap drift and unchanged original identity. The separate review module
checks dependency drift and missing published controls.

Backend HTTP and Docker-inspection responses are fixtures. The tiny `abc`
declared-LiDAR source is intentionally unvalidated. These tests prove preparation
control flow and retention only: they establish no real NAS issuer, genuine
LiDAR/scientific acceptance, live Docker compatibility, runtime deployment or
completed processing workflow.
