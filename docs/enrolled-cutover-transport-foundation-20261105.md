# Concrete transport foundation — not an activation controller

This checkpoint supplies structured, bounded Docker CLI execution, closed
container recipes, private configuration readback, operator HTTP and exact
read-only Gate receipt scripts, physical binding observation, and an original
main/external-flock holder. It does not yet implement the journal's phase driver,
backup/projection orchestration, or a qualified deployment recipe. It performs
no action on import and has not activated or changed an installed coordinator.

The initial ten focused tests passed on Linux in 0.630 seconds under UID 10001,
network isolation, one CPU, 768 MiB, a read-only worktree and bounded tmpfs. The
holder tests acquired the actual original main and external flocks, denied
competing holders, rejected forged descriptor proof, then released and verified
unchanged original files. The exact task container was removed. Three subsequent
tests add synthetic Docker-copy tar validation, read-only receipt-script checks,
and owned descendant timeout cleanup; the seven portable cases passed locally.
The expanded Linux suite has not yet been rerun at this checkpoint. No actual
Docker create/cp deployment qualification is claimed by these tests.

## Historical pilot record provenance

A bounded local history lookup located the original operator artifacts:

* `two-node-acquire-supervisor-DRAFT.py`, SHA-256
  `65c80f8e7d69dee6491ad6c318519cc0f345fdc0a439649399fe5a30b84b59e0`.
* `owned-node-drain-DRAFT.py`, SHA-256
  `b68a64c4a14004b302a5386ab6fb95d96b5a19ef7ad073af55e05948a6a4f277`.

The acquisition supervisor's wrapper writes each node's start and exit marker
from the same Docker-exec child in the coordinator container. The node denotes
the target maintenance schedule; even the main-node acquisition child executes
in the coordinator PID namespace. Each marker is an exclusive, private,
fsynced `{pid, boot, start}` record; the exit marker is written in `finally`.
It proves neither successful scheduling nor successful cancellation.

A read-only probe of the retained records checked all six sibling groups
against those exact closed schemas. All marker pairs, operation-directory,
node and authority correlations matched. The recorded boot matches the current
container boot, and none of the recorded PID/start identities is live in that
container namespace. The two additional cancellation-intent fields are
`proofSha256` and `archiveSha256`.

This establishes the recorded acquisition-child identities and their absence.
It is not a complete pilot-closure certificate: the local supervisor and later
release-process lifecycle, exact owned scheduling/cancellation receipts and
retained output custody remain separate predicates. The retained artifact
sources currently contain explicit disabled draft guards; this checkpoint does
not claim their bytes alone establish which approved execution occurred.
Host-PID absence must never substitute for the container-namespace check.

## Remaining concrete path

The phase driver must consume the single reviewed journal and independently
generated policy, bind every mutating completion to its current dispatch nonce,
retain uncertainty without replay, and obtain actual stopped-writer/physical
lease custody before backup or projection verification. It must qualify the
same-image bootstrap/final recipes and authenticated final runtime/worker
readiness before reopening. Missing pilot, NAS, legacy-delivery or worker gates
remain holds; this checkpoint does not create those authorities.
