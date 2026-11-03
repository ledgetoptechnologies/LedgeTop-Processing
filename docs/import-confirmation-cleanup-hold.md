# Pending Viewer import confirmation

The reference engine and scratch cleanup executors deliberately refuse new
deletions in this candidate. A signed Viewer accepted-copy receipt proves that
Viewer retained bytes; it does not prove that the corresponding Viewer attempt
ingested those bytes or produced a usable model. This hold also applies to an
existing scratch cleanup intent, including an object already in quarantine.
The driver treats this specific hold as nonfatal so later work can continue.

The hold has no runtime override and is not itself an import-confirmation
implementation. Retained outputs consume disk. Operators must monitor capacity
and stop admitting work before space is exhausted; do not manually bypass the
hold based only on an accepted-copy receipt.

Before replacing the hold, the coordinated Viewer and Processing contract
must establish and durably verify, at minimum:

* An authenticated, conflict-fenced mapping between the Viewer attempt and the
  reference job, generation, environment, input, pipeline and result manifest.
* The exact accepted receipt and object closure, including object IDs, roles,
  byte sizes and hashes, linked to the archive actually ingested. The verified
  file must be consumed via the same descriptor or securely staged and rehashed;
  verification followed by an unchecked path reopen is insufficient.
* A durable Viewer import milestone bound to the attempt and resulting model,
  version and output, and a replay-safe signed confirmation outbox. A proposed
  `ready_for_review` milestone still requires agreement and qualification.
* Durable Processing verification of that confirmation against locally trusted
  job and receipt state, complete per-object coverage for scratch deletion, and
  complete result coverage for engine deletion. Missing, conflicting or stale
  confirmation must retain data.

The acceptance ledger and Viewer processing database are separate; recovery
must reconcile crashes between them without duplicate publication. TreeDar and
multi-object results need their own role and completion bindings. Existing
cleanup journals still govern uncertain operations and must not be reset merely
because the new hold is present. The staging photo-smoke removal route and other
temporary-file cleanup paths require separate review.
