# Worker fence proof experiment

This experiment is **not an authenticated network API, readiness guarantee,
or Viewer custody receipt**. It signs a challenge and complete scratch-binding
digest only after the exact local NodeGate, scratch fence and storage mount
have been checked and a scratch writer pin has been durably acquired. The pin
is deliberately not retired, even if signing fails or the response is lost.
No existing endpoint calls this code. Its `transferAuthorized`,
`processingAuthorized`, and `sourceCustodyAccepted` values are always false.

The verifier requires an independently pinned Ed25519 key and exact worker,
authority, instance, attempt, generation, job, environment, manifest and
derived digest, profile, input/output capacity and TTL via the binding digest;
the gate generation and a coordinator challenge are separately signed. A
strict maximum 60-second validity and caller-supplied current time are checked.
The current clock arguments are test inputs, not a trusted clock. There is no
network certificate binding, key enrollment or capability endpoint here.

This proof observes a fence at issuance; it does **not** establish that all
software honors the gate. The experimental NodeGate code persists a direct hold
before creating a readiness or reader pin, and refuses resolve, acknowledgement,
empty release, new reservation and ownership handover while that marker exists.
Failure before pin creation and an interrupted writer both remain held. There
is deliberately no automatic marker removal or usable completion recovery.
Older executables ignore this marker and may clear the gate, so rollback must
be fenced independently before any deployment. This does not establish
or that an authenticated caller has initiated the source transfer. An unwired
local primitive replaces the exact readiness pin with a fresh writer pin in one
scratch transaction while the caller holds the checked NodeGate. A crash after
the replacement retains the replacement pin; an unknown or reused old pin
fails closed. The experimental receiver can use this private primitive, but
no authenticated network endpoint, proof verification, cross-process handoff,
or durable replay invokes it. After a successful object write the normal
writer pin retires; subsequent operations use existing transfer fencing and
still need independently verified settlement/resolve rules. Exact proof bytes and the
challenge are not persisted for replay; a lost response leaves an inspectable
hold. Coordinator reservation still uses a synthetic freshness guard and
cannot consume this proof safely. Before activation, integrate a durable gate
hold reconciliation, authenticated pin handover, trusted clocks and key identity, authenticated
endpoint, exact durable replay, expiry/clock-skew policy, fail-closed pin
recovery, and coordinator transaction binding. Do not release pins on time or
failure, and do not infer worker availability from a signature alone.
