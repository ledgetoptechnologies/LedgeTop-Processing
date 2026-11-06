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
empty release, new reservation and ownership handover while that hold exists.
The hold is part of atomically replaced `gate.json` schema 2, so the reviewed
schema-1 NodeGate executable rejects it before mutating the gate. A prior
prototype's separate marker is also refused for inspection, never migrated
automatically. Failure before pin creation and an interrupted writer remain
held. There is deliberately no automatic downgrade or usable completion
recovery. Refusal must be verified for every actually installed old binary and
all alternate gate writers before deployment. This does not establish
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

One bounded Linux test loaded the actual `3fa2c5c6` NodeGate source (SHA-256
`e4080488cc7b131a96722e778dc13680aaff44f23f34d5660d8d5d273a09b6a3`) and
verified it raises `GateError` on a newly armed schema-2 hold. This is evidence
for that source version only, not an installed binary or every old path.

Future reconciliation must verify an authenticated, durable exact-reservation
coordinator decision, prove all old worker processes/readers and engine jobs
retired, inventory scratch and verify no unknown pins or files, and hold the
same NodeGate lock while fencing all new writer starts. It must journal its
decision and handle crashes before any atomic schema downgrade and directory
fsync. The exact cross-system ordering, restart replay, power-loss behavior and
proof of no stale process are not implemented; never remove a hold based only
on expiry, a boolean, an operator note or lost connectivity.
