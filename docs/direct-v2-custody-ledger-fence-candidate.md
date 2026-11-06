# Direct V2 local retention fence candidate

This is an unmounted worker-side change, not a Viewer custody acknowledgement.
After verifying the full source closure and syncing its local receipt, the V2
closure code now creates a retained scratch-ledger fence before retiring the
active writer pin. A crash before fence creation retains the pin; a crash after
creation retains the fence. The ledger refuses pre-existing cleanup intents at
fence creation, unstarted settlement and cleanup while fenced. Engine start
is permitted after the writer pin retires; terminal state conservatively keeps
the remaining capacity reservation. Fence metadata has a bounded count and
logical allowance, with immutable-row triggers and a one-way version marker
that older ledger code should reject. There is deliberately no release path.

This intentionally changes local retention for V2 closures: data and capacity
remain held indefinitely. Closure responses still state
`sourceCustodyAccepted=false`, `transferComplete=false`, and
`processingAuthorized=false`. A signed closure is not authority for Viewer to
delete originals. No coordinator receipt is issued and no Viewer behavior changes.

The unmounted worker HTTPS adapter also exposes a signed local retention
observation to the authenticated coordinator. It revalidates the closure and
looks up the persisted ledger fence, binding the fence, local closure hash and
exact signed closure hash into a domain-separated worker signature. The
coordinator verifies its pinned worker key, stores the observation within its
bounded handoff session allowance, and replays verified evidence without the
worker online. A lost response can be re-observed. This is a historical local
observation, not proof the worker remains online or that non-ledger writers are
excluded; custody and execution flags remain false. The journal schema and
marker lack a qualified online migration and older-writer fence and are not
wired into runtime startup.

The scratch ledger fences only its own cooperative APIs. It does not exclude
an older already-running binary, external filesystem writers or deletion
outside the ledger, and the logical metadata allowance is not a disk quota.
The cleanup executor `scratch_cleanup.py` requires a ledger cleanup intent
before its rename/unlink; fence acquisition rejects every prior intent and
ledger eligibility cannot issue another while retained. `source_spool.py`
also removes owned partial names during write recovery and directory audit,
and closure receipt publication removes only its own temporary file. Those
direct filesystem operations are not generally fenced by the new ledger
record: exclusive spool ownership and all possible callers must be qualified
before treating the fence as custody.
Worker and coordinator identity, online quiescence, power-loss durability,
actual installed mount and restart behavior, engine-input preservation, and an
authenticated explicit release protocol remain unqualified. Never enable or
mount this candidate based on synthetic tests alone.
