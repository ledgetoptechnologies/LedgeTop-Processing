# Private worker input-copy accounting bootstrap

The private daemon accepts an optional closed profile:

```json
"inputCopyAccounting": {"schemaVersion": 1}
```

Absence retains the existing default-off factory and driver behavior. Boolean
values, numeric substitutes and unknown fields are refused. The separately
versioned indexed composition below is the only additional accepted version.
Presence does not initialize or migrate state. The normal factory requires an
already completed bound bootstrap before constructing the ledger, worker,
outboxes or execution router. It passes the real opt-in flag to both
`ScratchLedger` and `ReferenceDriver`; the driver does not enable or backfill the
ledger.

## Separate explicit initialization

For a dedicated new local worker state root and a separate empty scratch root,
the source provides this explicit command:

```sh
python -m ops.bootstrap_worker_input_accounting --config /absolute/private/worker.json
```

This command has not been run on either processing PC. It creates no credential,
physical gate, engine container, listener, network request or NAS operation.
It holds the actual `DaemonStateLock` throughout initialization and retains all
state on failure. It does not change permissions on existing directories.

Initialization binds the literal state and scratch paths, both directory device
and inode identities, node, worker, worker instance, authority, engine ID, gate
generation, canonical configured origins and nonsecret budget/retention policy.
Credentials, TLS material and job inputs are excluded. A reflected credential
in the proposed identity is refused. The bound identity has an explicit size
limit. Engine version is checked by the normal worker factory and driver engine
pin; it is not a new immutable bootstrap identity.

The fixed `input-copy-bootstrap/` directory contains owner-private canonical JSON:

1. `intent.json`: version, purpose, random bootstrap ID, exact binding and binding
   digest. This precedes any ledger creation.
2. `ledger-created.json`: exact intent digest, scratch claim digest and inode,
   and ledger device/inode identity. This precedes irreversible enablement.
3. `completed.json`: exact intent and created-checkpoint digests and the enabled
   accounting version. It is written after checking the actual enabled ledger.

All records use exclusive creation, bounded reads/writes, file fsync and parent
directory fsync. They are never overwritten. The scratch root permanently holds
`.input-copy-bootstrap.json`, an exclusive claim bound to the same operation and
state/scratch identities. Another worker state root cannot borrow that scratch
root, including through a default-off configuration. The claim is not removed by
cleanup; future retirement requires a separate quiescent procedure.

## Admission and interruption behavior

Initial admission permits only the actual daemon lock in the state root and no
entries in scratch. Any preexisting driver, ledger, outbox, unknown file, symlink
or nonempty scratch refuses before a ledger is constructed or its marker is
enabled. This includes empty legacy journals. There is no automatic migration.
Later phases allow only the exact owned checkpoint files, ledger and claim.
The SQLite ledger must have the expected schema, no sidecars, the exact policy
configuration and no business reservations, results, receipts, pins or charges.
The existing control-accounting marker is never reset or silently backfilled.

An interruption after the ledger-created checkpoint can resume only with the
same intent, root identities, claim identity and ledger inode. Before enablement,
the known empty ledger must still be unlatched. After enablement but before the
completion record, the actual exact input schema/marker and empty business state
must validate; the marker is not enabled again. Once complete, repeated explicit
bootstrap calls only validate existing records and state.

An interruption after a ledger file is created but before its created checkpoint
is persisted deliberately holds. The unknown file is not adopted or deleted.
The same applies to an orphan bootstrap directory, partial/corrupt record, legacy
driver binding or unbound partial driver journal. Manual inspection and a future
qualified reconciliation procedure are required for these uncertain windows.

Normal startup validates actual root, claim and ledger identities, canonical
records, current input schema/configuration and an existing opted-in driver
binding's strict profile, physical identities, origins and core policy when
present. Complete optional router/option-profile binding validation remains in
the actual driver constructor; it is not duplicated by bootstrap preflight.
A completion digest alone grants no authority. The real
ledger constructor then revalidates its accounting and control-charge invariants.
Removing the profile while its bootstrap or scratch claim exists refuses before
durable component construction. Root replacement, profile/identity/policy drift,
missing marker or schema footprint, and a changed claim or ledger inode also hold.

## Scope and verification

This wires accounting into the canonical daemon factory; it does not provide an
`InputReleaseAuthority`, retained-original-source custody, delivery acceptance or
automatic input deletion. Existing result/engine cleanup flags retain their
separate defaults and behavior. No real worker configuration, deployment,
container update or processing acceptance is established by these source tests.

`tests/test_reference_worker_input_bootstrap.py` exercises real local SQLite,
directory durability and the actual factory/driver composition with synthetic
TLS/native fixtures. It covers explicit initialization and cold reopen,
pre-enable and post-enable interruption recovery, unknown-file refusal,
uncheckpointed-ledger retention, two workers sharing scratch, default-off
compatibility, identity/policy drift, root/claim replacement, symlinks, legacy
driver refusal, marker corruption, credential reflection and preservation of
the joint input/control-accounting fences.

## Indexed composition version 2 (source only)

The closed private request is:

```json
"inputCopyAccounting": {
  "schemaVersion": 2,
  "fetchJournal": {"schemaVersion": 1, "policyId": "indexed-worker-fetch-v1"}
}
```

It is a fresh ODM composition, not a ledger upgrade. The ledger still latches
`inputCopyAccountingVersion:1`. Default-off and version-1 bootstrap record shapes
remain unchanged. Version 2 adds its exact configuration and the actual compiled
indexed protocol/policy/schema descriptor to the durable binding. Its permanent
scratch claim has version 2 and a distinct indexed purpose/descriptor. Removing
fields or changing the request to version 1 cannot produce the stored binding.
TreeDar acquisition and combined preparation are not enabled by this profile.

Explicit bootstrap additionally initializes a fresh `driver.sqlite` through
`initialize_fresh_profile`. The journal owner creates the fixed empty base and
indexed schema, immutable profile footprint and policy identity in one owned
transaction. It does not insert a claim, reservation or driver settings binding.
`indexed-created.json` then pins that actual database inode, root identity,
bootstrap/binding identity and compiled descriptor, coupled to the existing
intent and ledger-created checkpoint digests. Completion version 2 binds the
indexed-created digest as well as the ledger checkpoint. No implicit migration,
old-journal adoption or marker repair is available.

If an indexed database is created but its checkpoint is not persisted, bootstrap
retains it and holds for inspection. A known indexed checkpoint can resume only
with the same actual root/database/claim identities and an empty pre-completion
business history. An interrupted post-enable operation does not enable the
ledger again. A changed compiled policy/schema is a binding mismatch, not
permission to update an old receipt.

Normal startup uses the real read-only journal profile verifier and a concrete
`IndexedFetchContext` before opening legacy ledger/driver components. An empty
driver settings binding is accepted only for the actual preinitialized indexed
profile whose complete base/indexed business history is still empty; it is not
a general exception for empty legacy journals. The actual driver records its
ledger version-1 input binding plus the exact indexed descriptor and receives
the verified context. Bootstrap and startup never manufacture caller-controlled
contexts or infer authority from a supplied hash.

For this composition, the service may omit the ordinary idle poll delay only
after the actual driver proves a completed single-object cursor advance. It
checks ownership and stop state and yields with zero sleep before the next
step. Idle, held, unavailable, retry and non-progress results retain ordinary
poll/backoff behavior. Caller JSON flags and numeric substitutes grant no fast
path. The driver owns the indexed background-acceptance cadence; the daemon adds
no separate heartbeat, grant renewal or batch engine-upload loop.

The health observer's pending-claim exception requires the real cross-thread
readiness lease, its exact initial-claim job/request identity and a read-only
check of the actual indexed profile and unowned pending record. Other jobs,
discovery/fabricated leases, sealed claims and changed profiles cannot use it.
Dispatch uses the immutable initial composition identity so clearing the public
context cannot fall back to the legacy `data='{}'` exception. The legacy
version-1 SQL path is retained.

Before any normal legacy ledger/clock observation, default-off and version-1
factories also check the actual owned driver file read-only for any
case-insensitive `indexed_*` SQLite namespace or a lone `indexedFetchJournal`
binding marker. A coherent ordinary core binding does not permit adoption of
foreign indexed state. The shared guard validates private file/path ownership,
single-link identity, bounded reads and unchanged identity across the snapshot;
it creates no tables or clock observations and preserves supported empty legacy
files. The nonindexed binding checker separately refuses the new marker.

`tests/test_reference_worker_indexed_profile.py` covers the real bootstrap,
canonical factory cold reopen, descriptor/claim receipts, interruption windows,
v1/v2 downgrade refusal, policy/schema drift, leased health byte invariance,
foreign/unleased pending work and service scheduling boundaries. The actual
inactive old `0b7` image also refused new, stripped-fetch, version-1 and omitted
profiles against a generated v2 fixture, preserving all ledger/driver/receipt
bytes. The actual prior source55 image refused a cold default ledger writer;
that narrower test is not a full source55 daemon/TLS startup proof.

The compiled numeric metadata limits remain candidate values until the journal
owner's worst-legal-input SQLite oracle and independent review qualify them.
Only version 2 receives the compiled `IndexedCapacityProbe` in the actual ledger
factory. It checks the real context/owner/root/device identities. For shared
state/scratch storage it withholds the one global metadata allowance still not
materialized as driver/rollback-journal blocks; it does not charge those blocks
twice. Separate scratch storage retains its raw free-space probe. The driver's
preclaim guard also accounts for same-device outstanding ledger reservations and
retained control holds. No request or private JSON field can supply a substitute
free-space function. Version-1/default factory probe composition is unchanged.
Bootstrap/compatibility tests do not establish quota, free-space accounting,
10,000-object hot-path performance, native upload responsiveness, real broker
expiry/recovery or installed-worker acceptance. This daemon adds no metadata
charge or free-byte estimate of its own. No version-2 host bootstrap, runtime
activation, source-custody issuer or automatic input deletion is established.
