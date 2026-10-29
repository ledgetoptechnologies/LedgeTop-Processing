# Private worker input-copy accounting bootstrap

The private daemon accepts an optional closed profile:

```json
"inputCopyAccounting": {"schemaVersion": 1}
```

Absence retains the existing default-off factory and driver behavior. Boolean
values, numeric substitutes, unknown fields and other versions are refused.
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
