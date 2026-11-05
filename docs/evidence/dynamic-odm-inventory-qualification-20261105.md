# Ordered ordinary ODM inventory qualification

Base: `40564e2`. Isolated branch: `codex/dynamic-odm-node-inventory-20261105`.
No deployed services, NAS, engine workloads, client datasets or credentials changed.

## Runtime behavior

`PROCESSING_PROTOTYPE_NODE_IDS` is the ordered fleet (1–8 unique IDs), with
`PROCESSING_NODE_<NORMALIZED_ID>_URL` and `_TOKEN` for each entry. Environment
suffix collisions and normalized endpoint aliases remain rejected. Ordinary
ODM startup no longer requires the literal `cluster` and `main` role names.
The legacy URL variables remain compatibility aliases for those two IDs.

`PROCESSING_NODE_<ID>_MAX_IMAGES` is an optional positive integer, at most
1,000,000, or the exact string `null` for unlimited configured capacity.
Omission preserves the old cluster `LOCAL_MAX_PHOTOS` cap (default 1,000), and
no configured photo cap on other nodes. A live backend's lower cap always wins.
Photo count excludes verified auxiliary inputs; byte limits retain their
existing behavior. New endpoint engine tokens cannot alias reference API
credentials. No Enum is emitted in wire metadata.

The existing authoritative scheduler already scans nodes in configured order,
uses one durable slot per node, honors production priority, and skips queue
entries ineligible for the currently available node without losing the older
entry's position on an eligible node. Existing three-node `/next` and atomic
`/claim` tests remain intact, including a capacity change between those calls.
This patch connects configured photo caps to that scheduler rather than adding
another queue or controller. It removes an unused inferred `main_node_id`
alias; the actual TreeDar executor still requires `main` and is unchanged.

## Inventory changes and retention

The authenticated reference runtime appends canonical inventory revisions,
including explicit node order, normalized engine origin, recovery binding,
worker ID/instance and result origin. Tokens are excluded. Previous revisions,
maintenance records, original job/node IDs, receipts and result history remain.
Capacity/order-only changes do not rewrite execution identity.

Adding an idle configured node and changing order are supported. Removing or
rebinding a node fails closed while it has running/held work, non-active
maintenance ownership, active/uncertain source readers, legacy Viewer node
history, or an unresolved reference result. Completed reference results require
the existing fully validated signed accepted-copy and assigned-worker
observation chain; merely inserting an observation row is insufficient.
No legacy completed-result retirement is inferred. Such history needs a future
explicit operator-reviewed retirement path. Cancellation/failure histories
without successful worker-observed copy also remain conservative holds.

On first upgrade, an existing recovery installation must start once with its
exact persisted recovery binding digest unchanged before adding/removing nodes.
The old digest cannot reconstruct an old map. Live claim worker ownership is
also checked before this first pin. A refused Queue constructor closes only its
own newly acquired lease, allowing the original configuration to reopen; it
does not discard reservations or clear another process's authority.

This is coordinator inventory migration only. It never rewrites worker-local
NodeGate pins/journals. Renaming or replacing a previously used physical worker
still needs its existing explicit owner handover. Unique Docker Engine IDs
identify configured daemons, not independently attested physical computers.
Installed host provisioning/update bridge wrappers still have separate
cluster/main assumptions and are not qualified as arbitrary-node onboarding.
A backend appearing healthy is not proof that its worker poller is alive.

## Evidence

Final bounded Linux command (read-only source, no network, one CPU, 1 GiB):

```text
python -m unittest tests.test_node_inventory tests.test_node_roles tests.test_reference_runtime tests.test_reference_acceptance_observed
Ran 83 tests in 2.510s — OK
```

Image: `sha256:57f01267c88439d872f6782b54511e314614a76145e32cfe2fad003e5ca0fac8`.
Task container `ltds-dynamic-inventory-tests` used `--rm` and exited; no owned
runner remains. Earlier native Windows checks passed 20 tests with the one
Linux-only receipt fixture skipped. Direct native runtime import lacked
`rfc8785`; the complete runtime suite above ran in the existing Linux image.

New coverage includes absent/null/explicit photo caps, generic endpoint config,
actual reference startup on a single generically named mapping node with no
legacy URLs, cap propagation/live cap downgrade, append-only add/order/remove,
refused removal preserving held job identity and coordinator reopen, pending
maintenance refusal, first-pin recovery digest mismatch, and actual signed V2
Viewer acceptance plus authenticated worker observation before rebind. A
corrupted stored observation ACK is rejected despite row presence; restoring
the exact valid ACK permits rebind and retains the completed job.

The receipt fixture is a synthetic dataset and signed local test peer. These
checks do not prove installed multi-PC configuration, actual heavy processing,
live Viewer UI scheduling, arbitrary-host onboarding or production readiness.

## Metadata quota followup and update-census compatibility

All retained `processing_node_inventory` revisions now contribute their exact
stored revision, UTF-8 JSON, digest and timestamp bytes to the existing shared
ReferenceStore metadata quota. The next revision reserves that exact byte cost
before insertion in the same transaction. Replaying the unchanged inventory is
read-only and remains possible at an exhausted budget. New-revision and initial
pin quota failures roll back without changing history. Focused tests cover
historical accounting, exact replay above budget and atomic refusal.

Read-only audit of the integrated updater at `5914409` found the existing
`ops/update_reference_coordinator.py` still intentionally rejects these newer
schemas. Its closed table allowlist (lines 136–139, checked at 169 and 205) does
not recognize `processing_node_inventory`. Existing current-V2 features also
introduce unsupported `reference_source_intakes`,
`reference_source_object_receipts`, `reference_source_grant_tokens`,
`reference_source_grant_renewals`, `reference_source_range_usage`,
`reference_source_range_requests`, `reference_source_object_pins`,
`reference_recovery_startup`, `reference_recovery_receipts`,
`reference_worker_acceptance_observations`, and the four `source_reader_*`
tables (`binding`, `epochs`, `pins`, `fences`). A real empty ReferenceStore
confirmed the first three source tables already exceed that allowlist.

The same updater's path allowlist at line 145 rejects `source-readers` and its
`lease` file; nonempty Viewer/spool trees also exceed its reviewed empty-native
baseline. Its generic zero-row fence at lines 216–218 would reject nonempty
reference claims/recovery/observation histories even if their table names were
added. `compatible()` lines 267–292 recognizes only the reviewed owned-schedule
and Viewer-acceptance schema additions, so it has no inventory schema migration
exception. The per-file 64 MiB census bound (line 148) is separate from logical
metadata quota; logical bytes do not prove a SQLite file fits that size because
of page overhead. These are updater limitations, not grounds to bypass census.
No updater guard, file bound, zero-row check or rollback compatibility rule was
weakened by this patch; updating a current-V2 populated coordinator needs a
separately reviewed exact census/backup/rollback qualification.
