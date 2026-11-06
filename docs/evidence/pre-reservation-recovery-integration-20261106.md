# Original-claim recovery integration qualification

The review candidate now implements source-range-v2 ODM recovery before a
reservation commits, including an initial lost claim reply. It preserves the
original job, claim token/request, worker instance and reservation intent; it
does not requeue or start another engine job. Reservation validation is shared
with ordinary admission. Transfer creation, phase/status change and recovery
receipt commit in the same writer transaction with the existing metadata quota.

Runtime recovery refreshes authenticated current node capacity/options before
this pre-start transition, using one paired private catalog snapshot where the
installed runtime supplies it. A fresh Store's optimistic initial defaults are
not admission evidence. Started/schema2/schema3 continuation remains independent
of catalog availability. Historical exact receipts remain read-only.

## Independent runs and corrections

Frozen Processing `5dced2b201f512ffb45677ac2eda03672f4ea610`, archive SHA256
`396066b2c2dd1a194807709ee36e9cd952eb0f0897cbc96b6fe69a855d8784e1`,
ran **371 tests in111.349 seconds:366 passed and5 errors**, no skips. This was
not a passing selection. Modules:

```text
tests.test_reference_source_prereservation_recovery
tests.test_reference_source_fetch_process_restart
tests.test_reference_source_processing_recovery
tests.test_reference_recovery
tests.test_reference_driver
tests.test_reference_store
tests.test_reference_worker_api
tests.test_reference_runtime
tests.test_reference_worker_catalog
tests.test_enrolled_startup_health
tests.test_enrolled_coordinator_driver
```

The five errors occurred before their runtime-discovery assertions. A test-only
exception observer identified the original constructor refusal: a reused
two-node runtime configuration did not match the existing singleton queue
authority. The original production guard correctly rejected it. Test-only
`4704059` (root `82f6736`) supplies the exact queue inventory; `21bea86` (root
`c96d6d4`) also binds its configuration to the actual fixture worker's engine,
authority, origin, generation, output budget and option definitions. No runtime
catch, authority guard, engine factory or admission check was bypassed.

Frozen `c96d6d4`, archive SHA256
`1190a32ec938d90eee0551424dd9cbdacf8e03f5fd952660e693f4ca1bfd6d9c`,
then passed **5/5 runtime-discovery cases in1.238 seconds**, no skips. All
`processing/` production files are byte-identical between these two checkpoints.
The targeted rerun qualifies the corrected fixture cases; it is not described
as a new full371-test pass.

Both root runners used one CPU, UID/GID10001, network none, read-only frozen
source, private tmpfs and no Docker socket; memory was768MiB for the broad run
and512MiB for the targeted rerun. Named and task-label container inventories were
empty after exit. Logs are retained in local temporary artifacts:

- `ltds-integrated-prereservation-4667d2131c3a406dba05e83f5f5799c3/test.log`.
- `ltds-runtime-prereg-5302ab6f9cd741928b9e0ccc98d6be14/test.log`.

## Evidence scope

The broad passing cases include twenty direct original-claim recovery tests
and three new actual child SIGKILL/TLS cases: claim commit before reply,
reservation never submitted, and reservation committed before reply. They
require original ownership/reservation and exactly one synthetic engine
initialize/commit. Other cases cover lost recovery replies, concurrent exact
requests, stale authority/revision, quota rollback, changed custody, local
allocation/expiry/Gate/native-task refusal and current capacity changes.

The five corrected cases construct the actual ReferenceRuntime/Store using the
existing synthetic source custody; TLS context loading alone is fixture-mocked.
They verify unavailable/changed catalog refusal, a fresh POST refresh after a
valid GET, no probe under an invalid claim, paired snapshot use once per turn,
and schema3 processing recovery without catalog access.

Native geometry is synthetic, source storage is local temporary storage, and
these runs do not establish installed SSH/systemd, NAS power-loss durability,
real Viewer import, SIGKILL during delivery or client-data quality. The separate
22-phase Docker forward proof uses immutable packaged source `18750d4`, not this
new recovery code; a final rebuilt combined runtime still needs qualification.
