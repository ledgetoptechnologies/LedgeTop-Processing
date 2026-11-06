# Integrated source recovery and bootstrap independence

Frozen Processing source `7be3ee2` integrates Claude's finalized recovery head
`c59f600` by selecting its 24 non-merge commits and retaining its merge
resolutions. All 14 paths changed from `6661fed` to `c59f600` were byte-identical
to the finalized peer tree after integration. Existing root catalog/runtime,
activation, installer and corrected source-owner tests remained preserved.

Full Git archive SHA-256:
`b79dc3533e62f24a7cc1fb1c9f9634024094626d3e7813b42e9be1e90d8b5960`.

Independent Linux selection: **521/521 passed in 148.143 seconds**, no skips:

```text
tests.test_reference_source_processing_recovery
tests.test_reference_source_fetch_recovery
tests.test_reference_source_fetch_process_restart
tests.test_reference_recovery
tests.test_reference_driver
tests.test_reference_source_grant_renewal
tests.test_scratch_policy
tests.test_reference_source_intake
tests.test_reference_runtime
tests.test_reference_worker_catalog
tests.test_bootstrap_worker_catalog_independence
```

The runner used one CPU, 768 MiB, UID/GID10001, network none, a read-only frozen
source mount, private `/tmp` tmpfs, no Docker socket, and an init process to reap
children. Handle71287 and named container `ltds-root-recovery-7be3ee2` exited0;
the container auto-removed and its task-label inventory was empty. Log is the
local temporary artifact `ltds-integrated-recovery-45871049062e40dd9e77a6242b04f238/test.log`.

Coverage includes real child SIGKILL/TLS fetching recovery and normal coordinator
restart around durable source and execution phases. Native processing is a
synthetic engine. Delivery tests use real coordinator/worker sender and receipt
handling with a signed synthetic Viewer acceptance producer, not actual Viewer
import. SIGKILL during delivery, real photogrammetry, installed hosts and NAS
power-loss durability are not established by this selection.

The bootstrap regression starts the normal application with worker ports bound
but not listening. Its outbound observer calls the original urllib transport;
it does not synthesize readiness. Constructor/start/health perform no catalog
requests. Open options/admission attempt the real worker endpoints and refuse503.
After sealing and restarting the same schema2 bootstrap, health succeeds with
the gate still frozen and without discovery; frozen admission refuses before
reading its body. Legacy rows and artifact bytes remain unchanged.

## Explicit remaining work

The selected source still deliberately holds a restart before reservation with
`source_recovery_claim_unreserved`; it is not a complete unattended workflow.
Follow-on patches beginning at original `8824e53` are selected for review after
this checkpoint, but have not passed this test selection. Their new reservation
and lost-claim replay semantics require separate integrated qualification,
including fresh runtime capacity discovery rather than default Store snapshots.

The separate actual Docker22-phase operation `5d8f9f96-c53f-4fb0-875b-ad11bbb5737c`
held after eight completed phases on an immediate startup-health observation.
Its same bootstrap later returned200 in a separately labelled diagnostic. That
is not a successful full forward run, a resume, or a rollback qualification.
The bounded startup-readiness fix and a new reviewed operation remain required.
