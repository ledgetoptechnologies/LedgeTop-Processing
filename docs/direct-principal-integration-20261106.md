# Direct intake and principal integration candidate

This branch combines the authenticated V2 source-principal changes with the
direct-intake prototype. It is a review and integration candidate, not a
deployed service or evidence of a working Viewer-to-engine pipeline. Direct
admission remains unwired to the public API and its worker-local closure is not
a coordinator source-custody acknowledgment. Viewer must retain its sources.

On 2026-11-06 a network-isolated Docker test ran 145 focused direct-transfer,
admission, source-principal, replay and intake-mode tests: 141 passed and four
were skipped because the specific old-code fixtures were not supplied. An
earlier run without `/tmp` as a separate mount failed three mount preconditions;
the successful run used a disposable `/tmp` tmpfs. These results do not prove
an installed mount, old-binary rollback, a real engine, or result delivery.

Outstanding gates include durable worker custody with authenticated coordinator
acknowledgment, enforced writer isolation and crash recovery, safe retention and
explicit release, API and Viewer integration, installed worker activation,
multi-node operation and an end-to-end real-data test. Do not enable this
candidate for client jobs on the basis of the synthetic tests.

The reviewed worker retention fence was subsequently merged into this
candidate. A second isolated run exercised 79 tests, with two old-gate
fixtures unavailable. The actual pre-fence scratch implementation from
`c4f86f73` was supplied (Git blob `175c6c85ae3f85853a47889b5757b34134d69e55`)
and its startup refusal test ran. This does not fence a process that was
already running or prevent filesystem operations outside the ledger. The
local fence remains indefinite, and neither Viewer acknowledgment nor an
authenticated release has been implemented.

The candidate also retains partial uploads during audit and rejects V2
readiness and writes after the custody fence. A 53-test isolated check passed
with three old-code fixture skips. Reconciliation now holds jobs containing
uncertain partials instead of automatically adopting or deleting those files;
operator-safe recovery and cross-process spool confinement remain unfinished.

The synthetic HTTPS worker and coordinator can now exchange and journal a
signed local-retention observation. This is historical evidence of what the
worker observed, not proof of current reachability, filesystem isolation,
survival of disk rollback, Viewer custody acceptance, or processing approval.
The merged observation and spool-fence check ran 54 tests with three skipped
old-code fixtures. Journal corruption and old-writer handling require review.

An opt-in local listener owner is included for integration: it reports listener
failures and drains active uploads before bounded shutdown. The merged focused
run covered 42 tests with three old-code fixture skips, including a real TLS V2
upload and drain. It is not connected to a host service or scheduler, and its
status is not proof of worker readiness.

Retention observations use the distinct V2 protocol and signature domain;
prior experimental V1 journals are held for explicit migration. The journal
detects missing or modified observation history. It has a 4,096-record lifetime
ceiling and refuses new observations at capacity until a safe archival scheme
is qualified. The merged check ran 79 tests with four old-code fixture skips.

The opt-in direct listener can require a pinned client certificate, but its
mapped principal is now bound to a pre-enrolled reservation in the candidate.
The merged synthetic check ran 140 tests with six older-code fixture skips,
including actual loopback mTLS reservation and transfer. The bearer alone is
not an installed coordinator identity. A candidate coordinator sender now
submits the reservation over mutual TLS and retries the same durable operation
after an uncertain response; this is not custody acceptance. A combined
synthetic run passed 96 tests with three fixture skips. Dynamic worker
enrollment, credential provisioning, actual runtime wiring, process-death
recovery and the public Viewer route remain unfinished.

A separate opt-in private mTLS snapshot listener and bounded page journal are
included as a candidate. A synthetic integration test exercises an actual
signed source attestation, direct reservation and source-only inspector. It is
not a Viewer custody acknowledgment. A merged run completed 98 tests with two
fixture-dependent skips. The current journal has a 128-session lifetime cap
with no qualified retirement, and selected pages and GCP evidence are not
verified. Listener enrollment, actual Viewer sender, crash recovery and
installed operation remain unqualified; do not activate it for client jobs.

Snapshot journal V2 reserves logical space based on the signed page stream and
intent size instead of the maximum for every session. A merged run passed 72
tests with three external-fixture skips. This is not a physical SQLite/WAL disk
quota, and old journal migration, safe 24-hour retirement and large-workload
performance remain unqualified.

The private candidate can receive selected pages and verify their signed
ordering against the stored non-GCP source and pipeline; its result remains
non-custodial. An integrated synthetic run passed 36 tests with five optional
fixture skips. GCP/derived evidence, actual Viewer transport, and installed
restart and old-writer fencing remain unqualified.

The worker readiness prototype can journal a request and replay an unexpired
proof while its exact pin remains present. Uncertain failures retain the pin;
no automatic reclaim is authorized. An integrated run passed 100 tests with
12 external-fixture skips. This journal is not wired to an authenticated worker
endpoint and does not prove installed restart recovery. Its retained history
also has an unqualified 4,096-record lifetime limit and scan cost.

The worker can retain and revalidate a signed direct inventory in a
quota-charged immutable ledger row in synthetic tests. An integrated run
passed 139 tests with 17 optional fixture skips. The 64-record lifetime cap,
physical SQLite/WAL usage, old-writer exclusion and reconstruction of a live
upload listener remain unqualified; inventory retention is not custody.

An internal worker preparation candidate checks signed input, preflights the
scratch mount and atomically writes a bound node reservation and direct hold.
It retains uncertainty on any failed attempt. A combined local run passed 100
tests with 10 external-fixture skips. It has no authenticated daemon endpoint
or provisioned fleet-policy trust, and does not establish old-process fencing or
installed recovery; it must not be used to activate a node.

An optional worker-service preparation listener and coordinator policy path
are implemented. The policy remains off by default and can be enabled only
by explicit root-owned coordinator enrollment and matching protected worker
configuration. The daemon entrypoint does not yet have an installed enrollment
record. Worker enrollment, installation, process restart and per-job upload
listener lifecycle remain unqualified.

A policy-disabled worker can reconstruct a read-only observation listener
after a synthetic process restart. It rejects upload and closure mutations;
the coordinator treats the presence observation as uncertain instead of a
durable receipt. A merged run passed 62 tests with four optional skips.
Automatic transfer recovery and installed host restart remain open.

The candidate coordinator now journals an exact preparation intent and holds
the node while its request is in flight; reservation requires the journaled
proof. Missing preparation evidence fails closed on startup or replay. The
merged synthetic queue/freeze/reservation suite passed 62 tests. Earlier
unjournaled candidate state needs explicit reconciliation; old-running binary
fencing, intent retirement and production runtime wiring are not qualified.
Root-owned enrollment does not fence an older running coordinator or an older
executable, and does not stop a root-privileged service modifying its own
enrollment. A reviewed non-root service, process retirement and rollback plan
are required before activation; this candidate performs no installation.
