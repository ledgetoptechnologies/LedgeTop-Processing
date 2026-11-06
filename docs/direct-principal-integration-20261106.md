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

The current direct TLS listener does not authenticate a client certificate.
The per-boundary bearer is not an installed coordinator identity; do not use
this candidate as a runtime coordinator-to-worker authorization grant.
