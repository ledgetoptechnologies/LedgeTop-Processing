# Inactive Viewer activation gate candidate

This is a source-only candidate, not a deployable handshake. The coordinator
stores an immutable binding in the same SQLite database as the reference job,
and worker discovery and claim require it without skipping the shared
production-first FIFO head. Existing unbound queued and running jobs fail
closed; recovery, worker reservation, grant retrieval and execution phase
advancement cannot use an unbound legacy claim. The staging photo-smoke path
is separate and is not covered by this gate.

The internal `activate_viewer_binding` method accepts a closed version-1
binding with schemaVersion, protocol `ltds-viewer-activation-v1`, activationId,
principalId, submissionId, jobId, generation, environment, viewerTaskId,
viewerAttemptId, requestDigest, manifestSha256 and pipelineDigest. It checks
the immutable job record in an immediate transaction, persists the canonical
binding and digest once, and permits only exact replay. Activation and attempt
IDs cannot be reused under the same principal/environment. A caller must supply
an independently authenticated Viewer principal and its environment: the current environment-only
bearer is insufficient. **No network endpoint or authenticated principal
adapter is provided**, so no production caller can activate this candidate.

The existing source grant is version 1 and does not bind activation digest,
coordinator authority or execution epoch. A fresh claim observation is not an
enduring fence on an already-issued token. The worker therefore holds before
grant retrieval, fetching, combined preparation or any new engine launch until
an activation-bound, broker-enforced grant protocol is implemented. Already
executing work can still be observed and delivered. This deliberate hold means
the candidate cannot process new reference jobs yet.

Rollout requires a jointly versioned Viewer submission/binding/activation
protocol, an authenticated principal boundary, idempotent Viewer outbox and
lost-response recovery, a source grant that binds the activation digest and
authority/epoch and is enforced by both broker and worker, and migration and
rollback that drains/fences older workers and invalidates old usable grants.
Database checks in this source cannot stop a previously deployed worker that
already holds a v1 grant. Existing unbound active/held jobs need explicit
reconciliation, never automatic activation or discarded results. The queue
will intentionally block on an unactivated priority head until activation or a
safe explicit cancellation/reconciliation.

The focused synthetic gate tests are `python -m unittest
tests.test_reference_viewer_activation`. On this candidate the two older suites
`tests.test_reference_store` and `tests.test_reference_worker_api` ran 31 tests
and reported 9 failures and 16 errors: their setup leaves jobs unactivated and
then expects discovery/claim or reservation to succeed. Nine errors were the
explicit activation conflict and seven dereferenced a missing claim or next-job;
six failures expected HTTP 200 for a now-refused claim, one expected a next-job,
one injected a queue-head change after assuming that next-job exists (the
callback fails before the intended assertion), and one expected a claim-insert
trigger to fire even though the gate prevented that insert. These failures were from the earlier baseline; the fixtures now use explicit
prepared bindings and a test-only grant readiness mock, as described below. The broader suite and real process-death/mixed-version tests remain
unqualified. Never deploy this candidate merely because its isolated gate
tests pass.


The provisional prepared-submission API stores the caller-supplied authenticated
principal, environment, request digest and protocol provenance in the same
transaction as the job. It provides no authentication or network endpoint; a
real trusted identity contract remains required. Environment-only recovery cannot
read prepared jobs, and legacy submissions cannot be adopted. Schema bootstrap
requires a version marker, exact definitions, uniqueness and foreign keys and
refuses incomplete or incompatible layouts. Until an activation-bound grant is
implemented, the coordinator also refuses discovery, claims and v1 grant use.
The legacy 31-test suites now use explicit prepared bindings and a test-only
mock future-grant readiness; those tests do not establish v2 grant safety.

The environment-only legacy POST /ltds/v1/jobs validates the request but
returns 503 viewer_prepared_submission_required before inserting a job.
Read-only access to already existing legacy job status remains available.
The original reference API suite expects legacy admission and needs separate
fixture migration; its downstream result and acceptance tests remain unqualified.
