# Inactive principal-scoped v3 activation candidate

Proposed authenticated POST `/ltds/prepared/v3/submissions/{submissionId}/activation`
requires closed JSON `{activationId,viewerTaskId,viewerAttemptId,envelopeSha256}`.
Identity and current policy come from the separate trusted registry and server
policy, not the request. A successful synthetic activation response is
`{activationId,bindingSha256,created}`; exact replay returns created=false.
Wrong principal/environment is 404 and changed binding or policy is 409.
New v3 prepare atomically records a canonical server policy snapshot; missing,
changed or corrupt snapshots hold. Older v3 rows without a snapshot require
explicit reconciliation and are not silently upgraded.

This route is unmounted and hard-held with HTTP 503 because the reference-v1
manifest cannot faithfully express the agreed dataset/source provenance. Viewer
requires durable dataset ID + manifest hash; stable dataset file IDs; separate
normalized relative paths and hashes; an exact per-attempt generated
`gcp_list.txt` snapshot as a text/plain auxiliary; original GCP CSV retained as
custody/provenance; and administrative CSV excluded from processing closure.
The source snapshot and GCP bytes, hashes and identities must be bound to the
Viewer task and attempt. Duplicate basenames cannot be silently flattened.
No mapping, authoritative source/grant binding, safe cleanup, worker launch or
live listener exists here. Tests mock readiness solely to exercise journal
transaction semantics; they do not prove source safety. Current policy registry
freshness, peer authentication, restart deployment fence and agreed HTTP schema
remain required before any live enablement.
