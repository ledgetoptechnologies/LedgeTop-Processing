# Inactive v3 Viewer read contract candidate

Proposed principal-authenticated GET paths are
`/ltds/prepared/v3/submissions/{submissionId}/status`, `/result-manifest`, and
`/accepted-copies`. Status returns submissionId, jobId, generation,
environment, viewerTaskId, viewerAttemptId, activationId, envelopeSha256,
status, phase and activated. Unknown or wrong-principal/environment returns 404.
The result response proposes that same immutable binding plus `resultManifest`,
only after activation, succeeded status, accepted phase, and a closed validated
persisted result with matching job, generation, environment and pipeline digest.

Result-manifest remains hard-held with 503: current state does not prove the
v3 source/dataset/GCP and authenticated worker-to-result custody chain. Existing
receipt tables also contain legacy unsigned rows and do not reverify against an
independent current principal-scoped trusted-key registry, so accepted-copies
returns 503 rather than mislabeling retained receipts as v3 verified evidence.
A trusted worker source, current key/policy contract, signed receipt semantics,
complete source provenance and durable delivery protocol must be agreed before
these holds are removed. This proposal adds no binary retrieval, write,
cleanup authorization, worker launch or mounted route. Tests inject synthetic
terminal database state and mock future result readiness solely to exercise
binding and replay; they do not demonstrate valid execution or custody.
