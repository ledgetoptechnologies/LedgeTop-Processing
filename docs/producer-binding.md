# Producer identity for derivative-only recovery

Source candidate based on installed coordinator revision `1961b4aa`. Not deployed.

The HTTPS facade advertises `producerBinding.version=1` and
`pathTemplate=/task/{taskId}/producer-binding` only when the service implements it.
GET uses the existing environment-specific credential and returns 404 for tasks
outside that credential's environment. Plain HTTP is rejected. Responses use
`Cache-Control: no-store`.

The mapping is selected from the durable facade task and execution job in one
SQLite statement. Both must be successful; task phase must be complete. UUIDs,
producer generation, node, job environment/profile and immutable archive digest
must be consistent. No input files are enumerated, archive bytes rehashed, or
queue/result/database records changed.
The execution request UUID must match the facade generation's derived incarnation,
and the completed job's native task/node/environment record must agree too.

The exact response fields are:

- `schemaVersion: 1`, `providerTaskId`, `producerGeneration`, `environment`;
- `jobId`, `nativeTaskId`, `nodeId`, `archiveSha256`;
- `archiveEvidence: "producer-record-not-fresh-byte-verification"`;
- `bindingSha256`: lowercase SHA256 of UTF-8 `ltds-producer-binding-v1\0`
  followed by the other fields encoded as JSON with sorted keys, no insignificant
  whitespace, and no non-finite numbers.

The digest is a consistency checksum, **not a signature**. Authenticity comes
from the configured provider's verified HTTPS connection and credential scope.
`providerEndpoint` belongs to the Viewer's configured binding/resolver and is not
inferred from a Host header or returned as a caller-controlled value.

The Viewer must compare the result with its configured provider/environment,
attempt's provider task UUID and its independently verified downloaded archive
SHA256. Viewer initialization generation is a separate local counter and must
never be substituted for `producerGeneration`. Persist the producer binding
before adoption. A current response alone cannot prove the historical mapping
of an older reused task UUID: unknown legacy incarnation history requires
explicit reviewed adoption evidence, otherwise reject it.

This endpoint does not attest fresh archive availability, texture/geometry
quality, Viewer readiness, custody, or permission to delete files. Supported
derivative recovery still needs independently verified source/OBJ/MTL/texture/GLB
and output inventories, a fenced idempotent journal, atomic finalization and a
matching durable acceptance acknowledgement. Missing binding or verification
must reject adoption; there is no reconstruction retry or manual database fix.
