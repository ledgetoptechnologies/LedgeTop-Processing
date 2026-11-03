# ODM-only Viewer import confirmation v2: provisional verifier

This is an **inactive proposal**, not a confirmed Viewer wire contract. It has
no endpoint, persistence, trusted binding loader, cleanup release, or live
integration. The existing engine and scratch cleanup holds remain unchanged.
The version and field names below must be confirmed by the Viewer owner before
pairing; this is intentionally distinct from the older generic v1 proposal.

The first scope is one result manifest object only, with role `odm_archive`
and a nonzero byte count. Additional roles, TreeDar-only, and mixed ODM/TreeDar
manifests are rejected until their real intake and full-result semantics are
agreed. The Processing worker independently verifies a previously durable,
trusted job/generation/environment, input/pipeline/result digests, prebound
Viewer task/attempt IDs, Viewer issuer, accepted-copy envelope digest, and the
archive object's ID, byte count and SHA-256 from its own manifest. Accepted-copy
coverage must be complete and its existing Ed25519 receipt independently valid.

The closed signed confirmation object is:

```json
{
  "protocol": "ltds-viewer-odm-import-confirmation-v2",
  "schemaVersion": 2,
  "issuerId": "viewer-issuer",
  "keyId": "pinned-import-key",
  "confirmationId": "immutable-confirmation-id",
  "jobId": "processing-job-id",
  "generation": 1,
  "environment": "production",
  "inputManifestSha256": "<64 lowercase hex>",
  "pipelineDigest": "<64 lowercase hex>",
  "resultManifestSha256": "<64 lowercase hex>",
  "viewerTaskId": "prebound-viewer-task-id",
  "viewerAttemptId": "prebound-viewer-attempt-id",
  "modelId": "viewer-asserted-model-id",
  "modelVersionId": "viewer-asserted-version-id",
  "outputId": "viewer-asserted-output-id",
  "outputInventoryFormat": "viewer-model-output-inventory-v1",
  "outputInventorySha256": "<64 lowercase hex>",
  "acceptedCopies": [{
    "acceptanceReceiptId": "accepted-copy-receipt-id",
    "acceptanceStorageReceiptId": "accepted-copy-storage-id",
    "viewerReceiptId": "signed-viewer-receipt-id",
    "viewerStorageReceiptId": "signed-viewer-storage-id",
    "envelopeSha256": "<64 lowercase hex>"
  }],
  "objects": [{
    "id": "odm-archive-id", "role": "odm_archive", "byteSize": 1,
    "sha256": "<64 lowercase hex>"
  }],
  "archive": {
    "objectId": "odm-archive-id", "byteSize": 1, "sha256": "<64 lowercase hex>",
    "inventoryFormat": "viewer-processing-archive-evidence-v1",
    "inventorySha256": "<64 lowercase hex>"
  },
  "milestone": "ready_for_review",
  "readyAt": "2026-11-03T01:02:03.000Z",
  "signature": "<canonical unpadded base64url Ed25519 signature>"
}
```

The one envelope digest is SHA-256 of `reference_protocol.canonical_json` of
the exact accepted-copy `{acceptance,viewerReceipt}` envelope. Signed-copy and
import key IDs resolve through separately configured Ed25519 public-key pin
maps `{keyId:{issuerId,publicKeyPem}}`; both issuer identities must equal the
trusted expected Viewer issuer. The import signature uses domain bytes
`LTDS-VIEWER-ODM-IMPORT-CONFIRMATION-V2` plus one NUL byte, followed by the
canonical JSON body without `signature`. The stable confirmation digest is
SHA-256 of canonical JSON of the entire signed object.

Model/version/output IDs and both inventory hashes are **Viewer's signed
post-ingest assertions**, not values Processing pretends to know before
ingestion. The Viewer signer must issue these only after the exact prebound
attempt imports the accepted archive, confirms its byte count/hash, completes
required derivatives and atomically reaches durable `ready_for_review` under
its storage/writer fence. The verifier cannot itself prove this occurred and
does not accept inventory bytes. Compromised or incorrect authorized Viewer
issuance remains a trust risk.

Both inventory formats use SHA-256 of UTF-8 JavaScript `JSON.stringify` over
an array sorted by `relativePath` using JavaScript string ordering; each
object is constructed in `{relativePath,byteSize,sha256}` insertion order.
The labels identify different semantic inventories. Python sorted-key JSON and
Python Unicode path sorting are not interchangeable with that encoding. A
synthetic Node test exercises the encoding and Unicode ordering but is not an
actual Viewer ingestion or cross-repository wire test. A separate synthetic
Node-generated Ed25519 signature is verified by Python; it does not exercise
the Viewer signer.

Callers must supply independent trusted values and current public-key pins.
They must select both trust registries for the independently determined
environment and authorized Viewer issuer; a staging key trusted in production
could otherwise sign a production assertion. Shared keys cannot isolate those
environments. Callers
they must never populate expectations by copying the received confirmation.
The first verified canonical signed bytes/digest must be persisted atomically
with the binding, and every replay must compare against that first digest.
The pure verifier cannot enforce first-write persistence. Its returned bytes
are immutable and each decoded view is detached, but the result type is publicly
constructible and is not an authentication capability. A future raw request
handler must limit bytes before parsing and reject duplicate keys/invalid UTF-8.
Existing bounded input snapshots apply after parsing only.

Before enabling: obtain confirmed field names and shared signed cross-language
vectors; implement and verify pre-submission/returned-job mapping, Viewer
descriptor-safe archive intake and transactional readiness outbox; implement
durable first-proof/replay storage against the revised v2 verifier; and test
crash/retry, deletion gates, key rotation, actual Viewer persistence and live
recovery. The earlier provisional v1 store is not wired to this verifier.
