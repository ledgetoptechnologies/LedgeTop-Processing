# LiDAR provenance boundary

`LidarProvenanceClient` authenticates metadata from the same explicitly configured
HTTPS broker used for input grants. It does not establish acquisition provenance
from a filename, a LAS header, or an operator checkbox. The NAS issuer must derive
its decision from its trusted acquisition records and current revocation state.
The issuer route and its durable intent ledger are not installed by this client.

The fixed request is `GET /api/processing/reference/v1/provenance/{receiptId}`.
It uses the current input grant's bearer token and sends these redundant binding
claims: `X-LTDS-Job-Id`, `X-LTDS-Generation`, `X-LTDS-Environment`,
`X-LTDS-Input-Manifest-Sha256`, `X-LTDS-Pipeline-Digest`, `X-LTDS-Object-Id`,
`X-LTDS-Object-Sha256`, and `X-LTDS-Object-Bytes`. The issuer must compare them
with its persistent grant, submission intent and source records; request headers
are not authority. Existing input-object GETs retain their existing header contract.

The response is one closed JSON object:

```json
{
  "schemaVersion": 1,
  "inputManifestSha256": "<canonical manifest digest>",
  "pipelineDigest": "<canonical pipeline digest>",
  "provenance": "<the existing closed LiDAR provenance receipt object>"
}
```

The placeholder above represents an object, not a string in the actual protocol.
The receipt must bind the exact job, generation, environment, selected LiDAR
object, bytes and SHA-256. Synthetic acquisition, unapproved vertical datum,
incorrect units, redirects, encoded bodies, malformed JSON and unknown fields
are rejected. Metadata has a separate bounded allowance; it neither spends nor
expands the input object's byte grant. Defaults are 64 KiB, two simultaneous
reads and at most 30 seconds per request, also bounded by the grant's expiry.

Reserve a unique per-submission provenance intent ID before deriving the final
pipeline digest. Freeze that intent and digest before submission; seal the same
ID only after the authenticated job/generation/reservation are known. An ID may
never be resealed for another job, generation or content. A retry generation
needs a fresh intent and submission identity. Reusable acquisition observations
are separate immutable content-bound records.

The worker stores exact authenticated receipt evidence in a private synchronous
SQLite journal. It rereads the issuer before transferring input and again before
starting TreeDar. A new read invalidates cached usability before network I/O;
missing, revoked or unavailable responses cannot fall back to an earlier green
receipt. Verification recognizes only the exact persisted receipt within its
freshness window and unexpired grant. Clock regression fails closed. The engine
checks this again after input hashing and host probes, before reserving the
physical gate or mutating Docker. This is a bounded fresh-read guarantee, not
an atomic transaction with a remote issuer's later revocations.

Per-job execution routes persist the immutable claim and engine version/source
pins before transfer or engine intent. The router permits old accepted artifacts
to be read after an idle ODM upgrade; a changed runtime cannot resume an old
execution. TreeDar's controller and delivery journals still pin their full
configuration globally. Reopening those journals with a different image/source
is rejected. A future TreeDar upgrade needs an explicit historical-adapter and
journal migration strategy; automatic TreeDar upgrades are not ready.
Unbound legacy claims cannot be silently adopted. TreeDar-only routing is main
node only; combined ODM-to-TreeDar jobs remain rejected until their sequential
stage and final acceptance contract is implemented. Public TreeDar capability
and Gaussian splats remain disabled.

The source checkpoint passed 634 Linux tests without skips, including real
SQLite/control/outbox/gate integration with controlled HTTP and Docker edges.
The actual cached TreeDar 0.0.1 bootstrap also recovered three known synthetic
crowns and verified all 16 output hashes. Neither result proves genuine LiDAR
acquisition, an installed NAS issuer, or production worker activation.
