# Original ODM option evidence

This worker opt-in is source-only. It does not enable a worker, create transfer
grants, prove source custody, or authorize an engine mutation. Existing physical
gate, original claim, scratch, engine-version and transfer checks still apply.
The coordinator's durable catalog/admission implementation is a separate part
of this feature; these worker tests do not qualify that storage path.

An optional private worker configuration field enables the profile:

```json
"optionEvidence": { "schemaVersion": 1 }
```

The default omits this field. It sends no profile header and fetches no evidence.
The field is closed, supports only exact integer version1, and is ODM-only. The
canonical `ops.reference_worker_daemon` factory passes the validated decision to
the real `ReferenceDriver`. An enabled driver includes the version in its
immutable `driver_binding` record. Changing the live decision holds before
background work; changing it on cold reopen refuses the journal. Enabling an
existing journal requires a separately reviewed migration or fresh, explicitly
owned empty state. No migration, repair or deletion happens automatically. Old
driver binaries also refuse an enabled binding because it differs from their
exact expected binding. The coordinator-side downgrade fence is independent.

Enabled discovery and claim requests send `X-LTDS-Option-Evidence: 1`. The
coordinator can therefore refuse workers that do not implement this profile
without changing existing v1 claim or submission bodies.

Before the first ODM engine mutation, the driver replays its original
`claimRequestId`, verifies that the response still has the persisted original
claim token, submission and job binding, and requests:

```text
GET /ltds/worker/v1/jobs/{jobId}/option-evidence
Authorization: Bearer <existing worker bearer>
X-LTDS-Claim-Token: <original persisted claim token>
```

There is no request body. The special endpoint uses only the single
`X-LTDS-Claim-Token` header; the existing `X-Claim-Token` header remains unchanged
on other private endpoints. No token or token digest is returned in evidence.

The closed response has exactly these fields: `schemaVersion`, `authorityId`,
`jobId`, `generation`, `environment`, `nodeId`, `workerId`, `workerInstanceId`,
`claimRequestId`, `manifestSha256`, `pipelineDigest`, `capabilityFingerprint`,
`optionsFingerprint`, `catalogSha256`, and `selectedDefinitions`.

The client checks all original identities against its actual persisted claim
and configured worker, including the original capability/options fingerprints.
The envelope is at most512KiB. Definitions are at most262144 canonical bytes and
2048 entries, sorted by the exact requested option names with no duplicate,
missing or unsolicited definitions. `catalogSha256` identifies the coordinator
record; it is not independent proof of runtime configuration or engine identity.

After retrieving that metadata, the driver reads the actual engine's fresh
`/options`, retains the existing safe numeric/bool filter and scalar checks, and
compares the complete selected definition objects canonically. A changed help,
domain, default, type or any other definition metadata holds before
initialize/upload/commit. Empty selected options still require the original
evidence record and all identity checks. No static flag list is introduced.

Once an engine intent exists, this path is skipped. Expiration of the original
input grant, coordinator outage, or later evidence unavailability does not
invalidate collection, outbox retry or Viewer acceptance of existing work.
Those paths keep their original token, reservation and immutable delivery.

The focused tests use the actual driver, coordinator claim store, private TLS
transport and NativeEngine HTTP fixture. Their evidence response is test-owned
metadata derived from the real stored claim, not a fabricated production
authorization. They cover first-start equality, same-name/type metadata drift,
all original bindings, duplicate/bounded definitions, empty options, profile
drift and the canonical factory. A post-start clock beyond the unchanged grant
expiry plus evidence503 still reaches durable coordinator delivery with one
engine initialization and one evidence fetch. Full cache, cross-language
acceptance and deployed-container qualification remain separate evidence.
