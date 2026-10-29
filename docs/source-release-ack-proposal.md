# Source release ACK proposal for Viewer / Processing review

Status: proposal only; no endpoint, validator, issuer or cleanup is enabled.
Existing `contracts/source-retention-v1.contract.json` remains unchanged.

The candidate pure validator is `processing/reference_source_release_ack.py`;
its canonical synthetic fixture is `contracts/source-release-ack-v1.candidate.json`.
Shared machine-readable parity vectors are in
`contracts/source-release-ack-v1.negative-vectors.candidate.json`. Each has a
JSON value or UTF-8 wire text, shallow context overrides and expected validity.
Wire integer fields use integer tokens; `1.0` and `1e0` are rejected, as in the
existing Python reference decoder's positive-integer validation.
These check syntax and expected bindings only, perform no I/O, and are not wired
to an endpoint or cleanup path. The fixture's synthetic closure digests do not
represent resolved finality or cleanup. Peer Node validation and joint acceptance
remain pending; existing source-retention receipt bytes are unchanged.

Viewer must first persist its original-source custody receipt and a pin covering
the complete manifest in the same transaction. Its authenticated issuer returns
that existing receipt only after custody is established. Every relevant delete,
purge, cascade, source replacement, import rollback and trash collector checks
those pins. A capacity reservation is not custody. A separately permanent raw
CSV retention pin is never released by this protocol.

Processing needs an authenticated re-resolution route for that exact receipt,
bound to the actual client and job/generation/environment/manifest/pipeline.
It must resolve durable terminal acceptance and whole-parent finality as well.
Missing, mismatched or unavailable state holds local copies. A source receipt,
result acceptance, terminal status or elapsed TTL alone is not deletion authority.

After independently resolving those records, Processing can persist each existing
`input-copy-release` record (`processing/scratch_policy.py: RELEASE_FIELDS`),
release its own worker/downstream pins, and reconcile journaled exact local-copy
cleanup. It must account for every known worker/stage/generation copy of the
parent, including abandoned retry assignments and uncertain cleanup. Unknown
participants, active readers or unacknowledged children hold the final ACK.

Proposed closed compact ACK body (maximum 16 KiB, canonical JSON):

```
schemaVersion: 1
receiptType: "source_release_ack"
validationVersion: "ltds-source-release-ack-v1"
ackId
sourceRetentionReceiptId
sourceStorageReceiptId
jobId
generation
environment
inputManifestSha256
pipelineDigest
pipelineClosureSha256
localCleanupClosureSha256
acknowledgedAt
```

IDs use the existing ASCII slug bounds, digests use lower-case SHA256, generation
is the existing positive safe integer, and time is canonical UTC milliseconds.
The ACK binds the exact issued custody receipt. Its
`sourceRetentionReceiptId` refers to the same source receipt named
`sourceCustodyReceiptId` in the existing local `input-copy-release` record.
The authenticated Processing identity is transport/durable registration
authority, never a caller JSON field.
Receipt-role uniqueness is enforced in durable stores across source, storage,
result, terminal and ACK roles. The ACK ID is persisted before the first send;
exact replay returns the same response, changed bytes under that ID conflict.

`pipelineClosureSha256` is the already resolved whole-parent finality commitment;
it must not be reconstructed from a caller's unverified list. The new
`localCleanupClosureSha256` needs a jointly agreed domain and canonical fixture:
the durable sorted closure of all authorized copy holders, their exact release
record digests and terminal cleanup receipts. Its digest is a commitment to
independently resolved records, not proof of cleanup or authority by itself.
Root must supply that closure definition and actual resolver before a validator
or transport handler can be used as release authority.

Viewer validates authentication, ownership, complete binding and existing source
receipt state, then transactionally stores the ACK and retires only that job's
processing-custody pins. It must retain normal project originals and unrelated
holds. Releasing this pin is not an instruction to delete a dataset. The reply
returns the exact durable ACK identity and canonical body digest; Processing
retains its outbox until that response is durably reconciled. A lost response
retries the same ACK; neither side invents a new identity on timeout.

Endpoint paths and role-specific credentials remain to be agreed with Viewer.
Do not reuse result-acceptance handlers or source-retention issuance as ACKs.

Required shared Python/Node fixtures: exact success/replay, changed-ID replay,
wrong caller/client/environment/generation/source receipt/digests, duplicate
receipt roles, incomplete local/stage closure, unknown/uncertain old assignment,
revoked/missing custody, active downstream/read pin, open local cleanup intent,
lost response after commit, restart before send and after commit, concurrent
purge versus pin/ACK, and raw CSV retention after a valid processing ACK.
