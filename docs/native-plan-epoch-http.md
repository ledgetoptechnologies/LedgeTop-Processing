# Disabled native-plan epoch HTTP candidate

This unadvertised route is available only through explicitly constructed test
configuration with the hard-false qualification flag patched. Environment
configuration cannot enable it. It provides no production authorization,
transfer, dispatch, deletion or capacity reservation. The internal epoch's
old-writer, both-table-loss, non-quiescence and power-loss limitations remain.

The prefix is `/task/ledgetop/plan-epoch-v2/{taskUuid}/{operationId}/1/1`.
Task and operation IDs are canonical UUIDs; this candidate fixes generation
and epoch at 1. The final two components are expectations, not proof that a
client allocated the generation. Every request uses the existing environment
token and `X-LedgeTop-Capability`. At fresh `/open`, no reservation exists:
the server authenticates the shared environment bearer and validates only the
new capability's syntax before reading the body. That does not authenticate a
pre-existing capability, an individual user, engine ownership or allocation.
An environment-token holder can create fresh retained enrollment history up to
the configured fixed limit; separate principal identity, fair quota and admission
controls are not qualified.
Every later action checks the stored capability and epoch before reading a body
and rechecks them inside the mutation transaction.

All POSTs require exactly one Content-Length, application/json, no transfer or
content encoding and canonical bounded UTF-8 JSON. Open/revoke bodies are at
most 8192 bytes, manifest at most 4 MiB, intent/accept at most 65536 bytes.
Socket body reads have a 30-second monotonic deadline after prechecks; this is
not an end-to-end deadline for authentication, storage, OS stalls or scheduling.
GET has no body. Responses are bounded canonical UTF-8 JSON, no-store, without
redirects. The application suppresses request-path logging; proxy and external
logging must be separately configured not to expose query tokens.

* POST `/open`: closed `{schemaVersion:2,operationKind:"plan_bind",manifestId,manifestSha256}`.
* POST `/manifest`: exact canonical native input-manifest bytes.
* POST `/intent` and `/accept`: exact canonical native plan bytes, empty options only.
* POST `/revoke`: closed `{schemaVersion:2,requestId,expectedManifestSha256,expectedManifestBindingId,expectedPlanSha256}`;
  the final two fields are explicit lowercase hashes or null.
* GET on the prefix returns status.

Each successful response is the closed wrapper `{protocol,version,taskUuid,
operationId,environment,generation,epoch,result}`, where protocol is
`native-plan-epoch-v2-experiment`, version is 2, and generation/epoch are 1.
The result contains the exact internal open, manifest, intent, plan or closed
receipt described in `native-plan-epoch-prototype.md`; nested manifest and plan
receipts retain their own v1 versions and false readiness flags. Open status has
`open_unbound` or `open_bound`; closed status and revoke have
`closed_without_bind` or `bound_then_closed`, requestId, exact retained manifest
receipt or null, explicit plan-intent presence/hash, and exact plan receipt or
null. `closed_without_bind` means no **plan** accepted; a manifest and plan
intent may exist. Canonical wrapper bytes are identical for successful revoke
replay and subsequent closed status.

Exact open/manifest/intent/accept replays work while OPEN; `/open` replay
returns the original open acknowledgement even if later steps have advanced.
After close, those operations hold. An identical successful revoke returns the
retained receipt; a changed or stale expectation holds. Status is a transactional
observation, not a fence against an already-delayed POST. A local Viewer change
is invisible until a revoke commits. Clients must retain their own durable
intent and hold on unknown or ambiguous outcomes.

Errors are generic JSON: 400 malformed input, 401 invalid bearer, 404 missing
or bad capability/unknown route, 409 conflict/closed/uncertain identity, 411
invalid length, 413 body too large, 415 media/encoding, 408 body deadline and
503 operational failure. These categories do not establish cross-principal
occupancy secrecy: the bearer identifies an environment, not a user.

For a synthetic cross-language test in one POSIX network namespace, launch
`PROCESSING_SYNTHETIC_EPOCH_HTTP_FIXTURE=1 python -m tests.native_epoch_http_fixture`
with stdin/stdout pipes from this source tree. The first stdout line is JSON
with `host`, ephemeral `port` and `fixture`. The process uses newly created
temporary state, fixed synthetic credentials from `tests.test_native_manifest_binding`,
test-patched qualification and a synthetic healthy catalog. Send `STOP\n` to
stdin and wait for exit; it also stops after 120 seconds without that message.
The 120 seconds bounds the stdin wait, not total process lifetime or OS stalls;
the invoking test must impose its own process timeout. The launcher tracks
accepted handlers and waits up to five seconds for them after listener shutdown.
An uncertain cleanup retains the temporary state and reports its path. This
launcher is loopback HTTP only, not a TLS, production or engine test.
