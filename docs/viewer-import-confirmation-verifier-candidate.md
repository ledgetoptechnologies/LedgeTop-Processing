# Viewer import confirmation verifier: inactive proposal

This source-only candidate defines a domain-separated signature check. It is
not a Viewer agreement, persistence mechanism, network route, evidence of an
import, or permission to delete data. Both engine and scratch cleanup holds
remain in force. A compromised or incorrectly implemented authorized Viewer
signer could still attest to an import that did not occur.

The caller must first obtain a trusted durable binding for the Processing job,
Viewer task/attempt/model/version/output, output inventory digest, accepted-copy
envelopes, Viewer issuer, and (for ODM) archive inventory digest. Incoming
confirmation fields cannot establish their own expected identities. The exact
accepted-copy envelopes must already be independently retained; the verifier
checks their signatures and exact full, disjoint result-object coverage again.
The Viewer issuer is shared logically across accepted-copy and import keys;
separate public-key pin maps permit independent key rotation. TreeDar-only
results have no ODM archive; combined results require one archive and the full
role-object closure. This candidate does not attest that TreeDar artifacts were
actually imported.

**Current integration deadlock:** no trusted channel currently supplies the
Viewer model/version/output IDs or output and archive inventory digests to
Processing independently of the proposed confirmation. The verifier therefore
cannot yet be safely wired. Copying those expected values from the incoming
confirmation merely to satisfy the check would remove the intended binding;
keep the verifier inactive and the cleanup holds in place until the Viewer and
Processing owners agree on an authenticated source or a revised protocol that
establishes equivalent guarantees.

The closed signed body uses protocol `ltds-viewer-import-confirmation-v1`,
schema version 1 and domain `LTDS-VIEWER-IMPORT-CONFIRMATION-V1` followed by a
NUL byte. It includes issuer/key/confirmation IDs; job, generation, environment
and input/pipeline/result digests; Viewer task, attempt, model, version and
output IDs and output-manifest digest; accepted-copy envelope identities and
digests sorted by envelope digest; exact result objects in manifest order; an
ODM archive identity/size/hash and inventory digest (or null with no ODM); and
`ready_for_review` with a millisecond UTC timestamp. Signature is canonical
unpadded base64url Ed25519 over domain plus `reference_protocol.canonical_json`
of the body without signature. The confirmation digest hashes canonical JSON of
the entire signed object.

Before any hashing or canonicalization, all arguments are detached into bounded
JSON snapshots and share a conservative
16 MiB serialized-size budget and limits of 500,000 nodes, nesting depth 12,
10,000 list items, 64 map entries, and 8,192 UTF-8 bytes per string (128 bytes
per key). Only exact built-in JSON types and safe integers are accepted; cycles,
invalid Unicode, and custom container subclasses fail closed. These limits are
part of this proposal and must be negotiated with Viewer. The verifier returns
immutable canonical bytes and their digest; each `confirmation` property read
decodes a new detached copy, so mutations cannot alter the retained proof.
The caller must obtain coherent trusted expected values from its own durable
transaction; snapshotting cannot make separately read database rows atomic.
The result type is a data container, not an unforgeable capability: callers
must rely on the verification operation and durable binding, never on the
presence of an instance of that type alone. At every trust boundary, invoke
`verify_import_confirmation` on the decoded confirmation and independently
trusted bindings; never accept a caller-supplied
`VerifiedImportConfirmation` instance as authenticated proof. Its public
constructor checks snapshot consistency but does not verify a signature.

A future request handler must enforce an endpoint-specific raw body byte limit
(no greater than 16 MiB) while reading, before JSON parsing, and reject invalid
UTF-8 and duplicate JSON object keys. The verifier's bounded snapshot operates
on already-parsed values and cannot prevent parser-time resource exhaustion or
recover duplicate keys discarded by a JSON parser.

On first receipt the caller must atomically bind and persist the verified
confirmation digest before any action. Replays must supply that stored digest;
changed signed confirmations are refused. The pure verifier cannot enforce
first-writer persistence or prevent a caller from falsely declaring every call
initial. Its API deliberately offers no cleanup-release method.

Before integration: agree schema, trust boundaries and cross-language canonical
vectors with Viewer; implement authenticated task/attempt mapping, same-descriptor
verified intake and archive receipt binding; atomically persist Viewer readiness
and a retryable outbox; verify durable proof against local bindings and store it
across restarts; and test crash, replay, key rotation, and worker cleanup paths.
