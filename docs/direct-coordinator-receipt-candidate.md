# Candidate direct coordinator source receipt

**Review draft, not active API or proof of custody.** The standalone schema,
signature functions and SQLite replay experiment are not wired to Viewer,
coordinator, worker, freeze/backup, quotas, cleanup or any network listener.
Never sign or return this retention guarantee merely because the candidate
validator passes. Real issuance first requires authenticated principal and
authority, verified durable worker and source closure, trustworthy storage and
an independently verified Viewer destination. It does not authorize source
release or result acceptance; those have separate protocols.

`contracts/direct-coordinator-source-receipt-v1.schema.json` defines the closed
envelope and body. The signature input is the ASCII domain
`ltds.viewer.source-acceptance/v1` followed by one NUL byte and RFC 8785/JCS of
the envelope without `signature`. The Ed25519 signature is unpadded base64url.
Receiver pins issuer, key and coordinator authority independently and compares
the exact canonical expected body. Object IDs must be sorted in ASCII order
within each list and unique across source and derived lists; each size is a
positive safe integer and each hash is lowercase SHA-256. The JSON Schema is
structural: implementations must also enforce these semantic rules, the total
count, canonical input bytes, real calendar timestamp and detached signature.
In particular JavaScript must reject noncanonical raw input such as `1.0`
before parsing erases its lexical distinction from `1`.

The receipt commits to the retained, retrievable and independently verifiable
signed snapshot/original-path inventory and engine mapping via digests; it
does not contain the original paths. A digest without retained authorized
evidence cannot reconstruct or verify path provenance. A separate, agreed
path-inventory protocol must preserve exact UTF-8 path bytes and IDs without
normalization or aliasing, reject unsafe paths, and define GCP CSV/image/CRS and
attempt-derived mapping. Do not infer that the existing V1 basename-only
manifest or this receipt implements that protocol.

## Sizing for 10,000 photos

This candidate uses one `sourceObjects` array and one `derivedObjects` array,
up to 10,000 objects **each**, 20,000 combined, and an 8 MiB JCS body. It does not embed or
duplicate the path inventory. The synthetic test builds 10,000 photo entries
plus one derived entry within this budget; it does not establish installed
upload speed, memory usage, database capacity or support for a 10,000-photo
job. Oversized inputs must be held explicitly; never truncate an inventory.

**Integration blocker:** the current direct worker closure and receiver use
`reference_protocol.MAX_OBJECTS = 10,000` across source and derived objects.
Therefore 10,000 photos plus one derived object exceed that worker limit even
though the isolated receipt accepts them. The existing reference HTTP default
and sender receipt limit are 4 MiB; the candidate body alone allows 8 MiB plus
envelope overhead, and its future route has no negotiated bound. There is no
working path or demonstrated capacity for that 10,001-object case. Agree and
implement consistent per-layer count, transfer, persisted metadata and response
limits (or independently verified paginated commitments), then test real
admission, closure, response, restart and exact replay end-to-end. Do not raise
shared V2 limits merely to accommodate this unwired prototype.

An isolated `ltds-direct-10k-each-v1` prototype adds distinct 10,000 source and
10,000 derived ceilings plus a 16 MiB worker manifest bound. Its profile is
included in the immutable worker scratch binding and signed worker closure;
the prior direct profile retains its 10,000 combined ceiling and V2 is not
changed. This is not a negotiated network implementation: coordinator and
worker must authenticate supported profiles, persist the selected profile in
the coordinator reservation and Viewer receipt, and reject downgrade or
replay under a different profile. The actual route must separately enforce
wire bounds before allocation and provide a verified pagination/response
contract; the existing 4 MiB sender must not be silently bypassed. Full
10,001-object durable transfer, disk quota, restart and resource-use tests
remain outstanding. The isolated boundary snapshots a validated manifest and
indexes objects once; its local completion revalidates the complete inventory.
This avoids repeated manifest scans within that boundary, but has not been
profiled or qualified at scale. The 16 MiB check canonicalizes an already
parsed object: it is not a network allocation defense. A future listener must
enforce raw byte and nesting limits before parsing and reject duplicate keys.

For design review, propose a maximum exact relative path length of 1,024 UTF-8
bytes and 255 bytes per component, subject to the actual participating
filesystems and the Viewer's existing upload rules. A future path inventory
should use independently hashed pages (proposed maximum 256 KiB per page,
128 pages, and 32 MiB total), commit ordered page hashes in
the signed root and retain the complete pages for authorized replay. Page
boundaries, byte/node/page totals, max path rules and cross-language vectors
must be agreed and tested; a proposed maximum is not an assertion that the
present Viewer or Processing supports it. Do not lower an existing user-facing
limit silently.

## Exact replay and remaining gates

The isolated SQLite experiment uses `(viewerPrincipalId, operationId)` and the
signed `submissionIntentSha256` as an idempotency binding. It requires FULL
synchronous transactions, persists exact canonical signed bytes before return,
returns those exact bytes for identical replay and refuses changed body,
identity, key or receipt. The full signed request, authority, mode revision,
worker receipt, object inventory and destination must be compared by the real
authenticated adapter; a hash supplied by the caller without reconstructing
the committed request is insufficient. An authenticated GET must recover lost
responses. The experiment does not establish request authorization, custody,
physical filesystem guarantees, freeze/migration compatibility, key lifecycle
or Viewer acknowledgements. Pin-accounting recovery and the path/GCP closure
are still unimplemented. Old V2 payloads and receipts remain unchanged.

The checked-in interoperability vector contains a **public synthetic** RFC
8032 test key, synthetic IDs and hashes only. The Node test verifies a fixed
ASCII/safe-integer vector; its small canonicalizer is deliberately not a
general-purpose JCS implementation. Production clients need independently
qualified RFC 8785 implementations and vectors including Unicode path pages,
failure/restart boundaries, invalid types, duplicates and trust changes.

The initial six-test run at `4be566a1` used the existing local image, offline:
`docker run --rm --name ltds-receipt-tests-c7d3205c --network none --read-only --user 10001:10001 --cpus 1 --memory 768m --pids-limit 96 --tmpfs /tmp:rw,nosuid,nodev,size=64m --mount 'type=bind,source=C:/Projects/processing-direct-coordinator-receipt-vectors-20261106,target=/src,readonly' --workdir /src --entrypoint python sha256:bc48c6a81300a7c9b610c6e0f3f4d7cb8aa599075645b2ff1ac1c891dc0ce8c4 -B -m unittest tests.test_direct_coordinator_receipt_candidate`.
It returned `...... Ran 6 tests in 0.097s OK`; it was a prototype-only test.

An independent bounded Linux run of the standalone worker-boundary tests at
`935fdd40` passed 8/8 (local synthetic boundary only, not a network or Viewer
test). Its retained evidence is `C:/Users/fstor/AppData/Local/Temp/ltds-boundary-935fdd40-4c2b`:
archive SHA-256 `e3a6292a26e962e7b5fd9bdfb303892f4e4eb9f13a5de202044c71682c0c5aba`,
log SHA-256 `9b9aef77410d846e3fdec552333988c75b1e719affd79a63c899f0e9d50900f5`.
