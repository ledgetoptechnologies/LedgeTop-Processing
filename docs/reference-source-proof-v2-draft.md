# Provisional source proof v2 vectors

These detached validators and golden documents are for cross-language review.
They do not issue or authenticate a proof, transfer bytes, checkpoint files,
rotate grants, or enable a new runtime. The authenticated authority that binds
a root digest to the exact job, reservation and original source manifest is not
yet implemented. A digest supplied by the same untrusted source as its document
does not authenticate that document.

Wire documents use canonical UTF-8 JSON with sorted keys, compact separators,
ASCII identifiers, integer-only numeric fields and no duplicate or unknown
fields. Exact canonical bytes are required; whitespace, alternate numeric forms,
negative zero and escaped alternatives are rejected. The SHA-256 inputs are
the ASCII domain below, one NUL byte, then the canonical document bytes:

- Root: `ltds-source-root-v2`
- Object descriptor: `ltds-source-object-v2`
- Proof page: `ltds-source-page-v2`

The root binds job/generation/environment, the original manifest digest,
4 MiB chunk size and the sorted exact input object closure to object descriptor
digests. An object descriptor binds its ID, complete size and SHA-256 to an
ordered array of page digests. Pages identify their object and zero-based page
index and contain at most 256 chunks. The prescribed count, offset and length
are exact: all chunks are 4 MiB except the final chunk. An empty object has no
chunks or pages and uses the SHA-256 of the empty byte string. The existing v1
submission validator rejects zero-byte inputs; this skeleton does not change it.

The caller must obtain each expected digest from a previously authenticated and
validated parent, validate the descriptor against the original input object, and
validate every page against that descriptor. Those preconditions are not enforced
by a capability or opaque validated type in this draft. Chunk hashes still must
be checked against actual bytes. Exact noncanonical-input rejection, size limits,
and golden documents are tested in Python; Node independently recomputes their
canonical bytes and domain-separated hashes. The vector and all caps are
provisional until both sides agree and qualify the integrated protocol.
The limits in this skeleton apply per document and per object; it does not yet
enforce an aggregate chunk, page, request, storage or I/O budget for a whole
job. A runtime must enforce those budgets before fetching proof pages.

No crash-safe partial-file checkpoint, authenticated proof lookup, coordinator
grant rotation or transport exists here. In particular, validating a page does
not authorize a download or establish durable NAS retention.
