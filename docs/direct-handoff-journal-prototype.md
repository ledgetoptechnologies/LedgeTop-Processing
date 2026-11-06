# Coordinator direct handoff journal prototype

This is an unwired in-process seam, not a production Viewer API. A future
listener must independently supply transport truth and a principal-scoped
bearer. The journal checks that principal against the already held direct
reservation and selected profile, checks the immutable job, manifest, authority,
node and worker bindings, and records each object intent before invoking the
actual TLS worker sender. Successful worker observations are revalidated and
persisted. A concurrent send, lost response or process death leaves an intent
held; no retry, requeue, reservation release or source-deletion authority is
inferred from it. `status` is observation only.

After every recorded object is observed, the journal records a closure intent,
invokes the sender, verifies the pinned worker's signed closure and persists it.
Exact receipt replay re-verifies the signature and bindings. This is only a
worker-local closure observation, with `sourceCustodyAccepted=false` and
`processingAuthorized=false`. It is not a coordinator-to-Viewer receipt and
does not establish result `fullyAccepted` or authorize any cleanup.

The coordinator freeze operation covers the synchronous worker call and the
journal precharges logical per-session/object metadata from the shared quota.
Those allowances are not a physical SQLite/WAL growth bound. The prototype
does not establish authentic Viewer source snapshot/path or GCP provenance,
live capability freshness, safe uncertain-pin recovery, installed durable
storage, actual worker provisioning, cross-service request schema, or
authenticated replay after a lost Viewer response. Never mint the candidate
retention guarantee from this journal alone. Keep originals and all unresolved
reservations until a separately qualified recovery and acceptance protocol says
otherwise.

For a long-lived handoff instance, the manifest is fully checked when the
session opens and again before closure; per-object operations use an immutable
sender index, scalar binding checks and SQLite-guarded input fields. The shared
quota path uses bounded per-session counters maintained by transaction triggers;
reopen and completion perform a full object/orphan census. This relies on the
coordinator owning its database and triggers: same-process code with arbitrary
schema access is not sandboxed. Recreating a handoff instance for each object
would repeat the full census and is not a qualified integration. The earlier
prototype handoff-table schema is refused for inspection, not automatically
migrated.
