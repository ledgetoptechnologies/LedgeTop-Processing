# Native acceptance confirmation candidate

Successful `POST /ltds/v1/jobs/{jobId}/acceptance` responses add `receiptId`
and `receiptSha256`. The digest is the lowercase SHA-256 of the validated
receipt encoded with Processing's reference-v1 canonical JSON: UTF-8, sorted
object keys, compact separators, literal Unicode and finite values. The exact
same receipt ID and digest are returned on a successful exact replay; `created`
distinguishes initial insertion from replay. Receipt arrays retain their order.

The store returns only after its database transaction context commits. If the
transaction or response is interrupted, the caller must retain its durable
outbox and retry the identical receipt; a missing or failed response does not
prove whether an earlier attempt committed. A conflicting response is not a
confirmation. This confirmation identifies a coordinator metadata tombstone;
it is not independent proof that Viewer stored files or that cleanup is safe.
Before either confirming a replay or relaying receipts to a worker, the store
revalidates each persisted receipt's canonical payload, digest and job binding.
This detects inconsistent rows but is not protection against a writer able to
change both the payload and digest; durable writer fencing remains necessary.
Malformed or inconsistent stored result/receipt metadata is reported as the
sanitized HTTP 409 `stored_reference_inspection` on the public and private
reference adapters, without modifying the affected records. Receipt identities
are globally reserved across success and terminal outcomes, so accepting a new
success receipt inspects those global collision records: one corrupt foreign
row can hold new success acknowledgements across environments until inspected.
The private worker relay examines the requested job's receipt rows only. The
terminal metadata acceptance path applies the same global collision-state
check, and validates stored terminal receipts and failure metadata before a
private relay. These checks preserve suspect rows and return the same inspection
status; they do not authorize artifact cleanup for terminal failures.
The reference listener emits `X-LedgeTop-Reference-Inspection: 1` only for its
own sanitized stored-state HTTP 409. The worker driver recognizes that header
on its authenticated coordinator connection without reading the response body,
reports a held background job (also alongside active foreground work), and
keeps its acceptance checkpoint. Other 409 responses retain their existing
pending/conflict behavior. This is a status observation, not an independently
durable operator alert; a future poll must re-observe the stored hold.
For a 409, any present but duplicate or malformed inspection header produces a
held protocol error; a contradictory marker on HTTP 200 also holds. A 409 with
no marker retains legacy pending behavior, so an intermediary that strips the
marker can conceal inspection and needs separate operational checks.

The additional fields are backwards-compatible for consumers that tolerate
unknown JSON fields. Strict consumers must update before using this candidate.
Cross-language checks against the selected Viewer checkout use the optional
`LT_VIEWER_CANDIDATE_DIR` setting; an unset setting is reported as a skipped test.
