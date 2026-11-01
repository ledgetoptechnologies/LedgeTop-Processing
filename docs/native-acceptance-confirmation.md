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

The additional fields are backwards-compatible for consumers that tolerate
unknown JSON fields. Strict consumers must update before using this candidate.
Cross-language checks against the selected Viewer checkout use the optional
`LT_VIEWER_CANDIDATE_DIR` setting; an unset setting is reported as a skipped test.
