# Proposed v2 paired authority states — not approved

This is a design and a test-only model, not a wire protocol, endpoint, authority
implementation or basis for enabling v2. The services have distinct durable
stores: there is no atomic transaction spanning them. All operations below bind
the original request hash, delivery ID, claim lineage, worker instance, job,
generation, manifest, reservation, environment and coordinator epoch. Exact
canonical schemas and error envelopes remain to be agreed.

1. Coordinator persists `PINNED` under current claim authority before issuing a
   scoped grant. The pin blocks reassignment and survives expiry, revocation,
   disconnect and restart. Pin identity includes a unique delivery and original
   actor. If issuance or delivery is ambiguous, it stays pinned.
   The job pin and each delivery's stream fence are separate durable records;
   stopping one stream does not free the job or authorize another delivery.
2. Viewer verifies its own connection peer and signed grant, consults the
   current authority/fence through a qualified channel, and atomically records
   request, unique delivery, full byte/request charge and `CHARGED` in its
   authoritative ledger. No data may stream before that commit. No duplicate
   delivery streams or refunds follow lost acknowledgments. A crash between
   stores or during streaming is a held reconciliation state, not absence.
3. Viewer streams only for that committed delivery while the coordinator pin
   remains enforced. Expiry or revocation prevents new starts; what stops an
   already running stream must be an actual enforceable process/handle fence,
   not a timer, callback, disconnected TCP socket, or untrusted assertion.
4. Processing verifies exact complete bytes, fsyncs its owned file and parent
   directory, and commits a receipt binding the full identity, size and digest.
   Receipt is evidence of local bytes, not alone proof that the sender stopped.
   An ACK must be authenticated and idempotently recorded at both sides. The
   modeled order is Viewer acknowledgment then coordinator acknowledgment,
   each scoped to exact original request, delivery and receipt digest; a lost
   response replays only the exact recorded ACK. Either missing record holds.
   ACK records may precede independently verified stream quiescence, but cannot
   release the pin until it is established. After release, a current-authority
   exact-binding terminal lookup and exact ACK/release replay return the same
   retained terminal result without reopening the pin or starting a stream;
   stale, conflicting or unanchored observations hold.
   Actual authentication, durable writes and cross-store reconciliation are
   still unspecified and not supplied by the abstract model.
   pin remains held until the coordinator verifies both exact durable outcome
   and independently qualified sender quiescence; failure stays held.
   A separately authorized terminal cancellation may release the job after
   qualified quiescence and exact terminal accounting, including proven absence
   or a failed charged delivery; its charge is not automatically refunded.
   Exact authorized terminal lookup and cancellation replay after a lost reply
   return the same cancellation; stale or conflicting bindings remain held.
   An absence requires independently authenticated current-ledger coverage of
   the entire admission interval; for a charged delivery it requires exact
   charge evidence. Missing, pruned or unknown accounting cannot cancel.
   Crash after stream fencing but before cancellation, adoption or release
   retains the job pin and permits only exact idempotent reconciliation.
5. Recovery uses an independently authenticated, auditable adoption with a
   fresh strictly increasing recovery generation and actor, exact predecessor
   identity and enforceable fence evidence. It cannot mutate the original
   request identity or reset a charge. Adoption alone grants no second stream;
   any new delivery requires independently charged authority and a defined
   idempotency and partial-data policy. Until all checks pass, hold.
   A new delivery after adoption is deliberately outside this model.

For reconciliation, `recorded` must identify a durable charged record from a
consistent authenticated ledger snapshot. `absent` additionally requires a
trusted current ledger identity/epoch, a monotonic retained-history watermark
covering the entire possible admission interval, and a fenced predecessor. A
lost anchor, pruned interval, stale epoch or ambiguous in-flight request means
`unknown`. The watermark's units and durable authority are not yet agreed.
Restored stores cannot certify their own freshness: require an independent
monotonic anchor or fail-closed offline re-enrollment that also isolates every
old writer. No automatic pin release or charge reset follows restore.

Proposed trust boundaries: end-to-end service mTLS where feasible. If a proxy
terminates TLS, authenticate both proxy and backend over a protected channel,
strip client-provided identity headers, and bind any forwarded identity to the
verified connection; an unauthenticated header is never actor proof. Coordinator
signs versioned domain-separated grants for Viewer; Viewer signs scoped ledger
observations for Processing. Ed25519/key ID is a proposal, not a settled choice.
Trust stores need explicit issuer/audience/operation, rotation overlap, key
revocation and bounded clock rules. Signature lifetime limits new admission; it
does not terminate an already started stream or release a pin. No trusted key,
revocation system, monotonic anchor or enforceable remote stop is installed by
this draft.

Synthetic `tests/test_reference_v2_authority_model.py` models selected state
invariants only. Its exact actor/epoch/delivery tuples are synthetic labels,
not authenticated proofs; it cannot prove
cross-process fencing, external storage durability, TLS, signing, or live Viewer
custody. Qualification also requires process-kill tests at each cross-store
boundary, stale binaries, real proxy behavior, revocation midstream and restored
database/anchor tests in the actual topology.
