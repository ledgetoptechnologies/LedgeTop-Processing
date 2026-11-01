# Disabled v2 client journal: prototype boundary

The client persists a canonical original request binding and distinct delivery
identity before any future network operation. Dispatch is one-way: once marked
started, the same delivery cannot be sent again, including after a crash before
an outcome is known. Same IDs with changed bindings hold. A new delivery is not
permitted merely because an observation says absent or unknown; it requires
current authenticated authority, durable server-side budget admission and any
required recovery adoption. Old generation/epoch/holder values are never
rewritten; adoption is a separate fenced audit record.

The existing broker v2 authorization, reconciliation signature, ledger epoch,
watermark, adoption and network contracts are not implemented or finalized.
For that reason no candidate method is wired to an endpoint or runtime, and
all qualification flags remain false. Synthetic verification keys and local
observations in tests are not proof of server authorization. Nonempty-state
migration, SQLite restore, old binaries, external writers and live deployments
are unqualified. Durable intent does not establish that a remote stream stopped.

The current synthetic HMAC observations and adoptions deliberately do not
implement the proposed Viewer wire. In particular, the separately disabled
`ABSENCE_QUALIFIED` gate must never be enabled from its placeholder fence flag:
it has no qualified server watermark/retention, issuer, audience, live authority
or stream-stop proof. Adoption never authorizes new deliveries. New delivery
creation is held entirely until an authenticated budget and authority contract
is implemented. Tests include an owned child exiting after the local dispatch
marker; that is process-death evidence only, not a remote stream or power-loss
recovery qualification.
