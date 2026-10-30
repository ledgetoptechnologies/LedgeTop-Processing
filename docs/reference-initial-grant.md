# Initial grant delivery profile

This optional source profile connects the actual ReferenceRuntime, Queue,
ReferenceStore, authenticated ReferenceControlAPI and existing private worker
grant route. It is disabled when private `initialGrantDelivery` configuration
is absent. No deployment or configuration is performed by this source change.

The private field is a closed object with `schemaVersion: 1`, stable ASCII
`issuerRealm` and `keyId`, `maxEpochs: 0`, positive `maxLifetimeMs`, and
`brokerCapabilities`. The latter has the closed fields of the renewable grant
capability contract. It is an operator-pinned observation of the actual broker,
not a client-requested escalation or proof the observed software is installed.
Processing never receives the Viewer's HMAC key.

New reference jobs admitted under this profile receive an immutable marker in
the same metadata transaction. Admission checks whole-object response and
concurrent byte ceilings, aggregate byte/request ceilings, and **epoch-zero**
request capacity for every allowed physical GET attempt. The derived policy is
`maxEpochs: 0`, original reservation byte budget, and
`attemptBudget: min(3*N, N+10)`. Admission conservatively checks the actual v3
driver's three-times-input reservation.100 broker requests cannot cover a10k
job's10010 attempts by appealing to future epochs. The independent10k total
object/4MiB request bounds remain unchanged; paging and50GB performance are
unqualified. Current downloads use full-object200 responses, not Range/206.

The actual Viewer route is `POST /ltds/v1/jobs/{jobId}/initial-grant`. Its closed
envelope is `schemaVersion: 2`, `profile: "ltds-renewable-grants-v1"`, `request`,
`issuanceSeed`, and the unchanged v1-shaped `grant`. The closed request and seed
follow the shared epoch-zero HMAC proposal: epoch integer0, null predecessor,
canonical request UUID, original worker reservation expiry/nonce and manifest,
claimed node and worker, exact server-owned issuer realm/key ID, object-array
order, policy and capability hashes. The bearer must have canonical unpadded
base64url43-character/32-byte shape. Shape and authenticated binding checks are
**not HMAC verification or proof of the issuer's secret/key lifecycle**.

Before constructing that envelope, the original authenticated submitting Viewer
reads `GET /ltds/v1/jobs/{jobId}/initial-grant-context`. This opt-in route returns
18 closed fields: `schemaVersion`, `profile`, `jobId`, `generation`,
`environment`, `nodeId`, `workerId`, `manifestSha256`, `pipelineDigest`,
`objectClosureSha256`, `transferRequest`, `issuerRealm`, `keyId`,
`transferPolicy`, `brokerCapabilities`, `contextId`, `mayDownload` and
`contextSha256`. The profile is `ltds-initial-grant-context-v1`, version is 1,
and `mayDownload` is false. The digest covers canonical public fields excluding
itself. It contains no credential fingerprint, including transitively through a
private-record hash. The independent random context ID is minted in the same
transaction as the original worker claim and fences its exact incarnation.

The submitting bearer is privately bound at admission. GET authenticates that
same principal and checks current generation, live reservation and immutable
parent claim. Changed or expired claims, absent context, or an already accepted
grant return generic 409 (unresolved), never a replacement context. A rotated
bearer in the same environment cannot take over an admitted job; administrative
principal continuity is not implemented. These private bindings are not returned.

POST must carry exactly one `X-LTDS-Initial-Context-SHA256` header containing the
GET digest. It checks the original principal and recomputes that public digest
from the same transaction's current parent/reservation/policy before insertion.
The closed envelope and 14-field delivery receipt remain unchanged. Preserve
the original digest for exact historical POST replay; GET after acceptance is
not an ACK recovery API. A 409, 404, timeout or failed POST never proves that a
grant was not accepted and never permits reissuance or raw-v1 fallback.

The existing protected listener and environment-scoped Viewer principal perform
authentication before any private JSON/body decoding. Current job generation,
parent queue/reference claim node/worker/token hash/coordinator authority and
admission marker are checked in the same transaction. Fresh delivery requires
an actual running claim and awaiting-transfer reservation with live, bounded
expiry. The private grant and immutable delivery receipt commit atomically in
the existing Queue database. A failed/unknown result cannot justify reissuance.

Exact envelope replay returns byte-stable historical receipt after phase changes
and expiry, provided original generation, parent claim and immutable records
still match. It never rotates a token, extends a lease or asserts a stream has
drained. Changed nonce, request, token, issuer or generation holds. The receipt
contains request/grant/envelope hashes and `mayDownload: false`; it conveys no
download permission and does not revoke separately issued authority. Worker
reads the unchanged original private v1 grant through its existing authenticated
route and must still enforce normal expiry, size/hash and ownership checks.

Raw v1 grant POST is blocked for marked jobs, including direct store fallback.
Preexisting **unmarked** jobs retain v1 behavior and are not adopted into this
profile. Compatible-source startup refuses omitted/changed/incomplete latched
policy before Queue schema/clock writers or Viewer writer construction. This
cannot fence old pre-feature binaries; deployment must stop/fence those writers.
No automatic policy migration, key-ID rotation or history deletion is supplied.
This context ABI requires fresh profile state; earlier experimental initial-only
state without the immutable claim table/principal markers holds at startup.

Renewal/successor routes remain unavailable. Actual authenticated reservation
extension, read-pin/stream drain, source custody and purge enforcement, secure
Viewer epoch-zero issuance and exact bearer recovery, installed broker limits,
scientific processing and result custody/release remain separate gates. TLS
callbacks mocked in private-config construction tests do not qualify TLS; the
HTTP route tests use the real protected loopback listener with synthetic data.

The public synthetic fixture is
`contracts/reference-initial-grant-v1.contract.json`. Its key and token are test
material only. It binds complete request/seed/envelope/receipt bytes and an
initial-only policy; no production credential is minted by this module.
`contracts/reference-initial-context-v1.contract.json` separately pins the
18-field public descriptor, canonical bytes and POST header. Neither fixture
is credential, custody or lease authority.
