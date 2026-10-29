# Opt-in offline metadata admission

This source feature allows the existing reference-v1 provider to queue metadata
while both native engines are offline. It does not upload files, issue or renew
grants, establish source custody, start processing, or enable TreeDar/combined
processing. Nothing in this document authorizes installation or activation.

The private coordinator runtime may explicitly add
`"catalogAdmission":{"schemaVersion":1,"maxAgeSeconds":3600}` and independent
`routingBindings` for **both** cluster and main. Each binding fixes engineId,
gateGeneration and engineVersion. Existing configured native origins must be
literal IP addresses; HTTPS requires a verified IP SAN. The mode is independent
of the optional routing fallback switch and recovery routes. Missing policy is
the historical fresh-only behavior and neither opens nor creates catalog state.

Canonical workers must explicitly add `"optionEvidence":{"schemaVersion":1}`.
See [reference-option-evidence.md](reference-option-evidence.md). Old closed
configuration loaders reject these fields. No profile is enabled automatically.

## What is verified and what is cached

The first valid snapshot is obtained from actual fixed-origin `/info` and
`/options` responses from every configured native node. The bounded shared
transport has one nonblocking deadline per response, no DNS/proxy/redirects,
8 KiB HTTP headers and 256 KiB maximum JSON. It preserves complete dynamic flag
definitions, validates unique names, exact per-node definition hashes and the
exact-definition fleet intersection, then applies the existing safe numeric and
boolean definition filter. There is no static flag list. Native HTTP is trusted
configured local transport, not cryptographic Docker/container attestation.

Authentication precedes discovery. Only explicit pre-connect network
unavailability permits the last verified complete snapshot to be reused within
its finite age. Authentication, TLS, malformed responses, version mismatch,
unknown/partial responses, backward clock movement and expired data invalidate
and hold admission. A complete new fresh snapshot can restore valid state.
Neither serving cached data nor an unavailable observation renews `observedAt`.
Before any native I/O a FULL transaction records a fixed-size observation ID.
Preallocated invalidation and exact clock controls close fallback without
growing the cache row or requiring spare logical quota. Failed/unknown final
writes leave that durable pending ID, which also closes fallback after restart.
Only a complete new verified fleet observation can restore such held state.
Successful partial node info/definitions are retained for comparison: known
application-version or full-definition drift invalidates the whole cache even
when the other node is unavailable. Partial catalogs are never merged.
The fixed-size controls include a 64-byte checksum bound to the closed control
tuple, associated payload digest and private scope; total control allowance is
176 bytes. A valid-looking marker bit flip or cross-generation control copy
holds read/cold reopen without repair. This detects accidental corruption and
does not authenticate data against an arbitrary database administrator.
The persisted high-water timestamp prevents a backward clock from renewing age.
Corrupt records, schema/trigger drift and unknown write acknowledgments hold;
there is no automatic repair or blind replay of a failed write.

The current singleton snapshot is scoped to the existing authority, configured
origins, independent engine/gate/version bindings, worker identities, routing
size policy and maximum age. TLS trust roots and verification policy are pinned.
A private configuration digest also binds native credentials and the actual
TLS roots/protocol/options/verification state across restart; no credential,
credential hash or scope digest is exposed in public/private responses or
diagnostic headers. Token/CA changes while offline require explicit migration
and cannot silently reuse or delete the old cache.
A different scope requires a separately reviewed migration, not cache adoption.

## Public v1 remains closed

The capabilities, options, submission, claim and result JSON bodies are
unchanged. In this explicit profile, `engines.odm.available` means **metadata
admission is enabled**, not that a worker is online or idle. Even historical
fresh discovery did not establish execution idleness. Scheduling still observes
actual worker/native readiness, maintenance and the one global execution slot.

Actual HTTP responses expose only allowlisted diagnostics:
`X-LTDS-Catalog-Source: fresh|cached` and
`X-LTDS-Catalog-Observed-At: <original UTC Unix seconds>`. They are not fresh
health evidence. The immutable Viewer867 provider accepts the existing bodies;
it does not currently present these new headers as a UI status. Adding online
or freshness fields to its closed capabilities JSON would require a separate
agreed wire change.

## Durable job evidence and worker closure

Queue-owned SQLite tables hold one bounded `reference_catalog_state` and one
immutable `reference_option_evidence` record per accepted job. Job insertion and
its selected **full original definitions** commit in the same existing FULL
Queue transaction, with both byte counts included in the existing metadata
quota. A new catalog never rewrites prior job evidence. Definitions are at most
256 KiB and a complete cache is at most 1 MiB plus fixed control fields. SQLite
is owned, regular, one-link, 4096-byte pages, DELETE format, at most 32768 pages;
WAL headers/sidecars are refused before opening SQLite. The database and rollback
headroom remain a coordinator metadata-storage requirement, not worker scratch.
No completed job/evidence retention cleanup is introduced here.

The coordinator requires an authenticated opt-in header on worker discovery and
claim so historical unaware workers cannot take an evidence-bearing job.
Private read-only GET
`/ltds/worker/v1/jobs/{jobId}/option-evidence` requires the existing worker bearer
and exactly one `X-LTDS-Claim-Token` header. It returns schemaVersion 1 plus the
actual original authority, job, generation, environment, node, worker, worker
instance, claimRequestId, manifest and pipeline bindings, original capability/
options/catalog fingerprints and sorted full selected definitions. There are
no credentials, credential hashes or filesystem paths in the response. The
server checks the original claim inside one Queue snapshot. This metadata is
neither an engine-start permit nor source authority.

Immediately before the first real ODM mutation, the canonical worker fetches
that original record, validates every original claim binding and reads actual
fresh `/options`; selected definitions must match exactly, including metadata.
A removed flag, same-name/type changed definition or altered evidence holds
before initialize/upload/commit. Once an engine intent exists, collection,
outbox delivery and Viewer acceptance do not require renewed option evidence
or an extended source-grant TTL. Original job/token/reservation identities stay
unchanged.

## ABI, migration and rollback

Installation requires the authoritative coordinator and workers to be drained
under their actual owned maintenance gates. Initial schema installation refuses
queued/running/held jobs; existing terminal history is retained. The explicit
Queue mode is passed from already validated runtime settings before startup.
Tables and three permanent jobs INSERT/UPDATE/DELETE triggers are installed
atomically. The triggers invoke `reference_catalog_writer_v1()`, registered only
by the compatible explicit Queue mode. Old binaries lack that function and
cannot mutate the queue; new mode-off binaries refuse the catalog ABI before
their startup overlay. Exact persisted table and trigger SQL and marker are
checked on compatible cold reopen; a missing marker/trigger is a hold, not an
invitation to recreate state.

Restoring an old config or backend image is not a database downgrade procedure.
Disabling the mode on the same state requires a separately reviewed export or
migration preserving jobs and evidence; no automatic drop, repair, hash rewrite
or profile conversion exists. This fence does not protect against an arbitrary
database administrator dropping the fence, nor replace the required exclusive
daemon ownership and drain of already-running work.

## Qualification boundaries

Focused tests exercise actual native HTTP and authenticated TLS, both-node
outage, cache/coordinator restart, production FIFO, zero prefetched source/grant
or claim while offline, strict evidence identity, exact definition drift,
atomic job/evidence rollback, unknown cache commit acknowledgments, invalidation,
age/clock failure, corruption, quotas, permanent writer fence and actual wire
diagnostic headers. Source fixtures are synthetic and do not qualify real
geospatial data, Docker recipe identity, NAS custody, metadata issuers or live
activation. The separately opt-in joint oracle reuses the exact pinned Viewer867
provider/broker/acceptance implementation and the canonical worker factory;
its results must be recorded before claiming complete end-to-end qualification.
