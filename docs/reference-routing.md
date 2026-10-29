# Reference worker-pull routing (source only)

This optional source policy is not installed in the coordinator currently running
application source6bb. It changes scheduling selection, not the physical gate,
engine admission, result authority, source custody or public reference-v1 wire.

Default `PROCESSING_REFERENCE_MAIN_FALLBACK=0` preserves the original fixed
cluster-small/main-large routing. Explicit opt-in requires both configured nodes,
private reference runtime configuration, fixed native origins and existing worker
resultOrigin/resultBearer plus verified private CA. Independent closed `routingBindings` metadata is required when fallback is on:
`{"cluster":{"engineId":"<fixed Engine ID>","gateGeneration":1,"engineVersion":"<fixed version>"},"main":{...}}`.
It lives in the private reference runtime configuration alongside existing
workers, without enabling `recoveryBindings` or recovery routes. Existing files
without this optional key retain their default behavior; older binaries reject
an added unknown key, so private-config rollback/migration must be qualified
before later activation. The current installed private file is unchanged.
Health compares actual queue authority, fixed configured worker ID/instance and
each explicit engine ID/gate generation/version. The fresh native /info version
must match too. All are configured/scheduling observations, not Docker proof.

Both advisory GET /next and atomic POST /claim use the same selector. Observations
are newly sampled for each operation; advisory selection never reserves work and
is re-evaluated before claim. SQLite BEGIN IMMEDIATE still rechecks maintenance,
the production-first/FIFO global head and the single running/held global slot.
A running staging job is never preempted by later production. TreeDar remains
main-only and occupies the same slot; this policy does not enable TreeDar or
combined pipeline execution.

Small means at most1000 photos and20GiB by the configured local limits (inclusive).
The healthy eligible small node wins even when main polls first. Main fallback
requires explicit small-node unavailability or maintenance/ineligibility and a
healthy eligible main observation. A worker daemon being offline while its
native engine remains up is a real unavailable case. Authentication failure,
untrusted TLS, mismatched identity, malformed/incomplete response, unsupported
legacy health404 and busy work are unknown/busy, not evidence of offline/idle.
Both worker and native channels are independently sampled even when one reports
unavailable. Unknown/busy/version or identity mismatch on either dominates
unavailable on the other. They hold rather than overflow. Maintenance cannot override auth/busy uncertainty.

Private TLS health is the existing Reader-role GET /ltds/worker/v1/health and its
odm_only service_liveness profile. Native /info independently supplies bounded
queue/version/image-count observations. Neither proves Docker identity, an empty
upload directory, actual resource headroom or physical gate ownership. Actual
workers retain all existing local admission checks immediately before mutation.
Awaiting Viewer acceptance may coexist with a subsequent job only when the
worker reports no pending execution and the global queue slot is free; retained
scratch, artifact leases and cleanup rules remain independent.

At most one sampling operation per selector runs at once, using two bounded
node reads concurrently; contention holds after a bounded wait. No health reads
are needed while the global slot is owned or no reference head is queued.
Explicit fallback requires literal IP origins (including verified IP SANs for
private TLS), so no unbounded DNS resolution is part of selection. Requests use
fixed origins and verified TLS for private health, no redirects or proxies,
bounded8KiB HTTP headers/16KiB JSON metadata and one nonblocking socket deadline
covering connect, TLS handshake, headers and body. Length-framed JSON is required;
chunked/compressed/redirect responses hold. This is deliberately stricter than
the disabled historical clients. Configured TLS roots, verification flags,
protocol limits/options and ciphers are pinned and checked before/after sampling;
widening the same SSLContext is a policy change, not implicit trust. Responses and
credentials are not persisted to the queue or diagnostics. Configured policy
and transport replacement holds rather than silently altering selection.

The existing Viewer capability/catalog admission remains conservative: if a
native engine is unavailable, new Viewer submissions may still get503 from
fleet catalog discovery. This policy can route already queued metadata and can
handle worker-daemon-only outage with native HTTP still healthy; it does not
claim to implement the complete NAS-backed queue or custody workflow.

The focused tests use synthetic job metadata (no genuine acquisition/scientific
claim), actual HTTP native fixtures, actual Reader-authenticated TLS health
fixtures, actual WorkerAPI over TLS and the real Queue/ReferenceStore. They cover
concurrent main-first preference, daemon-only and native-only outage, unknown,
auth/TLS/identity/version/busy holds, maintenance, fresh claim re-selection,
configured limits, production/global head, TreeDar main-only/shared held slot,
original claim replay and default-off/independent runtime settings assembly.
Actual slow header/TLS-handshake/TLS-header/TLS-body socket tests exercise total
deadlines and joined fixtures; DNS exclusion and in-place CA/protocol trust drift
are separately refused.
Worker health fixture bodies are not actual daemon admission evidence; canonical
daemon health/service tests separately qualify that producer implementation.
