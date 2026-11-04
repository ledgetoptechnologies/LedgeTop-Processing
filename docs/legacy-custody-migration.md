# Legacy eager-custody sidecars

Two experimental sidecar formats both used version `1`. The older format has
seven fields and used `unavailable`; the newer format has additional operator
and retry fields. Version alone does not distinguish them.

With eager custody enabled, the observer endpoint
`GET /v1/prototype/legacy-custody-census` inventories completed active task
rows in pages. It is advisory: it does not inspect historical incarnations,
prove that writers are fenced, or authorize a cutover or deletion. Missing and
invalid records require inspection; a `current` format result does not prove
archive integrity or downstream acceptance.

An operator can explicitly call
`POST /v1/prototype/jobs/<job-id>/custody/migrate-legacy` with an operator
credential and a printable ASCII `note`. The operation accepts only the exact
older shape and typed task identity. It commits a durable custody intent first,
preserves the older sidecar as `.ledgetop-custody-legacy-v1.json`, and writes a
new `refused/legacy_custody_requires_inspection` record. Even an old `verified`
record is held rather than trusted. Nothing is deleted and no Viewer acceptance
is inferred. The existing operator retry may subsequently perform its own
evidence checks; an operator must first investigate provenance and identity.

An interrupted backup write or a differing existing backup requires manual
inspection. A crash after intent but before conversion leaves the old sidecar
and intent in place, so the same explicit operation can be retried. The design
assumes the existing single-coordinator lock and trusted task storage; it is
not a cross-process file fence and cannot reconstruct missing sidecars or older
incarnations. These remain upgrade gates. Keep the feature disabled until its
runtime integration, audit policy and legacy census are independently reviewed.
