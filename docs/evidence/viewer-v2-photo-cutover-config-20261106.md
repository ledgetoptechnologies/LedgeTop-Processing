# Viewer V2 photo cutover configuration — 2026-11-06

This is a preparation checklist, not a deployment record. Beau controls all
TrueNAS administration and must perform any TrueNAS changes. The last Viewer
runtime metadata previously supplied by Beau showed source intake disabled;
that historical observation does not establish the current live configuration.
Do not enable these settings against the existing coordinator until the
replacement coordinator, identity pins and rollback are ready together.

## Minimal Viewer configuration

Use the same deployment identity and compatible settings in the Viewer API and
worker services. Their actual runtime environments and shared durable storage
must be checked in the installation; a candidate image or generated config is
not evidence of live configuration.

- `PROCESSING_PLATFORM_ENABLED=true` and an explicit
  `VIEWER_DEPLOYMENT_ENVIRONMENT` of `production` (or `staging` for a separately
  isolated staging instance). Set `VIEWER_REFERENCE_SOURCE_INTAKE_ENABLED=true`;
  `VIEWER_REFERENCE_SOURCE_INTAKE_ENVIRONMENT`, if set, must match the deployment.
- `VIEWER_REFERENCE_COORDINATOR_ORIGIN` is the exact HTTPS coordinator origin.
  `VIEWER_REFERENCE_COORDINATOR_PROVIDER_ID` must identify the dedicated
  `nodeodm` provider whose endpoint is that exact origin;
  `VIEWER_REFERENCE_COORDINATOR_AUTHORITY_ID` pins the coordinator UUID.
  `VIEWER_REFERENCE_SOURCE_PRINCIPAL_ID` identifies the authorized principal,
  and `VIEWER_REFERENCE_COORDINATOR_BEARER_TOKEN` is its distinct secret.
- Supply three distinct Ed25519 signers and matching coordinator trust pins:
  `VIEWER_REFERENCE_SOURCE_ATTESTATION_ISSUER_ID`,
  `VIEWER_REFERENCE_SOURCE_ATTESTATION_KEY_ID`,
  `VIEWER_REFERENCE_SOURCE_ATTESTATION_PRIVATE_KEY_PEM`;
  `VIEWER_SOURCE_SNAPSHOT_ISSUER_ID`, `VIEWER_SOURCE_SNAPSHOT_KEY_ID`,
  `VIEWER_SOURCE_SNAPSHOT_PRIVATE_KEY_PEM`, `VIEWER_SOURCE_SNAPSHOT_PUBLIC_KEY_PEM`;
  `VIEWER_REFERENCE_COPY_ISSUER_ID`, `VIEWER_REFERENCE_COPY_KEY_ID`,
  `VIEWER_REFERENCE_COPY_PRIVATE_KEY_PEM`, and
  `VIEWER_REFERENCE_COPY_PUBLIC_KEY_PEM`. The snapshot and copy public keys must
  match their private keys. Protect secret values and do not print them in logs.
- Ensure the exact endpoint is admitted by the existing provider admission
  settings (`PROCESSING_PROVIDER_ORIGINS` or the applicable allowed CIDR), and
  that both services can validate the coordinator's HTTPS certificate using the
  appropriate trusted CA. Verify connectivity, hostname and CA in the actual
  deployment rather than disabling TLS verification.
- Leave `VIEWER_SOURCE_RELEASE_ENABLED=false` for the pilot: accepted originals
  and processing copies must remain retained. Leave
  `VIEWER_REFERENCE_ACCEPTANCE_ENABLED=false`: that flag enables a separate
  inbound accepted-copy API with its own ledger, service credential and signer.
  The tested V2 photo path instead has Viewer pull the result and POST its
  signed accepted-copy receipt to Processing; enabling the inbound API is not
  a prerequisite for that path.

`PROCESSING_WORKER_ENABLED` is parsed by this Viewer candidate's config, but a
source search found no runtime reference to it. Do not treat changing that flag
as a verified way to start or pause the worker. The worker process checks
`PROCESSING_PLATFORM_ENABLED`; identify and control the actual installed worker
service using the supported deployment mechanism.

## Evidence and remaining qualification

On the staging PC, a disposable network-disabled container using Viewer image
`sha256:7912cd757c11fff99c165ba81e4ed5d5626cf6a0d9fff34543c5a36f4b29168c`
ran V2 configuration validation with synthetic production-scoped identity and
three generated keys. V2 validation passed with inbound acceptance disabled;
reusing a signer and mismatching the deployment environment were rejected. An
initial test invocation had an overly narrow error-message filter; after fixing
the test filter, the reported checks passed. No real credentials were used.
This establishes config validation only, not actual API/worker startup, network
trust, installed credentials or production readiness.

The separately documented isolated 20-photo integration test processed real
photos, recovered a fixture lease, and delivered and published a matching Viewer
result. It used test seams and did not exercise the installed Viewer service or
physical worker gates; see `isolated-real-photo-cycle-20261106.md`.

Before any cutover: verify exact Viewer image revisions, shared database and
mounts, a working isolated rollback, coordinator public-key and bearer scopes,
matching provider identity, and HTTPS trust from both Viewer services. Exercise
actual API and worker startup and provider capabilities/options using the
staging coordinator and synthetic scoped keys. Confirm jobs already in flight,
drain the old admission path, and coordinate the Viewer and coordinator switch
so there is one scheduler and no unowned submissions. Then perform a small
installed photo run, verify status/logs and usable output, and separately test
two-node queueing and disconnect/restart recovery. Do not conflate passing
configuration validation with any of these installation checks.
