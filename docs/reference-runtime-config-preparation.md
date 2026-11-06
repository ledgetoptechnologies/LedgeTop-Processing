# Preparation of first-cutover runtime configuration

`python -B -m ops.prepare_reference_runtime_config --spec /private/spec.json`
is preparation only. It writes a new owner-private directory; it never stages
files, connects to a node/NAS, constructs a runtime, creates a lease, issues keys,
or changes the installed schema-1 configuration. Standard output is a bounded
hash/count receipt. On failure it reports no secrets and retains partial output;
an existing output directory is never reused or overwritten.

The closed version-1 input has `protocol=ltds-reference-runtime-preparation-v1`,
`preparationId`, `authorityId`, `listener`, `coordinatorOrigin`, ordered `nodes`,
`environments`, `forbiddenCredentials`, `streamBudget`, `sourceSpool`, and
`outputDirectory`. The spec itself and input artifacts must be private regular
files. Every artifact uses `{path,sha256,bytes,ownerUid}` and is read nofollow,
single-link, mode0600, with before/after descriptor identity and hash checks.
Inputs must be existing reviewed artifacts, never fixture credentials.

Each node supplies a pinned original enrollment `manifest`, copied original
`gate`, `endpoint`, `workerId`, `instanceId`, `resultOrigin`, `bearer`,
`resultBearer`, and `outputBudget`. Manifest and Gate supply true Docker daemon,
authority, origin, node and generation bindings; this tool does not guess them
from hostnames or replace them with current discovery. Tokens are private
artifacts containing exact printable bytes without a trailing newline.
`forbiddenCredentials` enumerates existing native/control/legacy credentials for
collision exclusion; callers must supply the complete reviewed inventory.

Each selected production/staging environment supplies `providerId` (the actual
dedicated Viewer nodeodm provider UUID), `principalId`, `bearer`, `viewerCaPath`,
and three distinct signers named `source`, `snapshot`, `copy`. Each signer is
`{issuerId,keyId,privateKey:<artifact>}`. All selected signing keys must differ,
including across environments. Only the Viewer packages contain private PEMs;
coordinator configuration contains derived source/copy public trust pins. Keys
are neither generated nor rotated. No assertion about existing key availability
is made by this tool.

Listener paths are direct children of `/run/processing-tls`. This preserves the
separate readonly TLS mount while ConfigStage later mounts its new operation
subdirectory at `/run/processing-reference`. `sourceSpool` is the exact existing
runtime namespace/marker/budget shape. Path metadata here is a declaration,
not proof of a mounted TrueNAS spool. No `SourceSpool` instance is constructed.

Outputs are `bootstrap.json` (schema2), `final.json` (schema6), one private
`viewer-<environment>.json` environment map, `public-trust.json`, and
`preparation-receipt.json`. Final has empty `viewerAcceptance` and no
`sourceRelease`. Viewer source cleanup is disabled. Viewer packages include
explicit deployment environment/platform settings and `NODE_EXTRA_CA_CERTS`;
the actual public CA mount and provider identity must still be checked when
applying the package. TrueNAS administration remains an operator action.

Build the existing startup policy and deployment bundle *after* these exact
config bytes exist, then use existing ConfigStage for UID10001 staging. The
preparation receipt explicitly says `runtimePreflightProven=false`. Actual
TLS/key readability, source mount+marker identity, catalog/worker readiness,
physical ownership and installed Viewer signer behavior remain separate gates.
The first-cutover policy currently requires its reviewed two-node inventory;
this preparer accepts bounded configured inventory without creating another
scheduler or broadening that policy.
