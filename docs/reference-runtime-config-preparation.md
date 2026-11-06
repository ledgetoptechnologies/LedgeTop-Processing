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
Generated role tokens are 32 to 4096 bytes and must all differ.
`forbiddenCredentials` enumerates existing native/control/legacy credentials for
collision exclusion; callers must supply the complete reviewed inventory. Each
entry has its own bound of 1 to 4096 printable bytes, because a native token can
be shorter than a generated role. An entry is only compared, never written or
reported, and being listed does not make a value usable as a role credential.

For a first installation without an enrollment manifest, supply `binding`
instead of `manifest` (exactly one is permitted). It pins a closed private JSON
artifact with `schemaVersion:1`,
`protocol:ltds-reference-node-preparation-binding-v1`, and the actual `host`,
`service`, `gate` objects. Host is `{hostname,bootId,dockerEngineId}`, service is
`{name,uid,gid,home}`, and gate is
`{path,sha256,nodeId,authorityId,generation,coordinatorOrigin}`. These follow the
installer's existing identity/path grammars and canonical original Gate path.
`dockerEngineId` and the bundle/readiness `engineId` all use the one original
Gate definition, `processing.node_gate.ENGINE`; node and worker slugs stay narrower.
The separately pinned copied Gate must match all binding fields and hash.
No config descriptor, unit hash, source archive or installed receipt is required
for this input. Both input modes are **preparation evidence**, never proof of
installation or current physical custody.

The noncircular first-install sequence is: capture real host/daemon/Gate binding
metadata; prepare runtime and Viewer packages; use the exact `final.json` bytes
in the existing worker composer with its documented pre-install manifest intent;
obtain the real worker config and unit hashes; then install separately and run
the existing fresh physical/readiness gates. The worker composer, not this
runtime tool, replaces its intent's zero config hash/one-byte placeholder after
producing actual config bytes. A runtime input never needs that placeholder or
an invented prerequisite worker config file.

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
# V2 source bearer principal binding

Schema 6 may explicitly set `sourceBearerPrincipals` as an environment-to-principal map, for example `{"production":"viewer-production"}`. Each configured environment must have a Viewer bearer and a trusted source-attestation key for the same principal. New V2 admissions and exact replays reject unbound bearers and proofs for a different principal; V1 credential behavior is unchanged. A schema-6 configuration without this map continues to load but cannot submit V2 jobs. The private runtime preparer emits the mapping from its existing explicit Viewer principal input. This admission binding alone does not implement per-principal authorization for every other job operation.
