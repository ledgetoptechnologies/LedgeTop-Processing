# New private worker configuration preparation

`ops/prepare_reference_worker_config.py` fills the missing preparation seam
between reviewed per-host artifacts and the install-only enrollment helper.
It never calls the worker factory, opens a Gate/lease, changes a service, creates
TLS keys or modifies an existing config. It runs as the intended service UID.
Only NEW owner-private output files are created; partial output is retained and
refused rather than adopted or overwritten.

The private specification is bounded to 64 KiB and has these exact fields:

- `schemaVersion: 1`, `protocol: "ltds-first-worker-private-preparation-v1"`.
- `manifest`: the existing closed first-enrollment manifest, with config path
  `<outputDirectory>/worker-config.json`, owner set to the preparer's service UID,
  placeholder config SHA of 64 zeroes, config bytes 1, and the correctly derived
  unit hash. All source/dependency/host/Gate/engine pins are reviewed inputs.
- `runtime`: descriptor for the exact selected private reference runtime JSON.
- `nativeCredential`: `{artifact: descriptor, key: "EXACT_ENV_KEY"}`. Only literal
  unexpanded `KEY=value` input is supported; this is not a shell-env evaluator.
- `tls`: `{ca: descriptor, certificate: descriptor, privateKey: descriptor}` for
  existing service-owned Processing files, not newly issued material.
- `listener`: `{host: literal IP, port: integer}` exactly matching the existing
  runtime worker's `resultOrigin`.
- `coordinator`: `{origin: reviewed HTTPS IP origin, listenerPort: internal port,
  certificate: descriptor}`. The origin/port mapping is explicit reviewed input;
  the generator checks the pinned existing coordinator certificate's IP SAN,
  CA signature, server purpose and validity, plus the runtime internal port.
- `policy`: exact `safetyFloor`, `transferTtl`, `cleanupGrace`, `pollSeconds`,
  `allowedEnvironments`, `cleanupEnabled`, `engineCleanupEnabled`,
  `sourceRangeV2Enabled`, `minimumTlsValiditySeconds`.
- `outputDirectory`: a NEW literal path beneath an existing owner-private parent.

Every descriptor is exactly `{path, sha256, bytes, ownerUid}`; input files must
be single-link, mode 0600, nofollow original paths, with exact hash/length/owner.
Source and dependency archives are streamed with the installer's bounded pin
policy. Existing selected worker IDs, instances, claim/result roles, recovery
authority/physical engine/generation/origin and output reservation are preserved.
The only generated secret is a new distinct maintenance-readiness bearer inside
the new private worker config. Initial activation is null; execution routing is
explicitly ODM-only. This does not claim main TreeDar installation readiness.

The generator writes `worker-config.json` and `enrollment-manifest.json` via
exclusive descriptor-relative creation in its new 0700 directory, mode 0600;
checks real `load_config` in process without `build_worker`; and returns only
config/manifest/unit hashes with `preparationOnly: true`. The CLI accepts only
`--spec /absolute/private/spec.json` and reports a sanitized hold on any error.
It has not been invoked against installed credentials by this implementation.
The generated manifest is service-owned staging material. The existing installer
CLI requires a root-owned 0600 manifest: the separately reviewed privileged
execution bundle must exclusive-copy and hash-verify these exact manifest bytes
into a NEW root-private file, preserving all config/artifact owner descriptors.
Do not relax the installer ownership check or change existing input ownership.

This validates pinned staging material, not live host/Docker boot identity,
external port mapping, complete idle state, current kernel lease ownership or
authenticated bootstrap-frozen readiness. Those remain original installer and
cutover checks. A changed source selection requires recomputing the entire
source/config/unit/bundle pin set before actual preparation or enrollment.
