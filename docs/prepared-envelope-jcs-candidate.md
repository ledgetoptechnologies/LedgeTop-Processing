# Inactive JCS prepared envelope candidate

This isolated pure validator proposes `ltds-viewer-prepared-envelope-jcs-v3`
and SHA-256 over the ASCII protocol name, a NUL byte, and RFC 8785 canonical
UTF-8 envelope bytes. It has no route, principal authentication, durable
replay store, activation, or cleanup effect. Viewer has agreed to investigate
JCS, not this exact schema/domain or executable candidate.

The candidate optionally imports Trail of Bits `rfc8785==0.1.4` (Apache-2.0,
Python >=3.8, pure Python). `requirements-jcs-candidate.txt` pins its universal
wheel SHA-256; install with `pip install --require-hashes --no-deps
--only-binary=:all: -r requirements-jcs-candidate.txt`. The existing Dockerfile
does not install this optional file; a reviewed production build and dependency
lock are needed before any runtime integration. No JCS dependency is imported
or required by legacy paths. Project: https://github.com/trailofbits/rfc8785.py;
specification: https://www.rfc-editor.org/rfc/rfc8785.

Raw UTF-8 bytes are bounded before parsing, nesting is pre-scanned, duplicate
keys are rejected, and numeric values must be finite and within the existing
Processing safe-number bounds. JCS normalizes -0 to 0 and integral floats to
integers; the candidate validates and derives existing server-owned submission
and pipeline digests from that normalized value. This does not change legacy
`canonical_json` or its digest semantics. Trusted, independently obtained
capability and option definitions determine available engines, allowed names,
numeric/bool/string types, and explicit enum/numeric domains. Definitions and
their trust, freshness, peer authentication, stable replay persistence and
post-validation engine execution need separate integrated work.

The test uses Python's pinned implementation and actual Node package
`canonicalize@2.1.0` (Apache-2.0; npm integrity
`sha512-F705O3xrsUtgt98j7leetNhTWPe+5S72rlL5O4jA1pKqBVQ/dT1O1D6PFxmSXvc0SUOinWS57DKx0I3CHrXJHQ==`)
for cross-language canonicalization, including a complete synthetic envelope.
Set `NODE_JCS_MODULE` to that installed module and put the candidate Python
dependency on `PYTHONPATH`, then run `python -m unittest
tests.test_prepared_envelope_jcs_candidate`. This is not a test of a Viewer
sender or agreement on a shared wire contract. Normalizing number forms may
change types relative to raw JSON; engine option semantics and signed-zero
policy must be explicitly agreed and tested against real NodeODM catalogs.

## Digest domains and unresolved integration

* The input manifest keeps the reference-v1 digest: Processing
  `reference_protocol.manifest_sha256` hashes Python `canonical_json` of the
  manifest excluding `sha256`. Viewer `referenceProtocol.manifestSha256`
  hashes its own `canonicalBytes` over the same restricted manifest. Their
  shared domain uses ASCII keys/basenames and safe integers; preserve existing
  cross-language manifest vectors and do not reinterpret existing hashes.
* Processing `validate_submission` computes `requestDigest` from the
  submission and `pipelineDigest` from its pipeline using Python
  `canonical_json`. `ReferenceStore._submit` recomputes and persists them;
  `reference_driver` recomputes for worker claims. Viewer treats the returned
  pipeline digest as an opaque binding in result and receipt validation. It
  does not compute the request or pipeline digest using JCS.
* Processing `reference_runtime.discovered_policy` computes
  `optionsFingerprint` from the discovered options and `capabilityFingerprint`
  from those options and fleet policy with Python `canonical_json`. Viewer
  compares these as opaque capability values. Do not relabel them JCS: option
  definitions can contain fractions.
* Viewer and Processing retain their existing restricted result-manifest and
  acceptance digests, which bind the opaque Processing pipeline digest.
  Outer JCS bytes do not make these nested digests JCS.

In this proposal only, an envelope first passes JCS normalization before
Processing computes its existing server-owned request and pipeline digests.
That makes `-0`/`0` and `1.0`/`1` equivalent at both levels. It deliberately
changes candidate-v3 digest inputs relative to a legacy submission containing
`-0.0` or `1.0`; it must never be applied to existing jobs, or silently mixed
with prepared-v2 replay. The present candidate does not modify `ReferenceStore`
or workers and does not prove the chain: integration must persist the version
and canonical normalized input, recompute consistently in store and worker,
separate v2/v3 replay identities, revalidate policy, and obtain Viewer agreement
on treating all server-owned hashes as opaque. Shared end-to-end digest-chain
vectors and compatibility tests are a release prerequisite.

The pure function validates the supplied current policy even on an exact
canonical replay. It does not establish whether an already accepted request
should replay against its original durable policy or a changed current policy;
policy freshness and authenticated recovery must be agreed before integration.
