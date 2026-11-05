# Actual bootstrap refusal: fixture credential roles

The capture-enabled full-flow qualification did not pass. It retained enough
private evidence to establish a fixture configuration defect instead of
assuming an HTTP readiness race. No installed host, NAS dataset, client input
or real engine processing was changed.

## Exact attempt

Operation `babc3f9b-7604-451c-8762-e5c01cfd95f1` used frozen `6ca7ed3`,
archive SHA256 `1186224ec5771d55b16f75c5fa5e39527f364a404b77102d124cc61adf22c29a`,
and candidate image `sha256:378559e42456017b965d1e2e38b56eb627274734aeb317a758d84c0bc99bb5ab`
(packaged processing source `18750d4d599026c18e569a9c67135e6c9fdc7d1a`).
The real693 predecessor remained distinct. The parent was UID10001, one CPU,
768MiB, private bridge/no host ports, with only supplemental socket GID0.
Only the shared builder was running at the fresh start check; it was untouched.

Eight phases completed through `bootstrap-created`. `start-bootstrap` stayed
pending; no freeze intent/drain/seal, final app or admission opening occurred.
The900-second budget was not reached: Docker timestamps were23:40:19.013736202Z
through23:41:20.626266924Z on November5,2026 (61.613seconds).

## Observed refusal, not assumed race

The first health body failed after its remote observer retired. Initial private
capture saw the app running and zero available log bytes. The separately
labeled fresh read-only diagnostic exec then became unavailable; its conservative
`mutationOutcomeUnknown=true` fact remains unchanged.

The retained bootstrap subsequently exited1. Bounded cold private log readback
obtained768bytes (SHA256 `1a2b3001bb416a0cbc42a5651cb2167ae05492658dc4ccb899e89b195c82ec84`).
It shows `processing.reference_runtime.RuntimeConfigurationError` from
`/app/processing/reference_runtime.py:260`, called by `app.py:40` before Queue
construction. The runtime intentionally redacts the detailed exception.

The exact preserved bootstrap config (SHA256
`7d445a3bb42f65dd730e684905291ea720cb54604832aa821255cfdeaf157ab9`) and
actual loaded environment establish both V2 Viewer bearer values overlap the
native compatibility credential set (count2). Values were never printed.
`load_reference_settings` explicitly forbids that intersection. The fixture
had accidentally reused preserved native Viewer credentials. The native
separation guard worked; no production readiness or timing change was justified.

## Minimal correction and meaningful regression

Fixture-only fix `27d2e9c` mints separate synthetic V2 production/staging Viewer
bearers. It preserves native API/operator/Viewer credentials exactly, as the
original-deployment preflight requires. Worker claim/result roles stay separate.

Frozen archive SHA256
`de694f95be36db55bcaf87cec632ae0763373882638218133aa9d3550e8e5d4c`
passed5/5 Linux tests in0.125seconds, zero skips, on existing image378:

- Actual `load_reference_settings` accepts generated bootstrap2 and final6,
  actual private TLS files, a local marked tmpfs spool and the real Config
  legacy credential set (including observer/local/main credentials).
- Native alias and cross-reference-role duplication are refused.
- Queue and runtime factories are forbidden during parsing.
- Three private failure-capture boundary checks still pass.

The test runner had no socket or ports, used UID10001/one CPU/512MiB/network
none, and was removed after exact owner/PID0/exit0/no-OOM checks. Its filtered
inventory was empty and no volume was created. Actual corrected full app
startup remains to be qualified; no mutation retry or rollback was run.

## Private preservation and authorized retirement

The complete private operation directory (647126bytes), plan/bundle/exact
configs, journal, initial diagnostic summary, terminal log, baseline, accepted
backup receipt/TAR and synthetic census key were copied cold. Production
`validate_tar` again matched467968bytes/202files/13directories. TAR SHA256:
`ecaac5466c6a6c59b22ca5ac56681422e7e0a933593f8e84063f32563318d846`.
`restoreAuthorized=false`. No keys or configuration values were printed.

After root-agent review, only these five stopped owned container identities
were retired without force: outer `bd556da6...`, bootstrap `c8da7317...`, holder
`8e6c4a13...`, original `284820ce...` and seed `0d050d6a...`. Fresh exact full
IDs/images/owner/PID0/exit/no-OOM/restart-no were checked before removal.

Only the five exact operation volumes (original state, lease, config, spool,
outer artifacts) were removed after fresh owner checks and complete closed
named-volume plus source/parent/child bind-alias census proved zero references.
Both operation-label container and volume inventories were empty. Private
host evidence remains. The unknown-observer fact was not cleared, the failed
phase was not accepted, authority was not reconciled, and no old operation
was replayed. Unrelated resources and the shared builder were untouched.
