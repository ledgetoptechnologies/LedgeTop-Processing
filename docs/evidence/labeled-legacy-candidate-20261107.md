# Revision-labeled legacy candidate — 2026-11-07

This is an inactive private-input validation, not an installed cutover or
readiness claim. The running coordinator, its service-owned configuration,
the existing workers and TrueNAS Viewer were not changed.

The corrected candidate image is
`sha256:038f822dc96f7db03bf5a8f0d7be7ceb171cba25ad77139114735673e3f6d557`.
Its `org.opencontainers.image.revision` label matches
`e277691844c2ec8440fb81a9dc9b66d7520a1954`. Docker image inspection showed
the filesystem layer list and image configuration other than labels match the
previous fixed e277 image. The independently pinned worker source remains
4705; the separate clock work is not included in this runtime candidate.

New cutover operation `7fa2c5db-470a-43d5-bbcb-a26ef2a4208e` has a startup
policy SHA-256 `96a3a7830ddb933960fb4eff7c929dd5b70487e0d8ce76ebf0a6e992df90a302`.
The policy binds all 16 required runtime sources, including the exact legacy
reader, and was independently regenerated inside the labeled image using the
b5ae controller code, read-only inputs and no network. Bootstrap and final
recipe SHA-256 values are
`2566dcf9178f34610e7a67ea6832b45f1b63bb9b4be25aa31b9e8ac3b7f116d6` and
`c3c2ee0943bf88f0c2d52a3b8a8172bdf31b666bafa5dc8caa70db27dc74caf7`.

Fresh typed observations were made against this exact image and recipe:

- Local spool record SHA-256
  `ce6a3d44a5b305f8bbdf465004ec8014d5f35190343bf3db5133a680ca6d255b`.
  The current bind, marker, UID and capacity were checked with a bounded
  fsync/readback; the test's own containers and volume were removed.
- Read-only retained legacy capture SHA-256
  `806039b48690db6590f3eb5837421cb0cc29bfe9f3479c447fe2cb8d9f2ea9e4`.
  It uses the exact candidate image, read-only state mount and b5ae helper pins.
  It is provisional until compared with frozen full custody and does not prove
  the live HTTP route.

The private directory
`photo-pilot-coordinator-inputs-20261106/candidate-e277-labeled-7fa2c5db-470a-43d5-bbcb-a26ef2a4208e/full-input-validation-draft/`
contains a populated plan SHA-256
`487fe8d6e6d6a65fd47aa01e6237eb0c79ee997014985c58beed2e83d5d7156e`
and bundle SHA-256
`d7ba0fcd7fdc9bc459199adc4780bd338144e156479432c3727e4ebec3da6110`.
The b5ae controller validated all private inputs, including the typed spool,
legacy capture, worker preparation review and regenerated policy. The supplied
config-reader callback read prepared input files, not service-staged files. A
read-only current count comparison matched, but is not a drained-state census.

Do not run this draft: full frozen custody and backup, the historical pilot
namespace check, worker installation/readiness, service-owned config staging,
live retained-result delivery, coordinated Viewer switch and rollback remain
open. Fresh observations are required if identity, counts or time bounds change.
