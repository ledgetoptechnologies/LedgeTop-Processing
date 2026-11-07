# Cluster worker offline preparation — 2026-11-06

These are private **preparation-only** artifacts on cluster PC `.89`; neither
worker was installed, started, enabled, or qualified as ready. The existing
installation and canonical unit/config were not changed. The intended later
maintenance operation must establish physical-gate and engine idleness, stop
new ingress, preserve the original installation, and use a separately reviewed
retirement/re-enrollment procedure before either new manifest is installable.
The current first-enrollment installer refuses the existing canonical unit and
config, so these artifacts alone do not provide an executable replacement or
rollback of that unit.

Under `/home/bkoltz/.local/share/ledgetop-processing-reference/`:

| Purpose | Operation ID | Prepared directory under `photo-pilot-worker-prep-20261106/` | Manifest SHA-256 | Config SHA-256 |
| --- | --- | --- | --- | --- |
| New production and staging worker candidate | `1454c60b-aad2-46e6-b80c-bdb737af9f32` | `prepared-offline-candidate-1454c60b-aad2-46e6-b80c-bdb737af9f32` | `0c23fa259dc930024b0be1a7b397b510c28bba52712440b5bf027b26af4d81c5` | `acbd571931a648be1b0852e315b0268780b76ebd8683172d6723200717b1cd4e` |
| New staging-only recovery candidate | `3c56ea3f-ecee-4531-9cc8-58264f909c4d` | `prepared-offline-rollback-3c56ea3f-ecee-4531-9cc8-58264f909c4d` | `ed3ea20e0898b168073a0098b9ffd7226f49ff67f7581d8dd97132b4c6dc1448` | `c429deb88bbede2c5213efd90b41130c363512fefdb5201db090c6e473a801ce` |

Their corresponding private preparation specs are in the same parent directory,
named `preparation-spec-offline-<purpose>-<operation-id>.json`. Spec SHA-256s
are `90036d6bfd4896edbc565424df83001670c51d2c5a639ec9c192c7f2457f15ab`
for the candidate and
`20bd54f91cee2cb152526967f2aa27ee8ba67bcb48f289c8a69417ec2111f67f`
for the staging-only recovery candidate. Unit SHA-256s, in the same order, are
`0a375677998cef0a15cb0a96697abcdb971bb647ae2bb406dd66f0917fd000bc` and
`95bf6b16575a104a2ff5338769b809cb3d1610e957a227e5759f3594967ab7fa`.

Preparation used the existing service account and installed Python environment.
The preparer and enrollment modules were checked against their exact commit
`4705ba3fad2d70f01507aa089aa1f5d2eee4e062` hashes. The existing preparer
verified the source and dependency archives, private input digests, original
Gate metadata, TLS materials and config schema. A separate readback checked
both manifests, rendered-unit digests, config file hashes and modes, distinct
operation IDs and absent installation roots. Both configurations explicitly
disable spool/engine cleanup and retain a 10 GiB safety floor. The recovery
candidate uses the earlier staging-only runtime and is not a substitute for
the original worker's protected install receipt or a production worker.

One earlier draft spec, operation `69f959b6-f871-4d97-8396-85b54ac4492b`,
failed the derived-unit digest check before any output directory or install was
created. It remains retained for inspection and must not be treated as a valid
bundle or replayed.

At readback the existing `ledgetop-processing-reference-worker.service` was
loaded, disabled and inactive with MainPID 0. This observation does not prove
the service never ran or that the native engine and Gate remain idle at a future
maintenance instant. No production cutover, live admission, worker readiness,
retirement, rollback, or successful first enrollment is implied.
