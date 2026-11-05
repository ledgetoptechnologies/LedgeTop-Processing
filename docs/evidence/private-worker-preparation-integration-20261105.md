# Private worker preparation integration

Reviewed source `9e2a052470733db5ec5303ebf0f95f467b26137e` includes the closed
new-private-config/manifest generator, its input contract and the literal-path
fix required by the existing daemon validator. Frozen full Git archive SHA256:
`5be191649cd1a09678cbf376c2c98339b736739513dde1618828f465ff18ebff`.

The independent combined Linux selection passed **48/48 tests in 16.580
seconds**, no skips:

```text
tests.test_prepare_reference_worker_config
tests.test_enrolled_config_stage
tests.test_reference_worker_daemon_routing
tests.test_enrolled_worker_readiness_probe
```

Read-only source, UID/GID10001, one CPU/512 MiB, network none and a bounded
temporary filesystem were used. Log:
`ltds-worker-preparation-9e2a052-h8uaxnxp/test.log` in local temporary artifacts.
The exact named runner exited0 and was removed; its filtered inventory was empty.

The generator retains exact worker/instance/authority/Gate bindings, existing
claim/result/native roles, reviewed policy, source/dependency descriptors and
existing TLS material. It checks certificate chain, key match, validity and IP
SANs, and creates only new private staging outputs with a distinct readiness
credential. Actual daemon `load_config` validates the generated fixture bytes
without constructing a worker, acquiring a Gate or starting a service. Public
output contains hashes, not credentials. Partial/existing outputs are retained
and refused rather than overwritten. Its service-owned manifest must be copied
and verified into a new root-private execution bundle by the privileged install
handoff; the installer root-owner requirement remains unchanged.

The agent's initial13-case run exposed `load_config(Path)` versus the existing
literal-string contract. Converting the argument to `str` fixed the integration;
no validator was weakened. The corrected frozen agent selection also passed
13/13. These overlapping selections are not added into a total suite claim.

All preparation tests use synthetic private fixtures and certificate material.
No actual credentials, config files, TLS files, installed workers or NAS settings
were read or changed. Current per-host source artifacts remain pinned to
`99578a6`; the newly integrated generator does not relabel them or establish
that later recovery changes are installed. Final source/config/manifest refresh,
guarded activation, installed custody and real Viewer return remain required.
