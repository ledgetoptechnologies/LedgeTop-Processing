# Integrated first-worker activation qualification

Review branch source `270692ae8777144c27704a8d9bbed2e673a84539` integrates the
controller activation journal, main cutover/rollback guards, initial service
helper, typed historical pilot closure, retained-terminal native census and
installed source/dependency/config/unit/TLS tree proof. Full frozen archive SHA256:
`3a25fdbd4b1e6f47f0cda100b656cb905a5fab8f9fc7f8ed70d4a2dabceb6ede`.

The independent Linux selection passed **98/98 tests in11.880seconds**, no skips:

```text
tests.test_first_worker_activation
tests.test_first_worker_activation_serialized
tests.test_enroll_reference_worker
tests.test_enrolled_coordinator_driver
tests.test_enrolled_coordinator_cutover
```

The bounded runner used one CPU/768MiB, network none, no Docker socket or host
ports, read-only source and task-private tmpfs mounts. Root was used only for
the installation fixture; actual serialized observer/reload bytes executed in
UID/GID10001 children from root-owned read-only installed source and a copied
interpreter with the actual four dependency distributions/metadata in an isolated
venv. Source/tree/private-file/unit/manifest parsing and kernel Gate/ingress-lock
checks remained real. Systemd, Docker, native transport and account lookup were
explicit external fakes. Mutated installed source and malformed request cases
failed at their intended guards before the external marker. This is not actual
SSH, systemd service activation, NodeODM activity or installed custody proof.

Log: `ltds-integrated-activation-270692a-fjj3d14u/test.log` in local temporary
artifacts. The named runner exited0 and auto-removed. The preceding independent
Linux source checkpoint selections passed41/41 at `c8fe4b3` and54/54 at
`5a0e201`; the test-author serialized selection passed3/3. These overlapping
selections are not added into a full-suite total.

## Review corrections retained

- A pending reload/start intent blocks conflicting main progression and the
  pre-direction-change rollback entry point. Lost replies are inspection-only;
  an accepted receipt still requires fresh service/readiness evidence.
- Native task-list emptiness is not required. Declared, identity-bound terminal
  legacy tasks can remain quarantined; queued/processing/unknown/changed task
  inventories hold. This grants no Viewer acceptance or cleanup authority.
- Ingress consumes the verified bounded configuration bytes and rechecks file
  identity/content immediately before service effects. The existing ingress
  lock is opened without creating or chmodding it and held through the helper.
- Installed tree proof is versioned and includes source/dependency leaves,
  private config/unit descriptors and actual TLS files. Existing system Python
  libraries remain an explicit host dependency, not a hermetic-image claim.
- Readiness `bootId` is an API-instance UUID. The helper separately checks the
  actual kernel boot UUID; those identities are never compared as equivalent.
- The test `/opt` tmpfs must explicitly allow execution of the copied
  interpreter. Its source remains read-only; no production guard was weakened
  to cure the first noexec fixture failure.

No source artifacts, credentials, permissions, live worker services, native
containers or NAS settings were changed. Host source artifacts still refer to
the earlier `99578a6` preparation and must be refreshed after recovery integration.
Reviewed manifests, real PKI/ingress/native inventory, current bootstrap custody,
tracked SSH/service acceptance and the complete actual forward/rollback remain
required before installed admission.
