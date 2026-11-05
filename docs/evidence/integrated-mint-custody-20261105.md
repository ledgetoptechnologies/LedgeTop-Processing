# Integrated initial grant, renewal and custody checks

At Processing `ee9b4791110aa2fb9aa1039785ebe6e55448bdac`, the following
integrated Linux selection passed **148/148**, no skips, in **43.760 seconds**:

```text
tests.test_reference_source_grant_mint_io
tests.test_reference_source_grant_renewal
tests.test_reference_source_readers
tests.test_enrolled_coordinator_custody_ops
tests.test_enrolled_worker_readiness_probe
```

The initial-grant reader changes from Claude `578578f` were selected through
root `fde3d12`. Both whole-object full-byte verification and ranged-source
verification now reserve reader custody before opening NAS objects. Stalled
open/read/close, freeze, release, uncertain close and changed claim cases are
included; uncertain descriptor retirement retains its pin and authority.

Custody commits `db79b6e` and `f81cf85` were selected as `7f4b045` and
`c90c8a1`. Worker readiness probe `75cbc5b` is selected as `1f6dfa1`.
Their synthetic tests cover private TAR inventory/bytes, durability-barrier
retry, explicit final Gate proof, source/config/process bindings and bounded
serialized read-only readiness. No remote worker was updated by these tests.

Execution used pinned existing Python dependency image
`sha256:57f01267c88439d872f6782b54511e314614a76145e32cfe2fad003e5ca0fac8`,
UID/GID10001, one CPU, 768 MiB, network disabled, read-only root/source mount
and a bounded 256 MiB private temporary filesystem. Container
`ltds-mint-custody-ee9b479` exited zero and its removal was verified. No new
dependency install, host/NAS mutation, engine task, deployment or source cleanup
was performed. This selection is not the full Processing suite or installed
NAS/worker/coordinator acceptance.
