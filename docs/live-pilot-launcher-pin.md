# Supervised pilot launcher: immutable entry point

The launcher is an experimental one-job tool, not a qualified release or a
replacement for the coordinator. Do not run it against a live provider until
its independent review and target checks are complete. The Docker fake
integration intercepts networking and ends after submission; it does not test a
live node, result conversion, or crash recovery.

The launcher code should not be read from a mutable working tree when it will
receive a staging credential. Use a trusted inline bootstrap that reads a
specific Git object and verifies its bytes before execution. Run this from the
Processing repository; `--help` performs no launch or credential access:

```powershell
@'
import hashlib, subprocess
commit = '847807e430823d9fdf219af394fd0699afaa5e58'
expected = 'edd618707406c726317cb9413c4ab2bce114cef986e8737fd1f270d24e46359c'
code = subprocess.check_output(['git', 'show', commit + ':tests/run_real_viewer_pilot.py'])
if hashlib.sha256(code).hexdigest() != expected:
    raise SystemExit('pinned launcher mismatch')
exec(compile(code, '<verified-pilot-launcher>', 'exec'), {'__name__': '__main__'})
'@ | python - --help
```

For a future reviewed launch, supply the launcher's required path arguments in
place of `--help`. Its fixed registry must already exist with the required
private ACL; it never changes permissions. Its one-shot marker applies to that
host/registry only. The preflight is a snapshot, not a reservation. The launcher
preserves the test container and all pilot state on success, failure, or
interrupted monitoring. The token is fetched over pinned SSH and supplied to
Docker over stdin; it is not an argument or environment variable.
`PILOT_REVIEWED_LAUNCHER` is a workflow guard, not an authentication boundary.
The process owner must protect the inline bootstrap itself and the Git
repository from untrusted changes; same-user or administrator compromise is
outside this test's threat model.
