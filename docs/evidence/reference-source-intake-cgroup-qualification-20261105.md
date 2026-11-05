# Reference source intake cgroup qualification

On 2026-11-05, the synthetic source intake fixture exercised the actual HTTPS
`/ltds/v2/jobs` route with a 10,000-photo metadata manifest declaring 50 GB.
The producer and signer ran in the Windows host process. The API server alone
ran in a Docker container limited to 256 MiB RAM and 0.5 CPU; its TLS port was
published only on host loopback. The fixture created or uploaded no photo
bytes, used a synthetic 60 GiB capacity answer, and disabled the physical mount
check. No worker claimed or processed the queued job.

The final read-only server container reported `memory.max=268435456`,
`cpu.max=50000 100000`, and `memory.peak=181211136` bytes. The server process
reported a 156,782,592-byte peak RSS. Cgroup `memory.events` deltas were
`max=0`, `oom=0`, and `oom_kill=0`. The 9,222,511-byte signed submission
returned 201 in 3.43 seconds; an exact replay returned 200 in 2.91 seconds.
While an incomplete source body held the
admission slot, a concurrent request received 429 with `Retry-After: 2` and a
closed connection, and capabilities GET returned 200. The incomplete body
timed out with 408; malformed authenticated JSON returned 400; later submission
and replay succeeded. The server held one queued job with 10,000 objects in
each manifest, 50,000,000,000 reserved bytes, and zero source PUTs or receipts.

The ordinary Linux/1 GiB focused suite also passed: 20 tests covering the
reference HTTP routes and the default dynamic TLS scale fixture.

Reproduce the constrained profile from the repository root in PowerShell:

```powershell
$repoRoot = (Get-Location).Path
$env:PYTHONPATH = "$repoRoot;C:\Users\fstor\AppData\Local\Temp\processing-rfc8785-20261104"
python tests/test_reference_dynamic_source_intake_scale.py --profile-cgroup
Remove-Item Env:PYTHONPATH
```

The profile runner uses image ID
`sha256:57f01267c88439d872f6782b54511e314614a76145e32cfe2fad003e5ca0fac8`
by default; set `LTDS_PROCESSING_TEST_IMAGE` to another compatible local image
if needed. It runs the server container read-only except for a private 64 MiB
`/tmp` tmpfs, publishes TLS only on `127.0.0.1`, and gives each run a unique
name and ownership label. Normal completion removes the container; emergency
cleanup inspects that exact label before stopping it. This qualifies metadata
admission and request backpressure only; it does not qualify real photo uploads,
storage capacity, processing, or result delivery.
