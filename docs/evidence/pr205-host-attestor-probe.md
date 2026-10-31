# Synthetic host PID attestor evidence

The adjacent `.py.txt` is the sanitized reference source used for the second
probe, preserved for review. It is deliberately not a packaged command or an
activation procedure. Before executing it in another environment, independently
review its Docker endpoint, isolation, ownership, resource and cleanup rules.
It starts and removes a synthetic container and creates a temporary socket.
The separate `pr205-host-attestor-probe-reviewed.py.txt` is a later, unrun
revision: it records the intended container identity before creation, treats
creation and inspection as uncertain until verified removal, and preserves its
socket directory and recovery metadata when the Docker outcome is unknown.
Local mocked tests cover create-timeout and inspection-failure retention; this
revised source was **not** run on the remote host.

On 2026-10-31, the source from Git commit
`e929c6424db5f5ab3b465d61bc6e0ac5c29805d5` at
`ops/authority_handoff.py` was read with `git show` as raw bytes, base64
encoded, substituted for the `SOURCE_B64` placeholder in the reference probe,
and passed to Python on the main processing host via an authenticated shell.
The source blob's SHA-256 was
`6055208c47361d4992fdb40d4381898060a9cb7a2b58d13cb39916395af986be`.
The probe used local image
`sha256:b655ce507f3ea3924bae9bdb93a6a8ecfa9e55941ab0f9f34f540dc101796a0c`.

The disposable container used a fresh unique name/label, user `10001:10001`,
network none, read-only root, no added capabilities, no-new-privileges,
0.25 CPU, 128 MiB memory and 32 PIDs. Its only host bind was the probe's own
fresh temporary socket directory; it had no Docker socket, live mounts,
client data or published ports. On that host, the unrelated host peer was
rejected. The container peer's `SO_PEERCRED` host PID matched the exact Docker
container PID and UID 10001; exact image/container and cgroup checks passed,
two `/proc` start-time observations matched, a pidfd was initially live and
then became readable on container process exit. This was done twice; only the
second run included the pidfd-exit check.

Both independently labelled containers and their own socket directories were
identity-checked and removed. A subsequent read-only label census found no
containers for either run. No unrelated or previously retained resources were
removed. This evidence is limited to the actual host and fixture process; it
does not qualify the descriptor transfer, candidate startup, admission,
supervisor death, old-writer fencing, trusted-root boundary or cutover.
