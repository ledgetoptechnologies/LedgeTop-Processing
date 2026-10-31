# Live coordinator-update gate bridge

`ops/coordinator_gate_bridge.py` transports fresh main-gate checks over two
independently authenticated SSH stdio connections owned by a trusted controller.
It creates no SSH connection, listener, daemon, key, grant, agent forwarding,
scheduled polling, or network port. Cluster does not authenticate to main.
Controller CLI/bootstrap work is separate. No live host operation is implied by
the source or synthetic tests.

## Required composition

Use fixed `BridgeBinding(engine_id, node_id, authority_id, coordinator_origin,
gate_generation, helper_sha256)` values for main and cluster. The generation,
authority and exact normalized origin must come from the reviewed existing
canonical gates. The main binding uses node `main`; do not invent a generation,
provision a missing gate, clear a reservation, or perform an ownership handover.
`helper_digest()` hashes the exact bridge and imported `processing/node_gate.py`
source bytes. Both remote installations and the controller must match the
reviewed helper digest. Root's SSH executable, account/host identity and pinned
host keys are an external trusted-controller responsibility; frame fields are
not SSH authentication.

The main SSH child runs
`serve_main(reader, writer, binding=..., operation_id=..., session_nonce=...,
engine_probe=..., timeout_seconds=10)`. Streams are binary stdin/stdout pipes,
not a PTY. For actual main runtime use
`MainEngineProbe(expected_image=RunSpec.main_image, executable='/usr/bin/docker',
endpoint='unix:///run/docker.sock', timeout_seconds=10)`, not a cached image
attestation or the identity-only fixture probe. `DockerEngineProbe(executable,
local_endpoint)` supplies the bounded identity-only local read for cluster's
canonical context; it is insufficient for main runtime readiness.

`MainEngineProbe` brackets every call with fresh Docker Engine ID reads and
verifies the fixed existing full main container ID
`78be3d38659e2b698c7611b6b60fad46e7632ef96aa21898a7c2edafece093c1`, exact
`/ledgetop-main-processing-nodeodx-1` name, immutable prepared image, running
state and positive PID. Docker health may be absent; a present health status
must be `healthy`. A foreign/recreated container is not adopted.

It reads only fixed `http://127.0.0.1:13004/info` and `/task/list` using direct
connections without credentials, redirects or proxies. It requires actual
`engine:odx`, integer queue count zero, valid measured versions and exactly an
empty task list, including no historical terminal task entries. Headers and
body have explicit bounds and one absolute monotonic deadline; trickling either
cannot extend it. This narrow HTTP profile accepts identity-encoded JSON with a
Content-Length or connection-close body; unsupported transfer encoding fails.

Tasks alone do not prove idle. Fixed read-only Docker exec commands list
`/var/www/tmp`; it must be empty or contain only `.gitignore`, proven by `stat`
to be one regular byte and by `cat` to contain exactly `*`. A bounded `docker
top ... -eo pid,args` inventory must contain exactly one Node server at
`/var/www/index.js`, at most one known shell wrapper and init, and no other
process or duplicate PID. Unregistered uploads, symlinks, processing/orphan
children and malformed inventories refuse the challenge. Nothing is deleted.
Raw process arguments, HTTP bodies, environment and credentials are not emitted
in bridge frames. The combined helper digest binds this readiness logic too.

The main holder enters `LocalGate(binding, engine_probe=...)`. It requires the
existing private canonical root, gate lock and state file. It captures creating
PID plus root/lock/state device and inode; every check verifies live FD/path
identity, owner/modes/link count, source digest, fresh local Docker identity,
exact node/authority/origin/generation and `reservation:null`. Its actual OS lock
remains held for the whole holder process context. Fork children cannot check or
unlock it. EOF or an error unwinds only this context. Gate state is never
provisioned, reserved, rewritten, resolved or cleared by this bridge.

The cluster executor composes:

```python
main_client = BridgeGateClient(
    sys.stdin.buffer, sys.stdout.buffer,
    binding=main_binding, operation_id=operation_id, session_nonce=session_nonce)
local_cluster = LocalGate(cluster_binding, engine_probe=cluster_engine_probe)
with GateSet(local_cluster, main_client) as gates:
    gates.check()
    # Reviewed updater calls gates.check() around its own mutations/reopens.
```

`GateSet.binding` contains only the public fixed `cluster`/`main` binding maps;
the live session nonce is not a durable checkpoint attestation. The factory can
be a callable returning that new context. `GateSet.check()` checks local cluster,
requests a fresh main response, then rechecks local cluster while both contexts
remain live. The cluster process must reserve stdout for bridge frames. Logs go
to sanitized stderr/private artifacts; final update results must not be printed
as additional stdout JSON.

The trusted controller constructs
`RootRelay(main_process, cluster_process, binding=main_binding,
operation_id=..., session_nonce=..., timeout_seconds=10).run()` with the two
actual already-launched SSH child processes. Each must expose binary
`stdin`/`stdout`, `poll()`, `wait()`, and `returncode`. Stderr is separate and must
not be merged into protocol stdout. `.run()` is single-use and returns `0` only
after a bound close frame, no trailing stdout, actual cluster exit status zero,
and a still-live main child. It closes the owned links on completion/error and
terminates its owned cluster child after failure; it never injects retries or
remote unlock/resolve commands. The launcher must reap its actual child
processes and treat transport/exit uncertainty as a held operation.

## Framing and freshness

Frames are closed JSON objects bounded to 4096 bytes including newline. Unknown
fields, duplicate keys, nonfinite numbers, non-JSON stdout, oversize frames,
truncated EOF and stale/reordered replies fail closed. Binary pipe reads and
writes are nonblocking. The caller's absolute monotonic deadline spans an entire
challenge write and reply read; incoming bytes cannot renew it. A partial frame
also has an absolute deadline once its first byte arrives, while an idle holder
can retain the actual lock during legitimate long offline work. There are no
background gate challenges or heartbeats. Windows uses local nonblocking pipe
readiness checks; this does not poll a remote gate or create an autonomous task.

The main holder emits one `main-gate-ready` with exact operation/session/binding,
a newly random holder nonce and captured gate identity. Ready alone is never
proof: `BridgeGateClient.__enter__` immediately sends its own unpredictable
challenge. Each `main-gate-check` has a strictly advancing sequence and fresh
64-hex nonce; main replies `main-gate-checked` only after its live check. Replies
must match the exact nonce, sequence, operation, session, helper, gate identity
and binding. The controller forwards only these checked exchanges and monitors
its actual child processes. There is no saved status/Ready replay path.

On a normal client context exit, the client sends one bound `main-gate-close`
using its last completed sequence/nonce. The controller then requires protocol
EOF and the actual zero exit status. A close frame is not another gate check and
cannot authorize any later mutation. Error exit sends no successful close.

## Failure boundary

A fresh reply proves a live point-in-time check, not an atomic distributed lock
around a later Docker operation: the main holder/controller can die immediately
after replying. The updater must retain durable exact drains on both nodes,
check the live contexts before each mutation and reopen, preserve intent before
external writes, and hold uncertainty. Loss never authorizes a cancellation,
rollback, peer-plan adoption, force removal or gate release. The next executor
check refuses progress after link loss. A child already inside an external
mutation may still have an unknown outcome; neither relay nor client repeats it.

Tests use synthetic temporary canonical gates, real local pipes/subprocesses and
forks, with injected Docker identity callbacks rather than live Docker calls.
They cover both-lock contention, controller/holder death and EOF, nonce/binding
replay refusal, absolute trickle deadlines, protocol close/exit proof, fork and
inode guards, and strict Windows pipe framing. They do not establish SSH access,
change production gates, or run a coordinator update.
