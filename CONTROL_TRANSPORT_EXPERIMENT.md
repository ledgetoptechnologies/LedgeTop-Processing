# Experimental process-isolated control transport

This candidate is inactive unless explicitly passed to `ReferenceDriver`. It is
not wired into the worker daemon or qualified for production. Its explicit
CA-only trust policy differs from the existing caller-supplied `SSLContext`;
client certificates are rejected. No deployment or migration is authorized by
this prototype.

The request uses an owned Python child and secret-bearing stdin. Requests share
a process-wide serialization lock. If child termination or reaping is
uncertain, the transport retains the handle and latches a held state that blocks
new child creation in that process. The latch and handle are not crash-persistent;
inspect any uncertain child before restarting the parent. The status function
returns only the child PID and process state.

The deadline bounds lock wait and child communication but cannot interrupt
process creation before `Popen` returns. The child bounds its own output, but
parent-side `communicate` is not an independent streaming bound against a
compromised executable. Termination/reaping waits are bounded and can add to
the request deadline; an OS refusal can leave an unreaped process and is an
inspection hold. File no-follow/nonblocking support depends on the platform.
The child uses a restricted environment and a configured, bounded CA whose bytes
are checked against an explicit SHA-256 digest. These checks do not establish
trust in the interpreter, installed modules, or ancestor directories.

On November 1, 2026, the reviewed synthetic Windows Job Object wrapper
`python test_control_outer_job_review.py` passed three tests: fake concurrent
latch and lock contention tests, and local TLS slow-drip header/body timeout
checks. The wrapper reported zero active job processes; test roots were
retained. This is not a full TLS policy, redirect, malformed-response, real
mutation/replay, end-to-end, or crash recovery qualification. The initial test
run had two fixture errors due to an incompatible synthetic path; these were
corrected before the passing run.
