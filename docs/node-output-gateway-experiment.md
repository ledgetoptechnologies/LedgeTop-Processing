# Node output gateway experiment

This isolated component is not wired into the scheduler or Viewer, and is not
qualified for deployment. It checks that an explicitly configured Docker
container publishes NodeODM port 3000 solely on a fixed loopback port, then
checks the container ID, PID and Docker start time before and after a bounded
output read. It returns a hash of that container incarnation with the lines.
The gateway itself binds to loopback only and requires a separate bearer token.
It does not expose the Docker API. It requires a local Unix Docker socket and
the operator must protect the local account that can use it.

Container continuity is weaker than task-stream continuity: NodeODM can
restart or recreate a task while the container continues running. Neither a
task incarnation nor child-process liveness is verified. A change of endpoint
ownership and a restart occurring between observations are held, but an
untrusted host-local process with control of Docker or the port is outside this
experiment's boundary. The response body has a wall-clock deadline, but the
HTTP header read is subject only to socket timeouts and still needs a strict
wall-clock bound. No
authenticated remote transport, supervision, retained log, real-node task
integration, deployment, or cleanup policy is supplied. Do not use this
container epoch to assert that output is complete or processing is running.

The synthetic tests use a loopback HTTP backend and simulated Docker inspect
results. The main PC's actual Docker mapping was read only and matched the
expected loopback-only mapping. `docker top` showed the container's PID 1 is
the sole `node` process at inspection time, but this point observation does
not prove it cannot later spawn a worker or recreate a task. No real task
output was read or modified.
