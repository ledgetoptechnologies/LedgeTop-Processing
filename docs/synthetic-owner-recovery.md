# Isolated owner and recovery fixtures

These files are research fixtures. They are disabled for production and must
not be installed as a service, connected to client data, or used to authorize a
live update. They build on `synthetic-streaming-gateway.md` and depend on its
local, checksum-pinned nftables test extraction; no binaries are committed.

A disposable user, network and PID namespace hosts a synthetic owner and
backend. A trusted parent passes an inherited Unix socketpair. The owner checks
peer UID and a synthetic token, journals open, drain and stop intentions, and
starts closed. The gateway streams a synthetic 1,048,577-byte request while
an nftables rule blocks direct backend traffic lacking the test-only mark.
The separate-UID variant drops the test worker to UID/GID 65534 and removes its
network capabilities. It tests failure to inherit the control descriptor, read
the owner's environment, mark a socket or reach the backend directly.

The trusted controller retains pidfds acquired while the old namespace init
and wrapper are alive. A fixture recovery grant compares their captured
PID/parent/start identities against the durable owner record and only duplicates
its own dead pidfds. Restart tests cover authenticated reopen after a recorded
stop and reject missing/live/unrelated wrapper descriptors, an unrelated init
witness, lost witness and crashes around open intentions. Recovery descriptors
are passed through the inherited socketpair using SCM_RIGHTS; framing and
cleanup tests cover truncated and excess descriptors. These are synthetic
processes and a trusted harness, not a production recovery protocol.

The test worker retains its inherited supplementary groups because the new
user namespace denies setgroups; the fixture does not establish safe group
isolation. Parent credentials and namespace descriptors are trusted. The
fixture helper remains directly launchable by a same-UID process and accepts
raw descriptors from its inherited control peer. The bootstrap token is chosen
by that peer. Neither a private socketpair nor Python object encapsulation
proves an exclusive host launcher or protects mutable SQLite state from the
host owner. A host root user, Docker administrator, or another process with
sufficient network privileges can bypass these experiments.

No persistent host service, complete Viewer/NodeODM HTTP contract, client
credentials, Docker socket policy, cgroup delegation, privileged restart
ownership or host firewall integration has been qualified. An owner crash can
require manual inspection; unverified state must remain closed. A separate
kernel-enforced and operationally provisioned authority, plus real outage,
restart and ingress tests, is required before considering deployment.

On Linux with the prerequisites described in the gateway fixture document,
the reviewed focused tests were run with bounded external watchdogs:

```
timeout 55s python3 -m unittest tests.test_fixture_bootstrap tests.test_separate_namespace_owner tests.test_namespace_owner_restart tests.test_namespace_recovery_grant tests.test_previous_init_witness -v
timeout 35s python3 -m unittest tests.test_separate_uid_owner tests.test_separate_uid_socket -v
```

The namespace harness owns its direct processes and uses WNOWAIT to reserve
the wrapper PID before process-group cleanup. An externally forced kill can
interrupt that cleanup; watchdog invocation alone is not proof that every
possible descendant was reaped. No live node or host firewall was changed.
