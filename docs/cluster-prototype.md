# Installed cluster prototype

On October 25, Beau confirmed the new SSD and authorized installation and staging
tests on the cluster PC, which has no production workload yet. SSH verified
`bkoltz@192.168.50.89`, hostname `cluster`, Samsung SSD 870, and approximately
234 GiB free on the root filesystem at installation.

Reviewed Processing source `c3fcb3a0016bb645e2bc7e111150b51576f38b10` is installed at:

```text
/home/bkoltz/.local/share/ledgetop-processing-prototype/c3fcb3a/
  source/              reviewed repository snapshot
  runtime.env          private credentials, mode 0600
  installation.json    installed identities and unchanged-service baseline
  acceptance.json      sanitized test results
  processingctl.py     private host command wrapper
  work/                persistent worker scratch/receipts, currently empty
  dispatch-state/      bounded dispatcher state
```

The new coordinator container is `ledgetop-processing-prototype-c3fcb3a`, bound
only to `127.0.0.1:18080`. It runs as UID 10001 with read-only root, a persistent
named state volume, 0.5 CPU and 256 MiB RAM, without Docker socket access. Docker
is enabled at boot; the coordinator restart policy is `unless-stopped`. Queue
recovery remains conservative. An actual host reboot was not performed.

The host's new Docker Engine is
`b557939c-6ac3-4e73-8b14-31caf7390f23`. Its canonical gate is bound to node
`cluster`, localhost origin, authority `7131882e-38ce-4d3e-99cc-4ba5a94d2ee8`,
generation 1. Do not reuse identity metadata from the old disk/Engine.

The stopped `c911e22` coordinator and original named volume are retained for
rollback/history. Its drained SQLite database was backed up into the new volume;
two prior successes and one prior failure were preserved. Existing `arm-rippers`,
`clusterodx` and `node-odx-gpu` containers retain their IDs, start times and restart
counts. Their routing and credentials were not changed. These direct ODX writers
remain outside the new cooperative admission gate.

## Host commands

The installed copy of [the control wrapper](../ops/processingctl.py) resides next
to the private runtime metadata, and loads the API token without printing it.
Use SSH to run it:

```sh
ssh bkoltz@192.168.50.89 'python3 /home/bkoltz/.local/share/ledgetop-processing-prototype/c3fcb3a/processingctl.py status'
ssh bkoltz@192.168.50.89 'python3 /home/bkoltz/.local/share/ledgetop-processing-prototype/c3fcb3a/processingctl.py submit-synthetic --environment staging'
ssh bkoltz@192.168.50.89 'python3 /home/bkoltz/.local/share/ledgetop-processing-prototype/c3fcb3a/processingctl.py dispatch --max-jobs 2'
```

`submit-synthetic` admits only generated fixture work; its `production` choice
tests queue priority and does not select real datasets. Dispatch is a bounded
foreground command for 1–10 queued synthetic jobs. It waits for exact completion
delivery while running. No continuously polling worker or worker boot service
is installed. If the host/worker process is interrupted, inspect the private
receipt and use the documented completion-only resume procedure before any new
dispatch. Do not clear an uncertain dispatcher guard to make it run again.

The API remains private to the host. An SSH tunnel can expose it temporarily on
an unused localhost port; no LAN firewall or public access change was made.
Viewer submission, arbitrary datasets, TrueNAS artifact transfer and real
NodeODX-to-TreeDar pipeline integration remain separate work.

## Acceptance

All 115 coordinator checks and 34 worker tests passed on the cluster's Python
3.14, including the Linux symlink case. Four real, small synthetic TreeDar jobs
then passed through the installed coordinator and actual canonical host gate:

- Production ran ahead of an earlier staging submission.
- A worker finished with the coordinator stopped and retained its completion.
- After independently confirming that engine container had exited, the delivery
  process was interrupted and resumed from the exact receipt after coordinator
  restart. Its original claim completed without running the engine again.
- Another worker stayed alive through a coordinator outage and automatically
  delivered its result when the connection returned.

The first test harness attempted a health request before the restarted service
was ready. Its saved pending receipt was recovered explicitly; no job was rerun
to bypass the readiness race. The final automatic reconnect test passed too.

Independent post-install inspection verified healthy/idle coordinator, idle gate,
unchanged existing services and private credential-file permissions. All four
tests' artifact directories, private receipts and stopped engine containers were
removed after hash verification. Historical resources were preserved. The queue
now retains six successes and one historical failure, with no queued/running/held
work. See [sanitized acceptance evidence](evidence/cluster-install-20261025.json).

Completion still returns metadata and hashes, not output file bytes. Outside
these disposable acceptance fixtures, worker output must remain until a verified
destination accepts it. See [ownership and recovery](resilience.md).
