# Synthetic partial-response and Viewer ledger restart

**Local synthetic rehearsal only.** No production endpoint, client data,
credential or TrueNAS is used. The Windows command from this worktree is
`python -m tests.paired_partial_drop_outer`.

The outer watchdog creates a random empty Windows temp root and random run ID,
then launches the owned launcher under a bounded Windows Job Object. The
launcher checks Processing runtime and contracts against commit
`3e4c5eacfd4df22169b9b08ae5e4b90c9e7da57a`, Viewer server against commit
`7c526b49d78423372c29eac98b95a16a270c2bd3`, and both golden copies against
SHA-256 `5c81c6973a1447efa64cd27dbac68393cea0395111fa087c5bd3c451b18dca41`.
It snapshots the two synthetic workload scripts into its own new root and
records their SHA-256 hashes. Source status checks are observations, not a
physical writer fence; all real mutations remain prohibited during the test.

The launcher checks the image ID exactly equals
`sha256:0bb8832e2ce0cf718ae6b5419fa79501a75f303a82ff4b32200888329f7ef668`.
Its history contains the `python3-gdal` package and its metadata identifies
Node 24.22.2; `/usr/bin/python3` is an unverified precondition and a missing
binary makes the run fail. Before writing the owned volume, the in-container
fixture prints the actual Python and Node versions and requires Node's built-in
SQLite import to succeed. It creates a new randomly named local Docker volume
with the run label after an absence observation; a competing create between
observation and creation is not excluded. It checks name, driver and label.

The exact `docker run` arguments are assembled in
`tests/paired_partial_drop_launcher.py`: random name and cidfile, same run
label, `--network none`, `--read-only`, `--tmpfs /tmp:rw,nosuid,noexec,size=128m`,
`--cap-drop ALL`, `--security-opt no-new-privileges`, one CPU, 1 GiB memory,
64 process limit, and `/usr/bin/python3` entrypoint. It bind-mounts only the
Processing `processing/`, `contracts/` and own script snapshot, and the Viewer
`apps/viewer/server/`, each read-only; the random named volume is the only
mounted writable data area. The process uses container root to access that new
root-owned volume, but no host Docker socket, host network or host write bind.
The container runs the Python Processing HTTP server, bounded loopback proxy,
and two sequential Node processes. The Viewer ledger fsyncs the synthetic
three-byte object and commits its outbox. The proxy obtains the first committed
acceptance response, forwards its headers and one body byte, and closes the
connection; the Viewer observes an uncertain result and exits 86 without a
ledger close. A fresh Node process reopens the ledger and retries the exact
receipt, observes `created: false`, then marks its outbox acknowledged.

The launcher has a 120-second owned Job deadline; the outer Job bounds the
launcher at 160 seconds. The in-container Node children have 25-second
deadlines and are owned by the container; the proxy and listener are shut down
and joined on normal exit. On any launcher failure or timeout, the outer
watchdog reads its own cidfile and inspects the exact container ID, expected
name, image, run label and owned volume; it additionally checks volume name,
driver and label before stopping an identified running container. Missing or
ambiguous identity is reported and **never** stopped by name. Container, named
volume and Windows temp root are intentionally retained; no recursive deletion
or Docker resource removal occurs. An adversarial creation race, already open
host writers, daemon failure, or host loss cannot be ruled out by labels.

This is a synthetic attempt to exercise a partial response and process restart.
It is not production TLS, a power-loss test, Viewer publication/bridge, genuine
custody, cross-host writer fencing or permission to clean producer results.

## Observed run

One bounded run passed on 2026-11-01 with Python 3.11.2, Node 24.22.2 and
built-in SQLite available in the pinned image. The test reported
`SYNTHETIC_PARTIAL_DROP_AND_LEDGER_RESTART_PASSED`; the exact owned container
exited with code 0 and the random volume was verified by its run label and
retained. This proves the described local synthetic path once; it does not
cover repeated stress, power loss, TLS, host/network interruption, or live
Viewer publication. Retained artifacts must not be removed until their owner
reviews them.
