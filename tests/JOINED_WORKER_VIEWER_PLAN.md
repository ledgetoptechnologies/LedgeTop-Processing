# Synthetic worker-to-Viewer rehearsal

Do not start or signal containers/processes until exact scripts have independent
ownership and failure-path review. The candidate below was reviewed and run;
later modifications require renewed review. Reuse the coordinator-restart
supervisor, modifying it only to export an atomic synthetic ready marker after
worker-to-coordinator receipt and keep the coordinator and worker alive until a
bounded Viewer completion marker. The exported marker contains the actual
persisted outbox binding, result manifest, synthetic bearer and HTTPS port; no
production data or credentials. The worker and coordinator remain owned by the
supervisor throughout Viewer retrieval.

Use separate uniquely named, exact-labeled, resource-limited Python and Node 24
containers sharing an owned `--network none` namespace. Pin both local image
IDs; snapshot committed Processing and Viewer source and record exact commits
and every overlaid fixture hash. A retained private bind root carries only the
synthetic marker, CA and Viewer SQLite/results. Start the Python supervisor,
wait boundedly for its atomic marker while checking its container remains live,
then run the Node fixture in the same network namespace. Never accept a marker
from an earlier run or start Viewer after coordinator ownership uncertainty.

The Node fixture must bind the actual worker-produced manifest and job identity
to a fresh Viewer candidate database, then use `stageAndHold` with an
`openObject` adapter issuing real TLS-authenticated HTTP GET to the coordinator
result proxy. Its response headers and async response stream must flow directly
into Viewer storage verification; no supplied or substituted object bytes.
Verify staged files' digests and held receipt, send that exact held receipt via
Viewer's actual candidate sender to the real Processing acceptance endpoint,
and verify Processing returns and persists the matching acceptance. A Viewer
completion marker allows the supervisor to confirm the receipt and stop only
its owned children. A matching receipt proves this synthetic acceptance path,
not an enabled production publication or permission to delete worker data.

The Windows Job watchdog must own the runner and reconcile every planned name
using exact run label and full ID after ambiguous create/start; kill only exact
owned IDs if necessary and verify container exits. Keep container, test roots,
databases and files; never delete recursively. Preserve separate log streams and
report any unconfirmed children or containers as uncertainty. A clean container
exit and zero Windows Job active processes are prerequisites for a PASS report.

## Synthetic evidence, 2026-11-01

Passing run `cbdc502edad448009cae924be687d681` used Processing
`4020f922990bca17598390868a8d378e80615154` and Viewer
`dd81a07464a988b9fe23d08b2b6a082580464aa9`. The retained root is
`C:\Users\fstor\AppData\Local\Temp\ledgetop-acceptance-outer-9iwjmwst`.
The exact-labeled Python container
`4ae459b0b66de59ada225de1fd86521e9ed7604e221fda038c66401be0290962`
and Viewer container
`6543792d05d243abed9fadb0eddee5f1cd007db11c9641a89afd8a6a3d5c4b90`
both exited 0; inner group exited 0 and Windows Job active count was zero.
No ownership uncertainty was reported. The container and files remain retained.

The first coordinator was killed and reaped after the synthetic engine commit;
the worker finished while it was offline. The restarted coordinator received
the matching worker outbox, Viewer staged the real proxy HTTP stream and sent
its held receipt, and the coordinator verified the exact persisted acceptance.
The engine's initialize/upload/commit/download counters each remained one.
Viewer's candidate outbox remains held even after matching response. Node warned
that IP-address TLS server names are deprecated; this is not a cross-host TLS test.
No real ODM, production Viewer publication, live client data, or power-loss
recovery was exercised.

Two earlier synthetic runs used Processing
`3005921ca0fe2df11024076da7b12e232bd6021f` and the same Viewer head:

* `085654c27ad74c4b83870d4bcc76a6c7` failed because the synthetic Viewer
  attempt table omitted `provider_id`. Root
  `C:\Users\fstor\AppData\Local\Temp\ledgetop-acceptance-outer-4uiv9lhz`;
  Viewer container `6e581a76c6adba95e7709c53e601280c620e76aa0635c89a84883a6ca3c8d94d`
  exited 1 and Python container `bba45e8fe2a664fe8462631de3e027dbb9f21c6adcf4076a0f7a63d3f63ed4e4`
  was stopped by the owned runner (137). Outer Job active count was zero.
* `c398ed3c92de4c919c31c34e23546c58` staged the proxy data and persisted one
  coordinator acceptance, but Viewer reported review because that Processing
  revision omitted receipt identity/digest in its response. Root
  `C:\Users\fstor\AppData\Local\Temp\ledgetop-acceptance-outer-4qqg5iei`;
  Viewer container `08ce60e9fc00142e76529836e3fecbe23863158dd423ba8073d1bf559b3cb748`
  exited 1 and Python container `a301f59b7cc1bc9ccec4d51b88d11da6ff30e4a111e3ed2a34d540472ddf2ca5`
  was stopped by the owned runner (137). Outer Job active count was zero.
  Neither failed run verified graceful inner process-group exit.

Passing snapshot fixture SHA-256:

* `worker_process_outage_fixture.py`: `50b68420d6a20e9171f505274c27000fc7a0253c6793605f1d13275581700549`
* `worker_process_outage_engine.py`: `f7f3fbc6f3ff0af14c1ef4fad1c6343e4500a09681636e627e028c5e0035903e`
* `joined_worker_viewer_coordinator.py`: `8a79a07be92ef4eb637ecb131e6bad086e69832dd38608cd76c41c5999b4a432`
* `joined_worker_viewer_supervisor.py`: `6ddda9b59f07019e0a13fa893f27c7cdd2055b5c50a830c16ca85492f5bfa19a`
* `joined_worker_viewer_watchdog.py`: `8ae1289fdd548c873657beba4569d3623d8179498c84dcfcb083469f5cb7157a`
* `joined_worker_viewer_sender.js`: `dc9c7e374e4664d3ffeaf6037d8d0dd134f0ec4715e10a8d7424c5cdb63cfa2c`

Passing image IDs: Python `sha256:aa943ce411423c7582288f18707ff66eddcbaeb510502d747a72aeb34f30d159`;
Node `sha256:c86ecead9e55a5210ac1727de38a4ca840bf03dcaf34dc9749addbf383b1920c`.
Viewer module SHA-256 values, in the runner's order:

* Consumer: `6cad93cd4f7abc24e5d17814a135d121f69e5e6b42c27d694f6fee2c1deb185e`
* Storage: `f8c04a1c4e0eab15ec4be3488721ced13374294270e23e72db85c8ad0c406871`
* Object: `eeebafc2926a11f517fd42dd685b1c20262b7611ad32039c8c399aa3ffe3774f`
* Receipt: `605799070cfd5a7bfbb684b384dba6ffbd9c8188f6ad971e1680d658d1f1fbd3`
* Bound sender: `72a97b7f6a72fb105a9c94540d561c6d5239963a2e53e423c3e3c131b44e4c6c`
* Acceptance: `987bf295713f35c618a834fb6ff3cbfb80d89aa5fdd66bd88b8cdbc84a5446da`
* Provider auth: `971c466e33604809bba3d8c2808c6147ea267851292a4d084204b708e58ef3a5`
* Sender revision: `2ca8ab749640538e8f126f12c6f1fb9ecd354cd18e2421dbd1e54678569258ce`
* Protocol: `5de970d4ee33e7b802bbe0c1eb99fec1713adfe005a5fcf60d2f4d8afa273028`
