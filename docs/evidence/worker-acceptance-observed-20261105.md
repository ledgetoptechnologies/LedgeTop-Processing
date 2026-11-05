# Worker observation qualification

The isolated candidate passed **70/70 Linux tests, no skips**, in 4.213 seconds:
`test_reference_acceptance_observed` (17 new cases), `test_reference_driver`,
`test_reference_worker_api` and `test_reference_api`. The first two pure contract
cases also passed on the Windows host. The final Linux run used one CPU/1 GiB,
read-only source/root, disabled external networking and a private 192 MiB
temporary filesystem in image
`sha256:57f01267c88439d872f6782b54511e314614a76145e32cfe2fad003e5ca0fac8`.
The task container exited and was removed.

New coverage exercises the actual Store journal, assigned Worker API and durable
driver with synthetic engine/source data. It proves original-claim auth after
completion, rejected wrong worker/token/environment/generation/digests, closed
metadata, per-request full coverage, conflicting/global observation identities,
quota rollback, stable server time and exact replay without a new quota/time,
coordinator reopen, and Viewer-scoped pending/read lookup without private receipt
or token exposure.

An exact committed observation also replays after a later custody hold, including
the dropped-reply/cleaned-result/reopened-driver case. A first observation under
held custody and a conflicting new request remain rejected. The released source
state is not implemented yet; enabling it must preserve this historical replay
contract and independently enforce the authenticated release policy.

Driver cases exercise foreground and background paths, 404/mismatched replies,
request persistence before contact, lost reply after coordinator commit, fresh
driver reopen after physical generic scratch-result cleanup, and completion only
after the matching acknowledgement. Persisted retry windows suppress both relay
GET and observation POST before due, saturate at a five-minute ceiling, survive
driver reopen and do not prevent the next job from entering claim processing.
Existing signed-copy cleanup still runs during a deferred observation.

One new case uses a real loopback HTTPS listener, verified synthetic CA and the
actual driver's HTTP client. It intentionally drops the first successful POST
reply after coordinator commit, then verifies exact request/ack replay and reads
the same acknowledgement through the Viewer-authenticated GET route. The owned
listener and request threads are joined on completion.

This is not installed runtime, NAS durability, real photo processing, licensed
conversion or source release qualification. Engine mutation and source data are
synthetic. The current observation driver fixture uses a photo-only engine
manifest, while retaining synthetic raw/derived GCP custody objects separately.
The pre-existing ODM driver's refusal of auxiliary GCP engine inputs was reported
for separate scoped work; these tests do not qualify GCP engine execution. No
source deletion or Viewer snapshot-hold retirement was enabled.
