# Enrolled full-phase fixture smoke

This qualifies the fixture foundations only. The actual22-phase Docker
forward sequence and declared-v2 rollback have **not yet run** at this checkpoint.
There was no installed PC, NAS, real engine processing, publication or deployment.

## Exact inputs

- Harness source: `9f208ce` (base `18750d4`), frozen Git archive8673280bytes.
- Archive SHA256: `1d0ac8ade2db5ccaf094d4c1670a09ca38fb8d310750894ebecdfe9543304ec5`.
- Packaged image: `sha256:378559e42456017b965d1e2e38b56eb627274734aeb317a758d84c0bc99bb5ab`.
- Packaged processing source: `18750d4d599026c18e569a9c67135e6c9fdc7d1a`.
- Frozen controller ops/processing source mounted read-only at `/qualification`.
  Worker readiness binds this controller source footprint; it does not claim
  that the processing image packages the `ops` modules.
- Controller: UID/GID10001, supplemental Docker-socket GID0, one CPU,768MiB,
  read-only root, private128MiB tmpfs,160PID limit, bridge and no host ports.

## Executed check

```sh
python -m unittest tests.test_enrolled_phase_fixture_profile -v
```

Four tests passed, zero skips,43.078seconds:

1. Two actual `daemon.build_worker` factories, `WorkerService`, private SQLite
   engine/driver pins, reader sessions, immutable loaded-source checks and TLS
   readiness; stable repeated proof.
2. The production `enrolled_worker_readiness_probe.probe` validates each
   planned manifest. Swapping another valid fixture into a planned slot fails.
   Mutating the actual committed engine pin also makes readiness fail.
3. Foreign task enrollment labels and arbitrary predecessor image/source/name
   substitutions are refused.
4. Test-only enrolled identity substitutions are restored after interruption.

The worker engine is `NativeNodeFixture`; coordinator polling is a controlled
no-job transport. Local thread-process control replaces systemd/SSH only.
These are explicit fixture seams, not connected SSH or installed-worker proof.

## Lifecycle

Operation `f004a420-1f28-4b9c-a08b-c5f97888775a`, controller
`ltds-phase-smoke-f004a4201f284b9c`, exact container
`1ed0fef7efc82ca9fbb8cbd2752f17208c723c4b4713007596a3a1a0abf14a46`.
The owned process finished normally. Inspected PID0, exit0, no OOM before
removal. The labeled artifact volume was checked for ownership and absence of
mounts, then removed. Exact operation-label inventory was empty afterward.
The shared builder and other containers were untouched.

The controller inherited an application healthcheck that correctly found no
native app at its local8080: the controller runs unittest, not that app. This
did not contribute acceptance evidence; the full harness controller should
disable its inherited healthcheck to avoid irrelevant observer processes.

## Remaining acceptance

Run separate actual forward and rollback operations using the reviewed frozen
harness, the real local693 predecessor image and the current immutable candidate.
Verify every production journal phase, full stopped-writer custody/TAR, declared
startup projections, frozen final runtime and original-image rollback. Forward
also checks actual final-to-both-worker catalog TLS connectivity, capacities and
fresh option propagation. Unknown mutations retain the resources and journal;
they never authorize guessed cleanup or replay.
