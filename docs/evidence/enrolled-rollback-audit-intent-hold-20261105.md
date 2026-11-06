# Actual declared rollback: journal complete, extra audit receipt held

Operation `f1d7fbb3-ca10-4e3b-b415-04569246996e` ran once from frozen root
`c731103f49d0250866b3317a20de76eb2dd83ec0`, archive SHA256
`813365230158d89a40e2b91a93c07e757c3fb0d2c43a2e4850842d6c7c133303`.
It used the same immutable candidate378/image-source187 and genuine693
predecessor pins as the successful forward receipt. One CPU/768 MiB, no host
ports, 900-second outer bound, coordinated quiet slot; no live host/NAS changes.

The actual production journal accepted **30 events**: the 20-phase forward
prefix through final-ready, then all 10 declared rollback phases. It reached
`rolled-back-rebaseline-required`, `admissionOpened=false`, `pending=null`.
Actual candidate stop, original physical lease acquisition, strict declared
rollback projection, candidate name retention, original name restoration,
original restart/readiness and restart-policy restoration passed. No admission,
SQLite restore, mutation replay, old operation reuse or second original start
occurred. The original ran healthily before the extra offline audit stopped it.

The overall qualification nevertheless **held**. The qualification-only
post-restart stopped-writer audit acquired a fresh existing holder incarnation,
ran the strict projection and then failed while retaining its evidence:
`CustodyHeld: original_intent_artifact_required`. Trace location:
`qualify_enrolled_phase_transport.py:87` →
`enrolled_coordinator_custody_ops.py:521` → `PrivateArtifacts.name:213`.
The audit's request omitted `operationId` and `planSha256`; the unchanged
production artifact validator correctly refused it. The successful code path
up to retain is diagnostic evidence only, not a durable accepted audit receipt
or an overall rollback qualification. No audit result was promoted or repaired.

Outer `b31096b95a6f6535c472522c8048862e1eff247fe8c638e5d1507bc4f89be012`
started `2026-11-06T01:28:22.844392653Z`, finished `01:33:39.403426741Z`:
316.559 seconds, exit1/PID0/no OOM, below the outer bound. Capture retained all
five inner containers, state/lease/config/spool volumes and outer artifacts;
`mutationOutcomeUnknown=false`. Exact original was stopped gracefully; the
fresh holder remained alive holding its original leases pending review. No
cleanup, release, retry or old-operation reconciliation is claimed here.

Complete cold operation/plan/bundle/config/journal/worker/log evidence was
preserved privately: 321 files, 1,399,756 bytes, private manifest SHA256
`69e31419ad80c52c4c6b9ef43fe7e28c7419a6c65b8a909243a51919e8bb288e`.
Original TAR independently revalidated against the retained keyed baseline:
202 files, 13 directories, 467,968 bytes, SHA256
`20854a1ab344bd395731ad4192e36cb546ce4ac2a017c7afeb0c375ced9db1bd`.
It remains evidence, not an authorized restore or substitute authority.

Fixture-only correction `4b35073f` adds the missing exact operation/plan binding
and validates `PrivateArtifacts.name` **before any audit stop/start**, rather
than discovering a malformed evidence request after physical work. It adds
actual private-artifact retain/load and wrong-operation/wrong-plan pre-effect
regressions to the existing boundary tests. Production validators and policies
are unchanged. A fresh actual operation is still required; the failed operation
must not be reused.

## Focused correction check and authorized retirement

Frozen correction checkpoint `3070ecaa` archive SHA256
`1c2ca81deebae3cf360e8b70df4ac476fc8d4cf1b8e92d7353b23cc5e8eb0ad4`
ran `python -m unittest tests.test_enrolled_post_restart_audit -q`:
**8/8 Linux cases passed in 0.007 seconds, zero skips**, including real
PrivateArtifacts retain/load and wrong-operation/wrong-plan refusal before any
effect. One CPU/512 MiB, network none, immutable image, frozen read-only source.
Runner b2cfb27a exited0/PID0/no OOM and was removed; its exact inventory was empty.
This proves the evidence-request fix, not a repeated actual rollback.

Before release, the complete CURRENT post-original-restart stopped state was
also exported, alongside original custody, configuration, journal, worker and
log evidence. The combined private inventory covered 526 files/1,865,713 bytes,
manifest SHA256
`65545c90f4f58b72577ae21b28865deff193ff3d4f412618f63f03357ecd44d6`.
A fresh production holder observer verified its actual readiness socket and both
kernel flocks. Exact holder ID/image/operation label/PID279043/start
`01:33:24.381584364Z` were checked before authorized one-shot SIGTERM release;
it exited0/PID0/no OOM at `01:38:04.219713801Z`. No original restart occurred.

Fresh enumeration found **six** task containers (outer, final, bootstrap,
holder, original, seed), not the five inner containers counted by capture, and
five task volumes. All six exact terminal image/label/PID/exit/no-OOM/restart
identities were verified before removal without force. Each exact volume also
required a fresh closed full-container named-volume and bind-parent/child alias
zero-reference census. Both task-label container/volume inventories were empty
afterward. All private host exports and failed audit/intent facts remain; no
receipt was rewritten or promoted and no old operation was retried.

Scope remains local Docker, test-scoped enrollment identity/Desktop exemption,
real factory TLS workers with synthetic engine/coordinator transport, no
connected SSH/systemd or installed/client-job acceptance.
