# Actual local Docker holder and custody qualification

The new opt-in `ops/qualify_enrolled_holder_transport.py` completed six actual
Docker checkpoints with exit0. This uses only disposable synthetic resources on
the explicitly pinned local daemon. It pulls/builds/installs nothing, activates
no service, contacts no live PC/NAS, and processes no client/engine dataset.

## Immutable inputs and honest profile boundary

- Image: `sha256:2e5675f85b2d8abfeec712dbe4055c497ded63c2a49480aac08ea1bd51552751`
- Packaged source revision: `d0c84fef4c6144f6e846b77c0705b4bf76ff72a8`
- Native CLI SHA256: `6c71cfe71faff949a602946a4df4799b563232e79d0b8b6fd343d0ef081c9621`
- Explicit test daemon: `7d7ff0fd-d590-413a-9301-42a3a08135f4`
- Successful operation: `180beffd-24b1-47d4-b52d-fb7e1898ae74`

The source/image are deliberately the existing immutable local artifacts; the
AuthorityLease module is the reviewed packaged implementation. Current reviewed
holder/exec/custody scripts are supplied using their production serialized
interfaces, not mounted application/import source. The production closed
lease-holder recipe is used unchanged: non-init PID1, UID10001, no healthcheck,
no network/ports, original state volume readonly, exact external bind writable
ONLY from the Mountpoint returned for a newly owned labeled lease volume, and
readonly source/rootfs with conservative resource caps.

The real installed693 ID/deployment constructor is not enrolled by this test.
`FixtureBoundaryProbe` supplies a clearly named component fixture profile with
synthetic original writer IDs. Its production recipe/container/writer/fresh
retirement/positive socket/kernel methods run unchanged. The host method has an
explicit Desktop fixture exception: this existing readonly Windows bind reports
mode0777 even though write access is false. It still verifies the externally
supplied exact CLI SHA, stable original descriptor, actual readonly mount,
no write access, explicit daemon and kernel boot identity on fresh checks. The
production Linux host guard remains unchanged and correctly refused that mode
in the first attempt. Both output flags make this limit explicit:
`productionHostEnrollmentQualified=false`, `desktopReadonly0777Exemption=true`.
No binary chmod/copy/install or source/admin ownership change was used.

Projection is fail-closed and intentionally outside this component scope: a
pinned qualification-only sentinel cannot pass startup projection validation.
Output also states `projectionQualified=false` and `fullCutoverQualified=false`.
Actual frozen final recreation/projection and the full phase CLI remain separate
acceptance gates, not implied by this success.

## Six actual checkpoints

1. The standalone pure-stdlib frozen693 fixture seeded two new owned mounts as
   UID/GID10001, with22 jobs,7 retained Viewer tasks,155 inputs,16 closed schedule
   receipts,7 ZIP/receipts/intents and24 opaque retained pilot files. Its producer
   actually exited0; the real stopped original writer was included in the
   production all-container state-volume/bind-parent alias census.
2. The actual non-init PID1 holder acquired BOTH original flocks. Its positive
   private socket peer/challenge, original open descriptors/kernel fdinfo and
   actual packaged authority checks succeeded. A separate independent namespace
   could acquire neither original flock while the holder was active.
3. Actual `TrackedExec` handled explicit `SystemExit(0)` body completion and
   verified PID/start retirement. A deliberate caller reply drop AFTER the
   actual dispatch RPC had successfully completed left a durable reservation;
   two subsequent recoveries inspected the same result/PID and did not execute
   the O_EXCL once-only body again. This is caller-reply-loss qualification, not
   a claim of arbitrary network/daemon ambiguity injection.
4. Actual tracked full original census fed `CustodyOps.capture`, then a real
   bounded Docker archive stream wrote a private controller-owned TAR. All202
   files/13 directories, exact original SQLite bytes, every file HMAC/mode and
   original UID/GID10001 matched. Existing completion readback and data-only TAR
   revalidation passed without a second archive RPC. The controller UID was0;
   no privileged extraction/chown was needed or performed.
5. A detached precisely identified owned observer remained alive. SIGTERM sent
   to the exact holder did NOT release its original flocks: the holder stayed
   running and a separate independent namespace still acquired neither lock.
   Actual PID/start matching proved the intended observer remained alive.
6. Only explicit release of that exact observer allowed namespace quiet. The
   holder exited0 with PID0; a new independent namespace then acquired BOTH
   original locks. No forced kill or timeout-based lease retirement was used.

Actual retained TAR SHA:
`cdaf4fb93b945ccb00a0867b2038f1a43600e7c4460e0efc1f58bb9ba2e31e34`.
This is a synthetic data digest, not a Viewer signed acceptance or deletion
receipt. The opaque fixture continues reporting pilotClosure/Viewer acceptance
false; no semantic ownership is invented for retained pilot bytes.

## Resources, unknown outcomes and cleanup

The harness reuses `QualificationDocker`'s sticky unknown-mutation rule and one
shared extracted `cleanup_owned` implementation; cleanup behavior was not
weakened or duplicated. An unresolved daemon mutation immediately retains
backing resources. An empty scan is never substituted for closure. Native four
portable checks also passed: unknown mutations do not scan/delete; foreign
containers are untouched; cleanup stops before volume removal after uncertainty;
and dropped replies occur only after confirmed delegate success.

Controller artifacts/key/registry/TAR are on a separately persistent, task-owned
volume, not ephemeral controller tmpfs. Unknown exit must retain it and the
controller for diagnosis. The caller must NOT use unconditional --rm or delete
this volume on failure. On known success, inner owned containers and volumes
were removed through the shared ownership guard. The outer controller's exact
label/PID0/exit0 was verified, then it was removed; its private artifact volume's
exact label and lack of mounts were verified before removal. Both the initial
known-failed Desktop permission diagnostic and the successful operation have
empty owned container/volume inventories. The unrelated shared builder was not
stopped, removed or modified.

The controller was capped0.5CPU/768MiB, holder0.25CPU/256MiB, transient probes
0.125CPU/128MiB; fixtures were readonly-rootfs/no-network and dropped capabilities.
This run's total elapsed duration was not separately recorded; success is the
actual exit0/six-checkpoint receipt above, not an invented timing estimate.

Run inside the reviewed readonly controller harness with its existing CLI/socket
and persistent artifact mount, passing `--image`, `--source-commit`,
`--expected-daemon-id`, `--expected-cli-sha256`, `--artifact-root` and a NEW
`--operation-id`. No argument contains a real credential. The opt-in profile
refuses a mismatched daemon/image/CLI or an already-existing operation directory.
