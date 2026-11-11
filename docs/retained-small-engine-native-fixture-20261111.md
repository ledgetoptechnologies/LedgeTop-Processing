# Isolated native mechanics rehearsal — source candidate

`ops/qualify_retained_small_engine_swap.py` defaults to printing a plan only.
`--run` requires separate review and root on staging90; it refuses processing
hosts89/88. It has not been remotely executed by this source author.

The harness loads the exact frozen helper by SHA256 in a private Python module,
then binds only that module to new nonce-labelled fixture paths, Compose project,
containers, named volume and internal Docker network. It calls the actual
`Runtime.change_ingress`, `stop_and_backup`, `recreate_and_verify` and
`persist_pin` methods. It does not use or create an HTTP coordinator, invoke
the live queue, rebuild images, pull images, publish ports or request a GPU.
Existing staging API/worker/proxy and shared builders are untouched.

Required cached engines are the fixed5ce base and9c retention child. Nginx must
already be cached as either the fixed84ed production ingress image or the
fixed697 staging proxy image; the chosen digest is recorded. Missing images
stop before Docker resource creation. Root must have the fixed8GiB base-export
budget plus8GiB reserve plus256MiB before the single full-filesystem export; the helper checks
backup headroom again. Engine is limited to0.5CPU/768MiB and nginx to0.25CPU/64MiB.

The disposable volume contains three freshly dated terminal task records plus
synthetic source/result sentinel files. Fresh dates let the original unpatched
5ce server start without intentionally expiring this fixture. This is **not** a
repeat of the separate expired-task retention tests. A distinctive tmp placeholder
mode/ownership is archived and checked after recreation. An owned writable-root
sentinel is copied into the old fixture container; its bytes must appear inside
the actual Docker export. No live filenames or client data are used.

Intentional topology exceptions are explicit: this fixture uses an internal
bridge with no host port bindings, not the production ingress's host network.
An engine-container Node HTTP adapter handles the helper's fixed13001/3001
observations inside that isolated network; the helper's curl503 observation is
redirected through the same adapter. There are no host HTTP requests. Only
the exact known Node14 wrapper banner is removed; unknown output fails closed.
Ingress inspection is overridden to check the fixture network/owned pins rather
than a production host-network identity. A read-only synthetic logger-config
bind uses the fixture's tmpfs/tmp; it is preserved through Compose recreation.
This does not qualify production network isolation or credential wiring.

The native nginx code must atomically close/reopen with mode0644 under umask077,
HUP successfully, observe503/200 and retire its prior worker PID set. This tests
actual reload/worker draining, not a long in-flight photogrammetry transfer.
Actual full-root export, GNU tar comparison, Compose no-start recreation,
configuration/mount equality, patched hashes, dynamic options/max1000, terminal
identity, retained files, tmp metadata and clean exit0/noOOM are required to pass.

Cleanup rechecks exact nonce labels/names, no container references and the volume
marker before removing only owned containers/volume/network. A failed partial
setup is covered; ambiguous cleanup is never automatically replayed. Successful
evidence is written before deleting the owned bulk backup directory, after
canonical path/marker/mount checks. A small root-private evidence directory
remains for parent review. Failed-run artifacts remain private for inspection.
Synthetic tokens are disposable; no production credential is read.

Fifteen bounded source tests verify ownership refusal, exact banner handling,
resource/port constraints, private-module isolation, default plan-only behavior,
partial-cleanup control flow and read-only HTTP adapters. These are source tests;
they do not prove the native rehearsal passed. Parent must review the frozen
script before executing and record actual results and remaining differences.

The first parent-run native attempt reached engine admission but failed proxy
startup before any backup/recreation/ingress swap. A separate bounded parent
nginx syntax check identified missing fastcgi/uwsgi/scgi temp directives: the
unprivileged, read-only fixture otherwise tried creating `/var/cache/nginx`
defaults. The fixture now explicitly puts all five temp directory kinds under
its existing tmpfs/tmp; production helper and root filesystem policy are unchanged.

Failures now retain exact-nonce container status/exit/OOM and private
`docker logs --tail80` evidence before cleanup, capped at64KiB and8seconds per
owned container. Logs and raw configuration are never printed. Proxy startup
records its last HTTP status and sanitized exception class/reason. Unknown
exception text is redacted so credentials/environment cannot leak into metadata.
Diagnostic failures do not suppress resource cleanup or replace the original
failure. Actual native success remains unproven until parent reruns the candidate.

The next parent rehearsal completed the actual stopped-engine backup, then
the unchanged helper refused a candidate host-configuration/volume comparison
before starting it. Candidate diagnostics now retain only its HostConfig,
Mounts and network-key list in an exact-container-ID private JSON, alongside
the existing pre-cleanup logs/state. A separate safe summary compares these
bindings with the old prepared container and reports differing field paths
only. It never publishes raw bindings, changed values, environment or tokens.
This characterizes Docker-generated differences; it does not relax the
production helper's comparison or claim the swap completed.

Parent's follow-up isolated no-backup diagnostic characterized the difference
as only OomKillDisable JSONnull versus booleanfalse. The separately committed
helper correction permits precisely that known default representation, with
true/missing/numeric values and every other changed field rejected. This
harness now pins that corrected helper's exact Git-LF source. Existing private
binding diagnostics remain available; a fresh full native pass is still needed.

The initial headroom gate no longer estimates exported bytes from Docker image
Size, which the observed daemon underreported. Its fixed requirement is
17,448,304,640bytes (8GiB export budget,8GiB reserve,256MiB fixture margin),
followed by the helper's quiesced writable-layer/data/reserve check. This is
restricted to the pinned base and is not a dataset-size eligibility limit.

The fixture baseline now creates a root-private empty `work/empty.env` and uses
that exact `--env-file` on its initial Compose invocation. The corrected helper
verifies and preserves that same binding during rendering/recreation, matching
the observed production metadata requirement instead of ignoring the label.
No actual production environment file or runtime credential is changed.
