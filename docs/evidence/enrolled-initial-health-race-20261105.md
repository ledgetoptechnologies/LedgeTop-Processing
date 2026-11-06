# Initial health timing hold and bounded observer

The actual local Docker forward qualification for operation
`5d8f9f96-c53f-4fb0-875b-ad11bbb5737c` **held** after eight completed phases,
through `bootstrap-created`, with `start-bootstrap` pending. This is not a
passed forward, rollback, installed-host, NAS, native-job or release proof.

The frozen fixture was `27d2e9c24443c96f9c47e22ffe382825682e22d9`, archive
SHA256 `de694f95be36db55bcaf87cec632ae0763373882638218133aa9d3550e8e5d4c`.
The packaged candidate was image
`sha256:378559e42456017b965d1e2e38b56eb627274734aeb317a758d84c0bc99bb5ab`,
source `18750d4d599026c18e569a9c67135e6c9fdc7d1a`; the genuine predecessor
image was `sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004`.
The controller had one CPU/768 MiB, no host ports and a 900-second outer bound.
Host enrollment and SSH/systemd were explicit local fixture seams.

The controller started at `2026-11-06T00:43:30.931174159Z` and exited 1 at
`00:45:11.498975022Z` (100.568 seconds, not the outer deadline). The single
Docker bootstrap start produced container `1b86e51cc7e1495feee77bbde0fa73d7ab768245c5926fcc1dec341965227379`,
StartedAt `00:45:06.014925933Z`. The immediate tracked health observer failed
with `remote_script_failed_after_retirement`. Capture then separately ran a
fresh, explicitly diagnostic read-only health request against that same app;
it returned HTTP 200, body SHA256
`e0585f428c17ed40e2315723775f690409aa5422409e95f759476c0b52489dde`.
The app remained running. No admission, Gate intent/drain/seal, final creation,
second start, journal reconciliation or rollback occurred. The later HTTP 200
does not accept the failed phase or alter its pending intent.

## Fix and verification

Commits `730f7bb` and `1b798df` add a startup-only, read-only observer. It retries
only an initial socket connect refusing with ECONNREFUSED, before any request
is sent. One monotonic 20-second deadline, also enforced by a Linux interval
timer across HTTP/header/body/parsing, fits inside the unchanged 30-second
tracked observer budget. The timer is cancelled before tracked retirement.
HTTP errors (before body read), invalid JSON/health, timeout, reset or lost
reply are not retried. PID1 start identity is checked throughout, then the driver
re-inspects exact container ID/image/running/PID/StartedAt. Gate, custody and
projection checks remain required. No Docker start, dispatch window, authority
or journal retry was added.

`e3825f4` applies the same initial observation to `start-exact-predecessor`.
Later predecessor readiness/restart-policy checks and other ordinary health
checks remain one-shot. Both pinned source revisions expose health
`status=ok, mode=routing_probe`; additional health fields remain allowed.

Final frozen `e3825f4` archive SHA256
`b2bab1be7fdf4ae9474f79f6a1eef938c34a72c529931526e5e3b01a1c3ab1bd`
ran `python -m unittest tests.test_enrolled_startup_health tests.test_enrolled_coordinator_driver -q`:
**19/19 passed, zero skips, 0.842 seconds**, one CPU/512 MiB/network none,
UID10001, immutable image and read-only frozen source. This includes an actual
delayed Linux TCP listener receiving exactly one authenticated GET, deadline,
auth/status/body/connection/reset/incarnation failures, driver restart/stopping
refusal, existing lost-start inspection and unchanged predecessor later checks.
The full 22-phase driver tests use synthetic CLI boundaries; this fix has not
yet passed another actual full forward or actual declared rollback.

An initial local archive-command syntax error caused an empty-source runner
to fail imports before any app effect; it was removed. Corrected earlier
17- and 18-case runs passed. Final runner
`85a0a81cd996f68c21c0a74107b2c45da682ba4fff624b6ae4b0319f478f4fa0`
exited 0/PID0/no OOM, was removed and its exact task inventory was empty.

## Retained evidence and retirement

Before cleanup, the complete private operation/config/plan/bundle/journal,
worker material, logs and original baseline/TAR were copied cold. The TAR was
independently streamed through the unchanged validator with its retained
private census key: 202 files, 13 directories, 467,968 archive bytes, SHA256
`91352b1eb3a4428f0ae8c97f82daa20dfaa0099200e1c55e3c74c0352ae70315`.
This backup is evidence only, never a restore or authority source.

With explicit root authorization and fresh ownership/image/PID checks, only
the exact synthetic bootstrap received SIGTERM. It exited 0/PID0/no OOM at
`00:54:16.571949096Z`; its resulting state was also copied cold (202 files,
364,829 bytes). Private host evidence totals 330 files/1,621,212 bytes; final
private file-manifest SHA256
`8195e9e54d5fa1422508a82af55891ac7b0d03e44fed95457d3a5c2495d65d93`.

All five exact owned containers (outer, bootstrap, holder, original and seed)
and five owned volumes were retired without force after terminal image,
owner, PID0, no-OOM/restart checks. Each volume removal additionally required
fresh closed full-container inventory proving no named-volume or POSIX
bind/parent/child alias references. Both task-label container and volume
inventories were empty afterward. Private host evidence and all failed-phase
facts remain; no original/user/shared resources were changed.
