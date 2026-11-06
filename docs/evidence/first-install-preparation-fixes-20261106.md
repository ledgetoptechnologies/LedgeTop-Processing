# First-install preparation fixes: test evidence

Tested source: `9ab30ea5bcadee5f07b6f71b4be0a3fd36702331`, on `791a8110`. It covers
audit findings 2 (engine ID grammar), 4 (runtime selection) and 5 (forbidden
inventory). This record is the only change after that commit.

All runs: Linux `python:3.13-slim`, 1 CPU, 768 MiB, `--network=none`, no Docker
socket, no host ports, source read-only, private tmpfs scratch, dependencies from
a read-only local copy. Logs were kept outside Git.

| Run | Profile | Result | Log sha256 |
|---|---|---|---|
| 1 | non-root, 11 modules | NOT GREEN: 153 run, 2 errors, 3 skipped | `b2a8fa677b66a489dec004dd76299596619c58581c42b2ba67bdeb889e81604e` |
| 2 | non-root, 11 modules | 153 run, OK, 3 skipped | `c89f0fb17be6630a6b14ccd1ac964de21b371aa556f29eeaee791465d9325549` |
| 3 | root, installer + 3 serialized | NOT GREEN: 22 run, 3 failures | `7b464d86c21d0a5e3abbb10c5e2f85a4e724d56a08221e1424027094be282741` |
| 4 | root, 3 serialized only | 3 run, OK, no skips | `8bb239f80ada2b2dc34abfcacc59f84968e04da896f60bb44aad478ca2ee77a2` |

## Failures and what changed

- **Run 1:** the runner gave uid 10001 ownership of the source copy, so the
  read-only source guards correctly refused
  (`test_preimport_footprint_matches_existing_readiness_and_serialized_body_executes`,
  `test_normal_factory_tls_readiness_is_bound_and_stable`). Run 2 used a root-owned
  copy that uid 10001 could not write. No guard or production code changed.
- **Run 3:** `test_enroll_reference_worker` passed 19 of 19. The 3 serialized cases
  failed in `setUp` because the slot had no task-owned tmpfs at `/opt`, so they
  tested nothing. Run 4 added a 128 MiB exec tmpfs at `/opt`. No capability was added.

## Profiles

- **Non-root (run 2):** uid, gid and groups 10001; `CapEff` and `CapPrm` 0; source not
  writable. Root staged the copy with CHOWN, SETUID and SETGID only, then dropped.
- **Root (runs 3 and 4):** uid 0, read-only root filesystem, `CapEff` and `CapBnd`
  `0xcb` (CHOWN, DAC_OVERRIDE, FOWNER, SETGID, SETUID). A `/proc` census after each
  run found no remaining processes, and the named container was absent afterwards.

## Result by scope

Together, runs 2, 3 and 4 give 150 non-root passes, 19 root installer passes and
3 root serialized passes. The 3 non-root skips are those same serialized cases.

## Not covered

This branch does not include typed pre-start commit `e9ff5abc`, so the combined
source has not been tested here. Nothing ran on a real host, NAS, real engine or
real Viewer. Coordinator origin aliases (audit finding 3) and
`workerQualificationSha256` are unchanged. Full discover was not run.
