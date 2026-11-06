# Read-only worker dependency refresh — November 6

Authenticated SSH as the existing UID1000 checked the two previously prepared
dependency artifacts sequentially at 03:41:53–03:41:54 UTC. No package or worker
was installed or executed; no remote files, services, permissions, leases, Gates
or NAS settings were changed. Each foreground Python probe had a 45-second alarm,
one-CPU affinity, isolated mode and bytecode disabled. Both SSH processes exited0.

| Check | Main .88 | Cluster .89 |
| --- | --- | --- |
| Original inventory SHA-256 | `82e8bc0e29a2d3251b088e4e6d0a7f4cbf7f2dcc1538f7a87743d62086cc7c65` | `673bcf3bf420cc091444e101ae2ab87b34ac43edce45de3f2c290d01847e03a0` |
| Dependency archive SHA-256 | `be2b9ffba448d629ea82d231468eb97a308ff42cebc95936bac6a6a338d815ff` | `75e27f7afb3aae42c8a827f0522d14b07a3fdb4b2291be832f7db389607aadb1` |
| Archive bytes | 34,437,120 | 46,110,720 |
| Exact regular archive files | 198 | 190 |
| Checked OS library inventory entries | 695 | 673 |
| Existing interpreter SHA-256 | `a2f33a6e006989270f4340528eb61f8f97366e00a5d1b602ac8672ea44fc56ae` | `52e0a13e60a981d8c4b6478be2ba5176f69da07948a056bf49cf6f077e30cb41` |

The private inventory/archive files were opened nofollow, checked single-link,
UID1000/mode0600, hashed, and descriptor/name identity checked before and after
reading. All archive members were regular files or directories in the recorded
namespace; each file's byte count and hash matched the pinned inventory. The
probe did not extract the archives or run their dependencies.

The OS library check separately read the recorded root-owned, non-writable files
and followed only the three already recorded main-host aliases to their exact
recorded resolved targets. Alias metadata and target hashes were checked
separately. The actual interpreter bytes also matched the existing pin.

## Diagnostic history and limits

Initial main-host probes failed because the temporary diagnostic treated alias
metadata byte counts as target sizes, then required every OS library file to be
single-link. The original inventory records alias metadata but hashes target
bytes, and main has 112 recorded OS library entries whose actual files have
multiple hard links. The final diagnostic checks those root-owned, non-writable
OS files using stable descriptor/name identity and exact original byte hashes;
it does not require their OS-managed link count to equal one. The cluster has
zero such entries. No production installer policy was modified. Private inputs
and dependency TAR members retain their original single-link/no-link policies.

This is preparation freshness evidence, not a complete OS shared-library audit,
an installed worker import/startup check, or future update safety. Fresh physical
and readiness checks remain required at installation and activation.

Local retained evidence under `C:/Users/fstor/AppData/Local/Temp/`:

- Probe `ltds-refresh-worker-artifacts-20261106.py`, SHA-256
  `0bf691f7c40cddc6e0669b45ece89716e2cf1d4468caafa9d50574481495d639`.
- Main receipt `ltds-refresh-worker-main-20261106.log`, SHA-256
  `b17cd0406eaf821c326b5d7f7f20b0b89865ef76a1da433d3871147e3864d602`.
- Cluster receipt `ltds-refresh-worker-cluster-20261106.log`, SHA-256
  `0955013a4aef74c173cb52bb14ae06bc7de512e573fac3d27a83b22357c02f2f`.

Original directories, package inputs and scope are recorded in
[the dependency preparation receipt](first-worker-dependency-artifacts-20261105.md).
