# Read-only worker installation inventory — November 5, 2026

Existing authenticated `bkoltz` SSH reached both hosts. Foreground helpers exited
normally. Checks read only bounded unit/file metadata, local source archives and
footprints, package metadata, Docker identity/port metadata and authenticated
loopback `/info`/`/options`. No task submission/list, service change, lock
acquisition, permission change, NAS access or client-data operation occurred.
No credentials, private configuration contents or raw option/help data were
printed. This is a dated observation, not an activation receipt.

| Observation | Cluster `.89` | Main `.88` |
| --- | --- | --- |
| Reference user unit | `not-found`, inactive/dead, PID 0 | Same |
| Exact user unit file | Absent | Absent |
| Standard `~/.config/ledgetop-processing/reference-worker.json` | Absent | Absent |
| Reference `current` link | Absent | Absent |
| Worker HTTPS port 18445 | No listener observed | No listener observed |
| User linger | `yes` | `yes` |
| Noninteractive `sudo -n -l` | Succeeded, `NOPASSWD: ALL` observed | Exit 1; no unrestricted capability proved |
| Native loopback port | `127.0.0.1:13001` | `127.0.0.1:13004` |
| Native API / engine version | NodeODM `3.6.2` / `3.6.2` | NodeODX `2.4.0` / `3.8.5` |
| Native maxImages / queueCount | `1000` / `0` | `null` / `0` |
| LAN ingress port / configured client count | `3001` / `2` | `3004` / `1` |
| Native option definition count | `82` | `80` |

Both engines report `pc-ept`, `gltf` and `3d-tiles` with type `bool`.
No cluster engine upgrade is needed merely to expose these output flags.
Sorted compact default-ASCII JSON option hashes:

- Cluster: `32887a4ed714b5f3073b007cffcd561cd95fbf202a1be31f54e71f197bf6d165`.
- Main: `06799c98377b84c2d1916e4c3a100fcf2b356072fc792df465d812f11d18c064`.

Native immutable images observed:

- Cluster: `sha256:5ce31be03711d280e7f97ee3820dd982b68227181165b3f430b037447c3d13b6`.
- Main: `sha256:b9d9a1a4603db9f092b790a6e58586fcf944758b7ca1149daefb2bd55dc6bdde`.

The existing cluster coordinator remains
`sha256:7fae9385fb5ca829783fe05eef2fdb1983abf4d491b2b9f795e55b617a5a0004`,
with `.89:3005`, `.89:18443` and loopback `18080`. No candidate was deployed.
Both canonical gate JSON snapshots had generation 1, matching node roles and no
reservation. Their UID/GID were 1000 and mode 0600; original lock files existed
with the same ownership/mode. Gate JSON SHA-256 values:

- Cluster: `5966c9cfbe78abd5f544a93fde4d86a62d69549e244297211a8bf35df05d5a05`.
- Main: `34c3744d14137f388793d25836c7737e713fdf485e01fc119d82106284dbf1f4`.

These snapshots and `taskQueueCount=0` do **not** prove complete idle state,
kernel-flock ownership, absence of direct writers, closed historical pilots or
safe source custody. Activation must obtain those independent current proofs.
`sudo -n -l` exit 1 does not prove that every specific privileged command is
denied; the exact required noninteractive installer invocation must be checked.

## Retained inactive preparations

Both hosts still retain
`~/.local/share/ledgetop-processing-reference/dependency-candidates/e11231a`:

- Source commit: `e11231a510642b516abda7c3a15049c8488e440e`.
- Archive: 5,120,000 bytes,
  `eacda078ac98c276dcadcfc67fcfed78eeeb5678399e0af50b20cf69d8a82101`.
- Local ext filesystem, UID/GID 1000, private source directory mode 0700.
- Python source footprint: 107 files, 2,031,818 bytes,
  `07980c6c8f630266cc114b513ea84bda6bfb26bd7cb19e9495b9d98406eeb219`.
- All 107 files and the source root remain writable by service UID 1000. They
  fail current immutable-source readiness and lack the new catalog relay.
- Preparation receipts still say `workerStarted:false`, `activationChanged:false`.
  Receipt hashes: cluster
  `f153b1620a40d262b7a7c49e256c0e26ec9fcb6387a66f038935437d4fdc917c`;
  main `e4cd8a049b24aaf9eb9603e381c74b35387bf8c476002a03288c683ee85cdf07`.

The retained `39e0825` dependency environments report cryptography 50.0.2,
cffi 2.1.1 and rfc8785 0.1.4. Cluster uses Python 3.14.4/pycparser 3.12;
main uses Python 3.10.12/pycparser 2.23. These observations are inputs to a new,
reviewed per-host dependency artifact; they do not authorize running mutable old
environments as the new installed worker.

The next stage is a separately reviewed first enrollment with new immutable
source/dependency inputs, private configuration and exact unit pins. Existing
preparations, source permissions, units, credentials and native engines remain
untouched by this inventory.
