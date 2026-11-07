# Cluster activation input preparation — 2026-11-07

These observations prepare a later controlled cutover; they do not authorize
activation, establish ongoing idleness or close ingress. The cluster coordinator
and ingress remained running, and no worker or processing task was started.

The private file `photo-pilot-worker-prep-20261106/native-inventory-cluster-empty-observed-20261106.json`
under `/home/bkoltz/.local/share/ledgetop-processing-reference/` is an actual
typed native quarantine inventory, SHA-256
`522c6fa4f51bebe8e1647780dfd508f86d8a46537dfb53ae2875cb1d9fe3fd25`,
mode 0600. After verifying the prepared candidate manifest and config hashes,
the existing `retained_native_census` adapter observed the expected native engine
version, zero queued work, and two stable empty task lists. This is a
point-in-time observation; the actual activation must revalidate the inventory
after ingress is closed.

The actual activation lock guard initially rejected the existing cluster ingress
lock because it was mode 0664. The parent directory was mode 0700 and owned by
the service account; a root `/proc` scan found no open descriptors or kernel
locks at that instant. Under the existing exclusive nonblocking flock, the exact
regular single-link lock was verified and changed in place to mode 0600, with
its inode, owner and contents preserved and the file and parent fsynced. The
existing ingress lock protocol still acquired the lock and blocked a competing
exclusive acquisition. The ingress proxy remained running and its authenticated
health endpoint returned HTTP 200. No ingress state was changed.

The private, user-owned preparation helper
`photo-pilot-worker-prep-20261106/capture-activation-policy-20261106.py` has
SHA-256 `6481cf836144104bdbad3afc223c17d0731d8a0e91ac4f6fd91dfac7566d8d8f`.
It checks the candidate manifest/config and native inventory pins, acquires the
existing ingress lock, requires the reviewed closed configuration, checks actual
ingress closure and the native census, and only then exclusively creates a
private policy file. Its current open-ingress invocation correctly refused and
created no policy. It is an unqualified preparation helper, not an activation
receipt or cutover authority; its source and dependencies require review before
use. The five directly imported 4705 modules were compared against the exact
Git blob hashes. No helper process remained after the check.

The prepared worker manifest pins the exact source archive and a footprint of
every Python file under `processing/` and `ops/`. If changed deployment tooling
is incorporated into that archive, regenerate and review the archive, footprint,
config readiness hash and affected manifests. Coordinator deployment separately
pins source commit, image, recipes, policy sources and worker settings. A tool
change executed exclusively from an external controller tree might preserve the
installed runtime pins, but the controller and serialized helper bindings still
need independent verification. Do not combine new code and old evidence without
checking these boundaries.
