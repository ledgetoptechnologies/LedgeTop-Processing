# Parallel scheduler Viewer result retry fixture

This isolated synthetic fixture pairs Processing `6912439f8c859bf60a9d3281014f27de0e5a78a7` with Viewer `ad0bf46611de44545825db9147e90b68195902ab`. It uses two fake engines and one job, simulated Viewer storage headroom, and a placeholder ZIP. Per-node slots and parallel Viewer dispatch are enabled, with distinct non-routable fake node addresses.

Real Viewer submission, coordinator HTTP and task storage, and Viewer SQLite ingestion are exercised. After simulated engine completion, a fake engine archive stream is interrupted after seven bytes. The fixture verifies the coordinator retains its partial and intent, retries, then verifies that Viewer ingests a receipt whose archive digest matches the retained coordinator archive.

The fixture creates a uniquely named, labeled network-isolated Docker container, mounts source and npm cache read-only, and removes the container after verifying ownership. It does not prove real-node processing, photogrammetry, actual storage capacity, power-loss behavior, node restart, simultaneous jobs, or deployment readiness. Execute only with trusted exact-source trees and an existing offline npm cache:

```
python tests/run_legacy_viewer_pair.py --processing <absolute fixture worktree> --viewer <absolute pinned Viewer worktree> --npm-cache <absolute npm cache>
```

A local run of this exact fixture passed on 2026-11-04: the seven-byte interruption produced no Viewer archive receipt, retry ingested six verified archive entries, and the attempt reached derivatives. This does not establish completion of derivative processing.
