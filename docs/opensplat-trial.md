# OpenSplat offline foundation

This first increment inspects an existing Gaussian PLY and generates a command
plan for a prepared COLMAP project. It runs without Docker, GPU packages or new
dependencies. It does not execute training, register an engine, enable Gaussian
capabilities, publish assets or mark a Viewer task ready.

## Inspect an existing result

From the repository root:

```powershell
python -m processing.opensplat_trial inspect C:\scratch\trial-output\scene.ply
python -m unittest tests.test_opensplat_trial -v
```

Inspection is read-only and streams at most 512 MiB / 2 million Gaussians in
1024-record batches. Headers are capped at 16 KiB. The accepted profile is
`opensplat-gaussian-ply-float32-v1`: binary little-endian PLY 1.0, one vertex
element, XYZ/normals, three DC coefficients, contiguous rest coefficients for
SH degree 0–3, opacity, three log scales and four quaternion parameters, in the
order written by OpenSplat. Plain XYZ/RGB clouds are rejected. Exact payload
size, finite floats and nonzero rotation are checked. A SHA-256 covers all bytes;
the report includes size, Gaussian count, SH degree and coordinate bounds.

Success returns exit 0 and a versioned `opensplat-offline-inspection` report.
Failure returns exit 2 and a stable validation code. No input is modified and no
output file is created; stdout may be redirected to a private report location.
The inspector refuses linked paths and detects ordinary concurrent file changes.
Use a quiescent local copy: this utility does not provide filesystem fencing
against malicious writers or establish deletion/cleanup authority.

Both `viewerReady` and `publicationEligible` remain false. Tool build, training
run and input provenance remain unverified, and CRS, units and transform remain
unknown. Structural validity does not establish visual quality, coordinate
alignment, memory-safe browser rendering or permission to serve the result.
The report is an inspection record, not an accepted-result manifest or receipt.

## Plan another isolated trial

```powershell
python -m processing.opensplat_trial plan C:\scratch\prepared-colmap C:\scratch\new-output --iterations 2000 --downscale 2
```

The plan is an argv array, never a shell command or subprocess. It requires an
images directory and the binary camera/image/sparse-point files at the project
root or `sparse/0`, matching upstream v1.2.2 dispatch. Root camera files take
precedence. A competing `transforms.json` is rejected because upstream would
choose nerfstudio. A direct `sparse/*.bin` export alone is not supported by this
pinned loader; prepare a compatible copy first. These are presence checks, not a
COLMAP parser, image-integrity check or proof of matching poses and photos.

Output must be fresh and outside the input tree. No directory is created. The
plan preserves the input frame by omitting `--center`; it does not assign metre
units or a geographic CRS. Iterations are bounded to 1–30000 and downscale to
1–8. Before actual execution, allocate an isolated scratch copy and working
directory, verify the exact engine/CLI, create the output directory and obtain
exclusive physical-node admission. OpenSplat may write preprocessing caches
beside its input; separating the scene output alone does not preserve originals.

## Source and integration boundaries

The initial format/CLI review uses OpenSplat v1.2.2 source commit
`62fd86fe3b62644b65890557ad9bf397abdc7cfb`, specifically
[model.cpp](https://github.com/WebODM/OpenSplat/blob/62fd86fe3b62644b65890557ad9bf397abdc7cfb/model.cpp),
[colmap.cpp](https://github.com/WebODM/OpenSplat/blob/62fd86fe3b62644b65890557ad9bf397abdc7cfb/colmap.cpp),
[input_data.cpp](https://github.com/WebODM/OpenSplat/blob/62fd86fe3b62644b65890557ad9bf397abdc7cfb/input_data.cpp)
and [opensplat.cpp](https://github.com/WebODM/OpenSplat/blob/62fd86fe3b62644b65890557ad9bf397abdc7cfb/opensplat.cpp).
This review pin does not authenticate a local binary or image. Later upstream
main was reviewed separately; do not substitute its loader behavior silently.

The Model Viewer owner confirmed that Gaussian asset discovery, import and a
renderer are not yet qualified. Existing GLB/tiles/EPT paths do not establish
Gaussian support. Raw PLY data stays private. A future producer/Viewer contract
needs explicit format/kind, byte/hash provenance, verified frame/transform,
bounded render/load behavior, browser qualification and scoped share/download
policy before publication.

Next increments, coordinated with Model Viewer and Merger:

1. Capture immutable engine build/options, original input hashes and source
   attempt identity in a durable trial record; bind output hashes to that run.
2. Qualify a local private Viewer renderer with the existing output, including
   visual QA, camera framing, memory limits and coordinate/transform handling.
3. Implement the engine runner with physical-node exclusion shared by ODM,
   TreeDar and OpenSplat; production priority, bounded resources, cancellation,
   crash/restart reconciliation and retained-data fencing.
4. Add the reviewed submission/result and acceptance contracts, then qualify
   an end-to-end run before capability advertisement or deployment.

The existing photo pipeline and the staging release remain with their current
owners. Canonical Processing notes reviewed on November 11, 2026:
`reviews/photo-queue-current-20261111.md` v37 / SHA-256
`99430456f64ba1450cbd3d83cbd638cb8f35ef66c0d9318c8874f76943f1b09b`
and `reviews/operational-followup-current-20261111.md` v7 / SHA-256
`b934d4f6bd79361799a0b341be6864c354e9ca710acacc9e7027769e5ba3709a`.
Those notes record live ownership, not authorization to replay transitions.
This new offline work does not install an Engineering Hub listener or establish
native wake delivery; coordination here uses the desktop's existing chat tools.
