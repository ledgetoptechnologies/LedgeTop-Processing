# NodeODX and TreeDar handoff requirements

The successful 16-photo trial proves upload, alignment, result validation and
cleanup through the coordinator. It does not provide a valid input for TreeDar
forest heights. The sparse reconstruction has no verified ground coverage or
projected metre point-cloud contract. Its artifacts were intentionally deleted;
metadata cannot reconstruct or validate the missing ground observations.

## Two distinct workflows

TreeDar's `pipeline` consumes LAS/LAZ and derives its own ground model, canopy
height model, crowns and features. It currently reads CRS without enforcing a
projected metre CRS. Ground class 2 is trusted when present; otherwise the
lowest-point morphological fallback estimates terrain. That fallback cannot
establish ground under closed canopy where photographs only observe treetops.
QC reports metadata and statistics but is not an admission gate. The CLI has no
external-DTM argument today. See the implementation in
`engines/treedar/src/treedar/{cli,terrain,qc}.py`.

A separate legitimate use of alignment is to connect photographs to trees from
an independently valid LiDAR run, through existing `raw-chips`/`photo-ortho`
commands. This requires supported camera poses/intrinsics, original photos,
matching metric DTM/CHM/crown grids, and surface Z in ellipsoidal metres matching
the OpenSfM reference altitude. No vertical-datum conversion occurs. See
[the engine's photo workflow](../engines/treedar/docs/photo-tools.md).

## Proposed admission boundary (not implemented)

Before a downstream forestry job becomes runnable, a verified artifact descriptor
must identify:

| Field | Evidence required |
| --- | --- |
| Source and stage | Genuine LiDAR or photogrammetry; alignment sparse versus dense product |
| Artifact identity | Immutable producer attempt, engine/version, SHA256, bytes and actual point count |
| Horizontal reference | Projected CRS and metre units, checked against the file itself |
| Vertical reference | Z units and datum; explicit shared reference with ground or surface products |
| Extent and resolution | Bounds, density/coverage, raster allocation budget and overlap |
| Ground provenance | Genuine classified ground or independently validated terrain model and coverage |
| Feature semantics | Which measured attributes exist; unavailable LiDAR attributes remain unavailable |

An `alignment_sparse` input must be rejected for forest-height processing with
`dense_metric_cloud_and_verified_ground_required`. Missing/unknown reference or
ground provenance must produce a concrete waiting/rejection reason before engine
execution. A descriptor alone is an assertion; the future adapter must inspect
the actual artifact and enforce the declared bounds.

Photogrammetry points must not acquire fabricated LiDAR return, intensity or
flight-line measurements by conversion. Existing LiDAR feature/classifier
assumptions require a separate photogrammetry assessment. A dense cloud alone
does not establish terrain under canopy, and file conversion does not repair it.

## Next implementation increments

1. Add a typed artifact/admission contract with synthetic descriptor and file
   fixtures, including negative cases for sparse alignment, unknown CRS/units,
   unknown ground and mismatched checksum/extent.
2. Implement durable parent/dependent job links and output acceptance receipts.
   A failed or uncertain parent must never admit a downstream run. Cleanup must
   wait for downstream consumption or an explicit retirement decision.
3. Add a bounded LiDAR adapter first, retaining verified ground and feature
   provenance. Develop external-DTM support separately if the photogrammetry
   workflow requires it; do not silently route around the current CLI limit.
4. Connect valid independent LiDAR products with an OpenSfM alignment for the
   existing photo tools, after coordinate/vertical-reference checks.
5. Validate real-data accuracy and production resource admission separately.
   Synthetic execution and one alignment trial establish neither.

No automatic NodeODX-to-TreeDar chain or new real-data run is enabled by this
document. The bounded synthetic dispatcher is the current execution increment.
