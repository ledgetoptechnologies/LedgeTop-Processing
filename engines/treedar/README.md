# TreeDar

TreeDar is the forestry engine in [LedgeTop Processing](../../README.md).
The previous standalone repository is retained for historical PRs; all new code
belongs in `LedgeTop-Processing/engines/treedar`. Run the commands below from this
engine directory:

```powershell
Set-Location C:\Projects\LedgeTop-Processing\engines\treedar
```

Aligned original-photo crown views and surface-projected overviews are available
through `raw-chips` and `photo-ortho`; see [the photo workflow](docs/photo-tools.md).

Self-hosted research pilot for Wisconsin forest LiDAR plus RGB orthophoto analysis. Initial scope: individually resolvable canopy trees, structural estimates, and species estimation with honest uncertainty.

## Start here

1. Read [CLAUDE.md](CLAUDE.md) for current instructions and boundaries.
2. Read the [original Claude handoff](docs/original-handoff.md), preserved as received.
3. Read the [Hermes review](docs/hermes-review.md) for proposed refinements, evaluation gates, and sources.

## Status

Desk research complete (2026-10-14). See [findings.md](findings.md) for the tool survey, prior work, public datasets, sensor and processing feasibility, a clean-room review of LiDAR360 Forestry, recommended scope, and open questions. Source notes are in [research_notes/](research_notes/). A local structural prototype and synthetic end-to-end tests are implemented. Field accuracy remains unvalidated.

Updates from Beau (2026-10-14):

- The owner of the R3 Pro and M350 (Todd) has given permission to use the unit's processing licenses.
- Todd will provide a few processed R3 Pro test datasets. They will be kept outside this repository. Land ownership, season, and size must be confirmed before analysis.

Next step: evaluate data quality and compare prototype outputs against field-confirmed measurements on authorized test datasets. Do not purchase equipment or assume a sensor purchase.

The GitHub repository holds the shared implementation and research notes. The current TreeDar Codex and Claude chats are linked through the Agent Message Bus for coordination.

No client datasets, hunting-land coordinates, credentials, or raw sensor data belong in Git history.

## Local prototype and tests

Beau authorized collaborative prototype implementation on 2026-10-16. The baseline processes local LAS/LAZ into terrain, canopy height, crown labels, and tree metrics. It does not estimate species or establish field accuracy. Dependency licenses and the tested environment are recorded in [docs/dependency-licenses.md](docs/dependency-licenses.md).

On Windows with Python 3.13, run from the repository root:

```powershell
./scripts/setup.ps1
```

This creates `.venv`, installs exact versions from `requirements-test.lock`, installs TreeDar, and runs pytest. Activation is unnecessary. The lock is tested on Windows x86-64 with CPython 3.13.14; other platforms and Python versions are unverified. The package metadata allows Python 3.11+, with compatible dependency resolution rather than this exact lock.

To rerun the tests:

```powershell
./.venv/Scripts/python.exe -m pytest
```

Tests generate small synthetic LAZ files in temporary directories and invoke the real CLI. They check three separated crowns on sloping ground, classified and unclassified ground handling, height and terrain recovery, matching raster grids and CRS, a bare-ground negative case, QC counts, and invalid inputs. They need no real dataset, model weights, network service, or GPU. Passing synthetic tests demonstrates this controlled pipeline case, not Wisconsin forest accuracy or commercial readiness.

For an explicitly authorized local dataset, keep input data local and write derivatives to the ignored `outputs/` directory:

```powershell
./.venv/Scripts/treedar.exe qc 'C:/local-data/example.laz' --out outputs/qc
./.venv/Scripts/treedar.exe run 'C:/local-data/example.laz' --out outputs/pilot --cell 0.5 --min-height 5
```

`run` produces `qc.json`, `dtm.tif`, `chm.tif`, `chm_raw.tif`, `crowns.tif`, `trees.csv`, `tree_features.csv`, and `summary.json`. `tree_features.csv` contains one row per detected tree with relative height summaries, return fractions, intensity, and upper-crown colour features. Colour is uncalibrated camera RGB; compare within a flight only. All-zero RGB samples are excluded, and unavailable colour features are blank.

Use `--no-qc` on `run` to skip the whole-file QC pass and writing `qc.json`, which makes repeated spatial-subset runs faster. The terrain, canopy, crown, tree, feature, and summary outputs are still produced. Run `qc` separately when a whole-file quality report is needed. Use a new output directory to avoid confusing a previously written `qc.json` with this run.

`qc.json` includes `strip_alignment`: relative offsets between `point_source_id` flight lines over overlapping smooth cells. These offsets are measured against the median of usable line means in each cell, so they do not establish absolute height accuracy or a vertical datum. Missing overlap or suitable smooth cells yields zero compared cells and no offset estimate. Treat point source IDs as flight lines only when the export uses them that way.

`chm.tif` is filtered: pits more than 1 m below their 3x3 median are raised, and cells more than `--spike-threshold` metres (default 5, 0 disables) above their 5x5 median are lowered. This is heuristic noise suppression and can lower a narrow real apex beside a gap; counts are in `summary.json` under `chm_filter`. `chm_raw.tif` is the surface before that filtering, but it still drops points more than 60 m above ground and fills empty cells from their highest neighbour, so it is not untouched observations.

Use a small approved spatial subset with `--bbox minx,miny,maxx,maxy` before a full cloud, and treat `--step N` as decimation that changes density and may miss treetops. Confirm source CRS and metre units before interpreting heights or crown area. Coordinates, rasters, logs, and outputs from real data must stay out of Git and external services.
