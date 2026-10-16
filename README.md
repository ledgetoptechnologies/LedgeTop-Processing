# TreeDar

Self-hosted research pilot for Wisconsin forest LiDAR plus RGB orthophoto analysis. Initial scope: individually resolvable canopy trees, structural estimates, and species estimation with honest uncertainty.

## Start here

1. Read [CLAUDE.md](CLAUDE.md) for current instructions and boundaries.
2. Read the [original Claude handoff](docs/original-handoff.md), preserved as received.
3. Read the [Hermes review](docs/hermes-review.md) for proposed refinements, evaluation gates, and sources.

## Status

Desk research complete (2026-10-14). See [findings.md](findings.md) for the tool survey, prior work, public datasets, sensor and processing feasibility, a clean-room review of LiDAR360 Forestry, recommended scope, and open questions. Source notes are in [research_notes/](research_notes/). No prototype or accuracy validation has been done yet.

Updates from Beau (2026-10-14):

- The owner of the R3 Pro and M350 (Todd) has given permission to use the unit's processing licenses.
- Todd will provide a few processed R3 Pro test datasets. They will be kept outside this repository. Land ownership, season, and size must be confirmed before analysis.

Next step: data quality check on Todd's test datasets, then confirm direction with Beau before building the prototype. Do not purchase equipment or assume a sensor purchase.

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

`run` produces `qc.json`, `dtm.tif`, `chm.tif`, `crowns.tif`, `trees.csv`, and `summary.json`. Use a small approved spatial subset with `--bbox minx,miny,maxx,maxy` before a full cloud, and treat `--step N` as decimation that changes density and may miss treetops. Confirm source CRS and metre units before interpreting heights or crown area. Coordinates, rasters, logs, and outputs from real data must stay out of Git and external services.
