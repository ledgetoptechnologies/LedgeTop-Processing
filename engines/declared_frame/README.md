# Declared 3D frame: design note and source candidate, 2026-11-23

Not deployed, not wired to a coordinator, not built into any image. The pinned engine
`sha256:d1b3a6af8806b867a96c75ba2b22e35c45fd937217ccb9860c3fcbe6e75499bd` (3.8.6)
stays refused. This is the source for a successor: a new immutable image that carries a
declared NAD83(2011) 3D frame and its reviewed epoch from control input to the DEM.

**Current result: `successor_incomplete`.** Some engine source that handles coordinates
was not available to port. While any remains, a declared run refuses before doing
anything. The list is `UNPORTED` in `engine_hooks.py`.

## Rules

- No relabelling, no constant height correction, no nonlinear shift in the camera fit.
- Heights change once on the way in (H to h) and once on the way out (h to H).
- Every project value comes from a private profile named by SHA-256 in a signed
  manifest. The committed profile is invented. Labels, CRS codes, roles and the epoch
  review digest of a real project are never committed.
- Only the profile's fit points enter the engine. Withheld points are refused until a
  render exists; excluded points always. Case and a `CHK-` prefix do not change a role.
- Headers without the marker take the same path as before.

## How the frame travels

| Step | Engine file | What changes |
|---|---|---|
| Read | `modules/gcp.py` | First line `SURVEY3D/2 PROFILE=... CRS=... EPOCH=...`. Raw digits are kept, then normalized once and sealed. The unpatched parser rejects this line |
| Hold | `modules/types.py` | Keeps the sealed contract; no UTM copy is written |
| Guard | `modules/location.py` | A declared frame cannot become a PROJ string or an EPSG code |
| Import | `opensfm/dataset.py` | Builds solver points from the contract, reads them back, checks everything. The reference file carries the contract digest |
| Transform | `opensfm/actions/export_geocoords.py` | Map projection within one datum, height carried. The engine's matrix is sealed |
| Cloud | `stages/odm_georeferencing.py` | Undoes that matrix and maps each point exactly |
| DEM | not ported | `dem_frame.bind_dem` is ready; the stage source is needed |
| Export | `dem_frame.py` | One conversion of one engine-derived DEM, synthetic only |

`gcp.py`, `location.py` and `dataset.py` are pinned to hashes read from the image. The
other three are pinned to saved copies; if the image differs, patching refuses.

**Why the cloud is mapped per point.** The engine turns its local east/north/up frame
into map coordinates with one matrix. Up is not height: 1.4 km from the origin the two
differ by about 0.148 m, far over a 0.010 m limit, whatever the datum. A linear matrix
can be undone exactly, so the cloud is mapped point by point afterwards. Cameras, the
fit, meshes and orthophotos are untouched, and the last two are not offered.

**Camera GPS** is in another frame and nothing here transforms it. The solver must run
with `bundle_use_gps: false`; a missing or other value refuses. The local origin is
only a choice of numbers and makes no frame claim. Whether alignment uses GPS any
other way needs `reconstruction.py` and `align.py`.

**Sink check.** Exact ID set, no duplicates, point count, longitude, latitude, axis
order, height, height flag, and each observation's image and position, read back from
the objects the solver gets.

## Provenance

Three people with separate keys, checked with `ssh-keygen -Y verify`:

| Role | Signs |
|---|---|
| Sealer | The manifest: image, every engine and candidate file, profile, grid, proj.db, runtime versions, command and container limits |
| Operator | One run: manifest digest, nonce, writer, ledger, purpose `synthetic` |
| Writer | The exact receipt bytes: nonce, manifest, authorization, ledger and everything observed |

The reviewer supplies the SHA-256 of the allowed-signers file. The launcher's record is
compared and never supplies an expected value. The once-only ledger is a durable folder
of exclusive claim files; a claim is made before acting and a crash leaves it in place.

A manifest can only list files the sealer actually hashed, so the sealer must hash the
full successor image. Signatures show who signed, not that the engine ran honestly.

## Local tests

```
DECLARED_FRAME_GRID    us_noaa_g2018u0.tif, 16,742,155 bytes, sha256
                       fa9a407ac7ee3f5a3694008e4bcd09ce9cc250452f0c3b11700a4960340abce2
DECLARED_FRAME_ENGINE  folder with the six files in engine_patches.py at their hashes

python -I -B engines/declared_frame/tests/test_core.py
python -I -B engines/declared_frame/tests/test_engine.py
python -I -B engines/declared_frame/tests/test_provenance.py
```

Without an input its tests skip with the reason; a skip is not a pass. `test_engine.py`
runs the real engine source with its libraries stubbed, so it shows what is carried and
refused, not PROJ, PDAL or solver numbers. The solver point API (`pymap`) is called from
general knowledge of OpenSfM and is unverified against this build; a difference stops
the run.

## PROJ/GDAL tests: recipe, NOT BUILT

`test_proj_runtime.py` has four tests. No local image has pyproj and GDAL. The closest
match to the engine is Ubuntu 24.04, whose packages are the engine's GDAL 3.8.4 and
system PROJ 9.4.0. One approved network step fills the pins; the run has no network.

```
FROM ubuntu:24.04@sha256:<pin>
RUN apt-get update && apt-get install -y --no-install-recommends \
      python3-gdal=3.8.4+dfsg-3ubuntu3 python3-numpy=<pin>
COPY pyproj-3.7.1-cp312-cp312-manylinux_<pin>_x86_64.whl /w/   # sha256 <pin>
COPY certifi-<pin>-py3-none-any.whl /w/                        # sha256 <pin>
RUN python3 -m venv --system-site-packages /v \
 && /v/bin/pip install --no-index --no-deps /w/*.whl
```

Needs `python3-venv` at a pinned version, or unpacking the wheels into a folder. Then:

```
docker run --rm --pull never --network none --read-only --tmpfs /tmp --cap-drop ALL \
  --security-opt no-new-privileges --cpus 1 --memory 1g --pids-limit 64 \
  -e DECLARED_FRAME_GRID=/grid/us_noaa_g2018u0.tif -e DECLARED_FRAME_CONTAINER=1 \
  -v <grid folder>:/grid:ro -v <engines folder>:/src:ro --entrypoint /v/bin/python \
  <image digest> -I -B /src/declared_frame/tests/test_proj_runtime.py
```

A pass there tests this code against those libraries. It does not qualify the engine.

## To finish the successor

1. The pinned source of each file in `UNPORTED`, from the image itself, with hashes.
2. Confirm the three saved copies against the image.
3. Port those files, including the DEM stage, and empty `UNPORTED` in review.
4. Add the in-engine collector for `gate.py`, build the image, seal the manifest.
5. Run the admitted synthetic gate. No result here admits processing.

Engine source is AGPL-3.0-or-later and is not stored here; the license of the pinned
OpenSfM fork still needs checking before any image is distributed.
`convert_dem.py` is the earlier reviewed converter with the horizontal CRS made a
parameter (from `b3a69ce5f78406d7f62f15db1e6809461577a373ff99797b81686ec426b4cc00`).
