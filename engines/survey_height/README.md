# Survey height integration: source candidate, 2026-11-23

Not deployed, not wired to a coordinator, not in any image. It targets one engine
only: image `sha256:d1b3a6af8806b867a96c75ba2b22e35c45fd937217ccb9860c3fcbe6e75499bd`
(engine 3.8.6). It authorizes no job, host change or processing of real data.

**Current result: the import handoff refuses.** The pinned engine cannot carry a
declared 3D frame, so opt-in survey input is normalized, checked and then stopped.
Nothing works around that.

## What each file does

| File | Purpose |
|---|---|
| `survey_frame.py` | Opt-in input. Keeps the raw bytes hash, header, coordinate digits and IDs, and checks units, axes, epoch and roles before any CRS library runs |
| `vertical_bridge.py` | Existing bridge: EPSG:7600+6360 XYZ ftUS to and from EPSG:6319 metres through the pinned grid |
| `import_handoff.py` | Normalizes once, checks each point against the independent reference, writes the working-frame contract, refuses the handoff |
| `patch_engine.py` | Adds the opt-in hook to a hash-checked copy of the engine reader (AGPL-3.0-or-later; engine source is not stored here) |
| `convert_dem_navd88.py` | Existing converter: Float64 EPSG:7541 ellipsoidal DEM to EPSG:7541+5703 |
| `export_once.py` | Allows that conversion once per source, synthetic contracts only |
| `grid_reference.py` | Independent reference in pure Python. No PROJ, no GDAL |
| `admission_gate.py` | `collect` records one invocation; `evaluate` judges it elsewhere |

`vertical_bridge.py` is the earlier reviewed bridge
(`9a87c2be1012235f1b2881494cb9cfe91b25de29d8513bdbadb3d70409f1b412`) with only the
class renamed. `convert_dem_navd88.py` is the later reviewed converter
(`b3a69ce5f78406d7f62f15db1e6809461577a373ff99797b81686ec426b4cc00`, which closes
the empty-coverage and mask findings against
`7be8564d95ec4c5a7d1010c427ec037f8d2161de4370d469edd4039c4eb4a8ad`) with only its
import line changed.

## Opt-in header

First line of the control file, then `E N H px py image ID`:

```
SURVEY3D/1 CRS=EPSG:7600+6360 UNITS=ftUS,ftUS,ftUS AXES=E,N,H REALIZATION=NAD83(2011) EPOCH=2010.00 GEOID=GEOID18 ROLES=SYNTHETIC
```

The engine's own parser rejects this line, so an engine without the hook stops.
Every other header is untouched, including a bare `EPSG:7600+6360`, which keeps its
existing lossy behaviour. `ROLES=SIX-FIT` accepts exactly 1a, 5a, 6a, 7a, 8a and 9a
with three marks each (18). 2a and 4a are refused until render qualification; 3a and
unsuffixed points are refused always.

## Where the engine loses the frame

| Place | What is lost | Binding |
|---|---|---|
| `modules/location.py:69-76` | A compound CRS goes through `to_proj4()`/`ImportFromProj4()`; the vertical datum is dropped | `d366ef27...` |
| `modules/gcp.py:34` | Only a CRS is kept. Epoch and realization have no field | `337e0df0...` |
| `modules/gcp.py:135,147-153` | Every file is rewritten as `WGS84 UTM`, a 2D CRS | `337e0df0...` |
| `modules/types.py:194,207` | The rewritten file replaces the original for the run | retained copy, installed hash not recorded |
| `modules/gcp.py:203-209` | OpenSfM file is EPSG:4979 under a fixed `"crs": "WGS84"` | `337e0df0...` |
| `opensfm/dataset.py:641-643` | The point reader discards the CRS | `142d860e...` |
| `opensfm/dataset.py:458-474` | The reference is lat/lon/alt with no frame | `142d860e...` |

Feeding the normalized EPSG:6319 values in would keep the number and relabel it as
WGS84. That is refused. `AUTHORIZED_FRAME_TRANSFORMS` is empty; only a reviewed
source change may add to it. OpenSfM's own `io.py` and `geo.py` were not available
to read, so what they do with a non-WGS84 `crs` value is unknown.

## Limits

- A contract digest proves consistency, not who produced it.
- The export adapter trusts the DEM's declared height reference. A real product
  needs separate proof of its frame. A DEM with its metadata removed in another
  process is not detected.
- The once-only ledgers live for one process. They are not durable custody.
- Epoch `2010.00` is a fixed policy value for this candidate, pending review.
- Limits of 0.010 m and 0.001 m are for this invented fixture only.
- Clouds, meshes, tiles, orthophotos and camera geometry are out of scope.

## Local tests

```
SURVEY_GEOID18_GRID    path to us_noaa_g2018u0.tif (16,742,155 bytes, sha256
                       fa9a407ac7ee3f5a3694008e4bcd09ce9cc250452f0c3b11700a4960340abce2)
SURVEY_ENGINE_SOURCE   folder with modules/gcp.py and modules/location.py at the pinned hashes

python -I -B engines/survey_height/tests/test_survey_height.py
python -I -B engines/survey_height/tests/test_engine_reader.py
python -I -B engines/survey_height/tests/test_proj_runtime.py   # also needs pyproj, GDAL, numpy
```

Without an input its tests are skipped with the reason; a skip is not a pass. The
grid is a pinned input and is never downloaded by this code.

## Gate procedure: documented, NOT RUN

Needs separate admission. Uses the immutable main image directly: no coordinator, no
route, no job, no photos, no cluster engine. `collect` refuses any engine whose
version or pinned files differ.

1. Unpack the reviewed commit and put the verified grid in the package folder, where
   the hook looks for it. Use a path with no space or `+`.

   ```
   COMMIT=<reviewed commit>; WORK=<new empty directory>; NONCE=$(openssl rand -hex 32)
   mkdir -p "$WORK/candidate" "$WORK/out"
   git archive "$COMMIT" engines/survey_height | tar -x --strip-components=1 -C "$WORK/candidate"
   cp <verified grid> "$WORK/candidate/survey_height/us_noaa_g2018u0.tif"
   (cd "$WORK/candidate/survey_height" && sha256sum us_noaa_g2018u0.tif *.py) > "$WORK/candidate.sha256"
   ```

2. Create, inspect and run one container by image ID.

   ```
   IMAGE=sha256:d1b3a6af8806b867a96c75ba2b22e35c45fd937217ccb9860c3fcbe6e75499bd
   CID=$(docker create --pull never --network none --read-only --tmpfs /tmp:rw,size=256m \
     --cap-drop ALL --security-opt no-new-privileges --cpus 1 --memory 1g --pids-limit 64 \
     --runtime runc -e NVIDIA_VISIBLE_DEVICES=void -w /candidate \
     -v "$WORK/candidate:/candidate:ro" -v "$WORK/out:/out:rw" \
     --entrypoint /usr/bin/python3 "$IMAGE" \
     -B -m survey_height.admission_gate collect --engine-root /code \
     --grid /candidate/survey_height/us_noaa_g2018u0.tif --nonce "$NONCE" --out /out/receipt.json)
   docker start -a "$CID"
   docker inspect "$CID" > "$WORK/inspect.json"
   ```

   Do not set `PYTHONPATH`, `PROJ_*` or `GDAL_*`: the image's own environment is part
   of what is being observed. No fallback image or host is allowed.

3. Build `launcher.json` from the inspection, not from the receipt: `image` (`.Image`),
   `container_id` (`.Id`), `exit_code` (`.State.ExitCode`), `network_mode`
   (`.HostConfig.NetworkMode`), `read_only` (`.HostConfig.ReadonlyRootfs`),
   `entrypoint` (`.Config.Entrypoint`), `nonce` (from `.Config.Cmd`),
   `receipt_sha256`, and `candidate_files` (name to hash for the `.py` files).

4. On a different machine with the pinned grid, at the same commit:

   ```
   cd engines
   python -B -m survey_height.admission_gate evaluate --receipt receipt.json \
     --launcher launcher.json --grid us_noaa_g2018u0.tif
   ```

   Exit 0 `synthetic_pass`, 3 `blocked_import_handoff`, 1 `fail`. On today's engine the
   expected result is 3: DEM, normalization and negative cases hold and the handoff
   refuses. No result admits processing.

5. Keep the receipt, launcher record, inspection and hashes together, then remove only
   this container. The launcher record is unsigned: the operator and a trusted
   receipt writer still have to be named.

Unverified until that run: that `/usr/bin/python3` in the image sees pyproj and GDAL,
and that GDAL 3.8.4 and pyproj 3.7.1 reproduce the results recorded on a newer PROJ.
