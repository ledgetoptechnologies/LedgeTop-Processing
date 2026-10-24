"""Field pack export on the synthetic forest, species label maps, and spread photo views."""

import csv
import hashlib
import json
import subprocess
import sys

import numpy as np
import pytest
import rasterio
from PIL import Image
from pyproj import Transformer
from rasterio.transform import from_origin

from synthetic_forest import TREES, write_forest
from treedar import classify
from treedar.fieldpack import POINT_DTYPE, build
from treedar.raw_chips import _spread


def cli(*args):
    return subprocess.run([sys.executable, "-m", "treedar", *map(str, args)],
                          capture_output=True, text=True, timeout=180)


def ok(result):
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def rows(path):
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


@pytest.fixture(scope="module")
def run(tmp_path_factory):
    """A real run of the synthetic forest with lasting IDs, plus fake photo inputs."""
    root = tmp_path_factory.mktemp("pack")
    forest = root / "forest.laz"
    write_forest(forest)
    run_dir = root / "run"
    ok(cli("run", forest, "--out", run_dir, "--no-qc", "--ids-registry", root / "ids.json"))
    ids = [int(r["tree_id"]) for r in rows(run_dir / "trees.csv")]
    raw = root / "raw"
    raw.mkdir()
    first = ids[0]
    for k in range(2):
        Image.new("RGB", (2400, 1800), (30, 90, 30)).save(raw / f"tree_{first}_v{k}.jpg")
    (raw / "views.json").write_text(json.dumps({str(first): [
        {"view": k, "photo": f"p{k}.jpg", "off_nadir_deg": 5.0 + k, "azimuth_deg": 90.0 * k,
         "filename": f"tree_{first}_v{k}.jpg"} for k in range(2)]}))
    chips = root / "chips"
    chips.mkdir()
    for tid in ids:
        Image.new("RGB", (300, 300), (60, 120, 60)).save(chips / f"tree_{tid}.png")
    ortho = root / "ortho.tif"
    with rasterio.open(ortho, "w", driver="GTiff", height=64, width=64, count=3, dtype="uint8",
                       crs="EPSG:32616", transform=from_origin(0, 32, 0.5, 0.5)) as ds:
        ds.write(np.full((3, 64, 64), 128, dtype=np.uint8))
    return {"root": root, "forest": forest, "run": run_dir, "raw": raw, "chips": chips,
            "ortho": ortho, "ids": ids}


@pytest.fixture(scope="module")
def pack(run):
    out = run["root"] / "fieldpack"
    info = build(run["run"], out, run["forest"], raw_photos=run["raw"],
                 ortho_chips=run["chips"], ortho=run["ortho"])
    return out, info


def test_pack_lists_every_tree_and_every_file_with_its_hash(run, pack):
    out, info = pack
    assert info == json.loads((out / "pack.json").read_text())
    assert info["schema"] == "treedar.fieldpack.v1" and info["trees"] == len(TREES)
    assert not out.with_name(out.name + ".partial").exists()
    on_disk = {p.relative_to(out).as_posix() for p in out.rglob("*") if p.is_file()}
    assert on_disk - {"pack.json"} == set(info["files"])
    for rel, meta in info["files"].items():
        data = (out / rel).read_bytes()
        assert meta == {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def test_trees_are_keyed_by_lasting_id_and_located_in_lon_lat(run, pack):
    out, _ = pack
    feats = json.loads((out / "trees.geojson").read_text())["features"]
    ids = {r["tree_id"]: r["stable_id"] for r in rows(run["run"] / "tree_ids.csv")}
    trees = {r["tree_id"]: r for r in rows(run["run"] / "trees.csv")}
    back = Transformer.from_crs("EPSG:4326", "EPSG:32616", always_xy=True)
    assert len(feats) == len(TREES)
    for f in feats:
        p = f["properties"]
        assert str(p["stable_id"]) == ids[str(p["tree_id"])]
        assert p["points"] == f"points/{p['stable_id']}.bin"
        x, y = back.transform(*p["top"])
        t = trees[str(p["tree_id"])]
        assert abs(x - float(t["x"])) < 0.01 and abs(y - float(t["y"])) < 0.01
        assert f["geometry"]["type"] in ("Polygon", "MultiPolygon")
        ring = np.array(f["geometry"]["coordinates"][0])
        assert np.all(np.abs(ring[:, 0]) <= 180) and np.all(np.abs(ring[:, 1]) <= 90)


def test_points_are_local_metres_around_the_treetop(run, pack):
    out, _ = pack
    for f in json.loads((out / "trees.geojson").read_text())["features"]:
        p = f["properties"]
        raw = (out / p["points"]).read_bytes()
        assert len(raw) % POINT_DTYPE.itemsize == 0
        pts = np.frombuffer(raw, dtype=POINT_DTYPE)
        assert pts.size == p["n_points"] > 0
        # Points sit inside the crown footprint, so within a crown diameter of the top.
        assert np.hypot(pts["x"], pts["y"]).max() <= p["crown_diameter_m"] + 1
        assert pts["z"].min() > -1.0
        assert abs(float(pts["z"].max()) - p["height_m"]) < 1.0


def test_points_are_capped_exactly(run):
    out = run["root"] / "small-pack"
    build(run["run"], out, run["forest"], max_points=50)
    for f in json.loads((out / "trees.geojson").read_text())["features"]:
        p = f["properties"]
        assert p["n_points"] == 50
        assert (out / p["points"]).stat().st_size == 50 * POINT_DTYPE.itemsize


def test_photos_are_resized_and_described(run, pack):
    out, info = pack
    feats = {f["properties"]["tree_id"]: f["properties"]
             for f in json.loads((out / "trees.geojson").read_text())["features"]}
    first = feats[run["ids"][0]]
    raws = [p for p in first["photos"] if p["kind"] == "raw"]
    assert [p["azimuth_deg"] for p in raws] == [0.0, 90.0]
    with Image.open(out / raws[0]["file"]) as im:
        assert max(im.size) == 1600
    assert all(any(p["kind"] == "ortho" for p in f["photos"]) for f in feats.values())
    assert info["photos"]["trees_with_raw"] == 1


def test_ortho_overview_has_lat_lon_bounds(pack):
    out, info = pack
    (south, west), (north, east) = info["ortho"]["bounds"]
    assert south < north and west < east
    with Image.open(out / info["ortho"]["file"]) as im:
        assert im.size == (info["ortho"]["width"], info["ortho"]["height"])


def test_existing_pack_and_missing_ids_are_refused(run, pack, tmp_path):
    out, _ = pack
    with pytest.raises(ValueError, match="already exists"):
        build(run["run"], out, run["forest"])
    bare = tmp_path / "bare"
    ok(cli("run", run["forest"], "--out", bare, "--no-qc"))
    with pytest.raises(ValueError, match="treedar ids"):
        build(bare, tmp_path / "p", run["forest"])


def test_cli_writes_the_default_pack_folder(run):
    result = ok(cli("fieldpack", run["run"], "--laz", run["forest"], "--max-points", "100"))
    assert f"{len(TREES)} trees" in result.stdout and "keep out of Git" in result.stdout
    assert (run["run"] / "fieldpack" / "pack.json").exists()


# Species label maps for the classifier.

def write_rows(path, header, data):
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        w.writerows(data)


def test_label_map_turns_species_into_trained_types(tmp_path):
    write_rows(tmp_path / "trees.csv", ["tree_id", "x", "y", "height_m", "crown_area_m2",
                                        "crown_diameter_m"],
               [[i, i, i, 10, 20, 5] for i in range(1, 5)])
    write_rows(tmp_path / "features.csv", ["tree_id", "n_points", "hag_cv"],
               [[i, 100, 0.1 * i] for i in range(1, 5)])
    write_rows(tmp_path / "labels.csv", ["tree_id", "label", "group"],
               [[1, "White Pine ", "a"], [2, "apple", "b"], [3, "not sure", "a"]])
    write_rows(tmp_path / "map.csv", ["label", "type"],
               [["white pine", "conifer"], ["apple", "broadleaf"], ["not sure", ""]])
    mapping = classify.read_label_map(tmp_path / "map.csv")
    ids, _, _, _, y, _ = classify.load_table(tmp_path / "trees.csv", tmp_path / "features.csv",
                                             tmp_path / "labels.csv", mapping)
    assert dict(zip(ids, y)) == {"1": "conifer", "2": "broadleaf", "3": "", "4": ""}

    write_rows(tmp_path / "labels.csv", ["tree_id", "label"], [[1, "ginkgo"], [2, "apple"]])
    with pytest.raises(ValueError, match="ginkgo"):
        classify.load_table(tmp_path / "trees.csv", tmp_path / "features.csv",
                            tmp_path / "labels.csv", mapping)


def test_starter_species_map_reads_and_maps_only_to_known_types():
    from pathlib import Path
    mapping = classify.read_label_map(Path(__file__).parents[1] / "docs" / "species_types.csv")
    assert set(mapping.values()) == {"conifer", "broadleaf", ""}
    assert mapping["tamarack"] == "conifer" and mapping["apple"] == "broadleaf"


# Spread view selection for raw photo crops.

def test_spread_keeps_overhead_first_then_the_most_different_direction():
    def row(angle, name, direction):
        d = np.array(direction, dtype=float)
        return (angle, name, 0, None, None, d / np.linalg.norm(d))
    scored = [row(2, "top", [0, 0, 1]), row(20, "east", [0.4, 0, 1]),
              row(21, "east2", [0.41, 0.01, 1]), row(25, "west", [-0.45, 0, 1])]
    assert [r[1] for r in _spread(scored, 3)] == ["top", "west", "east2"]
    assert [r[1] for r in _spread(scored, 9)][:1] == ["top"] and len(_spread(scored, 9)) == 4


def test_raw_chips_reject_unknown_selection(tmp_path):
    from treedar.raw_chips import generate_raw_chips
    with pytest.raises(ValueError, match="select"):
        generate_raw_chips(tmp_path, tmp_path, tmp_path, tmp_path / "o", select="random")


def test_colour_gets_display_gamma(tmp_path):
    import laspy
    from pyproj import CRS
    from rasterio.transform import from_origin as fo
    from treedar.fieldpack import write_points
    header = laspy.LasHeader(point_format=3, version="1.2")
    header.scales, header.offsets = np.array([0.001] * 3), np.zeros(3)
    header.add_crs(CRS.from_epsg(32616))
    cloud = laspy.LasData(header)
    cloud.x, cloud.y, cloud.z = np.array([0.5, 0.5]), np.array([0.5, 0.5]), np.array([1.0, 2.0])
    cloud.red = cloud.green = cloud.blue = np.array([65535 // 4, 65535], dtype=np.uint16)
    cloud.write(tmp_path / "c.laz")
    labels = np.ones((1, 1), dtype=np.int32)
    out = tmp_path / "points"
    write_points(tmp_path / "c.laz", labels, fo(0, 1, 1, 1), {1: (0.5, 0.5, 0.0)}, out, {1: "1"})
    pts = np.fromfile(out / "1.bin", dtype=POINT_DTYPE)
    assert list(pts["r"]) == [round(255 * 0.25 ** (1 / 2.2)), 255]
    write_points(tmp_path / "c.laz", labels, fo(0, 1, 1, 1), {1: (0.5, 0.5, 0.0)},
                 tmp_path / "linear", {1: "1"}, gamma=1)
    assert list(np.fromfile(tmp_path / "linear" / "1.bin", dtype=POINT_DTYPE)["r"]) == [64, 255]
