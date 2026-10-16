"""Local CLI tests for owner labels and maps; synthetic colours are not field truth."""

import csv
import json
import os
from collections import Counter
from pathlib import Path
import subprocess
import sys

import laspy
import numpy as np
import pytest
import rasterio
from pyproj import CRS
from rasterio.features import rasterize


def cli(*args):
    env = {**os.environ, "OMP_NUM_THREADS": "2", "OPENBLAS_NUM_THREADS": "2"}
    result = subprocess.run([sys.executable, "-m", "treedar", *map(str, args)],
                            capture_output=True, text=True, timeout=90, env=env)
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def read_csv(path):
    with Path(path).open(newline="") as stream:
        return list(csv.DictReader(stream))


def assert_png(path):
    # Decode the actual image, rather than accepting an empty placeholder.
    from PIL import Image
    with Image.open(path) as image:
        assert image.format == "PNG"
        assert min(image.size) >= 100
        image.verify()


def write_two_stands(path):
    """Twelve paraboloid crowns, six per stand, with stands 40 m apart.

    All coordinates are invented. EPSG exercises metadata export only.
    Dense ground samples avoid needing an external ground-classification tool.
    """
    gx, gy = np.meshgrid(np.arange(0.25, 70, 0.5), np.arange(0.25, 24, 0.5))
    x, y = gx.ravel(), gy.ravel()
    ground = 100 + 0.02 * x + 0.01 * y
    xs, ys, zs = [x], [y], [ground]
    classes = [np.full(x.size, 2, dtype=np.uint8)]
    colours = [np.zeros((x.size, 3), dtype=np.uint16)]
    for stand in range(2):
        for i, (cx, cy) in enumerate((
                (8.25, 8.25), (14.25, 8.25), (20.25, 8.25),
                (8.25, 14.25), (14.25, 14.25), (20.25, 14.25))):
            cx += 40 * stand
            distance2 = (x - cx) ** 2 + (y - cy) ** 2
            mask = distance2 <= 4.0 ** 2
            height = (12 + i) * (1 - 0.55 * distance2[mask] / 4.0 ** 2)
            xs.append(x[mask])
            ys.append(y[mask])
            zs.append(ground[mask] + height)
            classes.append(np.full(mask.sum(), 5, dtype=np.uint8))
            rgb = (4000, 22000, 4000) if stand == 0 else (28000, 13000, 4000)
            colours.append(np.tile(np.asarray(rgb, dtype=np.uint16), (mask.sum(), 1)))
    header = laspy.LasHeader(point_format=3, version="1.2")
    header.scales = [0.001] * 3
    header.offsets = [0, 0, 100]
    header.add_crs(CRS.from_epsg(32616))
    cloud = laspy.LasData(header)
    cloud.x, cloud.y, cloud.z = map(np.concatenate, (xs, ys, zs))
    cloud.classification = np.concatenate(classes)
    cloud.red, cloud.green, cloud.blue = np.concatenate(colours).T
    cloud.return_number = np.ones(len(cloud.points), dtype=np.uint8)
    cloud.number_of_returns = np.ones(len(cloud.points), dtype=np.uint8)
    cloud.write(path)


@pytest.fixture(scope="module")
def stands_run(tmp_path_factory):
    root = tmp_path_factory.mktemp("two_stands")
    source, run = root / "stands.laz", root / "run"
    write_two_stands(source)
    cli("run", source, "--out", run, "--no-qc")
    assert len(read_csv(run / "trees.csv")) == 12
    return source, run


def test_areas_and_labels_preserve_owner_input(stands_run, tmp_path):
    _, run = stands_run
    cli("areas", run)
    trees = read_csv(run / "trees.csv")
    assignments = read_csv(run / "tree_areas.csv")
    template = read_csv(run / "area_types.csv")
    assert {row["tree_id"] for row in assignments} == {row["tree_id"] for row in trees}
    assert {row["area"] for row in template} == {"A", "B"}
    assert Counter(row["area"] for row in assignments) == {"A": 6, "B": 6}
    assert all(row["label"] == "" and int(row["n_trees"]) == 6 for row in template)
    assert_png(run / "areas.png")
    with rasterio.open(run / "areas.tif") as areas, rasterio.open(run / "chm.tif") as canopy:
        assert areas.crs == canopy.crs
        assert areas.transform == canopy.transform
        assert areas.shape == canopy.shape
        assert set(np.unique(areas.read(1))) == {0, 1, 2}

    owner_content = b"area,label,n_trees\r\nA,conifer,6\r\nB,,6\r\n"
    (run / "area_types.csv").write_bytes(owner_content)
    cli("areas", run)
    assert (run / "area_types.csv").read_bytes() == owner_content
    # Exercise whitespace and blank omission through the labels CLI as well.
    named = tmp_path / "named.csv"
    named.write_text("area,label\n A , conifer \nB,   \n,ignored\n")
    cli("labels", run, "--area-types", named)
    labels = read_csv(run / "labels.csv")
    assert len(labels) == 6
    assert list(labels[0]) == ["tree_id", "label", "group"]
    assert {row["tree_id"] for row in labels} == {
        row["tree_id"] for row in assignments if row["area"] == "A"}
    assert all(row["label"] == "conifer" and row["group"] == "A" for row in labels)


def assert_map(run, predictions=None):
    doc = json.loads((run / "tree_map.geojson").read_text())
    trees = {int(row["tree_id"]): row for row in read_csv(run / "trees.csv")}
    assert doc["type"] == "FeatureCollection"
    assert doc["crs"]["properties"]["name"] == "urn:ogc:def:crs:EPSG::32616"
    assert Counter(feature["properties"]["tree_id"] for feature in doc["features"]) == {
        tid: 1 for tid in trees}
    for feature in doc["features"]:
        assert feature["type"] == "Feature"
        assert feature["geometry"]["type"] in {"Polygon", "MultiPolygon"}
        props = feature["properties"]
        tid = props["tree_id"]
        for attr in ("height_m", "crown_area_m2", "crown_diameter_m"):
            assert props[attr] == pytest.approx(float(trees[tid][attr]))
        if predictions is None:
            assert "type" not in props
        else:
            pred = predictions[str(tid)]
            assert props["type"] == pred["predicted"]
            assert props["type_confidence"] == pytest.approx(float(pred["confidence"]))
            assert props["label"] == pred["label"]
            assert props["is_labelled"] == (pred["is_labelled"] == "1")
    with rasterio.open(run / "crowns.tif") as crowns:
        # Polygon coordinates and CRS must recover the actual crown raster.
        recovered = rasterize([(f["geometry"], f["properties"]["tree_id"])
                               for f in doc["features"]], out_shape=crowns.shape,
                              transform=crowns.transform, dtype="int32")
        np.testing.assert_array_equal(recovered, crowns.read(1))
    assert_png(run / "tree_map.png")


def test_map_without_and_with_predictions(stands_run, tmp_path):
    _, run = stands_run
    cli("map", run)
    assert_map(run)
    predictions = tmp_path / "predictions.csv"
    rows = read_csv(run / "trees.csv")
    with predictions.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=["tree_id", "predicted", "confidence", "label", "is_labelled"])
        writer.writeheader()
        for i, row in enumerate(rows):
            writer.writerow(dict(tree_id=row["tree_id"], predicted="conifer" if i % 2 else "unknown",
                                 confidence=0.8 if i % 2 else 0.4, label="conifer" if i % 2 else "",
                                 is_labelled=int(i % 2)))
    cli("map", run, "--predictions", predictions)
    assert_map(run, {row["tree_id"]: row for row in read_csv(predictions)})


def test_pipeline_two_stands_has_no_independent_final_test(stands_run, tmp_path):
    source, _ = stands_run
    named = tmp_path / "owner_labels.csv"
    named.write_text("area,label\nA,conifer\nB,broadleaf\n")
    out = tmp_path / "pipeline"
    result = cli("pipeline", source, "--out", out, "--area-types", named)
    assert "No independent final test" in result.stdout
    for name in ("qc.json", "dtm.tif", "chm.tif", "chm_raw.tif", "crowns.tif",
                 "trees.csv", "tree_features.csv", "summary.json", "areas.tif",
                 "areas.png", "tree_areas.csv", "area_types.csv", "labels.csv"):
        assert (out / name).is_file(), name
    labels = read_csv(out / "labels.csv")
    assert Counter(row["label"] for row in labels) == {"conifer": 6, "broadleaf": 6}
    assert Counter(row["group"] for row in labels) == {"A": 6, "B": 6}
    assignments = {row["tree_id"]: row["area"] for row in read_csv(out / "tree_areas.csv")}
    assert all(row["group"] == assignments[row["tree_id"]] for row in labels)
    feats = {row["tree_id"]: row for row in read_csv(out / "tree_features.csv")}
    # The LAZ RGB survives segmentation and feature extraction, providing two
    # distinct synthetic inputs without asserting classification accuracy.
    per_class = {label: [float(feats[row["tree_id"]]["gcc_mean"]) for row in labels
                         if row["label"] == label] for label in ("conifer", "broadleaf")}
    assert abs(np.mean(per_class["conifer"]) - np.mean(per_class["broadleaf"])) > 0.1
    report = json.loads((out / "classify" / "classify_report.json").read_text())
    assert report["labelled_blocks"] == report["development_groups"] == 2
    assert report["grouping"] == "label group column"
    assert report["final_test_groups"] == 0
    assert report["final_test"] is None
    assert "development only" in report["final_test_note"]
    assert not (out / "classify" / "final_test_predictions.csv").exists()
    predictions = read_csv(out / "classify" / "predictions.csv")
    assert len(predictions) == len(labels) == 12
    assert all(row["partition"] == "development" and row["is_labelled"] == "1" for row in predictions)
    assert_map(out, {row["tree_id"]: row for row in predictions})


def test_disconnected_crown_exports_one_feature(tmp_path):
    """One tree may have multiple raster components but still needs one feature."""
    run = tmp_path / "disconnected"
    run.mkdir()
    transform = rasterio.transform.from_origin(0, 5, 1, 1)
    crowns = np.zeros((5, 5), dtype=np.int32)
    crowns[1, 1] = crowns[3, 3] = 7
    for name, array in (("crowns.tif", crowns), ("chm.tif", crowns.astype("float32"))):
        with rasterio.open(run / name, "w", driver="GTiff", height=5, width=5,
                           count=1, dtype=array.dtype, transform=transform, crs="EPSG:32616") as ds:
            ds.write(array, 1)
    (run / "trees.csv").write_text("tree_id,x,y,height_m,crown_area_m2,crown_diameter_m\n7,1.5,3.5,7,2,1.6\n")
    cli("map", run)
    assert_map(run)
