"""Actual Docker build and mounted synthetic pipeline; optional Linux engine."""

import csv
import json
import os
from pathlib import Path
import shutil
import subprocess
from uuid import uuid4

import numpy as np
from PIL import Image
import pytest
import rasterio

from synthetic_forest import TREES, write_forest


def docker_result(command, *, timeout=60):
    return subprocess.run(command, capture_output=True, text=True,
                          encoding="utf-8", errors="replace", timeout=timeout)


def test_docker_build_and_mounted_synthetic_pipeline(tmp_path):
    docker = shutil.which("docker")
    if docker is None:
        pytest.skip("Docker CLI is unavailable")
    try:
        engine = docker_result([docker, "info", "--format", "{{.OSType}}"], timeout=15)
    except (OSError, subprocess.TimeoutExpired):
        pytest.skip("Docker engine is unavailable")
    if engine.returncode != 0:
        pytest.skip("Docker engine is not running or not reachable")
    if engine.stdout.strip() != "linux":
        pytest.skip("This image requires the Docker Linux engine")

    project = Path(__file__).resolve().parents[1]
    tag = f"treedar-test-{uuid4().hex}:local"
    container = f"treedar-test-{uuid4().hex}"
    data, runs = tmp_path / "data", tmp_path / "runs"
    data.mkdir()
    # Bind-mounted synthetic outputs must be writable by the non-root user
    # on Linux too, independently of the test runner's UID.
    runs.mkdir(mode=0o777)
    runs.chmod(0o777)
    source = data / "synthetic.laz"
    write_forest(source)
    before = source.read_bytes()
    common = [docker, "run", "--rm", "--name", container, "--network", "none",
              "--cpus", "2", "--memory", "2g",
              "--mount", f"type=bind,source={data.resolve()},target=/data,readonly",
              "--mount", f"type=bind,source={runs.resolve()},target=/runs",
              "--env", "OMP_NUM_THREADS=2", "--env", "OPENBLAS_NUM_THREADS=2"]
    image_built = False
    try:
        built = docker_result([docker, "build", "--tag", tag, str(project)], timeout=1200)
        assert built.returncode == 0, built.stdout + built.stderr
        image_built = True
        probe = (
            "import os,pathlib; assert os.getuid()!=0; "
            "assert not pathlib.Path('/opt/treedar/Example Datasets').exists(); "
            "assert not pathlib.Path('/opt/treedar/tests').exists(); "
            "assert pathlib.Path('/data/synthetic.laz').is_file(); "
            "assert os.access('/runs',os.W_OK); "
            "assert not os.access('/data',os.W_OK); "
            "import numpy as np; from affine import Affine; from pyproj import CRS; "
            "from treedar.cli import _write_raster; "
            "_write_raster('/runs/cog-probe.tif',np.zeros((1024,1024),dtype='float32'),"
            "Affine(.5,0,0,0,-.5,512),CRS.from_epsg(32616)); print(os.getuid())"
        )
        identity = docker_result(common + ["--entrypoint", "python", tag, "-c", probe])
        assert identity.returncode == 0, identity.stdout + identity.stderr
        assert identity.stdout.strip() == "10001"
        with rasterio.open(runs / "cog-probe.tif") as cog:
            assert cog.driver == "GTiff" and cog.profile["tiled"]
            assert cog.overviews(1)
            assert cog.tags(ns="IMAGE_STRUCTURE")["LAYOUT"] == "COG"
        result = docker_result(common + [tag, "pipeline", "/data/synthetic.laz",
                                         "--out", "/runs/synthetic"], timeout=180)
        assert result.returncode == 0, result.stdout + result.stderr
        assert source.read_bytes() == before
        assert list(data.iterdir()) == [source]
        out = runs / "synthetic"
        manifest = json.loads((out / "manifest.json").read_text())
        assert manifest["input"] == {"name": source.name, "bytes": source.stat().st_size}
        assert manifest["command"] == "pipeline"
        assert "laz" not in manifest["params"] and "out" not in manifest["params"]
        assert str(data.resolve()) not in json.dumps(manifest)
        roles = {item["path"]: item for item in manifest["outputs"]}
        assert roles["dtm.tif"]["format"] == "cog"
        assert roles["report.html"]["role"] == "report"
        for item in manifest["outputs"]:
            relative = Path(item["path"])
            assert not relative.is_absolute() and ".." not in relative.parts
            assert item["role"] and item["format"] and (out / relative).is_file()
        assert "TreeDar Run Report" in (out / "report.html").read_text(encoding="utf-8")
        assert json.loads((out / "summary.json").read_text())["n_trees"] == len(TREES)
        with (out / "trees.csv").open(newline="") as stream:
            assert len(list(csv.DictReader(stream))) == len(TREES)
        with (out / "tree_features.csv").open(newline="") as stream:
            assert len(list(csv.DictReader(stream))) == len(TREES)
        with rasterio.open(out / "dtm.tif") as dtm, rasterio.open(out / "chm.tif") as chm:
            assert dtm.driver == chm.driver == "GTiff"
            assert dtm.profile["tiled"] and chm.profile["tiled"]
            assert dtm.tags(ns="IMAGE_STRUCTURE")["LAYOUT"] == "COG"
            assert dtm.crs.to_epsg() == chm.crs.to_epsg() == 32616
            assert dtm.transform == chm.transform and dtm.shape == chm.shape
            assert np.isfinite(chm.read(1)).all()
        for name in ("areas.png", "tree_map.png"):
            with Image.open(out / name) as image:
                assert min(image.size) >= 100
                image.verify()
        assert len(json.loads((out / "tree_map.geojson").read_text())["features"]) == len(TREES)
        report = docker_result(common + [tag, "report", "/runs/synthetic"])
        assert report.returncode == 0, report.stdout + report.stderr
    finally:
        # These identifiers are unique to this test; never prune shared images.
        docker_result([docker, "rm", "--force", container])
        if image_built and os.name != "nt":
            # On native Linux, UID 10001 owns nested output directories. Let
            # pytest's host user remove its own temporary tree after inspection.
            permissions = (
                "from pathlib import Path; "
                "[(p.chmod(0o777 if p.is_dir() else 0o666)) "
                "for p in Path('/runs').rglob('*') if not p.is_symlink()]"
            )
            docker_result([docker, "run", "--rm", "--network", "none",
                           "--mount", f"type=bind,source={runs.resolve()},target=/runs",
                           "--entrypoint", "python", tag, "-c", permissions])
        docker_result([docker, "image", "rm", "--force", tag])
