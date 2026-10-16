"""Actual Docker build and mounted synthetic pipeline; optional Linux engine."""

import csv
import json
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
    return subprocess.run(command, capture_output=True, text=True, timeout=timeout)


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
    try:
        built = docker_result([docker, "build", "--tag", tag, str(project)], timeout=1200)
        assert built.returncode == 0, built.stdout + built.stderr
        probe = (
            "import os,pathlib; assert os.getuid()!=0; "
            "assert not pathlib.Path('/opt/treedar/Example Datasets').exists(); "
            "assert not pathlib.Path('/opt/treedar/tests').exists(); "
            "assert pathlib.Path('/data/synthetic.laz').is_file(); "
            "assert os.access('/runs',os.W_OK); "
            "assert not os.access('/data',os.W_OK); print(os.getuid())"
        )
        identity = docker_result(common + ["--entrypoint", "python", tag, "-c", probe])
        assert identity.returncode == 0, identity.stdout + identity.stderr
        assert identity.stdout.strip() == "10001"
        result = docker_result(common + [tag, "pipeline", "/data/synthetic.laz",
                                         "--out", "/runs/synthetic"], timeout=180)
        assert result.returncode == 0, result.stdout + result.stderr
        assert source.read_bytes() == before
        assert list(data.iterdir()) == [source]
        out = runs / "synthetic"
        assert json.loads((out / "summary.json").read_text())["n_trees"] == len(TREES)
        with (out / "trees.csv").open(newline="") as stream:
            assert len(list(csv.DictReader(stream))) == len(TREES)
        with (out / "tree_features.csv").open(newline="") as stream:
            assert len(list(csv.DictReader(stream))) == len(TREES)
        with rasterio.open(out / "dtm.tif") as dtm, rasterio.open(out / "chm.tif") as chm:
            assert dtm.crs.to_epsg() == chm.crs.to_epsg() == 32616
            assert dtm.transform == chm.transform and dtm.shape == chm.shape
            assert np.isfinite(chm.read(1)).all()
        for name in ("areas.png", "tree_map.png"):
            with Image.open(out / name) as image:
                assert min(image.size) >= 100
                image.verify()
        assert len(json.loads((out / "tree_map.geojson").read_text())["features"]) == len(TREES)
    finally:
        # These identifiers are unique to this test; never prune shared images.
        docker_result([docker, "rm", "--force", container])
        docker_result([docker, "image", "rm", "--force", tag])
