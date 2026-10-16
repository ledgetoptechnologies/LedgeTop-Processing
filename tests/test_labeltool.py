"""Synthetic offline labeling pages, including their actual JavaScript export."""

import base64
import csv
import io
import json
from pathlib import Path
import re
import shutil
import subprocess

from PIL import Image
import pytest

from treedar import labeltool
from test_pipeline_e2e import cli


@pytest.mark.parametrize("per_area", [3, 15])
def test_sampling_represents_areas_and_caps_expected_counts(per_area):
    sizes = {"A": 2, "B": 20, "C": 200}
    rows = [{"tree_id": f"{area}-{i}", "area": area}
            for area, count in sizes.items() for i in range(count)]
    rows += [{"tree_id": "unassigned", "area": ""}]
    picked = labeltool.sample_trees(rows, per_area=per_area, seed=9)
    assert picked == labeltool.sample_trees(rows, per_area=per_area, seed=9)
    assert picked != labeltool.sample_trees(rows, per_area=per_area, seed=10)
    assert len({tid for tid, _ in picked}) == len(picked)
    assert {area for _, area in picked} == set(sizes)
    assert "unassigned" not in {tid for tid, _ in picked}
    for area, n in sizes.items():
        assert sum(a == area for _, a in picked) == min(n, max(per_area, round(.12 * n)))


def test_sampling_without_named_areas_is_empty():
    assert labeltool.sample_trees([]) == []
    assert labeltool.sample_trees([{"tree_id": "1", "area": ""}]) == []


def write_csv(path, columns, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture
def inputs(tmp_path):
    run, chips = tmp_path / "synthetic_run", tmp_path / "chips"
    run.mkdir()
    chips.mkdir()
    tree_rows = [dict(tree_id=str(n), x=12345678.91 + n, y=98765432.19 + n,
                      height_m=10 + n, crown_diameter_m=3 + n)
                 for n in range(1, 6)]
    write_csv(run / "trees.csv", list(tree_rows[0]), tree_rows)
    write_csv(run / "tree_areas.csv", ["tree_id", "area"], [
        {"tree_id": "1", "area": "A"}, {"tree_id": "2", "area": "A"},
        {"tree_id": "3", "area": "B"}, {"tree_id": "4", "area": "B"},
        {"tree_id": "5", "area": ""},
    ])
    for tid in (1, 2, 3, 5):
        Image.new("RGB", (320, 300), (20 * tid, 100, 30)).save(chips / f"tree_{tid}.png")
    return run, chips, tree_rows


def items_in(page):
    match = re.search(r"\bconst ITEMS\s*=\s*", page)
    assert match
    return json.JSONDecoder().raw_decode(page[match.end():])[0]


def runtime(page_path, *, picks=None, keys=None):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Optional script execution test needs an existing Node runtime")
    result = subprocess.run(
        [node, str(Path(__file__).with_name("labelpage_runtime.cjs"))],
        input=json.dumps({"html": str(page_path), "picks": picks or {}, "keys": keys or []}),
        text=True, capture_output=True, timeout=10,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def test_build_embeds_only_existing_chips_and_no_coordinates(inputs, tmp_path):
    run, chips, source = inputs
    page_path = tmp_path / "labels.html"
    assert labeltool.build(run, chips, page_path, per_area=15, seed=0) == 3
    page = page_path.read_text(encoding="utf-8")
    items = items_in(page)
    assert len(items) == 3
    assert {item["id"] for item in items} == {"1", "2", "3"}
    assert all(set(item) == {"id", "area", "h", "d", "img"} for item in items)
    for item in items:
        original = next(row for row in source if row["tree_id"] == item["id"])
        assert float(item["h"]) == original["height_m"]
        assert float(item["d"]) == original["crown_diameter_m"]
        with Image.open(io.BytesIO(base64.b64decode(item["img"], validate=True))) as image:
            assert image.format == "JPEG" and max(image.size) <= 240
            image.verify()
    assert not re.search(r"https?://|<script[^>]+src=|<link[^>]+href=", page, re.I)
    assert "__ITEMS__" not in page and "__CHOICES__" not in page
    assert "tree_id,label,group" in page
    assert all(str(row[coord]) not in page for row in source for coord in ("x", "y"))


def test_cli_builds_same_self_contained_page(inputs, tmp_path):
    run, chips, _ = inputs
    page_path = tmp_path / "cli.html"
    result = cli("labelpage", run, "--chips", chips, "--out", page_path, "--per-area", "15", "--seed", "7")
    assert result.returncode == 0, result.stdout + result.stderr
    assert len(items_in(page_path.read_text(encoding="utf-8"))) == 3


def test_render_and_keyboard_export_exclude_blank_picks(inputs, tmp_path):
    run, chips, _ = inputs
    page_path = tmp_path / "keys.html"
    labeltool.build(run, chips, page_path)
    items = items_in(page_path.read_text(encoding="utf-8"))
    result = runtime(page_path, keys=["1", "2", "3", "ArrowLeft", "3"])
    assert len(result["cards"]) == len(items)
    assert all(card.count("data:image/jpeg;base64,") == 1 for card in result["cards"])
    assert result["download"] == "labels.csv"
    assert result["exported"]["type"] == "text/csv"
    rows = list(csv.DictReader(io.StringIO(result["exported"]["text"])))
    assert rows == [{"tree_id": items[0]["id"], "label": "conifer", "group": items[0]["area"]}]
    assert result["stored"][items[1]["id"]] == ""
    assert result["stored"][items[2]["id"]] == ""


def test_export_ignores_stale_picks_for_absent_items(inputs, tmp_path):
    run, chips, _ = inputs
    page_path = tmp_path / "stored.html"
    labeltool.build(run, chips, page_path)
    items = items_in(page_path.read_text(encoding="utf-8"))
    result = runtime(page_path, picks={items[0]["id"]: "broadleaf", items[1]["id"]: "", "ghost": "conifer"})
    rows = list(csv.DictReader(io.StringIO(result["exported"]["text"])))
    assert rows == [{"tree_id": items[0]["id"], "label": "broadleaf", "group": items[0]["area"]}]


def test_empty_page_handles_keys_and_exports_header_only(inputs, tmp_path):
    run, _, _ = inputs
    missing = tmp_path / "no_chips"
    missing.mkdir()
    page_path = tmp_path / "empty.html"
    assert labeltool.build(run, missing, page_path) == 0
    assert items_in(page_path.read_text(encoding="utf-8")) == []
    result = runtime(page_path, keys=["1", "2", "3", "ArrowLeft", "ArrowRight"])
    assert result["cards"] == []
    assert result["exported"]["text"] == "tree_id,label,group\n"
