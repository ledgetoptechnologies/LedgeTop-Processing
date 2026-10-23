"""Lasting tree IDs: registry matching rules and real reruns of the synthetic forest."""

import csv
import json
import subprocess
import sys

import pytest
from pyproj import CRS

from synthetic_forest import TREES, write_forest
from treedar import treeids


def tree(tid, x, y, h=15.0):
    return {"tree_id": str(tid), "x": str(x), "y": str(y), "height_m": str(h)}


def seeded(*trees):
    registry = treeids.new_registry()
    treeids.assign(list(trees), registry, "run-001")
    return registry


def by_run_id(rows):
    return {r["tree_id"]: r for r in rows}


def test_seed_keeps_run_numbers_as_lasting_ids():
    registry = treeids.new_registry()
    rows = treeids.assign([tree(3, 0, 0), tree(7, 10, 0)], registry, "run-001")
    assert [(r["tree_id"], r["stable_id"], r["match"]) for r in rows] == [
        (3, 3, "seed"), (7, 7, "seed")]
    assert registry["next_id"] == 8
    assert registry["runs"][0]["seed"] == 2


def test_renumbered_and_shifted_rerun_keeps_ids():
    registry = seeded(tree(1, 0, 0), tree(2, 10, 0), tree(3, 0, 10))
    rerun = [tree(9, 0.4, 10.3, 15.5), tree(4, 9.6, -0.2, 14.0), tree(5, 0.1, 0.0)]
    rows = by_run_id(treeids.assign(rerun, registry, "run-002"))
    assert {k: r["stable_id"] for k, r in rows.items()} == {4: 2, 5: 1, 9: 3}
    assert all(r["match"] == "matched" and not r["review"] for r in rows.values())
    assert registry["runs"][-1]["missing"] == 0


def test_new_and_missing_trees_never_reuse_ids():
    registry = seeded(tree(1, 0, 0), tree(2, 10, 0))
    rows = by_run_id(treeids.assign([tree(1, 0, 0), tree(2, 30, 30)], registry, "run-002"))
    assert rows[1]["stable_id"] == 1
    assert rows[2] == {**rows[2], "stable_id": 3, "match": "new"}
    status = {t["id"]: t["status"] for t in registry["trees"]}
    assert status == {1: "active", 2: "missing", 3: "active"}
    # A missing tree found again keeps its ID; the next new tree gets a fresh one.
    rows = by_run_id(treeids.assign([tree(1, 10.2, 0), tree(2, 50, 50)], registry, "run-003"))
    assert rows[1]["stable_id"] == 2 and rows[2]["stable_id"] == 4
    assert {t["id"]: t["status"] for t in registry["trees"]}[2] == "active"


def test_height_tolerance_separates_trees_at_the_same_spot():
    registry = seeded(tree(1, 0, 0, 10.0))
    rows = treeids.assign([tree(1, 0, 0, 16.0)], registry, "run-002")
    assert rows[0]["match"] == "new" and rows[0]["stable_id"] == 2


def test_split_crown_flags_both_halves():
    registry = seeded(tree(1, 0, 0), tree(2, 20, 0))
    rows = by_run_id(treeids.assign(
        [tree(1, 0.4, 0), tree(2, -0.6, 0), tree(3, 20, 0)], registry, "run-002"))
    assert rows[1]["stable_id"] == 1 and rows[2]["stable_id"] == 3
    assert rows[1]["review"] == rows[2]["review"] == "possible_split"
    assert rows[1]["related_ids"] == "3" and rows[2]["related_ids"] == "1"
    assert not rows[3]["review"]
    # A flagged tree keeps its registered position, so the split cannot drag it.
    assert next(t for t in registry["trees"] if t["id"] == 1)["x"] == 0.0


def test_merged_crowns_flag_the_survivor_and_mark_the_other_missing():
    registry = seeded(tree(1, 0, 0), tree(2, 1.0, 0))
    rows = treeids.assign([tree(1, 0.3, 0)], registry, "run-002")
    assert rows[0]["stable_id"] == 1
    assert rows[0]["review"] == "possible_merge" and rows[0]["related_ids"] == "2"
    assert {t["id"]: t["status"] for t in registry["trees"]} == {1: "active", 2: "missing"}


def test_duplicate_run_tree_ids_are_rejected():
    with pytest.raises(ValueError, match="duplicate"):
        treeids.assign([tree(1, 0, 0), tree(1, 5, 5)], treeids.new_registry(), "run-001")


def test_labels_follow_lasting_ids_and_skip_review_or_missing():
    id_rows = [{"tree_id": "4", "stable_id": "1", "review": ""},
               {"tree_id": "5", "stable_id": "2", "review": "possible_split"}]
    labels = [{"stable_id": "1", "label": "oak", "group": "A1"},
              {"stable_id": "2", "label": "pine"},
              {"stable_id": "9", "label": "maple"},
              {"stable_id": "1 ", "label": ""}]
    rows, skipped = treeids.labels_for_run(id_rows, labels)
    assert rows == [{"tree_id": "4", "label": "oak", "group": "A1"}]
    assert skipped == ["2: possible_split", "9: not found in this run"]


def test_registry_requires_projected_metres():
    with pytest.raises(ValueError, match="metres"):
        treeids.new_registry(CRS.from_epsg(4326))


# Real CLI runs on the synthetic forest.

def cli(*args):
    return subprocess.run([sys.executable, "-m", "treedar", *map(str, args)],
                          capture_output=True, text=True, timeout=120)


def ok(result):
    assert result.returncode == 0, result.stdout + result.stderr
    return result


def id_table(run_dir):
    with (run_dir / "tree_ids.csv").open(newline="") as stream:
        return list(csv.DictReader(stream))


def lasting_ids_by_position(run_dir):
    """Map each truth tree to the lasting ID of the detection nearest to it."""
    with (run_dir / "trees.csv").open(newline="") as stream:
        trees = {r["tree_id"]: r for r in csv.DictReader(stream)}
    ids = {r["tree_id"]: r["stable_id"] for r in id_table(run_dir)}
    out = {}
    for truth in TREES:
        tid = min(trees, key=lambda t: (float(trees[t]["x"]) - truth.x) ** 2
                  + (float(trees[t]["y"]) - truth.y) ** 2)
        out[(truth.x, truth.y)] = ids[tid]
    return out


@pytest.fixture
def forest(tmp_path):
    source = tmp_path / "forest.laz"
    write_forest(source)
    return source


@pytest.mark.parametrize("settings", [
    ["--cell", "0.4"], ["--cell", "1.0"], ["--step", "2"], ["--preset", "conifer"],
    ["--min-height", "3", "--spike-threshold", "0"],
], ids=["cell-0.4", "cell-1.0", "step-2", "conifer", "low-min-height"])
def test_rerun_with_other_settings_keeps_every_tree_id(tmp_path, forest, settings):
    registry = tmp_path / "ids.json"
    first, second = tmp_path / "run-a", tmp_path / "run-b"
    ok(cli("run", forest, "--out", first, "--no-qc", "--ids-registry", registry))
    ok(cli("run", forest, "--out", second, "--no-qc", "--ids-registry", registry, *settings))
    assert {r["match"] for r in id_table(first)} == {"seed"}
    rows = id_table(second)
    assert len(rows) == len(TREES)
    assert {r["match"] for r in rows} == {"matched"} and not any(r["review"] for r in rows)
    assert lasting_ids_by_position(second) == lasting_ids_by_position(first)
    summary = json.loads((second / "summary.json").read_text())
    assert summary["tree_ids"]["matched"] == len(TREES)


def test_missing_tree_returns_with_its_id_and_labels_carry_over(tmp_path, forest):
    registry = tmp_path / "ids.json"
    full, partial, back = tmp_path / "full", tmp_path / "partial", tmp_path / "back"
    ok(cli("run", forest, "--out", full, "--no-qc", "--ids-registry", registry))
    original = lasting_ids_by_position(full)

    fewer = tmp_path / "fewer.laz"
    write_forest(fewer, trees=TREES[:2])
    ok(cli("run", fewer, "--out", partial, "--no-qc"))
    result = ok(cli("ids", partial, "--registry", registry))
    assert "2 matched, 0 new, 1 registered trees missing" in result.stdout
    gone = original[(TREES[2].x, TREES[2].y)]
    status = {str(t["id"]): t["status"] for t in json.loads(registry.read_text())["trees"]}
    assert status[gone] == "missing"

    ok(cli("run", forest, "--out", back, "--no-qc"))
    labels = tmp_path / "stable_labels.csv"
    labels.write_text("stable_id,label\n" + "".join(
        f"{sid},{name}\n" for sid, name in zip(original.values(), ["oak", "pine", "maple"])))
    result = ok(cli("ids", back, "--registry", registry, "--labels", labels))
    assert lasting_ids_by_position(back) == original
    assert "3 labels" in result.stdout
    with (back / "labels.csv").open(newline="") as stream:
        written = {r["tree_id"]: r["label"] for r in csv.DictReader(stream)}
    ids = {r["stable_id"]: r["tree_id"] for r in id_table(back)}
    assert written == {ids[sid]: name
                       for sid, name in zip(original.values(), ["oak", "pine", "maple"])}


def test_ids_twice_on_one_run_does_not_register_it_again(tmp_path, forest):
    registry, run = tmp_path / "ids.json", tmp_path / "run"
    ok(cli("run", forest, "--out", run, "--no-qc"))
    ok(cli("ids", run, "--registry", registry))
    before = (run / "tree_ids.csv").read_text()
    ok(cli("ids", run, "--registry", registry))
    assert (run / "tree_ids.csv").read_text() == before
    assert len(json.loads(registry.read_text())["runs"]) == 1


def test_registry_in_another_crs_is_refused_and_left_unchanged(tmp_path, forest):
    registry, run = tmp_path / "ids.json", tmp_path / "run"
    treeids.save_registry(registry, treeids.new_registry(CRS.from_epsg(32615)))
    before = registry.read_text()
    ok(cli("run", forest, "--out", run, "--no-qc"))
    result = cli("ids", run, "--registry", registry)
    assert result.returncode == 2 and "CRS differs" in result.stderr
    assert registry.read_text() == before and not (run / "tree_ids.csv").exists()


def test_pipeline_lists_tree_ids_without_leaking_the_registry_path(tmp_path, forest):
    registry, run = tmp_path / "ids.json", tmp_path / "run"
    ok(cli("pipeline", forest, "--out", run, "--no-qc", "--ids-registry", registry))
    manifest = json.loads((run / "manifest.json").read_text())
    roles = {item["path"]: item["role"] for item in manifest["outputs"]}
    assert roles["tree_ids.csv"] == "lasting_tree_ids"
    assert str(registry) not in json.dumps(manifest)
