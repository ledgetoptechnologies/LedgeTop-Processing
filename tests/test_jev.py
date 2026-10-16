"""Offline Jev integration: no key access and no network requests allowed."""

import csv
import json

import pytest

from treedar import cli, jev


@pytest.fixture(autouse=True)
def forbid_live_jev(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Jev tests must not access credentials or the network")
    monkeypatch.setattr(jev, "api_key", forbidden)
    monkeypatch.setattr(jev, "_post", forbidden)
    monkeypatch.setattr(jev.urllib.request, "urlopen", forbidden)


def write_csv(path, columns, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture
def tree_tables(tmp_path):
    trees = tmp_path / "private-file-trees.csv"
    features = tmp_path / "private-file-features.csv"
    labels = tmp_path / "private-file-labels.csv"
    ids = [f"private-tree-identifier-{n}" for n in range(4)]
    tree_rows = [dict(tree_id=tid, x=12345678.901 + n, y=87654321.234 + n,
                      height_m=11 + n, crown_area_m2=20, crown_diameter_m=5)
                 for n, tid in enumerate(ids)]
    write_csv(trees, list(tree_rows[0]), tree_rows)
    feature_rows = [dict(tree_id=tid, hag_p50_rel=.3, hag_p75_rel=.6, hag_p90_rel=.9,
                         hag_cv=.4, second_return_frac=.2, rcc_mean=.2, gcc_mean=.6,
                         bcc_mean=.2, exg_mean=.8, gcc_std=.1,
                         x=12345678.901, y=87654321.234, private_note="private-file-secret")
                    for tid in ids]
    write_csv(features, list(feature_rows[0]), feature_rows)
    write_csv(labels, ["tree_id", "label"], [
        {"tree_id": ids[0], "label": "conifer"},
        {"tree_id": ids[1], "label": "broadleaf"},
        {"tree_id": ids[2], "label": "conifer"},
    ])
    return trees, features, labels, tree_rows


def response_for_height(height):
    data = {
        11: ("unknown", {"conifer": .1, "broadleaf": .1, "unknown": .8}, 101, 2),
        12: ("broadleaf", {"conifer": .2, "broadleaf": .7, "unknown": .1}, 203, 3),
        13: ("conifer", {"conifer": .8, "broadleaf": .1, "unknown": .1}, 307, 5),
        14: ("unknown", {"conifer": .05, "broadleaf": .05, "unknown": .9}, 409, 7),
    }
    choice, probabilities, tokens_in, tokens_out = data[height]
    return {"model": "offline-fixture-model", "answers": {"tree_type": {
        "choice": choice, "confidence": probabilities[choice], "probabilities": probabilities,
    }}, "usage": {"input_tokens": tokens_in, "output_tokens": tokens_out}}


def test_requests_exclude_identifiers_and_preserve_unknown_probability(tree_tables, tmp_path):
    trees, features, labels, source_rows = tree_tables
    requests = []

    def fake_post(body):
        requests.append(body)
        return response_for_height(body["state"]["tree_height_m"])

    out = tmp_path / "jev"
    report = jev.run(trees, features, labels, out, post=fake_post, workers=1)
    assert len(requests) == report["trees_sent"] == 3
    assert report["model"] == "offline-fixture-model"
    for body in requests:
        serialized = json.dumps(body)
        for forbidden in ('"x"', '"y"', "tree_id", "private-file", "private-tree-identifier"):
            assert forbidden not in serialized
        for row in source_rows:
            for key in ("x", "y"):
                assert str(row[key]) not in serialized
        question = body["questions"]["tree_type"]
        assert question["type"] == "choice"
        assert set(question["criteria"]) == {"conifer", "broadleaf", "unknown"}

    assert report["input_tokens"] == 611
    assert report["output_tokens"] == 10
    assert report["estimated_cost_usd"] == pytest.approx(round(611 / 1e6 * .042, 6))
    evaluation = report["evaluation"]
    assert evaluation["n_labelled"] == 3
    assert evaluation["jev_chose_unknown"] == 1
    at_half = next(row for row in evaluation["coverage_table"] if row["threshold"] == .5)
    # The .8 unknown response leaves only .1 for either known class. Its
    # probabilities must not be renormalized to .5 and counted as covered.
    assert at_half["n_labelled"] == 2
    assert at_half["coverage"] == pytest.approx(2 / 3, abs=1e-4)
    assert at_half["accuracy"] == 1
    with (out / "jev_predictions.csv").open(newline="") as stream:
        predictions = list(csv.DictReader(stream))
    assert [row["tree_id"] for row in predictions] == [row["tree_id"] for row in source_rows[:3]]
    assert predictions[0]["predicted"] == "unknown"
    assert float(predictions[0]["p_unknown"]) == .8
    assert json.loads((out / "jev_report.json").read_text()) == report


def test_all_trees_sends_unlabelled_but_scores_only_labels(tree_tables, tmp_path):
    trees, features, labels, source_rows = tree_tables
    requests = []

    def fake_post(body):
        requests.append(body)
        return response_for_height(body["state"]["tree_height_m"])

    report = jev.run(trees, features, labels, tmp_path / "all", post=fake_post,
                     workers=1, include_unlabelled=True)
    assert len(requests) == report["trees_sent"] == len(source_rows)
    assert report["evaluation"]["n_labelled"] == 3
    assert report["input_tokens"] == 1020
    assert report["output_tokens"] == 17


def test_cli_missing_key_fails_without_registry_or_network_access(tree_tables, tmp_path, monkeypatch, capsys):
    trees, features, labels, _ = tree_tables
    monkeypatch.setenv("TYPESAFE_API_KEY", "")

    def missing_key():
        # Replace the whole resolver, including Windows registry fallback.
        raise ValueError("TYPESAFE_API_KEY is not set")

    monkeypatch.setattr(jev, "api_key", missing_key)
    out = tmp_path / "missing-key"
    status = cli.main(["jev", "--trees", str(trees), "--features", str(features),
                       "--labels", str(labels), "--out", str(out)])
    assert status == 2
    assert "TYPESAFE_API_KEY is not set" in capsys.readouterr().err
    assert not (out / "jev_report.json").exists()
