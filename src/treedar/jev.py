"""Compare TypeSafe Jev against the local classifier on the same trees.

Each tree is sent as a small text/JSON description of its measured features
with a Choice question over the tree types plus "unknown". No coordinates,
tree ids, imagery or file names are sent. Jev answers are predictions, never
labels. Requires the TYPESAFE_API_KEY environment variable (on Windows the
user-level value is also read if the current process predates it).
"""

from __future__ import annotations

import base64
import csv
import json
import os
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np

from . import classify

API_URL = "https://api.typesafe.ai/v1/systemone"
PRICE_PER_M_INPUT_TOKENS = 0.042  # vendor list price, USD, checked 2026-10-16

DEFAULT_CLASSES = {
    "conifer": "Needle-leaved tree such as pine, spruce, fir, hemlock or cedar",
    "broadleaf": "Broad-leaved deciduous tree such as oak, maple, aspen, birch or ash",
}

CONTEXT = ("Drone LiDAR survey in Wisconsin, flown 1 July (leaf-on). One detected tree crown. "
           "Heights are above ground. Relative heights are fractions of the tree's top height. "
           "Colour fractions come from the LiDAR unit's own camera, are uncalibrated, and are "
           "averaged over the upper half of the crown. Second-return fraction is the share of "
           "laser points that were a second echo, a rough measure of how porous the crown is.")


def api_key() -> str:
    key = os.environ.get("TYPESAFE_API_KEY", "").strip()
    if not key and os.name == "nt":
        import winreg
        try:
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as k:
                key = str(winreg.QueryValueEx(k, "TYPESAFE_API_KEY")[0]).strip()
        except OSError:
            key = ""
    if not key:
        raise ValueError("TYPESAFE_API_KEY is not set")
    return key


def _num(v, digits=3):
    return None if v in ("", None) else round(float(v), digits)


def tree_state(tree: dict, feat: dict) -> dict:
    """Description of one tree without location or identifiers."""
    return {
        "survey": CONTEXT,
        "tree_height_m": _num(tree["height_m"], 1),
        "crown_diameter_m": _num(tree["crown_diameter_m"], 1),
        "crown_area_m2": _num(tree["crown_area_m2"], 1),
        "relative_height_of_points": {"median": _num(feat.get("hag_p50_rel")),
                                      "p75": _num(feat.get("hag_p75_rel")),
                                      "p90": _num(feat.get("hag_p90_rel")),
                                      "coefficient_of_variation": _num(feat.get("hag_cv"))},
        "second_return_fraction": _num(feat.get("second_return_frac")),
        "upper_crown_colour": {"red_fraction": _num(feat.get("rcc_mean")),
                               "green_fraction": _num(feat.get("gcc_mean")),
                               "blue_fraction": _num(feat.get("bcc_mean")),
                               "excess_green": _num(feat.get("exg_mean")),
                               "green_fraction_std": _num(feat.get("gcc_std"))},
    }


IMAGE_REF = "crown-chip-7f3a"


def request_body(state: dict, classes: dict, model: str, image_png: bytes | None = None) -> dict:
    """Choice question over tree types plus unknown. With image_png, the crown's
    top-down image (outlined in yellow) is attached as an asset referenced from
    state; requests with images need the TypeSafe danger-zone image header."""
    criteria = dict(classes)
    criteria[classify.UNKNOWN] = "The measurements do not clearly indicate any of the other types"
    instructions = "Which type of tree is this, based on its measurements in `survey` context?"
    body = {"model": model, "state": dict(state), "questions": {}}
    if image_png is not None:
        body["state"]["crown_image"] = IMAGE_REF
        body["assets"] = {IMAGE_REF: {"type": "image", "format": "png",
                                      "data_base64": base64.b64encode(image_png).decode()}}
        instructions = ("Which type of tree is this? Use its measurements and `crown_image`, a "
                        "top-down view of the crown (outlined in yellow) from the same survey.")
    body["questions"]["tree_type"] = {"type": "choice", "instructions": instructions,
                                      "criteria": criteria}
    return body


def _post(body: dict, key: str, retries: int = 4) -> dict:
    data = json.dumps(body).encode()
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    if "assets" in body:
        headers["TypeSafe-Danger-Zone-Images"] = "true"  # experimental image support
    for attempt in range(retries + 1):
        req = urllib.request.Request(API_URL, data=data, method="POST", headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=60) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            if exc.code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(f"TypeSafe API error {exc.code}: {exc.read()[:200]!r}") from None
        except urllib.error.URLError:
            if attempt < retries:
                time.sleep(2 ** attempt)
                continue
            raise
    raise RuntimeError("unreachable")


def run(trees_csv, features_csv, labels_csv, out_dir, classes=None, model="jev-latest",
        workers=4, include_unlabelled=False, min_accuracy=0.85, min_coverage=0.70,
        post=None, split_csv=None, chips_dir=None) -> dict:
    """Ask Jev about each (labelled) tree and score its answers.

    With `split_csv` (classify's predictions.csv), the confidence threshold is
    chosen on the development trees and scored once on the same final test
    trees as the local classifier.

    `post(body) -> response dict` can be injected for tests; by default the
    real API is called with TYPESAFE_API_KEY.
    """
    classes = classes or DEFAULT_CLASSES
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    trees = {r["tree_id"]: r for r in classify._read_csv(trees_csv)}
    feats = {r["tree_id"]: r for r in classify._read_csv(features_csv)}
    labels = {r["tree_id"]: r["label"].strip() for r in classify._read_csv(labels_csv)
              if (r.get("label") or "").strip()}
    ids = [t for t in trees if t in feats and (include_unlabelled or t in labels)]
    if not ids:
        raise ValueError("no trees to send (no labelled trees with features)")
    if post is None:
        key = api_key()
        post = lambda body: _post(body, key)  # noqa: E731

    def ask(tid):
        image = None
        if chips_dir is not None:
            chip = Path(chips_dir) / f"tree_{tid}.png"
            image = chip.read_bytes() if chip.exists() else None
        return post(request_body(tree_state(trees[tid], feats[tid]), classes, model, image))

    with ThreadPoolExecutor(max_workers=workers) as pool:
        responses = list(pool.map(ask, ids))

    names = list(classes) + [classify.UNKNOWN]
    rows, tokens_in, tokens_out, model_used = [], 0, 0, None
    for tid, resp in zip(ids, responses):
        ans = resp["answers"]["tree_type"]
        probs = ans.get("probabilities", {})
        tokens_in += resp.get("usage", {}).get("input_tokens", 0)
        tokens_out += resp.get("usage", {}).get("output_tokens", 0)
        model_used = resp.get("model", model_used)
        rows.append({"tree_id": tid, "predicted": ans["choice"],
                     "confidence": ans.get("confidence"), "label": labels.get(tid, ""),
                     **{f"p_{c}": probs.get(c, 0.0) for c in names}})
    with open(out / "jev_predictions.csv", "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    report = {"model": model_used, "trees_sent": len(ids), "with_images": chips_dir is not None,
              "input_tokens": tokens_in,
              "output_tokens": tokens_out,
              "estimated_cost_usd": round(tokens_in / 1e6 * PRICE_PER_M_INPUT_TOKENS, 6),
              "price_note": "input-token list price only; check the TypeSafe bill for the actual amount"}
    lab = [r for r in rows if r["label"]]
    if lab:
        y = np.array([r["label"] for r in lab], dtype=object)
        cls = np.array(sorted(set(y)), dtype=object)
        # Probability mass on "unknown" or on classes absent from the labels is not a
        # vote for any labelled class; renormalising would hide abstention, so use raw p.
        proba = np.array([[r.get(f"p_{c}", 0.0) for c in cls] for r in lab])
        report["evaluation"] = classify.evaluate(y, cls, proba, min_accuracy, min_coverage)
        report["evaluation"]["jev_chose_unknown"] = int(sum(r["predicted"] == classify.UNKNOWN for r in lab))
        if split_csv:
            part = {r["tree_id"]: r.get("partition", "") for r in classify._read_csv(split_csv)}
            dev = np.array([part.get(r["tree_id"]) == "development" for r in lab])
            test = np.array([part.get(r["tree_id"]) == "final_test" for r in lab])
            if dev.any() and test.any():
                dev_eval = classify.evaluate(y[dev], cls, proba[dev], min_accuracy, min_coverage)
                threshold = dev_eval["chosen"]["threshold"] if dev_eval["chosen"] else 1.01
                report["development"] = dev_eval
                report["final_test"] = classify.at_threshold(y[test], cls, proba[test], threshold)
    (out / "jev_report.json").write_text(json.dumps(report, indent=2))
    return report
