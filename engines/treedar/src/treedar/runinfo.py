"""Run folder manifest and human-readable report.

A run folder is self-describing: `manifest.json` lists inputs, settings,
software version and every output with its role and format (for later import
into the 3D Viewer), and `report.html` summarises the run for a person. Both
can contain property information and stay out of Git.
"""

from __future__ import annotations

import base64
import csv
import datetime as dt
import html
import json
import subprocess
from pathlib import Path

import numpy as np

from . import __version__

ROLES = {
    "dtm.tif": ("ground_model", "cog"), "chm.tif": ("canopy_height", "cog"),
    "chm_raw.tif": ("canopy_height_unfiltered", "cog"), "crowns.tif": ("crown_labels", "cog"),
    "areas.tif": ("area_labels", "cog"), "ortho_rgb.tif": ("orthophoto_point_colour", "cog"),
    "tree_map.geojson": ("crowns", "geojson"), "trees.csv": ("tree_table", "csv"),
    "tree_features.csv": ("tree_features", "csv"), "tree_areas.csv": ("tree_areas", "csv"),
    "tree_ids.csv": ("lasting_tree_ids", "csv"),
    "area_types.csv": ("area_types_template", "csv"), "labels.csv": ("labels", "csv"),
    "qc.json": ("quality_report", "json"), "summary.json": ("run_summary", "json"),
    "tree_map.png": ("preview", "png"), "areas.png": ("area_map_preview", "png"),
    "classify/predictions.csv": ("tree_type_predictions", "csv"),
    "classify/classify_report.json": ("classifier_report", "json"),
    "classify/final_test_predictions.csv": ("final_test_predictions", "csv"),
    "report.html": ("report", "html"),
}


def _git_commit() -> str | None:
    try:
        out = subprocess.run(["git", "rev-parse", "--short", "HEAD"], capture_output=True,
                             text=True, timeout=5, cwd=Path(__file__).resolve().parent)
        return out.stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def write_manifest(run_dir, input_path=None, params=None, command=None) -> dict:
    run_dir = Path(run_dir)
    outputs = []
    for rel, (role, fmt) in ROLES.items():
        p = run_dir / rel
        if p.exists() and rel != "report.html":
            outputs.append({"path": rel, "role": role, "format": fmt, "bytes": p.stat().st_size})
    manifest = {
        "treedar_version": __version__,
        "git_commit": _git_commit(),
        "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "command": command,
        "params": params or {},
        "outputs": outputs + [{"path": "report.html", "role": "report", "format": "html"}],
    }
    if input_path:
        ip = Path(input_path)
        manifest["input"] = {"name": ip.name, "bytes": ip.stat().st_size if ip.exists() else None}
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def _load(path):
    p = Path(path)
    return json.loads(p.read_text()) if p.exists() else None


def _img(path) -> str:
    p = Path(path)
    if not p.exists():
        return ""
    data = base64.b64encode(p.read_bytes()).decode()
    return f'<img alt="{html.escape(p.stem)}" src="data:image/png;base64,{data}">'


def _table(rows, cols) -> str:
    head = "".join(f"<th>{html.escape(c)}</th>" for c in cols)
    body = "".join("<tr>" + "".join(f"<td>{html.escape(str(r.get(c, '')))}</td>" for c in cols)
                   + "</tr>" for r in rows)
    return f"<table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"


def _pct(v):
    return "n/a" if v is None else f"{v:.1%}"


def write_report(run_dir) -> Path:
    run_dir = Path(run_dir)
    qc = _load(run_dir / "qc.json")
    summary = _load(run_dir / "summary.json")
    manifest = _load(run_dir / "manifest.json") or {}
    cls = _load(run_dir / "classify" / "classify_report.json")
    jev = _load(run_dir / "jev" / "jev_report.json")
    sections = []

    facts = []
    if summary:
        facts += [("Trees detected", f"{summary['n_trees']:,}"),
                  ("Points used", f"{summary['n_points_used']:,}"),
                  ("Processing time", f"{summary['runtime_s']} s")]
    if qc:
        d = qc["density"]
        facts += [("Point count", f"{qc['point_count']:,}"), ("Coordinate system", f"EPSG:{qc['crs_epsg']}"),
                  ("Median density", f"{d['median']:.0f} points/m2 (all returns)")]
        sa = qc.get("strip_alignment") or {}
        if sa.get("compared_cells"):
            facts.append(("Flight line agreement", f"{sa['abs_offset_median_m'] * 100:.1f} cm median, "
                          f"{sa['abs_offset_p95_m'] * 100:.1f} cm 95th percentile "
                          f"({sa['flight_lines']} lines, {sa['compared_cells']:,} smooth cells)"))
    facts += [("TreeDar version", f"{manifest.get('treedar_version', __version__)} "
               f"({manifest.get('git_commit') or 'commit unknown'})"),
              ("Created", manifest.get("created_utc", ""))]
    sections.append("<h2>Run</h2>" + _table([{"Item": k, "Value": v} for k, v in facts], ["Item", "Value"]))

    trees_csv = run_dir / "trees.csv"
    if trees_csv.exists():
        rows = list(csv.DictReader(open(trees_csv, newline="")))
        if rows:
            h = np.array([float(r["height_m"]) for r in rows])
            d = np.array([float(r["crown_diameter_m"]) for r in rows])
            stats = [{"Measure": "Height (m)", **{k: f"{v:.1f}" for k, v in zip(
                         ("5%", "median", "95%", "max"), np.percentile(h, [5, 50, 95, 100]))}},
                     {"Measure": "Crown diameter (m)", **{k: f"{v:.1f}" for k, v in zip(
                         ("5%", "median", "95%", "max"), np.percentile(d, [5, 50, 95, 100]))}}]
            sections.append("<h2>Trees</h2>" + _table(stats, ["Measure", "5%", "median", "95%", "max"])
                            + _img(run_dir / "tree_map.png"))

    if cls:
        cv, ft = cls["cross_validation"], cls.get("final_test")
        rows = [{"Set": "Development (cross-validated)", "Trees": cv["n_labelled"],
                 "Accuracy, no unknowns": _pct(cv["accuracy_all"]),
                 "At chosen threshold": (f"{_pct(cv['chosen']['accuracy'])} correct, "
                                         f"{_pct(cv['chosen']['coverage'])} labelled") if cv["chosen"] else "none"}]
        if ft:
            at = ft["at_threshold"]
            rows.append({"Set": "Final test (held-out areas)", "Trees": at["n"],
                         "Accuracy, no unknowns": _pct(ft["accuracy_all"]),
                         "At chosen threshold": f"{_pct(at['accuracy'])} correct, {_pct(at['coverage'])} labelled"})
        cal = cls["cross_validation"]["calibration"]
        sections.append(
            f"<h2>Tree type classifier</h2><p>{cls['labelled_blocks']} label groups "
            f"({html.escape(cls['grouping'])}): {cls['development_groups']} development, "
            f"{cls['final_test_groups']} final test. Threshold {cls['threshold_used']:.2f}.</p>"
            + _table(rows, ["Set", "Trees", "Accuracy, no unknowns", "At chosen threshold"])
            + "<h3>Calibration (development)</h3>"
            + _table(cal, ["confidence_from", "confidence_to", "n", "mean_confidence", "observed_accuracy"]))
    if jev:
        ft = jev.get("final_test")
        jtxt = (f"{jev['trees_sent']} trees sent ({'with' if jev.get('with_images') else 'without'} images), "
                f"{jev['input_tokens']:,} input tokens, about ${jev['estimated_cost_usd']:.4f}.")
        if ft:
            jtxt += f" Final test: {_pct(ft['accuracy'])} correct, {_pct(ft['coverage'])} labelled on {ft['n']} trees."
        sections.append(f"<h2>Jev comparison</h2><p>{html.escape(jtxt)}</p>")

    if (run_dir / "areas.png").exists():
        sections.append("<h2>Named areas</h2>" + _img(run_dir / "areas.png"))
    outs = manifest.get("outputs", [])
    if outs:
        sections.append("<h2>Files</h2>" + _table(outs, ["path", "role", "format", "bytes"]))

    page = REPORT.replace("__BODY__", "\n".join(sections)).replace(
        "__TITLE__", html.escape(run_dir.name))
    out = run_dir / "report.html"
    out.write_text(page, encoding="utf-8")
    return out


REPORT = """<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>TreeDar Run Report</title>
<style>
:root { --bg:#f7f8f6; --fg:#1d2a1f; --muted:#5f6b60; --line:#d9dfd7; --card:#fff; }
@media (prefers-color-scheme: dark) { :root { --bg:#141814; --fg:#e7ede6; --muted:#9aa59a; --line:#334033; --card:#1d221d; } }
body { margin:0; background:var(--bg); color:var(--fg); font:15px/1.5 system-ui, sans-serif; }
main { max-width:1000px; margin:0 auto; padding:24px 16px 48px; }
h1 { font-size:24px; margin:0 0 4px; } h2 { font-size:18px; margin:32px 0 8px; } h3 { font-size:15px; }
p.sub { color:var(--muted); margin:0; }
table { border-collapse:collapse; width:100%; background:var(--card); font-size:14px; display:block; overflow-x:auto; }
th, td { text-align:left; padding:6px 10px; border-bottom:1px solid var(--line); white-space:nowrap; }
img { max-width:100%; height:auto; margin-top:12px; border:1px solid var(--line); border-radius:6px; }
</style></head><body><main>
<h1>TreeDar Run Report</h1>
<p class="sub">__TITLE__ &middot; contains property data: keep local</p>
__BODY__
</main></body></html>
"""
