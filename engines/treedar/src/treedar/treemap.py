"""Per-tree map outputs: crown polygons with attributes, and a preview image."""

from __future__ import annotations

import csv
import json
from pathlib import Path

import numpy as np
import rasterio
from rasterio import features as rfeatures

TYPE_COLOURS = {"conifer": "#1b6e3a", "broadleaf": "#9bd34a", "unknown": "#b0b0b0"}


def _read(path):
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh))


def write_geojson(run_dir, out_path, predictions_csv=None) -> int:
    """Crown polygons from crowns.tif joined with trees.csv (and predictions)."""
    run_dir = Path(run_dir)
    trees = {r["tree_id"]: r for r in _read(run_dir / "trees.csv")}
    preds = {r["tree_id"]: r for r in _read(predictions_csv)} if predictions_csv else {}
    with rasterio.open(run_dir / "crowns.tif") as ds:
        labels = ds.read(1)
        transform, crs = ds.transform, ds.crs
    # A crown label can be split into disconnected pieces; collect them so each
    # tree is exactly one Polygon or MultiPolygon feature.
    pieces: dict[str, list] = {}
    for geom, value in rfeatures.shapes(labels, mask=labels > 0, transform=transform):
        pieces.setdefault(str(int(value)), []).append(geom["coordinates"])
    feats = []
    for tid, rings in sorted(pieces.items(), key=lambda kv: int(kv[0])):
        t = trees.get(tid)
        if t is None:
            continue
        geom = ({"type": "Polygon", "coordinates": rings[0]} if len(rings) == 1
                else {"type": "MultiPolygon", "coordinates": rings})
        props = {"tree_id": int(tid), "height_m": float(t["height_m"]),
                 "crown_area_m2": float(t["crown_area_m2"]),
                 "crown_diameter_m": float(t["crown_diameter_m"])}
        p = preds.get(tid)
        if p:
            props.update({"type": p["predicted"], "type_confidence": float(p["confidence"]),
                          "label": p.get("label", ""), "is_labelled": p.get("is_labelled") == "1"})
        feats.append({"type": "Feature", "geometry": geom, "properties": props})
    doc = {"type": "FeatureCollection", "features": feats}
    if crs is not None and crs.to_epsg():
        doc["crs"] = {"type": "name", "properties": {"name": f"urn:ogc:def:crs:EPSG::{crs.to_epsg()}"}}
    Path(out_path).write_text(json.dumps(doc))
    return len(feats)


def render_preview(run_dir, out_png, predictions_csv=None):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import ListedColormap
    from matplotlib.patches import Patch
    run_dir = Path(run_dir)
    with rasterio.open(run_dir / "chm.tif") as ds:
        chm = ds.read(1)
    with rasterio.open(run_dir / "crowns.tif") as ds:
        crowns = ds.read(1)
    fig, ax = plt.subplots(figsize=(12, 11))
    ax.imshow(chm, cmap="gray", vmin=0, vmax=max(5.0, float(np.percentile(chm, 99.5))))
    legend = []
    if predictions_csv:
        preds = _read(predictions_csv)
        names = list(TYPE_COLOURS) + sorted({p["predicted"] for p in preds} - set(TYPE_COLOURS))
        code = {n: i + 1 for i, n in enumerate(names)}
        lut = np.zeros(int(crowns.max()) + 1, dtype=np.int32)
        for p in preds:
            tid = int(p["tree_id"])
            if tid < lut.size:
                lut[tid] = code[p["predicted"]]
        typed = lut[crowns]
        colours = [TYPE_COLOURS.get(n, "#d95f02") for n in names]
        ax.imshow(np.ma.masked_where(typed == 0, typed), cmap=ListedColormap(colours),
                  vmin=1, vmax=len(names), alpha=0.75, interpolation="nearest")
        counts = {n: sum(p["predicted"] == n for p in preds) for n in names}
        legend = [Patch(color=c, label=f"{n} ({counts[n]})") for n, c in zip(names, colours) if counts[n]]
    else:
        ax.imshow(np.ma.masked_where(crowns == 0, crowns % 20), cmap="tab20", alpha=0.5,
                  interpolation="nearest")
    if legend:
        ax.legend(handles=legend, loc="lower left", fontsize=11)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.set_title("Tree map")
    fig.tight_layout()
    fig.savefig(out_png, dpi=75)
    plt.close(fig)
