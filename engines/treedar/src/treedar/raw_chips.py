"""Original-photo crown crops, ranked by overhead viewing angle."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import rasterio
from PIL import Image, ImageDraw
from scipy import ndimage
from scipy.spatial import cKDTree
from skimage.measure import find_contours

from .photo_geometry import PhotoScene, load_surface


def _integer(value, name):
    try:
        number = int(value)
        if isinstance(value, (bool, np.bool_)) or (not isinstance(value, str) and number != value):
            raise ValueError
        return number
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _spread(scored, views):
    """Most overhead view first, then each next view as far in viewing
    direction from those already chosen as possible, so a tree is seen from
    several sides rather than from near-identical overhead shots."""
    chosen = [0]
    dirs = np.array([row[5] for row in scored])
    while len(chosen) < min(views, len(scored)):
        # Smallest angle to any chosen direction; take the candidate maximising it.
        sep = np.arccos(np.clip(dirs @ dirs[chosen].T, -1, 1)).min(axis=1)
        sep[chosen] = -1
        chosen.append(int(np.argmax(sep)))
    return [scored[k] for k in chosen]


def generate_raw_chips(opensfm_dir, images_dir, run_dir, out_dir, tree_ids=None,
                       views=3, search_radius_m=40, edge_margin=40, select="overhead") -> dict:
    """Write tree_ID_vK.jpg and views.json, without modifying originals.

    Complete finite crowns must be visible in front of an above-canopy camera.
    select="overhead" keeps the most overhead views; select="spread" keeps the
    most overhead view, then views from directions as different as possible.
    """
    views = _integer(views, "views")
    if views < 1:
        raise ValueError("views must be a positive integer")
    if select not in ("overhead", "spread"):
        raise ValueError("select must be 'overhead' or 'spread'")
    if not np.isfinite(search_radius_m) or search_radius_m <= 0:
        raise ValueError("search radius must be finite and positive")
    if not np.isfinite(edge_margin) or edge_margin < 0:
        raise ValueError("edge margin must be finite and nonnegative")
    run_dir, out = Path(run_dir), Path(out_dir)
    images_root = Path(images_dir).resolve()
    if out.resolve().is_relative_to(images_root):
        raise ValueError("chip output must be outside the original images directory")
    dsm, transform, crs = load_surface(run_dir)
    with rasterio.open(run_dir / "crowns.tif") as ds:
        if ds.shape != dsm.shape or ds.transform != transform or ds.crs != crs:
            raise ValueError("crowns and surface grids/CRS differ")
        crowns = ds.read(1, masked=True).filled(0)
    if not np.issubdtype(crowns.dtype, np.integer):
        raise ValueError("crown labels must be integers")
    wanted = None if tree_ids is None else {_integer(t, "tree ID") for t in tree_ids}
    if wanted is not None:
        wanted = {tid for tid in wanted if tid > 0}
    scene = PhotoScene(opensfm_dir, images_dir, crs)
    centres = np.asarray(scene.centres)
    index = cKDTree(centres[:, :2]) if len(centres) else None
    result = {}
    out.mkdir(parents=True, exist_ok=True)
    # Compress labels first: sparse external IDs must not allocate max(ID)
    # slices. Each crown is then inspected only inside its bounding box.
    ids, inverse = np.unique(crowns, return_inverse=True)
    slices = ndimage.find_objects(inverse.reshape(crowns.shape) + 1)
    for tid, bounds in zip(ids, slices):
        tid = int(tid)
        if tid <= 0 or (wanted is not None and tid not in wanted):
            continue
        if bounds is None:
            continue
        r0, c0 = bounds[0].start, bounds[1].start
        mask = crowns[bounds] == tid
        local_r, local_c = np.nonzero(mask)
        rr, cc = local_r + r0, local_c + c0
        z = dsm[rr, cc]
        if not np.isfinite(z).all():
            continue
        # A zero border closes outlines even at the raster edge.
        rings = [ring - 1 for ring in find_contours(np.pad(mask, 1).astype(float), 0.5)]
        if not rings:
            continue
        _, nearest = ndimage.distance_transform_edt(~mask, return_indices=True)
        points, lengths = [], []
        for ring in rings:
            ir = np.clip(np.rint(ring[:, 0]).astype(int), 0, mask.shape[0] - 1)
            ic = np.clip(np.rint(ring[:, 1]).astype(int), 0, mask.shape[1] - 1)
            ring_z = dsm[nearest[0, ir, ic] + r0, nearest[1, ir, ic] + c0]
            e, n = transform * (ring[:, 1] + c0 + 0.5, ring[:, 0] + r0 + 0.5)
            points.append(scene.to_topo(e, n, ring_z))
            lengths.append(len(ring))
        outline = np.concatenate(points)
        apex = int(np.argmax(z))
        e, n = transform * (np.array([cc[apex] + 0.5]), np.array([rr[apex] + 0.5]))
        top = scene.to_topo(e, n, z[apex:apex + 1])[0]
        if not np.isfinite(outline).all() or not np.isfinite(top).all() or index is None:
            continue
        scored = []
        for i in index.query_ball_point(top[:2], r=search_radius_m):
            delta = centres[i] - top
            if not np.isfinite(delta).all() or centres[i, 2] <= max(top[2], outline[:, 2].max()):
                continue
            u, v, front = scene.project(i, outline)
            u, v = np.asarray(u), np.asarray(v)
            camera = scene.camera(i)
            width, height = camera["width"], camera["height"]
            if (np.asarray(front).all() and np.isfinite(u).all() and np.isfinite(v).all()
                    and u.min() > edge_margin and u.max() < width - edge_margin
                    and v.min() > edge_margin and v.max() < height - edge_margin):
                angle = float(np.degrees(np.arctan2(np.hypot(*delta[:2]), delta[2])))
                scored.append((angle, scene.names[i], i, u, v, delta / np.linalg.norm(delta)))
        scored.sort(key=lambda row: (row[0], row[1]))
        picked = _spread(scored, views) if select == "spread" and scored else scored[:views]
        for rank, (angle, name, i, u, v, direction) in enumerate(picked):
            image = Image.fromarray(scene.photo(i))
            span = max(float(np.ptp(u)), float(np.ptp(v)))
            side = max(1, int(np.ceil(span * 1.5 + 60)))
            cx, cy = (u.min() + u.max()) / 2, (v.min() + v.max()) / 2
            left, upper = int(np.floor(cx - side / 2)), int(np.floor(cy - side / 2))
            chip = image.crop((left, upper, left + side, upper + side))
            draw = ImageDraw.Draw(chip)
            start = 0
            for length in lengths:
                ring = list(zip(u[start:start + length] - left, v[start:start + length] - upper))
                draw.line(ring + ring[:1], fill=(255, 214, 0), width=max(2, side // 160))
                start += length
            filename = f"tree_{tid}_v{rank}.jpg"
            chip.save(out / filename, quality=90)
            result.setdefault(str(tid), []).append({
                "view": rank, "photo": name, "off_nadir_deg": round(angle, 1),
                # Compass bearing from the tree to the camera (topocentric east/north).
                "azimuth_deg": round(float(np.degrees(np.arctan2(direction[0], direction[1]))) % 360, 1),
                "filename": filename,
            })
    (out / "views.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result
