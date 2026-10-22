"""Render aligned RGB photos on a TreeDar surface in bounded output tiles.

The surface is bilinearly sampled at output pixel centres. At the outer
half-cell, its edge sample is used only inside the raster's pixel footprint.
Missing surface samples and points uncovered by photos remain invalid in the
output mask; RGB zero itself is a valid colour, not a nodata sentinel.
"""

from __future__ import annotations

import math
from pathlib import Path
import time

import numpy as np
import rasterio
from rasterio.transform import array_bounds, from_origin
from rasterio.windows import Window
from scipy import ndimage
from scipy.spatial import cKDTree

from .photo_geometry import PhotoScene, load_surface


def _positive_integer(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, np.integer)) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return int(value)


def render_photo_ortho(opensfm_dir, images_dir, run_dir, out, cell=0.03,
                       bbox=None, candidates=6, edge_margin=20, tile_size=1024):
    """Write a tiled RGB GeoTIFF and return dimensions and coverage statistics.

    ``bbox`` is minx,miny,maxx,maxy in the run CRS and is clipped to the surface.
    Candidate cameras are ranked by horizontal distance in the scene's local
    frame; a camera that cannot cover a point yields to the next candidate.
    No colour balancing, occlusion detection, or seam blending is performed.
    """
    started = time.perf_counter()
    try:
        cell = float(cell)
        edge_margin = float(edge_margin)
    except (ValueError, TypeError) as exc:
        raise ValueError("cell and edge_margin must be finite numbers") from exc
    if not math.isfinite(cell) or cell <= 0:
        raise ValueError("cell must be finite and positive")
    if not math.isfinite(edge_margin) or edge_margin < 0:
        raise ValueError("edge_margin must be finite and nonnegative")
    candidates = _positive_integer(candidates, "candidates")
    tile_size = _positive_integer(tile_size, "tile_size")
    out = Path(out)
    output_path = out.resolve()
    if output_path.is_relative_to(Path(images_dir).resolve()):
        raise ValueError("output must stay outside the source images directory")
    protected = [Path(run_dir) / name for name in ("dtm.tif", "chm.tif", "chm_raw.tif", "crowns.tif")]
    protected.append(Path(opensfm_dir) / "reconstruction.json")
    if any(output_path == path.resolve() or
           (out.exists() and path.exists() and out.samefile(path)) for path in protected):
        raise ValueError("output must not overwrite a surface or reconstruction input")
    surface, transform, crs = load_surface(run_dir)
    surface = np.asarray(surface, dtype=np.float64)
    if surface.ndim != 2 or not all(surface.shape):
        raise ValueError("surface must be a nonempty two-dimensional grid")
    if (not np.isfinite(tuple(transform)).all() or transform.a <= 0 or transform.e >= 0
            or transform.b != 0 or transform.d != 0):
        raise ValueError("surface must have a finite north-up transform")
    bounds = np.asarray(array_bounds(*surface.shape, transform), dtype=float)
    if bbox is not None:
        try:
            requested = np.asarray(bbox, dtype=float)
        except (TypeError, ValueError) as exc:
            raise ValueError("bbox must contain four finite coordinates") from exc
        if (requested.shape != (4,) or not np.isfinite(requested).all()
                or requested[0] >= requested[2] or requested[1] >= requested[3]):
            raise ValueError("bbox must be minx,miny,maxx,maxy with min < max")
        bounds = np.array([max(bounds[0], requested[0]), max(bounds[1], requested[1]),
                           min(bounds[2], requested[2]), min(bounds[3], requested[3])])
    minx, miny, maxx, maxy = bounds
    if minx >= maxx or miny >= maxy:
        raise ValueError("bbox does not intersect the surface")
    dimensions = np.array([(maxx - minx) / cell, (maxy - miny) / cell])
    if not np.isfinite(dimensions).all() or (dimensions > 2**31 - 1).any():
        raise ValueError("output dimensions exceed GeoTIFF limits; increase cell size")
    width, height = map(math.ceil, dimensions)
    output_transform = from_origin(minx, maxy, cell, cell)
    scene = PhotoScene(opensfm_dir, images_dir, crs)
    centres = np.asarray(scene.centres, dtype=float)
    if (not len(scene.names) or centres.shape != (len(scene.names), 3)
            or not np.isfinite(centres).all()):
        raise ValueError("scene must contain finite camera centres")
    tree = cKDTree(centres[:, :2])
    ranks = min(candidates, len(scene.names))
    finite = np.isfinite(surface)
    finite_weight = finite.astype(np.float32)
    filled = np.where(finite, surface, 0)
    inverse = ~transform
    covered_pixels = surface_pixels = 0
    out.parent.mkdir(parents=True, exist_ok=True)
    # An internal mask is portable as a single file and preserves black pixels.
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True), rasterio.open(
        out, "w", driver="GTiff", width=width, height=height, count=3,
        dtype="uint8", crs=crs, transform=output_transform, tiled=True,
        blockxsize=256, blockysize=256, compress="deflate", photometric="RGB",
        BIGTIFF="IF_SAFER",
    ) as dst:
        for row in range(0, height, tile_size):
            for col in range(0, width, tile_size):
                h, w = min(tile_size, height - row), min(tile_size, width - col)
                rr, cc = np.mgrid[row:row + h, col:col + w]
                east = (minx + (cc.ravel() + 0.5) * cell)
                north = (maxy - (rr.ravel() + 0.5) * cell)
                fc, fr = inverse * (east, north)
                inside = ((fc >= 0) & (fc < surface.shape[1]) & (fr >= 0)
                          & (fr < surface.shape[0]) & (east < maxx) & (north > miny))
                sample = np.array([fr - 0.5, fc - 0.5])
                valid_weight = ndimage.map_coordinates(
                    finite_weight, sample, order=1, mode="nearest", prefilter=False)
                z = ndimage.map_coordinates(filled, sample, order=1, mode="nearest", prefilter=False)
                valid_surface = inside & (valid_weight >= 1 - 1e-10) & np.isfinite(z)
                surface_pixels += int(valid_surface.sum())
                rgb = np.zeros((h * w, 3), dtype=np.uint8)
                covered = np.zeros(h * w, dtype=bool)
                indices = np.flatnonzero(valid_surface)
                if len(indices):
                    points = scene.to_topo(east[indices], north[indices], z[indices])
                    # k=[1,...] keeps a rank dimension even with one camera.
                    _, near = tree.query(points[:, :2], k=list(range(1, ranks + 1)))
                    todo = np.ones(len(indices), dtype=bool)
                    for rank in range(ranks):
                        for camera_index in np.unique(near[todo, rank]):
                            camera_index = int(camera_index)
                            selected = np.flatnonzero(todo & (near[:, rank] == camera_index))
                            u, v, front = scene.project(camera_index, points[selected])
                            camera = scene.camera(camera_index)
                            image_width, image_height = camera["width"], camera["height"]
                            ok = (front & np.isfinite(u) & np.isfinite(v)
                                  & (u >= edge_margin) & (u <= image_width - 1 - edge_margin)
                                  & (v >= edge_margin) & (v <= image_height - 1 - edge_margin))
                            if np.any(ok):
                                photo = scene.photo(camera_index)
                                chosen = selected[ok]
                                rgb[indices[chosen]] = photo[np.rint(v[ok]).astype(int),
                                                             np.rint(u[ok]).astype(int)]
                                covered[indices[chosen]] = True
                                todo[chosen] = False
                        if not np.any(todo):
                            break
                window = Window(col, row, w, h)
                dst.write(rgb.reshape(h, w, 3).transpose(2, 0, 1), window=window)
                dst.write_mask(covered.reshape(h, w).astype(np.uint8) * 255, window=window)
                covered_pixels += int(covered.sum())
    return {"output": str(out), "width": width, "height": height, "cell_m": cell,
            "covered_pixels": covered_pixels, "surface_pixels": surface_pixels,
            "total_pixels": width * height,
            "coverage_fraction": covered_pixels / (width * height),
            "runtime_s": round(time.perf_counter() - started, 3)}
