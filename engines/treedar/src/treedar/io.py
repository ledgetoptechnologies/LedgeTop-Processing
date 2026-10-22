"""Point cloud reading."""

from __future__ import annotations

from pathlib import Path

import laspy
import numpy as np

OPTIONAL_FIELDS = ("scan_angle", "red", "green", "blue", "gps_time")
CORE_FIELDS = (
    "intensity",
    "return_number",
    "number_of_returns",
    "classification",
    "point_source_id",
)
CHUNK = 10_000_000


def _backend():
    """Prefer the multi-threaded lazrs backend when available."""
    for backend in (laspy.LazBackend.LazrsParallel, laspy.LazBackend.Lazrs):
        if backend.is_available():
            return backend
    return None


def open_las(path: str | Path):
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"point cloud not found: {path}")
    return laspy.open(path, laz_backend=_backend())


def read_crs(path: str | Path):
    with open_las(path) as reader:
        return reader.header.parse_crs()


def iter_chunks(path: str | Path, chunk_size: int = CHUNK):
    """Yield laspy point records in chunks to bound memory use."""
    with open_las(path) as reader:
        yield from reader.chunk_iterator(chunk_size)


def _to_dict(points, names) -> dict[str, np.ndarray]:
    out = {
        "x": np.asarray(points.x, dtype=np.float64),
        "y": np.asarray(points.y, dtype=np.float64),
        "z": np.asarray(points.z, dtype=np.float64),
    }
    for name in CORE_FIELDS + OPTIONAL_FIELDS:
        if name in names:
            out[name] = np.asarray(points[name])
    return out


def _repair_copc_cube(reader) -> bool:
    """Make the COPC octree cube usable for spatial queries.

    Some writers (seen in ROCK Desktop exports) leave the COPC info center and
    halfsize at zero. The cube can be rebuilt from the header bounds: its
    minimum corner sits at the header minimum and its edge is the largest
    extent. Returns False if a sampled node contradicts that geometry, in
    which case the caller should fall back to a full scan.
    """
    info, h = reader.copc_info, reader.header
    if info.halfsize > 0:
        return True
    half = float((h.maxs - h.mins).max() / 2)
    if half <= 0:
        return False
    info.center = h.mins + half
    info.halfsize = half
    # Verify on the deepest populated node: its points must lie in its voxel.
    entries = [e for e in reader.root_page.entries.values() if e.point_count > 0]
    if not entries:
        return False
    node = max(entries, key=lambda e: (e.key.level, e.point_count))
    size = 2 * half / 2 ** node.key.level
    pts = reader._fetch_and_decompress_points_of_nodes([node])
    lo = h.mins + size * np.array([node.key.x, node.key.y, node.key.z])
    tol = 2 * np.asarray(h.scales)
    for axis, values in enumerate((pts.x, pts.y, pts.z)):
        v = np.asarray(values)
        if v.min() < lo[axis] - tol[axis] or v.max() > lo[axis] + size + tol[axis]:
            return False
    return True


def _bbox_records(path: str | Path, bbox):
    """Yield point records that may fall in bbox, using a COPC spatial query
    when the file supports it, otherwise a chunked scan of the whole file."""
    if bbox is not None:
        try:
            with laspy.CopcReader.open(path) as reader:
                if _repair_copc_cube(reader):
                    minx, miny, maxx, maxy = bbox
                    yield reader.query(bounds=laspy.Bounds(
                        mins=np.array([minx, miny]), maxs=np.array([maxx, maxy])))
                    return
        except laspy.errors.LaspyException:
            pass  # not a COPC file
    with open_las(path) as reader:
        yield from reader.chunk_iterator(CHUNK)


def read_points(path: str | Path, bbox=None, step: int = 1) -> dict[str, np.ndarray]:
    """Read points as numpy arrays.

    bbox: (minx, miny, maxx, maxy) in the file CRS, or None for all points.
    step: keep every Nth point (after bbox filtering) for quick runs. For COPC
    files read through a spatial query, point order follows the octree, so
    which points a given step keeps differs from a sequential scan.
    """
    if step < 1:
        raise ValueError("step must be >= 1")
    with open_las(path) as reader:
        names = set(reader.header.point_format.dimension_names)
    parts: list[dict[str, np.ndarray]] = []
    retained = 0  # points kept by bbox so far, so decimation is global, not per chunk
    for chunk in _bbox_records(path, bbox):
        if bbox is not None:
            minx, miny, maxx, maxy = bbox
            x, y = np.asarray(chunk.x), np.asarray(chunk.y)
            keep = (x >= minx) & (x <= maxx) & (y >= miny) & (y <= maxy)
            if not keep.any():
                continue
            chunk = chunk[keep]
        n = len(chunk)
        if step > 1:
            # Decimate per chunk to bound peak memory; the phase keeps the
            # result identical to decimating the concatenated stream.
            chunk = chunk[(-retained) % step::step]
        retained += n
        if len(chunk):
            parts.append(_to_dict(chunk, names))
    if not parts:
        raise ValueError("no points found (empty file or bbox outside data)")
    return {k: np.concatenate([p[k] for p in parts]) for k in parts[0]}
