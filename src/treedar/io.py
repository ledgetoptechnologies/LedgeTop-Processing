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


def read_points(path: str | Path, bbox=None, step: int = 1) -> dict[str, np.ndarray]:
    """Read points as numpy arrays.

    bbox: (minx, miny, maxx, maxy) in the file CRS, or None for all points.
    step: keep every Nth point (after bbox filtering) for quick runs.
    """
    if step < 1:
        raise ValueError("step must be >= 1")
    parts: list[dict[str, np.ndarray]] = []
    with open_las(path) as reader:
        names = set(reader.header.point_format.dimension_names)
        for chunk in reader.chunk_iterator(CHUNK):
            if bbox is not None:
                minx, miny, maxx, maxy = bbox
                x, y = np.asarray(chunk.x), np.asarray(chunk.y)
                keep = (x >= minx) & (x <= maxx) & (y >= miny) & (y <= maxy)
                if not keep.any():
                    continue
                chunk = chunk[keep]
            parts.append(_to_dict(chunk, names))
    if not parts:
        raise ValueError("no points found (empty file or bbox outside data)")
    merged = {k: np.concatenate([p[k] for p in parts]) for k in parts[0]}
    if step > 1:
        merged = {k: v[::step] for k, v in merged.items()}
    return merged
