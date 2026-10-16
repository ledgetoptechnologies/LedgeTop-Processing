"""Small, deterministic point clouds with analytic truth, never survey data."""

from dataclasses import dataclass
from pathlib import Path

import laspy
import numpy as np
from pyproj import CRS


@dataclass(frozen=True)
class TreeTruth:
    x: float
    y: float
    height: float
    radius: float


TREES = (
    TreeTruth(8.25, 8.25, 12.0, 3.0),
    TreeTruth(23.25, 8.25, 18.0, 4.0),
    TreeTruth(15.25, 23.25, 15.0, 3.5),
)


def terrain_z(x, y, *, sloped=True):
    """Elevation in local synthetic metres; no real CRS or location."""
    return 100.0 + (0.04 * x + 0.025 * y if sloped else np.zeros_like(x))


def write_forest(path: Path, *, sloped=True, trees=TREES, classified=True):
    """Write ground and paraboloid crown returns on a half-metre grid.

    Every cell has a ground return. Crown apex samples are present exactly.
    This deliberately easy scene tests plumbing and height normalization,
    not performance in overlapping crowns or a leaf-on understory.
    """
    axis = np.arange(0.25, 32.0, 0.5)
    gx, gy = np.meshgrid(axis, axis)
    x, y = gx.ravel(), gy.ravel()
    z = terrain_z(x, y, sloped=sloped)
    xs, ys, zs = [x], [y], [z]
    classes = [np.full(x.size, 2 if classified else 1, dtype=np.uint8)]
    for tree in trees:
        distance2 = (x - tree.x) ** 2 + (y - tree.y) ** 2
        inside = distance2 <= tree.radius**2
        height = tree.height * (1.0 - 0.55 * distance2[inside] / tree.radius**2)
        xs.append(x[inside])
        ys.append(y[inside])
        zs.append(z[inside] + height)
        classes.append(np.full(inside.sum(), 5 if classified else 1, dtype=np.uint8))

    header = laspy.LasHeader(point_format=3, version="1.2")
    header.scales = np.array([0.001, 0.001, 0.001])
    header.offsets = np.array([0.0, 0.0, 100.0])
    # Synthetic local numbers only; CRS exercises export metadata handling.
    header.add_crs(CRS.from_epsg(32616))
    cloud = laspy.LasData(header)
    cloud.x, cloud.y, cloud.z = map(np.concatenate, (xs, ys, zs))
    cloud.classification = np.concatenate(classes)
    cloud.return_number = np.ones(len(cloud.points), dtype=np.uint8)
    cloud.number_of_returns = np.ones(len(cloud.points), dtype=np.uint8)
    cloud.point_source_id = np.where(np.asarray(cloud.x) < 16.0, 1, 2).astype(np.uint16)
    cloud.write(path)
    return trees
