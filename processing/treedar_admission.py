"""Read-only worker-side LiDAR admission before TreeDar allocates any raster.

LAS structure cannot establish acquisition provenance. A trusted verifier must
authenticate the closed, exact-file receipt. Synthetic data is explicitly labelled
and separately opted in; it is never reported as genuine sensor acquisition.
Geospatial dependencies are imported only inside the engine-image validation path.
Uncompressed LAS has a strict byte-layout count check. LAZ additionally requires
an authenticated source point-count attestation: laspy stops at the header count
and decoder EOF does not independently prove fixed-chunk compressed point count.
Authenticated source counts are not a whole-compression proof. Callers must keep
admitted scratch files immutable before and throughout subsequent engine execution.
"""
from dataclasses import dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat

from . import reference_protocol as protocol


class AdmissionError(ValueError):
    """Bounded reason code only; do not expose filenames or parser exceptions."""


@dataclass(frozen=True)
class AdmissionPolicy:
    max_bytes: int = 64 * 1024**2
    max_points: int = 100_000
    chunk_points: int = 4096
    max_xy_extent_m: float = 1000.0
    max_z_extent_m: float = 1000.0
    raster_cell_m: float = 0.5
    max_grid_cells: int = 4_000_000
    ground_cell_m: float = 1.0
    min_ground_points: int = 3
    min_ground_coverage: float = 0.8
    approved_vertical_datums: tuple[str, ...] = ('ellipsoidal', 'navd88')

    def __post_init__(self):
        for name in ('max_bytes', 'max_points', 'chunk_points', 'max_grid_cells', 'min_ground_points'):
            protocol._integer(getattr(self, name), name)
        if self.chunk_points > self.max_points or self.min_ground_points < 3:
            raise ValueError('invalid bounded admission policy')
        for name in ('max_xy_extent_m', 'max_z_extent_m', 'raster_cell_m', 'ground_cell_m'):
            value = getattr(self, name)
            if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
                raise ValueError('invalid metric admission policy')
        if (type(self.min_ground_coverage) not in (int, float)
                or not 0 < self.min_ground_coverage <= 1):
            raise ValueError('invalid ground coverage policy')
        if (type(self.approved_vertical_datums) is not tuple or not self.approved_vertical_datums
                or len(self.approved_vertical_datums) > 32
                or any(type(label) is not str or not re.fullmatch(r'[a-z][a-z0-9_]{0,47}', label)
                       or label.startswith('synthetic') for label in self.approved_vertical_datums)):
            raise ValueError('invalid approved vertical datums')


PROVENANCE_FIELDS = ('schemaVersion', 'receiptId', 'jobId', 'generation', 'environment',
    'objectId', 'objectSha256', 'byteSize', 'sourceKind', 'verticalUnits', 'verticalDatum')
COUNT_FIELDS = ('pointCount', 'countAssurance')


def _provenance(receipt, verifier, *, job_id, generation, environment, object_id,
                expected_sha256, expected_bytes, allow_synthetic, policy):
    try:
        fields = (*PROVENANCE_FIELDS, *COUNT_FIELDS) if isinstance(receipt, dict) and set(COUNT_FIELDS) & set(receipt) else PROVENANCE_FIELDS
        protocol._closed(receipt, fields, 'LiDAR provenance')
        receipt = json.loads(protocol.canonical_json(receipt))
        protocol._require(type(receipt['schemaVersion']) is int and receipt['schemaVersion'] == 1,
                          'unsupported provenance schema')
        for key in ('receiptId', 'jobId', 'objectId'):
            protocol._slug(receipt[key], key)
        protocol._integer(receipt['generation'], 'generation')
        protocol._integer(receipt['byteSize'], 'byteSize')
        protocol._environment(receipt['environment'])
        protocol._hash(receipt['objectSha256'], 'objectSha256')
        if 'pointCount' in receipt:
            protocol._integer(receipt['pointCount'], 'pointCount')
            protocol._require(receipt['countAssurance'] == 'trusted_acquisition_count_v1',
                              'unsupported source count assurance')
    except (ValueError, TypeError, KeyError):
        raise AdmissionError('invalid_provenance') from None
    if (receipt['jobId'], receipt['generation'], receipt['environment'], receipt['objectId'],
            receipt['objectSha256'], receipt['byteSize']) != (job_id, generation, environment,
            object_id, expected_sha256, expected_bytes):
        raise AdmissionError('provenance_binding_mismatch')
    if receipt['sourceKind'] not in ('lidar', 'synthetic_lidar'):
        raise AdmissionError('unsupported_acquisition_source')
    if receipt['sourceKind'] == 'synthetic_lidar' and allow_synthetic is not True:
        raise AdmissionError('synthetic_acquisition_not_enabled')
    if receipt['verticalUnits'] != 'metre':
        raise AdmissionError('unverified_vertical_metre_units')
    datum = receipt['verticalDatum']
    if type(datum) is not str or not re.fullmatch(r'[a-z][a-z0-9_]{0,47}', datum):
        raise AdmissionError('invalid_vertical_datum_declaration')
    if datum == 'synthetic_local':
        if receipt['sourceKind'] != 'synthetic_lidar' or allow_synthetic is not True:
            raise AdmissionError('synthetic_vertical_datum_not_enabled')
    elif datum not in policy.approved_vertical_datums:
        raise AdmissionError('vertical_datum_not_approved')
    if not callable(verifier):
        raise AdmissionError('trusted_provenance_verifier_required')
    try:
        verified = verifier(json.loads(protocol.canonical_json(receipt)))
    except Exception:
        raise AdmissionError('provenance_verification_failed') from None
    if verified is not True:
        raise AdmissionError('provenance_verification_failed')
    return receipt


def _snapshot(info):
    return (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns, info.st_nlink)


def _ground_noncollinear(basis, positions):
    """Accumulate an exact non-collinearity proof in LAS integer coordinates.

    LAS scales are positive per axis, so this affine mapping preserves
    collinearity. Python integers avoid overflow and floating-point thresholds
    that would make admission depend on point order or coordinate scale.
    """
    for raw_x, raw_y in positions:
        x, y = int(raw_x), int(raw_y)
        if not basis:
            basis.append((x, y))
        elif len(basis) == 1:
            if (x, y) != basis[0]:
                basis.append((x, y))
        else:
            (x0, y0), (x1, y1) = basis
            if (x1 - x0) * (y - y0) != (y1 - y0) * (x - x0):
                return True
    return False


def _grid(bounds, cell, maximum):
    minx, miny, maxx, maxy = bounds
    columns = math.floor((maxx - math.floor(minx / cell) * cell) / cell) + 1
    rows = math.floor((math.ceil(maxy / cell) * cell - miny) / cell) + 1
    if columns < 1 or rows < 1 or columns * rows > maximum:
        raise AdmissionError('grid_budget_exceeded')
    return rows, columns


def _header(reader, policy, size, np):
    header = reader.header
    scales, offsets = np.asarray(header.scales), np.asarray(header.offsets)
    minima, maxima = np.asarray(header.mins), np.asarray(header.maxs)
    if (not np.isfinite(scales).all() or (scales <= 0).any()
            or not np.isfinite(offsets).all() or not np.isfinite(minima).all()
            or not np.isfinite(maxima).all() or (maxima < minima).any()):
        raise AdmissionError('invalid_coordinate_metadata')
    if not 0 < header.point_count <= policy.max_points:
        raise AdmissionError('point_budget_exceeded')
    span = maxima - minima
    if (span[:2] <= 0).any() or (span[:2] > policy.max_xy_extent_m).any() or span[2] > policy.max_z_extent_m:
        raise AdmissionError('extent_budget_exceeded')
    bounds = (float(minima[0]), float(minima[1]), float(maxima[0]), float(maxima[1]))
    _grid(bounds, policy.raster_cell_m, policy.max_grid_cells)
    _grid(bounds, policy.ground_cell_m, policy.max_grid_cells)
    # Restrict this first adapter to common discrete-return formats. Internal
    # waveform payloads need a separate bounded byte-layout validator.
    if header.point_format.id not in (0, 1, 2, 3, 6, 7, 8):
        raise AdmissionError('unsupported_point_format')
    if not header.are_points_compressed:
        end = header.offset_to_point_data + header.point_count * header.point_format.size
        evlr_start = getattr(header, 'start_of_first_evlr', 0)
        evlr_count = getattr(header, 'number_of_evlrs', 0)
        if end != (evlr_start if evlr_count else size):
            raise AdmissionError('point_count_byte_layout_mismatch')
    crs = header.parse_crs()
    if (crs is None or not crs.is_projected or len(crs.axis_info) < 2
            or any(not math.isclose(axis.unit_conversion_factor, 1.0, rel_tol=0, abs_tol=1e-12)
                   for axis in crs.axis_info)):
        raise AdmissionError('projected_metre_crs_required')
    # A projected-CRS tag on arbitrary local coordinates is not sufficient.
    from pyproj import Transformer
    area = crs.area_of_use
    if area is None:
        raise AdmissionError('crs_area_of_use_required')
    transform = Transformer.from_crs(crs, crs.geodetic_crs, always_xy=True)
    for x in (bounds[0], bounds[2]):
        for y in (bounds[1], bounds[3]):
            longitude, latitude = transform.transform(x, y, errcheck=True)
            longitude_ok = (area.west <= longitude <= area.east) if area.west <= area.east else (longitude >= area.west or longitude <= area.east)
            if not math.isfinite(longitude) or not math.isfinite(latitude) or not longitude_ok or not area.south <= latitude <= area.north:
                raise AdmissionError('coordinates_outside_crs_area')
    return header, scales, minima, maxima, bounds, crs


def validate_lidar(path, *, job_id, generation, environment, object_id, expected_sha256,
                   expected_bytes, provenance, verify_provenance, policy=None, allow_synthetic=False):
    policy = AdmissionPolicy() if policy is None else policy
    if not isinstance(policy, AdmissionPolicy):
        raise ValueError('invalid admission policy')
    protocol._slug(job_id, 'jobId'); protocol._slug(object_id, 'objectId')
    protocol._integer(generation, 'generation'); protocol._environment(environment)
    protocol._hash(expected_sha256, 'expectedSha256'); protocol._integer(expected_bytes, 'expectedBytes')
    if expected_bytes > policy.max_bytes:
        raise AdmissionError('byte_budget_exceeded')
    provenance = _provenance(provenance, verify_provenance, job_id=job_id, generation=generation,
        environment=environment, object_id=object_id, expected_sha256=expected_sha256,
        expected_bytes=expected_bytes, allow_synthetic=allow_synthetic, policy=policy)
    original = Path(path).absolute()
    if original.is_symlink() or any(parent.is_symlink() for parent in original.parents):
        raise AdmissionError('owned_regular_input_required')
    descriptor = None
    try:
        import laspy
        import numpy as np
        descriptor = os.open(original, os.O_RDONLY | getattr(os, 'O_NOFOLLOW', 0) | getattr(os, 'O_NONBLOCK', 0))
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1 or before.st_size != expected_bytes:
            raise AdmissionError('input_file_identity_mismatch')
        digest = hashlib.sha256()
        while chunk := os.read(descriptor, 1024 * 1024):
            digest.update(chunk)
        if digest.hexdigest() != expected_sha256:
            raise AdmissionError('input_hash_mismatch')
        if _snapshot(before) != _snapshot(os.fstat(descriptor)):
            raise AdmissionError('input_changed_during_admission')
        os.lseek(descriptor, 0, os.SEEK_SET)
        with os.fdopen(os.dup(descriptor), 'rb') as stream:
            with laspy.open(stream, closefd=False, read_evlrs=False) as reader:
                header, scales, minima, maxima, bounds, crs = _header(reader, policy, expected_bytes, np)
                if header.are_points_compressed and 'pointCount' not in provenance:
                    raise AdmissionError('compressed_count_attestation_required')
                if 'pointCount' in provenance and provenance['pointCount'] != header.point_count:
                    raise AdmissionError('authenticated_point_count_mismatch')
                actual_min, actual_max = np.full(3, np.inf), np.full(3, -np.inf)
                points = ground_count = 0
                ground_cells = set()
                ground_basis = []
                ground_noncollinear = False
                for chunk in reader.chunk_iterator(policy.chunk_points):
                    points += len(chunk)
                    if points > policy.max_points:
                        raise AdmissionError('point_budget_exceeded')
                    xyz = np.column_stack((chunk.x, chunk.y, chunk.z))
                    if not np.isfinite(xyz).all():
                        raise AdmissionError('nonfinite_coordinates')
                    actual_min = np.minimum(actual_min, xyz.min(axis=0))
                    actual_max = np.maximum(actual_max, xyz.max(axis=0))
                    tolerance = np.maximum(scales, 1e-8)
                    if (xyz < minima - tolerance).any() or (xyz > maxima + tolerance).any():
                        raise AdmissionError('point_outside_header_bounds')
                    returns, totals = np.asarray(chunk.return_number), np.asarray(chunk.number_of_returns)
                    if (returns < 1).any() or (totals < returns).any():
                        raise AdmissionError('invalid_return_metadata')
                    ground = np.asarray(chunk.classification) == 2
                    ground_count += int(ground.sum())
                    ground_xy = xyz[ground, :2]
                    if not ground_noncollinear:
                        ground_noncollinear = _ground_noncollinear(
                            ground_basis, zip(np.asarray(chunk.X)[ground], np.asarray(chunk.Y)[ground]))
                    cells = np.floor((ground_xy - minima[:2]) / policy.ground_cell_m).astype(np.int64)
                    ground_cells.update(map(tuple, cells.tolist()))
                    if len(ground_cells) > policy.max_grid_cells:
                        raise AdmissionError('ground_grid_budget_exceeded')
                if points != header.point_count or points == 0:
                    raise AdmissionError('actual_point_count_mismatch')
                if 'pointCount' in provenance and points != provenance['pointCount']:
                    raise AdmissionError('authenticated_point_count_mismatch')
                if (np.abs(actual_min - minima) > tolerance).any() or (np.abs(actual_max - maxima) > tolerance).any():
                    raise AdmissionError('actual_bounds_mismatch')
                ground_cols = math.floor((maxima[0] - minima[0]) / policy.ground_cell_m) + 1
                ground_rows = math.floor((maxima[1] - minima[1]) / policy.ground_cell_m) + 1
                coverage = len(ground_cells) / (ground_cols * ground_rows)
                if (ground_count < policy.min_ground_points or len(ground_cells) < 3
                        or not ground_noncollinear
                        or len({x for x, _ in ground_cells}) < 2 or len({y for _, y in ground_cells}) < 2
                        or coverage < policy.min_ground_coverage):
                    raise AdmissionError('usable_class2_ground_required')
        # Timestamp granularity alone cannot reliably identify rapid writes on
        # every filesystem. Rehash the same descriptor after streamed validation.
        os.lseek(descriptor, 0, os.SEEK_SET)
        final_digest = hashlib.sha256()
        while chunk := os.read(descriptor, 1024 * 1024):
            final_digest.update(chunk)
        if (final_digest.hexdigest() != expected_sha256
                or _snapshot(before) != _snapshot(os.fstat(descriptor))
                or _snapshot(before) != _snapshot(original.lstat())):
            raise AdmissionError('input_changed_during_admission')
        authority = crs.to_authority()
        crs_label = ':'.join(authority) if authority and all(part.isascii() and part.replace('_', '').isalnum() and len(part) <= 32 for part in authority) else None
        return dict(admissionVersion=protocol.TREEDAR_ADMISSION_VERSION, jobId=job_id,
            generation=generation, environment=environment, objectId=object_id,
            objectSha256=expected_sha256, byteSize=expected_bytes, sourceKind=provenance['sourceKind'],
            provenanceReceiptId=provenance['receiptId'],
            genuineAcquisition=provenance['sourceKind'] == 'lidar', pointCount=points,
            groundPointCount=ground_count, groundCoverage=coverage, crs=crs_label,
            crsWktSha256=hashlib.sha256(crs.to_wkt().encode()).hexdigest(),
            pointCountAssurance='authenticated_source_count' if header.are_points_compressed else 'uncompressed_byte_layout_and_stream',
            verticalUnits='metre', verticalDatum=provenance['verticalDatum'],
            verticalDatumAssurance='authenticated_declaration_no_conversion',
            rasterGridCells=math.prod(_grid(bounds, policy.raster_cell_m, policy.max_grid_cells)))
    except AdmissionError:
        raise
    except ImportError:
        raise AdmissionError('engine_geospatial_dependencies_unavailable') from None
    except Exception:
        raise AdmissionError('invalid_or_unreadable_point_cloud') from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
