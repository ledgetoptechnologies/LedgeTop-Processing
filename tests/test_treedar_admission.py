import hashlib
import importlib.util
from pathlib import Path
import struct
import tempfile
import unittest
from dataclasses import replace
from contextlib import contextmanager
from unittest.mock import patch
import warnings

from processing.treedar_admission import AdmissionPolicy, AdmissionError, validate_lidar, _ground_noncollinear


DEPENDENCIES = all(importlib.util.find_spec(name) is not None for name in ('laspy', 'numpy', 'pyproj'))


def write_projected_forest(path, *, crs=32616, classified=True, shift=(500000, 4500000),
                  compressed=False, sparse_ground=False, invalid_returns=False):
    """Generate bounded synthetic LiDAR; returned count comes from constructed points."""
    import laspy
    import numpy as np
    from pyproj import CRS
    axis = np.arange(0.25, 32.0, 0.5)
    gx, gy = np.meshgrid(axis, axis)
    x, y = gx.ravel(), gy.ravel()
    z = 100 + 0.04*x + 0.025*y
    xs, ys, zs = [x], [y], [z]
    classes = [np.full(x.size, 2 if classified else 1, dtype=np.uint8)]
    if sparse_ground:
        classes[0][3:] = 1
    for tx, ty, height, radius in ((8.25,8.25,12,3), (23.25,8.25,18,4), (15.25,23.25,15,3.5)):
        distance = (x-tx)**2+(y-ty)**2
        inside = distance <= radius**2
        xs.append(x[inside]); ys.append(y[inside])
        zs.append(z[inside] + height*(1-0.55*distance[inside]/radius**2))
        classes.append(np.full(inside.sum(), 5, dtype=np.uint8))
    header = laspy.LasHeader(point_format=3, version='1.2')
    header.scales = np.array([0.001,0.001,0.001])
    header.offsets = np.array([shift[0],shift[1],100.0])
    if crs is not None:
        header.add_crs(CRS.from_user_input(crs))
    cloud = laspy.LasData(header)
    cloud.x = np.concatenate(xs) + shift[0]
    cloud.y = np.concatenate(ys) + shift[1]
    cloud.z = np.concatenate(zs)
    cloud.classification = np.concatenate(classes)
    cloud.return_number = np.ones(len(cloud.points), dtype=np.uint8)
    cloud.number_of_returns = np.ones(len(cloud.points), dtype=np.uint8)
    if invalid_returns:
        cloud.number_of_returns[0] = 0
    cloud.point_source_id = np.ones(len(cloud.points), dtype=np.uint16)
    path = Path(path).with_suffix('.laz' if compressed else '.las')
    cloud.write(path)
    return dict(path=path, pointCount=len(cloud.points), groundPointCount=int((cloud.classification == 2).sum()))


class GroundGeometryTests(unittest.TestCase):
    def test_near_duplicate_baseline_is_order_independent_across_chunks(self):
        # With a 1e-9 metre X scale, the first two points are 1 nm apart.
        # These are non-collinear even when their floating cross product is
        # below the former fixed 1e-6 threshold.
        points = [(0, 0), (1, 0), (0, 100), (10, 20)]
        for order in (points, list(reversed(points)), points[1:] + points[:1]):
            basis = []
            found = False
            for start in range(0, len(order), 2):
                if not found:
                    found = _ground_noncollinear(basis, order[start:start + 2])
            self.assertTrue(found)

    def test_collinear_points_and_large_integer_arithmetic(self):
        basis = []
        self.assertFalse(_ground_noncollinear(basis, [(0, 0), (1, 1), (2, 2)]))
        self.assertFalse(_ground_noncollinear(basis, [(2**31 - 1, 2**31 - 1)]))
        self.assertTrue(_ground_noncollinear(basis, [(-(2**31), 2**31 - 1)]))


class AdmissionPolicyTests(unittest.TestCase):
    def test_policy_rejects_unbounded_or_invalid_sizes(self):
        for change in ({'max_points': True}, {'chunk_points': 100001},
                       {'raster_cell_m': float('nan')}, {'max_xy_extent_m': float('inf')},
                       {'min_ground_coverage': 0}, {'min_ground_points': 2},
                       {'approved_vertical_datums': ('synthetic_local',)},
                       {'approved_vertical_datums': ('https://invalid.example',)},
                       {'approved_vertical_datums': ['navd88']}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                AdmissionPolicy(**change)


@unittest.skipUnless(DEPENDENCIES, 'requires cached TreeDar-image laspy/numpy/pyproj; host dependency skip')
class TreeDarAdmissionTests(unittest.TestCase):
    def setUp(self):
        import laspy
        import numpy as np
        from pyproj import CRS
        self.laspy, self.np, self.CRS = laspy, np, CRS
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'synthetic.las'
        self.policy = AdmissionPolicy()
        self.write_fixture()

    def write_fixture(self, **kwargs):
        self.path = write_projected_forest(self.path, **kwargs)['path']

    def arguments(self, **updates):
        data = self.path.read_bytes()
        digest = hashlib.sha256(data).hexdigest()
        provenance = dict(schemaVersion=1, receiptId='synthetic-provenance-1', jobId='j1',
            generation=1, environment='staging', objectId='lidar1', objectSha256=digest,
            byteSize=len(data), sourceKind='synthetic_lidar', verticalUnits='metre', verticalDatum='synthetic_local')
        if self.path.suffix == '.laz':
            provenance.update(pointCount=4555, countAssurance='trusted_acquisition_count_v1')
        values = dict(job_id='j1', generation=1, environment='staging', object_id='lidar1',
            expected_sha256=digest, expected_bytes=len(data), provenance=provenance,
            verify_provenance=lambda receipt: receipt == provenance,
            policy=self.policy, allow_synthetic=True)
        values.update(updates)
        return values

    def validate(self, **updates):
        return validate_lidar(self.path, **self.arguments(**updates))

    def patch_double(self, offset, value):
        data = bytearray(self.path.read_bytes())
        struct.pack_into('<d', data, offset, value)
        self.path.write_bytes(data)

    def test_bounded_real_projected_las_and_laz_synthetic_fixture(self):
        for compressed in (False, True):
            with self.subTest(compressed=compressed):
                self.write_fixture(compressed=compressed)
                before = self.path.read_bytes()
                report = self.validate()
                self.assertEqual(report['pointCount'], 4555)
                self.assertEqual(report['groundPointCount'], 4096)
                self.assertEqual(report['groundCoverage'], 1)
                self.assertEqual(report['crs'], 'EPSG:32616')
                self.assertEqual(report['rasterGridCells'], 4096)
                self.assertEqual(report['sourceKind'], 'synthetic_lidar')
                self.assertFalse(report['genuineAcquisition'])
                self.assertEqual(report['verticalDatum'], 'synthetic_local')
                self.assertEqual(report['verticalDatumAssurance'], 'authenticated_declaration_no_conversion')
                self.assertEqual(report['pointCountAssurance'], 'authenticated_source_count' if compressed else 'uncompressed_byte_layout_and_stream')
                self.assertEqual(before, self.path.read_bytes())

    def test_provenance_missing_untrusted_unknown_and_photogrammetry_rejected(self):
        for change, code in (({'provenance': None}, 'invalid_provenance'),
                             ({'verify_provenance': None}, 'trusted_provenance_verifier_required'),
                             ({'verify_provenance': lambda _: False}, 'provenance_verification_failed'),
                             ({'verify_provenance': lambda _: 1}, 'provenance_verification_failed'),
                             ({'allow_synthetic': False}, 'synthetic_acquisition_not_enabled')):
            with self.subTest(change=change), self.assertRaisesRegex(AdmissionError, code):
                self.validate(**change)
        for source in ('photogrammetry', 'unknown', None):
            values = self.arguments()
            values['provenance']['sourceKind'] = source
            with self.assertRaisesRegex(AdmissionError, 'unsupported_acquisition_source'):
                validate_lidar(self.path, **values)

    def test_exact_provenance_binding_and_explicit_metre_z_required(self):
        for key, value in (('jobId','other'), ('generation',2), ('environment','production'),
                           ('objectId','other'), ('objectSha256','f'*64), ('byteSize',1),
                           ('verticalUnits','foot'), ('url','https://invalid.example')):
            values = self.arguments()
            values['provenance'][key] = value
            with self.subTest(key=key), self.assertRaises(AdmissionError):
                validate_lidar(self.path, **values)

    def test_vertical_datum_is_explicit_approved_and_never_inferred_from_epsg(self):
        for datum, code in (('https://example.invalid/datum', 'invalid_vertical_datum_declaration'),
                            ('../datum', 'invalid_vertical_datum_declaration'),
                            ('unknown', 'vertical_datum_not_approved')):
            values = self.arguments()
            values['provenance']['verticalDatum'] = datum
            with self.subTest(datum=datum), self.assertRaisesRegex(AdmissionError, code):
                validate_lidar(self.path, **values)
        values = self.arguments()
        del values['provenance']['verticalDatum']
        with self.assertRaisesRegex(AdmissionError, 'invalid_provenance'):
            validate_lidar(self.path, **values)
        values = self.arguments()
        values['provenance']['sourceKind'] = 'lidar'
        with self.assertRaisesRegex(AdmissionError, 'synthetic_vertical_datum_not_enabled'):
            validate_lidar(self.path, **values)

    def test_geographic_feet_missing_or_falsely_located_crs_rejected(self):
        for crs, shift, code in ((4326,(0,0),'projected_metre_crs_required'),
                                (2236,(500000,4500000),'projected_metre_crs_required'),
                                (None,(500000,4500000),'projected_metre_crs_required'),
                                (32616,(0,0),'coordinates_outside_crs_area')):
            with self.subTest(crs=crs,shift=shift):
                self.write_fixture(crs=crs,shift=shift)
                with self.assertRaisesRegex(AdmissionError, code):
                    self.validate()

    def test_no_or_sparse_class2_ground_rejected_without_morphological_fallback(self):
        for kwargs in ({'classified':False}, {'sparse_ground':True}):
            self.write_fixture(**kwargs)
            with self.assertRaisesRegex(AdmissionError, 'usable_class2_ground_required'):
                self.validate()

    def test_collinear_ground_is_unusable_even_with_relaxed_coverage_policy(self):
        cloud = self.laspy.read(self.path)
        cloud.classification[:4096] = 1
        cloud.classification[self.np.arange(64)*65] = 2
        cloud.write(self.path)
        with self.assertRaisesRegex(AdmissionError, 'usable_class2_ground_required'):
            self.validate(policy=replace(self.policy,min_ground_coverage=0.01))

    def test_point_extent_grid_and_byte_budgets_enforced_before_engine(self):
        for policy, code in ((replace(self.policy,max_points=4554), 'point_budget_exceeded'),
                             (replace(self.policy,max_xy_extent_m=20), 'extent_budget_exceeded'),
                             (replace(self.policy,max_grid_cells=100), 'grid_budget_exceeded'),
                             (replace(self.policy,max_bytes=100), 'byte_budget_exceeded')):
            with self.subTest(code=code), self.assertRaisesRegex(AdmissionError, code):
                self.validate(policy=policy)
        self.patch_double(179, 900000.0)  # LAS 1.2 max X header field.
        with self.assertRaisesRegex(AdmissionError, 'extent_budget_exceeded'):
            self.validate()

    def test_nonfinite_scale_offset_and_bounds_are_rejected(self):
        for offset, value in ((131,float('nan')), (131,0.0), (155,float('inf')), (179,float('nan'))):
            self.write_fixture()
            self.patch_double(offset, value)
            with self.subTest(offset=offset), self.assertRaisesRegex(AdmissionError, 'invalid_coordinate_metadata'):
                self.validate()

    def test_finite_scale_that_overflows_actual_xyz_is_rejected(self):
        self.patch_double(131, 1e308)
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', RuntimeWarning)
            with self.assertRaisesRegex(AdmissionError, 'nonfinite_coordinates'):
                self.validate()

    def test_verifier_receives_detached_receipt_and_cannot_relabel_synthetic_report(self):
        def verify(receipt):
            receipt['sourceKind'] = 'lidar'
            return True
        self.assertFalse(self.validate(verify_provenance=verify)['genuineAcquisition'])

    def test_file_mutation_or_path_replacement_after_streaming_is_rejected(self):
        original_open = self.laspy.open
        for replace_path in (False, True):
            self.write_fixture()
            @contextmanager
            def changed(*args, **kwargs):
                with original_open(*args, **kwargs) as reader:
                    yield reader
                data = bytearray(self.path.read_bytes())
                if replace_path:
                    retained = self.path.with_name('retained-original.las')
                    self.path.rename(retained)
                else:
                    data[-1] ^= 1
                self.path.write_bytes(data)
            with self.subTest(replace_path=replace_path), patch.object(self.laspy, 'open', changed):
                with self.assertRaisesRegex(AdmissionError, 'input_changed_during_admission'):
                    self.validate()

    def test_ground_with_tiny_first_baseline_is_admitted_in_either_order(self):
        cloud = self.laspy.read(self.path)
        cloud.x = 500000 + (self.np.asarray(cloud.x) - 500000) / 4
        cloud.change_scaling(scales=self.np.array([1e-8, 0.001, 0.001]))
        cloud.X[1] = cloud.X[0] + 1
        cloud.Y[1] = cloud.Y[0]
        cloud.write(self.path)
        first = self.validate()
        cloud = self.laspy.read(self.path)
        cloud.points = cloud.points[self.np.arange(len(cloud.points) - 1, -1, -1)]
        cloud.write(self.path)
        second = self.validate()
        self.assertEqual(first['pointCount'], second['pointCount'])
        self.assertEqual(first['groundPointCount'], second['groundPointCount'])
        self.assertEqual(first['groundCoverage'], second['groundCoverage'])

    def test_point_count_header_tamper_and_invalid_return_metadata_rejected(self):
        for count in (4554,4556):
            self.write_fixture()
            data = bytearray(self.path.read_bytes())
            struct.pack_into('<I', data, 107, count)  # LAS 1.2 point count.
            self.path.write_bytes(data)
            with self.subTest(count=count), self.assertRaisesRegex(AdmissionError, 'point_count_byte_layout_mismatch'):
                self.validate()
        self.write_fixture(invalid_returns=True)
        with self.assertRaisesRegex(AdmissionError, 'invalid_return_metadata'):
            self.validate()

    def test_laz_requires_authenticated_source_count_not_decoder_eof(self):
        self.write_fixture(compressed=True)
        values = self.arguments()
        del values['provenance']['pointCount']
        del values['provenance']['countAssurance']
        with self.assertRaisesRegex(AdmissionError, 'compressed_count_attestation_required'):
            validate_lidar(self.path, **values)
        for count in (4554, 4556):
            self.write_fixture(compressed=True)
            data = bytearray(self.path.read_bytes())
            struct.pack_into('<I', data, 107, count)
            self.path.write_bytes(data)
            with self.subTest(count=count), self.assertRaisesRegex(AdmissionError, 'authenticated_point_count_mismatch'):
                self.validate()

    def test_count_attestation_fields_are_closed_and_verified_together(self):
        self.write_fixture(compressed=True)
        for change in ({'pointCount': True}, {'countAssurance':'header_only'}, {'pointCount':0}):
            values = self.arguments()
            values['provenance'].update(change)
            with self.assertRaisesRegex(AdmissionError, 'invalid_provenance'):
                validate_lidar(self.path, **values)
        values = self.arguments()
        del values['provenance']['countAssurance']
        with self.assertRaisesRegex(AdmissionError, 'invalid_provenance'):
            validate_lidar(self.path, **values)
        values = self.arguments()
        values['provenance']['pointCount'] = 4554
        values['verify_provenance'] = lambda receipt: receipt['pointCount'] == 4555
        with self.assertRaisesRegex(AdmissionError, 'provenance_verification_failed'):
            validate_lidar(self.path, **values)

    def test_actual_file_hash_and_byte_identity_fail_closed(self):
        values = self.arguments()
        values['expected_sha256'] = 'f'*64
        values['provenance']['objectSha256'] = 'f'*64
        with self.assertRaisesRegex(AdmissionError, 'input_hash_mismatch'):
            validate_lidar(self.path, **values)
        values = self.arguments()
        values['expected_bytes'] += 1
        values['provenance']['byteSize'] += 1
        with self.assertRaisesRegex(AdmissionError, 'input_file_identity_mismatch'):
            validate_lidar(self.path, **values)

    def test_symlink_and_hardlink_are_not_owned_single_file_inputs(self):
        alias = self.path.with_name('alias.las')
        try:
            alias.symlink_to(self.path)
        except OSError:
            self.skipTest('symlinks unavailable to host account')
        with self.assertRaisesRegex(AdmissionError, 'owned_regular_input_required'):
            validate_lidar(alias, **self.arguments())
        alias.unlink()
        alias.hardlink_to(self.path)
        with self.assertRaisesRegex(AdmissionError, 'input_file_identity_mismatch'):
            self.validate()


if __name__ == '__main__':
    unittest.main()
