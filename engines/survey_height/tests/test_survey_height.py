"""Standard library tests. Grid tests need SURVEY_GEOID18_GRID; see README.md.

The bridge and converter doubles below are built from the independent reference,
so these tests check contracts and refusals. PROJ numbers are checked against
fixtures/recorded-proj-witness.json here and on PROJ itself in test_proj_runtime.py.
"""
import copy
import hashlib
import json
import math
import os
import sys
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent))
sys.dont_write_bytecode = True

from survey_height import admission_gate as gate  # noqa: E402
from survey_height import import_handoff as handoff  # noqa: E402
from survey_height.export_once import ExportLedger, export_dem  # noqa: E402
from survey_height.grid_reference import (  # noqa: E402
    EPSG_7541, EPSG_7600, GRID_SHA256, GeoidGrid, GridUnavailable, US_SURVEY_FOOT_M)
from survey_height.survey_frame import (  # noqa: E402
    EXCLUDED_LABELS, FIT_LABELS, SurveyRefused, is_opt_in, read_survey)

GRID_PATH = os.environ.get('SURVEY_GEOID18_GRID')
NEEDS_GRID = unittest.skipUnless(
    GRID_PATH, 'needs us_noaa_g2018u0.tif, sha256 ' + GRID_SHA256)


class Refusals(unittest.TestCase):
    def refused(self, code, action, *args, **kwargs):
        with self.assertRaises(SurveyRefused) as caught:
            action(*args, **kwargs)
        self.assertEqual(caught.exception.code, code)
        return caught.exception


class ReferenceBridge:
    """Test double with the same interface as the PROJ bridge."""

    def __init__(self, grid):
        self.grid = grid

    def survey_to_ellipsoid(self, xyz):
        lon, lat = EPSG_7600.inverse(xyz[0], xyz[1])
        return lon, lat, xyz[2] * US_SURVEY_FOOT_M + self.grid.undulation(lon, lat)

    def ellipsoid_to_survey(self, working):
        lon, lat, h = working
        east, north = EPSG_7600.forward(lon, lat)
        return east, north, (h - self.grid.undulation(lon, lat)) / US_SURVEY_FOOT_M


def declared_contract():
    """A well-formed contract without needing a grid."""
    survey = read_survey(gate.survey_bytes())
    body = {
        'schema': handoff.SCHEMA,
        'source': {key: getattr(survey, key) for key in (
            'raw_sha256', 'raw_header', 'crs', 'units', 'axes', 'realization', 'epoch',
            'geoid', 'roles')},
        'working_frame': dict(handoff.WORKING_FRAME),
        'operation': handoff.NORMALIZATION_OPERATION, 'grid_sha256': GRID_SHA256,
        'vertical_application_count': 1,
        'points': [{'id': 'SYN-1', 'raw_xyz': list(gate.POINT_RAW),
                    'working': [-88.75, 43.1, 215.447]}],
    }
    return seal(body)


def seal(body):
    body = {key: value for key, value in body.items() if key != 'sha256'}
    return {**body, 'sha256': handoff.canonical_sha256(body)}


class RawInputTests(Refusals):
    def test_raw_tokens_header_and_bytes_are_kept(self):
        raw = gate.survey_bytes(raw=('870677.43220', '593234.5696', '820.20830'))
        survey = read_survey(raw)
        self.assertEqual(survey.points(), {'SYN-1': ('870677.43220', '593234.5696', '820.20830')})
        self.assertEqual(survey.raw_header, gate.HEADER)
        self.assertEqual(survey.raw_sha256, hashlib.sha256(raw).hexdigest())
        self.assertEqual((survey.crs, survey.units, survey.epoch),
                         ('EPSG:7600+6360', 'ftUS,ftUS,ftUS', '2010.00'))

    def test_legacy_headers_are_not_opt_in(self):
        for header in gate.LEGACY_HEADERS:
            self.assertFalse(is_opt_in((header + '\n1 2 3 4 5 a.jpg').encode()))
            self.refused('survey_opt_in_required', read_survey, gate.survey_bytes(header))

    def test_every_spelling_of_the_marker_is_claimed(self):
        for prefix in ('survey3d/1 ', 'SURVEY3D/2 ', '﻿SURVEY3D/1 ', '  SURVEY3D'):
            self.assertTrue(is_opt_in((prefix + 'x').encode()))
        self.refused('unsupported_survey_version', read_survey,
                     gate.survey_bytes(gate.HEADER.replace('SURVEY3D/1', 'SURVEY3D/2')))

    def test_header_refusals(self):
        for old, new, code in (
                ('7600+6360', '7600', 'unsupported_frame'),
                ('7600+6360', '4979', 'unsupported_frame'),
                ('7600+6360', '32616', 'unsupported_frame'),
                ('7600+6360', '7600+5703', 'unsupported_frame'),
                ('NAD83(2011)', 'WGS84', 'unsupported_frame'),
                ('ftUS,ftUS,ftUS', 'ftUS,ftUS,m', 'unsupported_units'),
                ('ftUS,ftUS,ftUS', 'ft,ft,ft', 'unsupported_units'),
                ('AXES=E,N,H', 'AXES=N,E,H', 'unsupported_axes'),
                ('2010.00', '2026.40', 'unsupported_epoch'),
                (' EPOCH=2010.00', '', 'unsupported_epoch'),
                ('GEOID18', 'GEOID12B', 'unsupported_geoid'),
                (' ROLES=SYNTHETIC', '', 'unsupported_role_profile'),
                (' ROLES=SYNTHETIC', ' ROLES=SYNTHETIC EPOCH=2010.00', 'malformed_survey_header'),
                (' ROLES=SYNTHETIC', ' ROLES=SYNTHETIC EXTRA=1', 'malformed_survey_header')):
            with self.subTest(new=new):
                self.refused(code, read_survey, gate.survey_bytes(gate.HEADER.replace(old, new)))

    def test_invalid_coordinates(self):
        for bad in ('nan', 'inf', '-inf', '1e3', '1_0', '0x10', '', '1,000'):
            with self.subTest(bad=bad):
                raw = gate.survey_bytes(raw=(*gate.POINT_RAW[:2], bad))
                code = 'malformed_survey_mark' if bad == '' else 'invalid_coordinate'
                self.refused(code, read_survey, raw)

    def test_point_identity(self):
        base = gate.survey_bytes().decode()
        self.refused('malformed_survey_mark', read_survey, base.replace(' SYN-1', '').encode())
        self.refused('duplicate_mark', read_survey, base.replace('-2.jpg', '-1.jpg').encode())
        moved = base.replace('820.2083 100 100 synthetic-2', '820.2084 100 100 synthetic-2')
        self.refused('conflicting_point_coordinates', read_survey, moved.encode())
        two = base.replace('synthetic-3.jpg SYN-1', 'synthetic-3.jpg SYN-2')
        self.refused('coincident_points', read_survey, two.encode())

    def test_unreadable_and_oversized(self):
        self.refused('survey_text_unreadable', read_survey, gate.survey_bytes() + b'\xff\xfe')
        self.refused('survey_file_too_large', read_survey, gate.survey_bytes() + b'#' * 2**21)
        self.refused('survey_marks_required', read_survey, gate.HEADER.encode())
        self.refused('raw_bytes_required', read_survey, gate.survey_bytes().decode())


class RoleTests(Refusals):
    HEADER = gate.HEADER.replace('SYNTHETIC', 'SIX-FIT')

    def file(self, labels, marks=3):
        rows = [f'{1000 + n}.5 2000.5 800.5 10 10 p{m}.jpg {label}'
                for n, label in enumerate(labels) for m in range(marks)]
        return '\n'.join([self.HEADER] + rows).encode()

    def test_exact_six_fit_labels_and_eighteen_marks(self):
        survey = read_survey(self.file(('1a', '5a', '6a', '7a', '8a', '9a')))
        self.assertEqual((len(survey.points()), len(survey.marks)), (6, 18))

    def test_evaluation_points_wait_for_render(self):
        for label in ('2a', '4a', 'CHK-2a', 'CHK-4a', 'chk-2a'):
            with self.subTest(label=label):
                self.refused('evaluation_point_before_render', read_survey,
                             self.file((*FIT_LABELS, label)))

    def test_excluded_and_unsuffixed_points(self):
        self.assertEqual(len(EXCLUDED_LABELS), 10)
        for label in (*EXCLUDED_LABELS, 'CHK-3a', 'CHK-1'):
            with self.subTest(label=label):
                self.refused('excluded_point', read_survey, self.file((*FIT_LABELS, label)))

    def test_membership_must_be_exact(self):
        self.refused('fit_membership_mismatch', read_survey, self.file(FIT_LABELS[:5]))
        self.refused('fit_membership_mismatch', read_survey, self.file(FIT_LABELS, marks=2))
        self.refused('unknown_point', read_survey, self.file((*FIT_LABELS, '10a')))
        self.refused('unknown_point', read_survey, self.file((*FIT_LABELS[:5], 'CHK-9a')))

    def test_profiles_do_not_mix(self):
        self.refused('synthetic_ids_required', read_survey,
                     gate.survey_bytes().replace(b'SYN-1', b'1a'))
        self.refused('unknown_point', read_survey, gate.survey_bytes(self.HEADER))


class ContractTests(Refusals):
    def test_relabelled_frames_are_refused_even_when_resealed(self):
        for crs in ('EPSG:4979', 'EPSG:32616', 'EPSG:26916', 'EPSG:7912'):
            body = declared_contract()
            body['working_frame'] = {**body['working_frame'], 'crs': crs}
            self.refused('working_frame_relabelled', handoff.verify_contract, seal(body))

    def test_digest_and_operation(self):
        body = declared_contract()
        self.refused('working_frame_digest_mismatch', handoff.verify_contract,
                     {**body, 'operation': 'other'})
        for change in ({'vertical_application_count': 0}, {'vertical_application_count': 2},
                       {'grid_sha256': '0' * 64}, {'points': []}):
            self.refused('working_frame_operation_mismatch', handoff.verify_contract,
                         seal({**body, **change}))
        self.refused('unsupported_epoch', handoff.verify_contract,
                     seal({**body, 'source': {**body['source'], 'epoch': '2020.00'}}))
        self.refused('working_frame_required', handoff.verify_contract, None)

    def test_reader_output_must_carry_every_field(self):
        contract = declared_contract()
        full = {**handoff.WORKING_FRAME, 'heights_m': {'SYN-1': 215.447}}
        handoff.require_preserved(contract, full)
        for key in handoff.WORKING_FRAME:
            error = self.refused(handoff.REFUSED, handoff.require_preserved, contract,
                                 {**full, key: 'WGS84'})
            self.assertEqual(error.details['lost'], [key])
        for height in (250.0, 215.458, math.nan, None, '215.447'):
            self.refused('engine_height_mismatch', handoff.require_preserved, contract,
                         {**full, 'heights_m': {'SYN-1': height}})

    def test_no_transform_is_authorized_and_unknown_engines_are_refused(self):
        self.assertEqual(handoff.AUTHORIZED_FRAME_TRANSFORMS, ())
        with tempfile.TemporaryDirectory() as folder:
            self.refused('engine_import_boundary_unqualified',
                         handoff.cross_import_handoff, declared_contract(), folder)
            modules = Path(folder) / 'modules'
            modules.mkdir()
            for name in ('gcp.py', 'location.py'):
                (modules / name).write_text('# other engine')
            self.refused('engine_import_boundary_unqualified',
                         handoff.cross_import_handoff, declared_contract(), folder)


class ExportTests(Refusals):
    def convert(self, source, destination, **options):
        self.calls += 1
        return {'source_sha256': options['source_sha256'], 'output_sha256': 'b' * 64,
                'grid_sha256': GRID_SHA256, 'valid_pixels': 3}

    def export(self, source_sha256='a' * 64, contract=None, ledger=None, convert=None):
        return export_dem('in.tif', 'out.tif', source_sha256=source_sha256, grid_path='g.tif',
                          contract=contract or declared_contract(),
                          ledger=ledger or ExportLedger(), convert=convert or self.convert)

    def setUp(self):
        self.calls = 0

    def test_one_conversion_per_source_and_never_of_an_output(self):
        ledger = ExportLedger()
        self.assertEqual(self.export(ledger=ledger)['vertical_application_count'], 1)
        self.refused('second_conversion', self.export, ledger=ledger)
        self.refused('second_conversion', self.export, source_sha256='b' * 64, ledger=ledger)
        self.assertEqual(self.calls, 1)

    def test_synthetic_only_and_contract_required(self):
        body = declared_contract()
        real = seal({**body, 'source': {**body['source'], 'roles': 'SIX-FIT'}})
        self.refused('synthetic_dem_only', self.export, contract=real)
        self.refused('working_frame_digest_mismatch', self.export,
                     contract={**body, 'points': ['x']})
        self.assertEqual(self.calls, 0)

    def test_converter_refusal_keeps_its_code_and_uses_no_allowance(self):
        def fail(*args, **kwargs):
            raise ValueError('explicit_epsg7541_horizontal_required')
        ledger = ExportLedger()
        self.refused('explicit_epsg7541_horizontal_required', self.export,
                     ledger=ledger, convert=fail)
        self.export(ledger=ledger)


class ProjectionTests(unittest.TestCase):
    def test_round_trip_and_recorded_proj_origin(self):
        recorded = json.loads((HERE.parent / 'fixtures/recorded-proj-witness.json').read_text())
        origin = recorded['dem']['geotransform']
        east, north = EPSG_7541.forward(-88.75, 43.1)
        self.assertAlmostEqual(east, origin[0], delta=1e-6)
        self.assertAlmostEqual(north, origin[3], delta=1e-6)
        for crs in (EPSG_7541, EPSG_7600):
            lon, lat = crs.inverse(*crs.forward(-88.4, 43.5))
            self.assertAlmostEqual(lon, -88.4, delta=1e-11)
            self.assertAlmostEqual(lat, 43.5, delta=1e-11)

    def test_survey_feet_and_metres_agree(self):
        feet, metres = EPSG_7600.forward(-88.6, 43.3), EPSG_7541.forward(-88.6, 43.3)
        for a, b in zip(feet, metres):
            self.assertAlmostEqual(a * US_SURVEY_FOOT_M, b, delta=1e-3)

    def test_grid_file_must_match(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaises(GridUnavailable):
                GeoidGrid(Path(folder) / 'absent.tif')
            wrong = Path(folder) / 'wrong.tif'
            wrong.write_bytes(b'wrong')
            with self.assertRaises(GridUnavailable):
                GeoidGrid(wrong)


@NEEDS_GRID
class GridTests(Refusals):
    @classmethod
    def setUpClass(cls):
        cls.grid = GeoidGrid(GRID_PATH)
        cls.bridge = ReferenceBridge(cls.grid)
        cls.survey = read_survey(gate.survey_bytes())

    def normalize(self, bridge=None, survey=None, ledger=None):
        return handoff.normalize_once(survey or self.survey, bridge or self.bridge, self.grid,
                                      ledger or handoff.Ledger())

    def test_same_size_wrong_bytes(self):
        with tempfile.TemporaryDirectory() as folder:
            wrong = Path(folder) / 'wrong.tif'
            data = bytearray(Path(GRID_PATH).read_bytes())
            data[-1] ^= 1
            wrong.write_bytes(data)
            with self.assertRaises(GridUnavailable):
                GeoidGrid(wrong)

    def test_independent_reference_matches_recorded_proj_output(self):
        recorded = json.loads((HERE.parent / 'fixtures/recorded-proj-witness.json').read_text())
        point, dem = recorded['point'], recorded['dem']
        h = point['navd88_m'] + self.grid.undulation(point['longitude'], point['latitude'])
        self.assertAlmostEqual(h, point['ellipsoidal_m'], delta=1e-9)
        gt = dem['geotransform']
        for cell in dem['navd88_cells']:
            x, y = cell['column'] + 0.5, cell['row'] + 0.5
            lon, lat = EPSG_7541.inverse(gt[0] + x * gt[1] + y * gt[2],
                                         gt[3] + x * gt[4] + y * gt[5])
            self.assertAlmostEqual(dem['ellipsoidal_m'] - self.grid.undulation(lon, lat),
                                   cell['navd88_m'], delta=1e-9)

    def test_outside_coverage(self):
        for lon, lat in ((-150.0, 43.0), (-88.0, 10.0), (-88.0, 70.0)):
            with self.assertRaises(GridUnavailable):
                self.grid.undulation(lon, lat)

    def test_normalizes_once_with_raw_record(self):
        ledger = handoff.Ledger()
        contract = handoff.verify_contract(self.normalize(ledger=ledger))
        point, = contract['points']
        self.assertEqual(point['raw_xyz'], list(gate.POINT_RAW))
        self.assertEqual(contract['vertical_application_count'], 1)
        self.assertAlmostEqual(point['working'][2], 215.4472, delta=0.001)
        self.refused('already_normalized', self.normalize, ledger=ledger)
        self.refused('already_normalized', self.normalize, survey=contract)

    def test_identity_ballpark_and_nonfinite_operations(self):
        self.refused('vertical_operation_identity', self.normalize, gate._Identity(self.bridge))
        self.refused('vertical_operation_mismatch', self.normalize, gate._Ballpark(self.bridge))

        class Altered(ReferenceBridge):
            change, back_change = (0.0, 0.0, 0.0), 0.0

            def survey_to_ellipsoid(self, xyz):
                return tuple(a + b for a, b in zip(super().survey_to_ellipsoid(xyz), self.change))

            def ellipsoid_to_survey(self, working):
                e, n, h = super().ellipsoid_to_survey(
                    tuple(a - b for a, b in zip(working, self.change)))
                return e, n, h + self.back_change

        for change, back, code in (
                ((0, 0, 0.0101), 0, 'vertical_operation_mismatch'),
                ((0, 0, 34.5528 * 2), 0, 'vertical_operation_mismatch'),
                ((0, 0, math.nan), 0, 'nonfinite_normalization'),
                ((0, 0, math.inf), 0, 'nonfinite_normalization'),
                ((2e-8, 0, 0), 0, 'horizontal_operation_mismatch'),
                ((0, 0, 0), 0.0034, 'round_trip_mismatch')):
            bridge = Altered(self.grid)
            bridge.change, bridge.back_change = change, back
            with self.subTest(code=code, change=change):
                self.refused(code, self.normalize, bridge)

    def test_height_range(self):
        for height in ('99999', '-9999'):
            survey = read_survey(gate.survey_bytes(raw=(*gate.POINT_RAW[:2], height)))
            self.refused('height_out_of_range', self.normalize, survey=survey)

    def test_entry_point_needs_the_pinned_grid_and_stops_at_the_handoff(self):
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / 'gcp.txt'
            target.write_bytes(gate.survey_bytes())
            self.refused('verified_geoid18_grid_required', handoff.import_survey_gcp, target,
                         folder, grid_path=Path(folder) / 'absent.tif', bridge=self.bridge)
            self.refused('engine_import_boundary_unqualified', handoff.import_survey_gcp, target,
                         folder, grid_path=GRID_PATH, bridge=self.bridge)


@NEEDS_GRID
class GateVerdictTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.grid = GeoidGrid(GRID_PATH)

    def setUp(self):
        bridge = ReferenceBridge(self.grid)
        contract = handoff.normalize_once(read_survey(gate.survey_bytes()), bridge, self.grid,
                                          handoff.Ledger())
        values, inverse = copy.deepcopy(gate.SAMPLES), {}
        for row, column in gate.WITNESSES:
            x, y = column + 0.5, row + 0.5
            lon, lat = EPSG_7541.inverse(
                gate.AFFINE[0] + x * gate.AFFINE[1] + y * gate.AFFINE[2],
                gate.AFFINE[3] + x * gate.AFFINE[4] + y * gate.AFFINE[5])
            values[row][column] -= self.grid.undulation(lon, lat)
            inverse[f'{row},{column}'] = gate.SAMPLES[row][column]
        files = {'import_handoff.py': 'c' * 64}
        self.receipt = {
            'schema': gate.SCHEMA, 'nonce': 'n' * 32, 'workload': {'photos': 0, 'jobs': 0},
            '_file_sha256': 'd' * 64, 'candidate_files': files,
            'engine': {'version': '3.8.6', 'files': dict(gate.ENGINE_FILES)},
            'runtime': {'python': 'x', 'gdal': 'x', 'osr_proj': 'x', 'pyproj': 'x',
                        'pyproj_proj': 'x', 'pyproj_network': False, 'osr_network': False,
                        'system_proj_db_sha256': gate.PROJ_DB_SHA256, 'grid_path': '/g.tif',
                        'grid_sha256': GRID_SHA256},
            'writer': {'container': 'e' * 12, 'executable': '/usr/bin/python3'},
            'normalization': {
                'contract': contract,
                'operation': '+step +proj=vgridshift +grids=/g.tif +multiplier=1 +step',
                'round_trip_ftus': [float(v) for v in gate.POINT_RAW]},
            'dem': {
                'values': values, 'inverse_heights_m': inverse, 'affine': list(gate.AFFINE),
                'bands': 1, 'size': [2, 2], 'data_type': 'Float64', 'mask': gate.MASK,
                'nodata': gate.NODATA, 'unit': 'm', 'compound_7541_5703': True,
                'height_reference': 'NAVD88_GEOID18_METRE', 'transform_count': '1',
                'source_sha256_before': 'a' * 64, 'source_sha256_after': 'a' * 64,
                'output_sha256': 'b' * 64,
                'receipt': {'source_sha256': 'a' * 64, 'output_sha256': 'b' * 64,
                            'vertical_application_count': 1, 'valid_pixels': 3,
                            'grid_sha256': GRID_SHA256}},
            'negatives': dict(gate.NEGATIVES),
            'legacy_compatibility': {header: True for header in gate.LEGACY_HEADERS},
            'reader_crossing': {'status': 'refused', 'code': handoff.REFUSED},
        }
        self.launcher = {
            'image': gate.IMAGE, 'container_id': 'e' * 64, 'nonce': 'n' * 32, 'exit_code': 0,
            'network_mode': 'none', 'read_only': True, 'receipt_sha256': 'd' * 64,
            'entrypoint': ['/usr/bin/python3'], 'candidate_files': files,
        }

    def verdict(self):
        result = gate.evaluate(self.receipt, self.launcher, self.grid)
        self.assertIs(result['admits_processing'], False)
        return result

    def test_refused_crossing_is_blocked_not_passed(self):
        result = self.verdict()
        self.assertEqual((result['verdict'], result['failed']),
                         ('blocked_import_handoff', ['reader_crossing']))

    def test_only_a_preserved_frame_and_height_can_pass(self):
        height = self.receipt['normalization']['contract']['points'][0]['working'][2]
        good = {'status': 'preserved', 'frame': dict(handoff.WORKING_FRAME), 'height_m': height}
        self.receipt['reader_crossing'] = good
        self.assertEqual(self.verdict()['verdict'], 'synthetic_pass')
        for change in ({'height_m': 250.0}, {'height_m': None},
                       {'frame': {**good['frame'], 'crs': 'EPSG:4979'}},
                       {'status': 'refused', 'code': 'verified_geoid18_grid_required'}):
            self.receipt['reader_crossing'] = {**good, **change}
            self.assertEqual(self.verdict()['verdict'], 'fail', change)

    def test_every_binding_is_required(self):
        cases = {
            'image': ('launcher', ['image'], 'sha256:' + '9' * 64),
            'same_invocation': ('launcher', ['nonce'], 'm' * 32),
            'isolated': ('launcher', ['network_mode'], 'bridge'),
            'receipt_bytes': ('launcher', ['receipt_sha256'], '0' * 64),
            'entrypoint': ('launcher', ['entrypoint'], ['/usr/bin/node']),
            'candidate_files': ('launcher', ['candidate_files'], {'x.py': '0' * 64}),
            'engine_version': ('receipt', ['engine', 'version'], '3.6.2'),
            'engine_files': ('receipt', ['engine', 'files', 'modules/gcp.py'], '0' * 64),
            'no_workload': ('receipt', ['workload', 'photos'], 1001),
            'network_off': ('receipt', ['runtime', 'osr_network'], True),
            'grid': ('receipt', ['runtime', 'grid_sha256'], '0' * 64),
            'proj_db': ('receipt', ['runtime', 'system_proj_db_sha256'], None),
            'operation': ('receipt', ['normalization', 'operation'], '+proj=noop'),
            'normalization_round_trip': (
                'receipt', ['normalization', 'round_trip_ftus', 2], 820.2117),
            'dem_affine': ('receipt', ['dem', 'affine', 2], 0.0),
            'dem_mask': ('receipt', ['dem', 'mask'], [[255, 255], [255, 255]]),
            'dem_nodata': ('receipt', ['dem', 'values', 1, 0], 0.0),
            'dem_units': ('receipt', ['dem', 'unit'], 'ftUS'),
            'dem_shape': ('receipt', ['dem', 'data_type'], 'Float32'),
            'dem_frame': ('receipt', ['dem', 'compound_7541_5703'], False),
            'dem_metadata': ('receipt', ['dem', 'transform_count'], '2'),
            'dem_source_unchanged': ('receipt', ['dem', 'source_sha256_after'], '0' * 64),
            'dem_output_bound': ('receipt', ['dem', 'output_sha256'], '0' * 64),
            'dem_applied_once': ('receipt', ['dem', 'receipt', 'vertical_application_count'], 2),
            'dem_round_trip': ('receipt', ['dem', 'inverse_heights_m', '0,0'], 220.0011),
            'negative_cases': ('receipt', ['negatives', 'identity_operation'], 'accepted'),
            'legacy_headers_unchanged': (
                'receipt', ['legacy_compatibility', 'EPSG:7600'], False),
        }
        for name, (target, path, value) in cases.items():
            with self.subTest(name=name):
                self.setUp()
                node = getattr(self, target)
                for key in path[:-1]:
                    node = node[key]
                node[path[-1]] = value
                result = self.verdict()
                self.assertEqual(result['verdict'], 'fail')
                self.assertEqual(result['failed'], sorted([name, 'reader_crossing']))

    def test_each_witness_limit_is_ten_millimetres(self):
        for row, column in gate.WITNESSES:
            for shift, expected in ((0.0099, 'blocked_import_handoff'), (0.0101, 'fail'),
                                    (-0.0101, 'fail'), (math.nan, 'fail')):
                self.setUp()
                self.receipt['dem']['values'][row][column] += shift
                self.assertEqual(self.verdict()['verdict'], expected)

    def test_wrong_height_in_a_sealed_contract_fails(self):
        contract = self.receipt['normalization']['contract']
        contract['points'][0]['working'][2] += 0.0101
        self.receipt['normalization']['contract'] = seal(contract)
        self.assertEqual(self.verdict()['failed'], ['normalization_height', 'reader_crossing'])

    def test_missing_data_fails_closed(self):
        for key in list(self.receipt):
            self.setUp()
            del self.receipt[key]
            self.assertEqual(self.verdict()['verdict'], 'fail', key)


if __name__ == '__main__':
    unittest.main(verbosity=2)
