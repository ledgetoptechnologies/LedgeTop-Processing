"""Standard library tests of the profile, contract, sink, ledger, frame chain and gate."""
import copy
import math
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from support import *  # noqa: E402,F403
from support import (  # noqa: E402
    AFFINE, EAST, GRID_PATH, NEEDS_GRID, NONCE, NORTH, OFFSET, ORIGIN, PLACES, PROFILE,
    RAW_PROFILE, SIZES, FrameRefused, GeoidGrid, Ledger, LocalFrame, ReferenceBridge, Refusals,
    chain, declared_contract, dem_frame, frame_contract, load_profile, normalize_once,
    profile_with, rows, seal, sink_items, survey_bytes, survey_frame)

from declared_frame import engine_hooks, gate  # noqa: E402
from declared_frame.geodesy import GridUnavailable, apply_affine, invert_affine  # noqa: E402

read = survey_frame.read_survey
HEADER = survey_frame.header_for(PROFILE)


class ProfileTests(Refusals):
    def test_digest_comes_from_outside(self):
        for digest in ('0' * 64, None, ''):
            self.refused('profile_digest_mismatch', load_profile, RAW_PROFILE, digest)

    def test_unsupported_values(self):
        for code, change in (
                ('unsupported_frame', {'source': {'realization': 'WGS84'}}),
                ('unsupported_frame', {'source': {'geoid': 'GEOID12B'}}),
                ('unsupported_epoch', {'source': {'epoch': 'UNKNOWN'}}),
                ('verified_geoid18_grid_required', {'grid': {'sha256': '0' * 64}}),
                ('camera_gps_frame_unqualified', {'camera_gps': 'use'}),
                ('epoch_review_required', {'kind': 'controlled'}),
                ('role_profile_invalid', {'roles': {'withheld': ['SYN-A']}}),
                ('role_profile_invalid', {'roles': {'excluded': ['syn-a']}}),
                ('role_profile_invalid', {'roles': {'fit': []}}),
                ('profile_malformed', {'kind': 'other'}),
                ('profile_malformed', {'source': {'extra': 'x'}}),
                ('profile_malformed', {'source': {'units': 'm m'}})):
            with self.subTest(change=change):
                self.refused(code, profile_with, **change)

    def test_controlled_profile_needs_a_review_digest(self):
        profile = profile_with(kind='controlled', epoch_review='e' * 64)
        self.assertEqual(profile.working_frame['epoch'], '2010.00')


class RawInputTests(Refusals):
    def test_raw_record_is_kept(self):
        data = rows()
        for row in data[:3]:
            row[2] += '0'
        survey = read(survey_bytes(data=data), PROFILE)
        self.assertEqual(survey.points()['SYN-A'], tuple(data[0][:3]))
        self.assertEqual((survey.raw_header, survey.profile_sha256), (HEADER, PROFILE.sha256))

    def test_legacy_headers_are_not_opt_in(self):
        for header in ('EPSG:4979', 'WGS84 UTM 19N', '+proj=utm +zone=19', 'EPSG:6525+6360'):
            self.refused('survey_opt_in_required', read, survey_bytes(header), PROFILE)

    def test_every_spelling_of_the_marker_is_claimed(self):
        for prefix in ('survey3d/2 ', 'SURVEY3D/9 ', chr(0xFEFF) + 'SURVEY3D/2', '  SURVEY3D'):
            self.assertTrue(survey_frame.is_opt_in((prefix + 'x').encode()))
        self.refused('unsupported_survey_version', read,
                     survey_bytes(HEADER.replace('SURVEY3D/2', 'SURVEY3D/1')), PROFILE)

    def test_header_must_equal_the_profile(self):
        for old, new, code in (
                ('6525+6360', '6525', 'unsupported_frame'),
                ('6525+6360', '4979', 'unsupported_frame'),
                ('NAD83(2011)', 'WGS84', 'unsupported_frame'),
                ('ftUS,ftUS,ftUS', 'ftUS,ftUS,m', 'unsupported_units'),
                ('AXES=E,N,H', 'AXES=N,E,H', 'unsupported_axes'),
                ('2010.00', '2026.40', 'unsupported_epoch'),
                (' EPOCH=2010.00', '', 'unsupported_epoch'),
                ('GEOID18', 'GEOID12B', 'unsupported_geoid'),
                (PROFILE.sha256, '0' * 64, 'profile_digest_mismatch'),
                ('AXES=', 'AXES=E,N,H AXES=', 'malformed_survey_header'),
                ('AXES=', 'OTHER=1 AXES=', 'malformed_survey_header')):
            with self.subTest(new=new):
                self.refused(code, read, survey_bytes(HEADER.replace(old, new)), PROFILE)

    def test_invalid_coordinates(self):
        for bad in ('nan', 'inf', '1e3', '1_0', '0x10', '1,000'):
            data = rows()
            data[0][2] = bad
            self.refused('invalid_coordinate', read, survey_bytes(data=data), PROFILE)

    def test_point_identity(self):
        data = rows()
        data[1][2] += '1'
        self.refused('conflicting_point_coordinates', read, survey_bytes(data=data), PROFILE)
        data = rows()
        data[1][5] = data[0][5]
        self.refused('duplicate_mark', read, survey_bytes(data=data), PROFILE)
        data = rows()
        for row in data[3:6]:
            row[:3] = data[0][:3]
        self.refused('coincident_points', read, survey_bytes(data=data), PROFILE)
        self.refused('malformed_survey_mark', read,
                     survey_bytes(data=[row[:6] for row in rows()]), PROFILE)

    def test_unreadable_and_oversized(self):
        self.refused('survey_text_unreadable', read, survey_bytes() + bytes((255, 254)), PROFILE)
        self.refused('survey_file_too_large', read, survey_bytes() + b'#' * 2**21, PROFILE)
        self.refused('survey_marks_required', read, HEADER.encode(), PROFILE)
        self.refused('raw_bytes_required', read, survey_bytes().decode(), PROFILE)


class RoleTests(Refusals):
    def with_label(self, label):
        data = rows()
        for row in data[6:9]:
            row[6] = label
        return survey_bytes(data=data)

    def test_withheld_and_excluded_never_enter_the_fit(self):
        for label in ('SYN-W', 'syn-w', 'CHK-SYN-W', 'chk-syn-w', 'CHK-CHK-SYN-W'):
            self.refused('withheld_point_before_render', read, self.with_label(label), PROFILE)
        for label in ('SYN-X', 'CHK-SYN-X', 'syn-x'):
            self.refused('excluded_point', read, self.with_label(label), PROFILE)

    def test_membership_is_exact(self):
        for label in ('SYN-D', 'syn-c', 'CHK-SYN-C'):
            self.refused('unknown_point', read, self.with_label(label), PROFILE)
        self.refused('fit_membership_mismatch', read, survey_bytes(data=rows()[:6]), PROFILE)
        self.refused('fit_membership_mismatch', read, survey_bytes(data=rows()[:8]), PROFILE)


class ContractTests(Refusals):
    def test_relabelled_frames_are_refused_even_when_resealed(self):
        for change in ({'crs': 'EPSG:4979'}, {'crs': 'EPSG:32619'}, {'epoch': '2020.00'},
                       {'axes': 'latitude,longitude,ellipsoidal_height'},
                       {'height': 'NAVD88'}):
            body = declared_contract()
            body['working_frame'] = {**body['working_frame'], **change}
            self.refused('working_frame_relabelled', frame_contract.verify_contract, seal(body),
                         PROFILE)

    def test_other_changes(self):
        body = declared_contract()
        verify = frame_contract.verify_contract
        self.refused('working_frame_digest_mismatch', verify, {**body, 'kind': 'x'}, PROFILE)
        self.refused('working_frame', verify, None, PROFILE)
        self.refused('profile_digest_mismatch', verify, body, profile_with(
            roles={'excluded': ['SYN-Z']}))
        self.refused('source_frame_mismatch', verify,
                     seal({**body, 'source': {**body['source'], 'epoch': '1997.00'}}), PROFILE)
        self.refused('epoch_review_mismatch', verify, seal({**body, 'epoch_review': 'x'}), PROFILE)
        for change in ({'vertical_application_count': 2}, {'grid_sha256': '0' * 64}):
            self.refused('working_frame_operation_mismatch', verify, seal({**body, **change}),
                         PROFILE)
        self.refused('fit_membership_mismatch', verify,
                     seal({**body, 'points': body['points'][:2]}), PROFILE)
        self.refused('duplicate_point', verify,
                     seal({**body, 'points': body['points'] + body['points'][:1]}), PROFILE)


class SinkTests(Refusals):
    def setUp(self):
        self.contract = declared_contract()
        self.items = sink_items(self.contract)

    def verify(self, items=None, sizes=SIZES):
        return frame_contract.verify_sink(self.contract, PROFILE, items or self.items, sizes)

    def test_complete_sink_is_sealed(self):
        sink = self.verify()
        self.assertEqual((sink['points'], sink['observations']), (3, 9))
        self.assertEqual(sink, self.verify(list(reversed(self.items))))

    def test_ids_and_counts(self):
        self.refused('sink_count_mismatch', self.verify, self.items[:2])
        self.refused('sink_count_mismatch', self.verify,
                     self.items + [{**self.items[0], 'id': 'SYN-W'}])
        self.refused('sink_duplicate_id', self.verify, self.items[:2] + self.items[:1])
        self.items[0]['id'] = 'syn-a'
        self.refused('sink_id_mismatch', self.verify)

    def test_swapped_ids_do_not_pass(self):
        self.items[0]['id'], self.items[1]['id'] = self.items[1]['id'], self.items[0]['id']
        self.refused('sink_horizontal_mismatch', self.verify)

    def test_axis_order(self):
        item = self.items[0]
        item['longitude'], item['latitude'] = item['latitude'], item['longitude']
        self.refused('sink_axis_order', self.verify)

    def test_coordinates(self):
        for key, change, code in (('longitude', 1e-11, 'sink_horizontal_mismatch'),
                                  ('latitude', 1e-11, 'sink_horizontal_mismatch'),
                                  ('height', 1e-8, 'sink_height_mismatch'),
                                  ('height', 27.0, 'sink_height_mismatch'),
                                  ('height', math.nan, 'sink_nonfinite')):
            self.setUp()
            self.items[1][key] += change
            self.refused(code, self.verify)
        self.setUp()
        self.items[1]['has_height'] = False
        self.refused('sink_height_mismatch', self.verify)

    def test_observations(self):
        def drop(o): o.pop()
        def repeat(o): o.append(list(o[0]))
        def foreign(o): o[0][0] = 'other.jpg'
        def moved(o): o[0][1] += 1e-9
        def swapped(o): o[0][1], o[0][2] = o[0][2], o[0][1]
        for change in (drop, repeat, foreign, moved, swapped):
            self.setUp()
            change(self.items[2]['observations'])
            self.refused('sink_observation_mismatch', self.verify)
        self.setUp()
        self.refused('sink_image_missing', self.verify, None, {'image-0.jpg': (4000, 3000)})


class LedgerTests(Refusals):
    def setUp(self):
        self.folder = self.enterContext(tempfile.TemporaryDirectory())

    def test_claims_outlive_the_object_and_the_run(self):
        Ledger(self.folder, run_nonce=NONCE).claim('export', '1' * 64)
        for nonce in (NONCE, 'b' * 64):
            self.refused('second_conversion', Ledger(self.folder, run_nonce=nonce).claim,
                         'export', '1' * 64)
        Ledger(self.folder, run_nonce=NONCE).claim('normalize', '1' * 64)
        self.refused('already_normalized', Ledger(self.folder, run_nonce=NONCE).claim,
                     'normalize', '1' * 64)

    def test_entries_are_this_runs(self):
        Ledger(self.folder, run_nonce='b' * 64).claim('export', '2' * 64)
        mine = Ledger(self.folder, run_nonce=NONCE)
        mine.claim('normalize', '1' * 64)
        self.assertEqual(mine.entries(), [{'step': 'normalize', 'key': '1' * 64}])

    def test_invalid_use(self):
        self.refused('durable_ledger_required', Ledger, self.folder + '/absent', run_nonce=NONCE)
        self.refused('run_nonce_required', Ledger, self.folder, run_nonce='')
        ledger = Ledger(self.folder, run_nonce=NONCE)
        for step, key in (('../x', '1' * 64), ('export', '../x'), ('export', None)):
            self.refused('ledger_key_invalid', ledger.claim, step, key)

    def test_damage_is_not_an_empty_ledger(self):
        (Path(self.folder) / ('export-' + '1' * 64 + '.json')).write_text('')
        ledger = Ledger(self.folder, run_nonce=NONCE)
        self.refused('ledger_damaged', ledger.entries)
        self.refused('second_conversion', ledger.claim, 'export', '1' * 64)


class ExactMapTests(unittest.TestCase):
    def test_round_trips(self):
        frame = LocalFrame(*ORIGIN)
        for lon, lat, h in ((-71.5, 43.5, 0.0), (-71.46, 43.53, 310.0), (-71.6, 43.4, -20.0)):
            back = frame.to_geodetic(frame.to_local(lon, lat, h))
            for a, b, limit in zip(back, (lon, lat, h), (1e-11, 1e-11, 1e-7)):
                self.assertAlmostEqual(a, b, delta=limit)
        inverse = invert_affine(AFFINE)
        point = apply_affine(inverse, apply_affine(AFFINE, (100.0, -50.0, 7.0)))
        for a, b in zip(point, (100.0, -50.0, 7.0)):
            self.assertAlmostEqual(a, b, delta=1e-8)
        with self.assertRaises(ValueError):
            invert_affine([[1, 0, 0, 0], [1, 0, 0, 0], [0, 0, 1, 0]])

    def test_engine_matrix_is_undone_exactly(self):
        record = chain(declared_contract())['georeference']
        lon, lat, h = -71.49, 43.51, 222.0
        engine = apply_affine(AFFINE, LocalFrame(*ORIGIN).to_local(lon, lat, h))
        east, north, height = dem_frame.exact_point(record, PROFILE, engine)
        expected = PROFILE.output.forward(lon, lat)
        for a, b in zip((east, north, height), (*expected, h)):
            self.assertAlmostEqual(a, b, delta=1e-5)

    def test_a_linear_matrix_alone_misses_the_gate_limit(self):
        # Why the cloud is mapped per point: up in a local frame is not height.
        up = LocalFrame(*ORIGIN).to_local(-71.49, 43.51, 200.0)[2]
        self.assertGreater(abs(up - 200.0), 0.14)

    def test_pipeline_states_every_step_and_no_datum_or_constant_shift(self):
        text = dem_frame.exact_pipeline(chain(declared_contract())['georeference'], PROFILE)
        self.assertEqual([s.split()[0] for s in text.split('+step ')[1:]],
                         ['+proj=affine', '+inv', '+inv', '+proj=tmerc'])
        for word in ('grids', 'geoid', 'towgs84', 'helmert', 'datum'):
            self.assertNotIn(word, text)


class ChainTests(Refusals):
    def setUp(self):
        self.contract = declared_contract()
        self.chain = chain(self.contract)
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.ledger = Ledger(self.folder, run_nonce=NONCE)
        self.source = self.folder / 'dem.bin'
        self.source.write_bytes(b'invented dem')
        self.rebind(dem_frame.file_sha256(self.source))
        self.calls = 0

    def rebind(self, digest):
        self.chain['dem'] = seal({**self.chain['dem'], 'dem_sha256': digest})

    def convert(self, source, destination, **options):
        self.calls += 1
        self.assertEqual(options['horizontal_epsg'], PROFILE.output_epsg)
        return {'source_sha256': options['source_sha256'], 'output_sha256': '4' * 64,
                'grid_sha256': 'x', 'valid_pixels': 3}

    def export(self, value=None, profile=PROFILE, convert=None):
        return dem_frame.export_dem(
            self.source, self.folder / 'out', chain=value or self.chain, profile=profile,
            grid_path='g', ledger=self.ledger, convert=convert or self.convert)

    def test_export_once_across_restarts(self):
        self.assertEqual(self.export()['vertical_application_count'], 1)
        self.ledger = Ledger(self.folder, run_nonce='b' * 64)
        self.refused('second_conversion', self.export)
        self.assertEqual(self.calls, 1)
        self.assertTrue(self.ledger.claimed('export', '4' * 64))

    def test_failed_conversion_keeps_its_claim(self):
        def fail(*args, **kwargs):
            raise ValueError('declared_horizontal_required')
        self.refused('declared_horizontal_required', self.export, convert=fail)
        self.refused('second_conversion', self.export)

    def test_dem_must_be_the_engines(self):
        self.source.write_bytes(b'another dem')
        self.refused('dem_not_engine_derived', self.export)
        for missing in self.chain:
            part = {key: value for key, value in self.chain.items() if key != missing}
            self.refused('engine_derived_frame_required', self.export, part)
        self.refused('engine_derived_frame_required', dem_frame.verify_chain, None, PROFILE)
        self.assertEqual(self.calls, 0)

    def test_every_link_is_bound(self):
        for name, key in (('sink', 'contract_sha256'), ('georeference', 'sink_sha256'),
                          ('georeference', 'contract_sha256'), ('georeference', 'working_frame'),
                          ('georeference', 'output_epsg'), ('cloud', 'georeference_sha256'),
                          ('dem', 'cloud_sha256'), ('dem', 'working_frame'), ('dem', 'epsg'),
                          ('dem', 'vertical_application_count')):
            value = copy.deepcopy(self.chain)
            value[name] = seal({**value[name], key: 'other'})
            # Re-link everything after the change, so only the changed link is wrong.
            after = [('sink', 'georeference', 'sink_sha256'),
                     ('georeference', 'cloud', 'georeference_sha256'),
                     ('cloud', 'dem', 'cloud_sha256')]
            order = [step[0] for step in after] + ['dem']
            for parent, child, link in after[order.index(name):]:
                value[child] = seal({**value[child], link: value[parent]['sha256']})
            with self.subTest(link=(name, key)):
                self.refused('frame_chain_broken', self.export, value)
            value[name]['extra'] = 1
            self.refused(name + '_digest_mismatch', self.export, value)

    def test_controlled_profiles_are_not_exported_here(self):
        profile = profile_with(kind='controlled', epoch_review='e' * 64)
        body = declared_contract()
        contract = seal({**body, 'kind': 'controlled', 'profile_sha256': profile.sha256,
                         'epoch_review': 'e' * 64})
        value = {**self.chain, 'contract': contract}
        value['sink'] = seal({**value['sink'], 'contract_sha256': contract['sha256']})
        value['georeference'] = seal({**value['georeference'],
                                      'contract_sha256': contract['sha256'],
                                      'sink_sha256': value['sink']['sha256']})
        value['cloud'] = seal({**value['cloud'],
                               'georeference_sha256': value['georeference']['sha256']})
        value['dem'] = seal({**value['dem'], 'cloud_sha256': value['cloud']['sha256']})
        self.refused('synthetic_dem_only', self.export, value, profile)

    def test_cloud_is_complete_and_correct(self):
        record = self.chain['georeference']
        good = [(p, dem_frame.exact_point(record, PROFILE, p))
                for p in ((1.0, 2.0, 3.0), (50.0, 60.0, 70.0), (-9.0, 8.0, 7.0))]
        bind = lambda **change: dem_frame.bind_cloud(record, PROFILE, **{
            'source_sha256': '1' * 64, 'output_sha256': '2' * 64, 'source_count': 3,
            'output_count': 3, 'witnesses': good, **change})
        bind()
        self.refused('cloud_incomplete', bind, output_count=2)
        self.refused('cloud_incomplete', bind, source_count=0, output_count=0)
        self.refused('cloud_witnesses_required', bind, witnesses=good[:2])
        for axis in range(3):
            moved = [list(pair) for pair in good]
            moved[1][1] = [v + (0.0101 if i == axis else 0) for i, v in enumerate(good[1][1])]
            self.refused('cloud_point_mismatch', bind, witnesses=moved)

    def test_dem_format(self):
        base = dict(cloud_sha256='2' * 64, dem_sha256='3' * 64, epsg=PROFILE.output_epsg,
                    affine=[0.0, 1.0, 0.0, 0.0, 0.0, -1.0], data_type='Float64', unit='m')
        bind = lambda **change: dem_frame.bind_dem(
            self.chain['cloud'], self.chain['georeference'], PROFILE, **{**base, **change})
        bind()
        for change in ({'epsg': 32619}, {'data_type': 'Float32'}, {'unit': 'ftUS'},
                       {'affine': [math.nan] * 6}):
            self.refused('dem_frame_mismatch', bind, **change)
        self.refused('frame_chain_broken', bind, cloud_sha256='9' * 64)

    def test_georeference_needs_the_checked_sink(self):
        make = lambda **change: dem_frame.georeference(**{
            'contract': self.contract, 'sink': self.chain['sink'], 'profile': PROFILE,
            'origin': ORIGIN, 'affine': AFFINE, 'offset': OFFSET, **change})
        make()
        self.refused('sink', make, sink=None)
        self.refused('frame_chain_broken', make,
                     sink=seal({**self.chain['sink'], 'contract_sha256': '0' * 64}))
        self.refused('georeference_nonfinite', make, origin=(math.nan, 0, 0))
        self.refused('affine_not_invertible', make, affine=[[0.0] * 4] * 3)


@NEEDS_GRID
class GridTests(Refusals):
    @classmethod
    def setUpClass(cls):
        cls.grid = GeoidGrid(GRID_PATH)
        cls.bridge = ReferenceBridge(cls.grid)

    def setUp(self):
        self.folder = self.enterContext(tempfile.TemporaryDirectory())
        self.count = 0

    def normalize(self, bridge=None, data=None, ledger=None):
        self.count += 1
        return normalize_once(read(survey_bytes(data=data), PROFILE), PROFILE,
                              bridge or self.bridge, self.grid,
                              ledger or self.fresh())

    def fresh(self):
        folder = Path(self.folder) / str(self.count)
        folder.mkdir(exist_ok=True)
        return Ledger(folder, run_nonce=NONCE)

    def test_normalizes_once_and_durably(self):
        ledger = self.fresh()
        contract = frame_contract.verify_contract(self.normalize(ledger=ledger), PROFILE)
        self.assertEqual([p['raw_xyz'] for p in contract['points']],
                         [rows()[n][:3] for n in (0, 3, 6)])
        for point in contract['points']:
            lon, lat, height = PLACES[point['id']]
            self.assertAlmostEqual(point['working'][2], height + self.grid.undulation(lon, lat),
                                   delta=1e-4)
        self.refused('already_normalized', self.normalize, ledger=ledger)
        self.refused('already_normalized', normalize_once, contract, PROFILE, self.bridge,
                     self.grid, self.fresh())

    def test_wrong_operations(self):
        class Altered(ReferenceBridge):
            change, back, drop = (0, 0, 0), 0, False

            def survey_to_ellipsoid(self, xyz):
                lon, lat, h = super().survey_to_ellipsoid(xyz)
                if self.drop:
                    h = xyz[2] * self.profile.survey_unit_m
                return lon + self.change[0], lat + self.change[1], h + self.change[2]

            def ellipsoid_to_survey(self, working):
                e, n, h = super().ellipsoid_to_survey(
                    tuple(a - b for a, b in zip(working, self.change)))
                return e, n, h + self.back
        for values, code in (
                ({'drop': True}, 'vertical_operation_identity'),
                ({'change': (0, 0, 0.5)}, 'vertical_operation_mismatch'),
                ({'change': (0, 0, 0.0101)}, 'vertical_operation_mismatch'),
                ({'change': (0, 0, 54.5)}, 'vertical_operation_mismatch'),
                ({'change': (0, 0, math.nan)}, 'nonfinite_normalization'),
                ({'change': (2e-8, 0, 0)}, 'horizontal_operation_mismatch'),
                ({'back': 0.0034}, 'round_trip_mismatch')):
            bridge = Altered(self.grid)
            for key, value in values.items():
                setattr(bridge, key, value)
            with self.subTest(values=values):
                self.refused(code, self.normalize, bridge)

    def test_heights_and_coverage(self):
        data = rows()
        for row in data[:3]:
            row[2] = '99999'
        self.refused('height_out_of_range', self.normalize, data=data)
        data = rows()
        for row in data[:3]:
            row[0] = '-90000000'
        self.refused('vertical_operation_failed', self.normalize, data=data)

    def test_wrong_grid_bytes(self):
        wrong = Path(self.folder) / 'wrong.tif'
        raw = bytearray(Path(GRID_PATH).read_bytes())
        raw[-1] ^= 1
        wrong.write_bytes(raw)
        for path in (wrong, Path(self.folder) / 'absent.tif'):
            with self.assertRaises(GridUnavailable):
                GeoidGrid(path)


@NEEDS_GRID
class GateTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.grid = GeoidGrid(GRID_PATH)

    def setUp(self):
        saved = engine_hooks.UNPORTED
        self.addCleanup(setattr, engine_hooks, 'UNPORTED', saved)
        folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        ledger = Ledger(folder, run_nonce=NONCE)
        contract = normalize_once(read(survey_bytes(), PROFILE), PROFILE,
                                  ReferenceBridge(self.grid), self.grid, ledger)
        self.chain = chain(contract)
        dem = self.chain['dem']
        a, witnesses = dem['affine'], []
        for row, column, before in ((0, 0, 220.0), (0, 1, 245.5), (1, 1, 281.25)):
            x, y = column + 0.5, row + 0.5
            lon, lat = PROFILE.output.inverse(a[0] + x * a[1] + y * a[2],
                                              a[3] + x * a[4] + y * a[5])
            witnesses.append({'row': row, 'column': column, 'before': before, 'inverse': before,
                              'after': before - self.grid.undulation(lon, lat)})
        out = '4' * 64
        self.receipt = {
            'chain': self.chain, 'ledger_id': 'ledger-1', 'network': {'pyproj': False, 'osr': False},
            'ledger': [{'step': 'normalize', 'key': contract['source']['raw_sha256']},
                       {'step': 'export', 'key': dem['dem_sha256']}, {'step': 'export', 'key': out}],
            'negatives': dict(gate.NEGATIVES), 'workload': {'photos': 0, 'jobs': 0},
            'export': {'dem_frame_sha256': dem['sha256'], 'source_sha256': dem['dem_sha256'],
                       'output_sha256': out, 'vertical_application_count': 1},
            'dem': {'witnesses': witnesses, 'affine_after': list(a), 'size_before': [2, 2],
                    'size_after': [2, 2], 'mask_before': [[255, 255], [0, 255]],
                    'mask_after': [[255, 255], [0, 255]], 'masked_cells': 1,
                    'nodata_before': -9999.0, 'nodata_after': -9999.0, 'data_type': 'Float64',
                    'unit': 'm', 'compound_epsg': [PROFILE.output_epsg, 5703],
                    'height_reference': 'NAVD88_GEOID18_METRE', 'transform_count': '1',
                    'source_sha256_after': dem['dem_sha256'], 'output_sha256': out}}
        self.manifest = {'profile_sha256': PROFILE.sha256}

    def verdict(self, verify=None):
        result = gate.evaluate(
            {'profile': RAW_PROFILE}, trust_sha256='t', sealer='s', grid=self.grid,
            verify=verify or (lambda bundle, **kw: (
                self.manifest, {'ledger_id': 'ledger-1'}, self.receipt)))
        self.assertIs(result['admits_processing'], False)
        return result

    def test_blocked_while_anything_is_unported(self):
        self.assertEqual(self.verdict()['verdict'], 'blocked_unported')
        engine_hooks.UNPORTED = ()
        self.assertEqual(self.verdict(), {'verdict': 'synthetic_pass', 'failed': [],
                                          'admits_processing': False})

    def test_provenance_refusal_is_final(self):
        def refuse(bundle, **kwargs):
            raise FrameRefused('writer_signature_invalid')
        self.assertEqual(self.verdict(refuse)['failed'], ['writer_signature_invalid'])

    def test_each_requirement(self):
        engine_hooks.UNPORTED = ()
        cases = {
            'once_only_ledger': (['ledger'], []), 'ledger_custody': (['ledger_id'], 'other'),
            'negatives': (['negatives', 'sink_axis_order'], 'accepted'),
            'workload': (['workload', 'photos'], 1), 'network_off': (['network', 'osr'], True),
            'dem_heights': (['dem', 'witnesses', 0, 'after'], 250.0),
            'dem_round_trip': (['dem', 'witnesses', 1, 'inverse'], 245.5011),
            'dem_grid_kept': (['dem', 'size_after'], [3, 2]),
            'dem_mask_kept': (['dem', 'mask_after'], [[255, 255], [255, 255]]),
            'dem_nodata_kept': (['dem', 'nodata_after'], 0.0),
            'dem_format': (['dem', 'data_type'], 'Float32'),
            'dem_frame': (['dem', 'transform_count'], '2'),
            'dem_source_kept': (['dem', 'source_sha256_after'], '0' * 64),
            'export_bound': (['export', 'vertical_application_count'], 2),
        }
        for name, (path, value) in cases.items():
            self.setUp()
            engine_hooks.UNPORTED = ()
            node = self.receipt
            for key in path[:-1]:
                node = node[key]
            node[path[-1]] = value
            with self.subTest(name=name):
                self.assertEqual(self.verdict()['failed'], [name])

    def test_limits(self):
        for shift, verdict in ((0.0099, 'synthetic_pass'), (0.0101, 'fail'), (-0.0101, 'fail'),
                               (math.nan, 'fail')):
            self.setUp()
            engine_hooks.UNPORTED = ()
            self.receipt['dem']['witnesses'][2]['after'] += shift
            self.assertEqual(self.verdict()['verdict'], verdict)

    def test_missing_data_fails_closed(self):
        for key in list(self.receipt):
            self.setUp()
            engine_hooks.UNPORTED = ()
            del self.receipt[key]
            self.assertEqual(self.verdict()['verdict'], 'fail', key)


if __name__ == '__main__':
    unittest.main(verbosity=2)
