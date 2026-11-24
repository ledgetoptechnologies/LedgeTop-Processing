"""Run the real engine source with the hooks applied; only its libraries are stubbed.

Needs DECLARED_FRAME_ENGINE: a folder with the six files named in engine_patches.py at
their pinned hashes. Engine source is not in this repository.
"""
import importlib.util
import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path

try:
    import numpy  # noqa: F401  loaded once; each test resets sys.modules
    import numpy.ma  # noqa: F401
except ImportError:
    numpy = None

sys.path.insert(0, str(Path(__file__).resolve().parent))
from support import (  # noqa: E402
    GRID_PATH, NEEDS_GRID, NONCE, ORIGIN, PLACES, PROFILE, RAW_PROFILE, SIZE, FrameRefused,
    GeoidGrid, LocalFrame, ReferenceBridge, Refusals, rows, seal, survey_bytes)

from declared_frame import dem_frame, engine_hooks, engine_patches  # noqa: E402

SOURCE = os.environ.get('DECLARED_FRAME_ENGINE')
NEEDS_ENGINE = unittest.skipUnless(
    SOURCE and numpy, "needs numpy and the pinned engine files: DECLARED_FRAME_ENGINE")


class _Anything(types.ModuleType):
    def __getattr__(self, name):
        if name.startswith('__'):
            raise AttributeError(name)
        return type(name, (), {})


class _Crs:
    parsed = []
    axis_info = [types.SimpleNamespace(unit_conversion_factor=1.0)] * 3

    def __init__(self, text):
        self.text = text

    @classmethod
    def from_epsg(cls, code):
        cls.parsed.append(f'epsg:{code}')
        return cls(f'epsg:{code}')

    @classmethod
    def from_proj4(cls, text):
        cls.parsed.append(text)
        return cls(text)

    def to_epsg(self):
        return 4979

    def to_proj4(self):
        return self.text


class _Transform:
    def __init__(self, *args):
        pass

    def TransformPoint(self, x, y, z):  # noqa: N802
        return -71.5, 43.5, z


class _Point:
    def __init__(self):
        self.observations = []
        self.lla, self.has_altitude = {}, False

    def add_observation(self, observation):
        self.observations.append(observation)


class _Converter:
    def __init__(self, lat, lon, alt):
        self.lat, self.lon, self.alt = lat, lon, alt

    def to_topocentric(self, lat, lon, alt):
        return LocalFrame(self.lat, self.lon, self.alt).to_local(lon, lat, alt)


class _Photo:
    width, height = SIZE

    def __init__(self, name):
        self.filename = name


def _stubs():
    pyproj = types.ModuleType('pyproj')
    pyproj.CRS, pyproj.Proj, pyproj.Transformer = _Crs, object, object
    osr = _Anything('osgeo.osr')
    osr.SpatialReference = lambda: types.SimpleNamespace(
        ImportFromEPSG=lambda *a: 0, ImportFromProj4=lambda *a: 0,
        SetAxisMappingStrategy=lambda *a: 0)
    osr.CoordinateTransformation, osr.OAMS_TRADITIONAL_GIS_ORDER = _Transform, 0
    osgeo = types.ModuleType('osgeo')
    osgeo.osr = osr
    result = {'pyproj': pyproj, 'osgeo': osgeo, 'osgeo.osr': osr}
    modules = types.ModuleType('modules')
    modules.__path__ = []
    result['modules'] = modules
    for name in ('log', 'io', 'system', 'multispectral', 'progress', 'photo', 'context',
                 'cropper', 'point_cloud', 'osfm', 'boundary', 'align', 'utils'):
        result[f'modules.{name}'] = _Anything(f'modules.{name}')
        setattr(modules, name, result[f'modules.{name}'])
    modules.log.INFO = modules.log.WARNING = modules.log.ERROR = lambda *a: None
    modules.io.file_exists = os.path.isfile
    modules.progress.progressbc = types.SimpleNamespace()
    opensfm = types.ModuleType('opensfm')
    opensfm.__path__ = []
    result['opensfm'] = opensfm
    for name in ('config', 'features', 'geo', 'io', 'masking', 'pygeometry', 'pymap', 'rig',
                 'types', 'pysfm', 'dataset_base'):
        result[f'opensfm.{name}'] = _Anything(f'opensfm.{name}')
        setattr(opensfm, name, result[f'opensfm.{name}'])
    opensfm.geo.TopocentricConverter = _Converter
    opensfm.io.json_load = json.load
    opensfm.io.open_wt = lambda path: open(path, 'w')
    opensfm.io.json_dump = lambda value, stream, *a: json.dump(value, stream)
    opensfm.pymap.GroundControlPoint = _Point
    opensfm.pymap.GroundControlPointObservation = type('Observation', (), {})
    opensfm.pymap.GroundControlPointRole = types.SimpleNamespace(GCP='gcp')
    for name in ('fiona', 'fiona.crs', 'pipes', 'PIL', 'PIL.PngImagePlugin'):
        result[name] = _Anything(name)
    return result


def _load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    parent, _, leaf = name.rpartition('.')
    if parent:
        setattr(sys.modules[parent], leaf, module)
    spec.loader.exec_module(module)
    return module


@NEEDS_ENGINE
class EngineCase(Refusals):
    patched = True

    def setUp(self):
        self.work = Path(self.enterContext(tempfile.TemporaryDirectory()))
        before = dict(sys.modules)
        self.addCleanup(lambda: (sys.modules.clear(), sys.modules.update(before)))
        sys.modules.update(_stubs())
        _Crs.parsed.clear()
        for name in engine_patches.PATCHES:
            raw = (Path(SOURCE) / name).read_bytes()
            target = self.work / 'engine' / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(engine_patches.apply(name, raw) if self.patched else raw)
        self.location = self.load('modules.location', 'modules/location.py')
        self.gcp = self.load('modules.gcp', 'modules/gcp.py')
        self.types = self.load('modules.types', 'modules/types.py')
        self.run_state()

    def load(self, name, path):
        return _load(name, self.work / 'engine' / path)

    def run_state(self):
        run = self.work / 'run'
        for folder in ('state', 'ledger'):
            (run / folder).mkdir(parents=True)
        (run / 'profile.json').write_bytes(RAW_PROFILE)
        (run / 'manifest.json').write_text(json.dumps({'profile_sha256': PROFILE.sha256}))
        (run / 'authorization.json').write_text(json.dumps({'run_nonce': NONCE}))
        for name, value in (('RUN', run), ('UNPORTED', ())):
            self.addCleanup(setattr, engine_hooks, name, getattr(engine_hooks, name))
            setattr(engine_hooks, name, value)
        self.run = run
        if GRID_PATH:
            real = engine_hooks.open_survey
            self.addCleanup(setattr, engine_hooks, 'open_survey', real)
            self.addCleanup(setattr, engine_hooks, 'GeoidGrid', engine_hooks.GeoidGrid)
            grid = GeoidGrid(GRID_PATH)
            engine_hooks.GeoidGrid = lambda path: grid
            engine_hooks.open_survey = lambda path: real(path, bridge=ReferenceBridge(grid))

    def write(self, text=None):
        path = self.work / 'gcp.txt'
        path.write_bytes(text if text is not None else survey_bytes())
        return str(path)

    def photos(self):
        return [_Photo(f'image-{n}.jpg') for n in range(4)]


class UnpatchedTests(EngineCase):
    """What the pinned engine does today, so the hooks are shown to change it."""
    patched = False

    def test_it_stops_on_the_marker_without_the_hooks(self):
        with self.assertRaises(RuntimeError) as caught:
            self.gcp.GCPFile(self.write())
        self.assertNotIsInstance(caught.exception, FrameRefused)

    def test_it_relabels_any_accepted_file(self):
        source = self.gcp.GCPFile(self.write(b'EPSG:6319\n-71.5 43.5 222.7 1 2 image-0.jpg A\n'))
        copy = source.create_utm_copy(str(self.work / 'utm.txt'))
        self.assertEqual(Path(copy).read_text().splitlines()[0], 'WGS84 UTM 19N')


class PatchTests(EngineCase):
    def test_wrong_source_is_never_patched(self):
        for name in engine_patches.PATCHES:
            raw = (Path(SOURCE) / name).read_bytes()
            with self.assertRaisesRegex(ValueError, 'engine_source_not_pinned'):
                engine_patches.apply(name, raw + b'\n')

    def test_legacy_files_read_and_convert_as_before(self):
        original = _load('modules.gcp_original', Path(SOURCE) / 'modules/gcp.py')
        for header in ('EPSG:4979', 'WGS84 UTM 19N', 'EPSG:6525', '+proj=utm +zone=19'):
            path = self.write((header + '\n1.5 2.5 3.5 10 20 image-0.jpg A\n').encode())
            seen = []
            for module in (original, self.gcp):
                _Crs.parsed.clear()
                item = module.GCPFile(path)
                utm = Path(item.create_utm_copy(str(self.work / 'utm.txt'))).read_text()
                out = module.GCPFile(path).export_opensfm_json(
                    str(self.work / 'out.json'), self.photos())
                seen.append((utm, Path(out).read_text(), item.entries, list(_Crs.parsed)))
            self.assertEqual(seen[0], seen[1])
            self.assertIsNone(item.survey)

    def test_nothing_starts_while_a_consumer_is_unported(self):
        engine_hooks.UNPORTED = ('something',)
        error = self.refused('successor_incomplete', self.gcp.GCPFile, self.write())
        self.assertEqual(error.details, {'unported': ['something']})
        self.assertEqual(list((self.run / 'ledger').iterdir()), [])

    def test_run_state_is_required(self):
        for name, code in (('profile.json', 'declared_state_missing'),
                           ('manifest.json', 'declared_state_missing'),
                           ('authorization.json', 'declared_state_missing')):
            saved = (self.run / name).read_bytes()
            (self.run / name).unlink()
            self.refused(code, self.gcp.GCPFile, self.write())
            (self.run / name).write_bytes(saved)
        (self.run / 'manifest.json').write_text(json.dumps({'profile_sha256': '0' * 64}))
        self.refused('profile_digest_mismatch', self.gcp.GCPFile, self.write())


@NEEDS_GRID
class CarryTests(EngineCase):
    """The declared frame from the control file to the cloud, through patched source."""

    def reconstruction(self):
        value = self.types.ODM_Reconstruction.__new__(self.types.ODM_Reconstruction)
        value.photos, value.georef, value.gcp, value.multi_camera = self.photos(), None, None, None
        return value

    def georeference(self):
        value = self.reconstruction()
        self.coords = str(self.work / 'coords.txt')
        value.georeference_with_gcp(self.write(), self.coords, str(self.work / 'utm.txt'),
                                    str(self.work / 'model.txt'))
        return value

    def dataset(self, config=None):
        module = self.load('opensfm.dataset', 'opensfm/dataset.py')
        data = module.DataSet.__new__(module.DataSet)
        data.data_path = str(self.work / 'opensfm')
        Path(data.data_path).mkdir(exist_ok=True)
        data.config = {'bundle_use_gps': False} if config is None else config
        data.images = lambda: [f'image-{n}.jpg' for n in range(4)]
        data.io_handler = types.SimpleNamespace(
            isfile=os.path.isfile, open_rt=open, open_wt=lambda p: open(p, 'w'))
        return data

    def loaded(self):
        value, data = self.georeference(), self.dataset()
        value.gcp.export_opensfm_json(data._ground_control_points_file(), self.photos())
        return value, data

    def test_types_keeps_the_frame_and_writes_no_utm_copy(self):
        value = self.georeference()
        self.assertFalse((self.work / 'utm.txt').exists())
        self.assertEqual(value.georef.contract['working_frame'], PROFILE.working_frame)
        self.assertEqual(value.gcp.survey.contract['source']['epoch'], '2010.00')
        self.assertEqual(value.gcp.entries, [' '.join(row) for row in rows()])
        self.assertEqual(_Crs.parsed, [])
        again = self.types.ODM_GeoRef.FromCoordsFile(self.coords)
        self.assertEqual(again.contract, value.georef.contract)
        self.assertEqual(again.utm_offset(), value.georef.utm_offset())

    def test_frame_cannot_be_flattened(self):
        value = self.georeference()
        for action in (value.georef.proj4, value.get_proj_srs, value.georef.srs.to_epsg,
                       value.gcp.wgs84_utm_zone,
                       lambda: value.gcp.make_resized_copy('x', 0.5),
                       lambda: value.gcp.make_filtered_copy('x', str(self.work)),
                       ):
            self.refused('declared_frame_unported', action)
        for action in (lambda: self.location.transformer(value.georef.srs, _Crs('x')),
                       lambda: self.location.transformer(_Crs('x'), value.georef.srs),
                       lambda: self.location.proj_srs_convert(value.georef.srs)):
            error = self.refused('declared_frame_unported', action)
            self.assertEqual(error.details, {'at': 'legacy coordinate transformation'})

    def test_refused_utm_copy_deletes_nothing(self):
        value = self.georeference()
        kept = self.work / 'kept.txt'
        kept.write_text('kept')
        self.refused('declared_frame_unported', value.gcp.create_utm_copy, str(kept))
        self.assertEqual(kept.read_text(), 'kept')

    def test_stage_command_runner(self):
        module = self.load('stages_georeferencing', 'stages/odm_georeferencing.py')
        code = 'import sys; print(sys.stdin.read().upper())'
        self.assertEqual(module._declared_run([sys.executable, '-c', code], 'ab').strip(), 'AB')
        with self.assertRaises(Exception):
            module._declared_run([sys.executable, '-c', 'raise SystemExit(2)'], '')

    def test_reopening_reuses_one_normalization(self):
        first = self.gcp.GCPFile(self.write()).survey.contract
        self.assertEqual(self.gcp.GCPFile(self.write()).survey.contract, first)
        self.assertEqual(len(list((self.run / 'ledger').iterdir())), 1)
        claim, = (self.run / 'ledger').iterdir()
        saved = claim.read_bytes()
        claim.unlink()
        self.refused('declared_state_changed', self.gcp.GCPFile, self.write())
        claim.write_bytes(saved)
        state, = (self.run / 'state').iterdir()
        state.write_text(json.dumps(seal({**first, 'working_frame': {'crs': 'EPSG:4979'}})))
        self.refused('working_frame_relabelled', self.gcp.GCPFile, self.write())
        state.unlink()
        self.refused('already_normalized', self.gcp.GCPFile, self.write())

    def test_coords_file_is_bound(self):
        self.georeference()
        Path(self.coords).write_text(
            engine_hooks.COORDS_HEADER + ' ' + '0' * 64 + '\n1 2\n')
        self.refused('declared_state_changed', self.types.ODM_GeoRef.FromCoordsFile, self.coords)

    def test_control_file_is_unusable_by_a_reader_without_hooks(self):
        _, data = self.loaded()
        value = json.loads(Path(data._ground_control_points_file()).read_text())
        self.assertNotIn('points', value)
        self.assertNotIn('crs', value)

    def test_dataset_hands_the_solver_every_point_unchanged(self):
        value, data = self.loaded()
        for checkpoints in (False, True):
            points = data.load_ground_control_points(checkpoints)
            self.assertEqual([p.id for p in points], list(PLACES))
            for point, item in zip(points, value.georef.contract['points']):
                lla = point.lla
                self.assertEqual([lla['longitude'], lla['latitude'], lla['altitude']],
                                 item['working'])
                self.assertEqual(len(point.observations), 3)
        sink = json.loads((Path(data.data_path) / 'declared_sink.json').read_text())
        self.assertEqual((sink['points'], sink['observations']), (3, 9))

    def test_engine_objects_are_checked_not_assumed(self):
        _, data = self.loaded()
        pymap = sys.modules['opensfm.pymap']

        def altered(change):
            class Point(_Point):
                def __setattr__(self, name, value):
                    super().__setattr__(name, change(name, value))
            return Point
        for change, code in (
                (lambda n, v: {**v, 'altitude': v['altitude'] + 27} if n == 'lla' and v else v,
                 'sink_height_mismatch'),
                (lambda n, v: {**v, 'latitude': v['longitude'], 'longitude': v['latitude']}
                 if n == 'lla' and v else v, 'sink_axis_order'),
                (lambda n, v: False if n == 'has_altitude' else v, 'sink_height_mismatch'),
                (lambda n, v: 'SAME' if n == 'id' else v, 'sink_duplicate_id')):
            pymap.GroundControlPoint = altered(change)
            self.refused(code, data.load_ground_control_points)

    def test_camera_gps_and_missing_images_refuse(self):
        _, data = self.loaded()
        for config in ({}, {'bundle_use_gps': True}):
            data.config = config
            self.refused('camera_gps_frame_unqualified', data.load_ground_control_points)
        data.config = {'bundle_use_gps': False}
        data.images = lambda: ['image-0.jpg']
        self.refused('sink_image_missing', data.load_ground_control_points)
        value = self.gcp.GCPFile(self.write())
        self.refused('sink_image_missing', value.export_opensfm_json,
                     str(self.work / 'x.json'), self.photos()[:2])

    def test_other_control_paths_refuse(self):
        _, data = self.loaded()
        self.refused('declared_frame_unported', data.load_gcp_coordinate_system)
        self.refused('declared_frame_unported', data.output_coordinate_system)
        self.refused('declared_frame_unported', data.save_ground_control_points, [])

    def test_reference_carries_the_frame(self):
        _, data = self.loaded()
        data.save_reference(_Converter(*ORIGIN))
        self.assertEqual(data.load_reference().lat, ORIGIN[0])
        path = Path(data._reference_lla_path())
        stored = json.loads(path.read_text())
        path.write_text(json.dumps({**stored, 'declared_frame': 'x'}))
        self.refused('reference_frame_mismatch', data.load_reference)
        path.write_text(json.dumps(stored))
        Path(data._ground_control_points_file()).unlink()
        self.refused('reference_frame_mismatch', data.load_reference)

    def affine(self, data, converter=_Converter(*ORIGIN)):
        export = self.load('opensfm.actions_export', 'opensfm/actions/export_geocoords.py')
        east, north = PROFILE.output.forward(ORIGIN[1], ORIGIN[0])
        offset = self.value.georef.utm_offset()
        matrix = [[0.9998, 0.0021, 0.0, east - offset[0]], [-0.0021, 0.9998, 0.0, north - offset[1]],
                  [0.0, 0.0, 1.0, 0.0], [0, 0, 0, 1]]
        seen = {}

        def matrix_for(reference, projection, offs):
            seen['height'] = projection(ORIGIN[1], ORIGIN[0], 12.5)[2]
            return numpy.array(matrix)
        export.geo = types.SimpleNamespace(get_proj_transform_matrix=matrix_for)
        pyproj = sys.modules['pyproj']
        pyproj.Transformer = types.SimpleNamespace(from_crs=lambda *a, **k: types.SimpleNamespace(
            transform=lambda x, y, errcheck: PROFILE.output.forward(x, y)))
        data.load_reference = lambda: converter
        export.run_dataset(data, f'EPSG:{PROFILE.output_epsg}', True, False, False, False,
                           str(self.work / 'matrix.txt'), offset)
        return seen

    def test_engine_matrix_is_sealed_and_height_is_only_carried(self):
        self.value, data = self.loaded()
        data.load_ground_control_points()
        with self.subTest('output frame'):
            export = self.load('opensfm.actions_export', 'opensfm/actions/export_geocoords.py')
            data.load_reference = lambda: _Converter(*ORIGIN)
            self.refused('output_frame_mismatch', export.run_dataset, data, '+proj=utm +zone=19',
                         True, False, False, False, 'x')
        self.assertEqual(self.affine(data), {'height': 12.5})
        record = json.loads((Path(data.data_path) / 'declared_georeference.json').read_text())
        self.assertEqual(record['contract_sha256'], self.value.georef.contract['sha256'])

    def test_a_different_local_frame_refuses(self):
        self.value, data = self.loaded()
        data.load_ground_control_points()

        class Other(_Converter):
            def to_topocentric(self, lat, lon, alt):
                east, north, up = super().to_topocentric(lat, lon, alt)
                return east, north, up + 2e-6
        self.refused('engine_local_frame_mismatch', self.affine, data, Other(*ORIGIN))

    def stage(self, data, change=lambda points: points, **tree):
        module = self.load('stages_georeferencing', 'stages/odm_georeferencing.py')
        source, output = self.work / 'dense.ply', self.work / 'model.laz'
        engine = [(10.0, 20.0, 230.0), (900.0, -700.0, 260.0), (-1200.0, 1100.0, 215.0)]
        source.write_bytes(b'invented')
        record = json.loads((Path(data.data_path) / 'declared_georeference.json').read_text())
        mapped = change([dem_frame.exact_point(record, PROFILE, p) for p in engine])
        self.pipeline = None

        def run(command, text):
            if command[1] == 'pipeline':
                self.pipeline = json.loads(text)
                output.write_bytes(b'invented cloud')
                return ''
            points = engine if command[-1] == str(source) else mapped
            if '--stats' in command:
                return json.dumps({'stats': {'statistic': [{'count': len(points)}] * 3}})
            chosen = [points[int(i)] for i in command[3].split(',')]
            return json.dumps({'points': {'point': [
                {'X': p[0], 'Y': p[1], 'Z': p[2]} for p in chosen]}})
        module._declared_run = run
        stage = module.ODMGeoreferencingStage.__new__(module.ODMGeoreferencingStage)
        stage.process(types.SimpleNamespace(crop=0), {
            'reconstruction': self.value, 'tree': types.SimpleNamespace(**{
                'filtered_point_cloud': source, 'odm_georeferencing_model_laz': output,
                'opensfm': data.data_path, 'odm_align_file': None, **tree})})
        return json.loads((Path(data.data_path) / 'declared_cloud.json').read_text())

    def ready(self):
        self.value, data = self.loaded()
        data.load_ground_control_points()
        self.affine(data)
        return data

    def test_cloud_stage_maps_every_point_and_seals_it(self):
        data = self.ready()
        cloud = self.stage(data)
        self.assertEqual(cloud['count'], 3)
        kinds = [s['type'] for s in self.pipeline[1:]]
        self.assertEqual(kinds, ['filters.ferry', 'filters.projpipeline', 'writers.las'])
        self.assertNotIn('filters.transformation', json.dumps(self.pipeline))

    def test_cloud_stage_refuses_loss_shift_and_side_paths(self):
        data = self.ready()
        out = self.work / 'model.laz'
        for change, code in ((lambda p: p[:2], 'cloud_incomplete'),
                             (lambda p: [(x, y, z + 0.02) for x, y, z in p], 'cloud_point_mismatch'),
                             (lambda p: [(y, x, z) for x, y, z in p], 'cloud_point_mismatch')):
            out.unlink(missing_ok=True)
            self.refused(code, self.stage, data, change)
            self.assertFalse((Path(data.data_path) / 'declared_cloud.json').exists())
        out.unlink(missing_ok=True)
        self.refused('declared_frame_unported', self.stage, data, odm_align_file='x')


if __name__ == '__main__':
    unittest.main(verbosity=2)
