"""First synthetic gate. `collect` runs inside the engine; `evaluate` runs elsewhere.

`collect` records what it observed in one invocation and decides nothing. `evaluate`
recomputes every expected height from the pinned grid without PROJ or GDAL and
compares. A pass here qualifies an invented fixture only; it never admits a job.
"""
import argparse
import hashlib
import json
import math
import os
import platform
import sys
import tempfile
from pathlib import Path

from . import import_handoff as handoff
from .export_once import ExportLedger, export_dem
from .grid_reference import EPSG_7541, EPSG_7600, GRID_SHA256, GeoidGrid, US_SURVEY_FOOT_M
from .patch_engine import LOCATION_SHA256, ORIGINAL_SHA256, PATCHED_SHA256, patch_gcp_source
from .survey_frame import SurveyRefused, read_survey

SCHEMA = 'survey-height-gate/v1'
IMAGE = 'sha256:d1b3a6af8806b867a96c75ba2b22e35c45fd937217ccb9860c3fcbe6e75499bd'
ENGINE_VERSION = '3.8.6'
ENGINE_FILES = {
    'modules/gcp.py': ORIGINAL_SHA256,
    'modules/location.py': LOCATION_SHA256,
    'modules/config.py': '0891bea8580c9699bda39b2c31c58e83725b2f593ad6cf0f757ac6219bdd3501',
    'run.py': '4e93ce9b7c0f8b0a66fcff62e915b0601b11acaf24d4f8a91ef31e606a1e28bc',
    'SuperBuild/install/bin/opensfm/opensfm/dataset.py':
        '142d860ed8d059a3b5785060dd48f5b1f4ac103af58e04937e870ce69880ea05',
    'SuperBuild/install/bin/opensfm/opensfm/reconstruction.py':
        'ab2e1953c54908247791cf30cb022d6bc5223669b0d0cfa919e99072c2caffb8',
}
PROJ_DB_SHA256 = '153ec8cd6041a94c5dda34bc517e0c6c7dda341720c467f3d157619a62527322'
WITNESS_M, ROUND_TRIP_M = 0.010, 0.001
NODATA = -9999.0
AFFINE = [265383.012, 3200.0, 170.0, 180818.258, 230.0, -2700.0]
SAMPLES = [[220.0, 245.5], [NODATA, 281.25]]
MASK = [[255, 255], [0, 255]]
WITNESSES = ((0, 0), (0, 1), (1, 1))
HEADER = ('SURVEY3D/1 CRS=EPSG:7600+6360 UNITS=ftUS,ftUS,ftUS AXES=E,N,H '
          'REALIZATION=NAD83(2011) EPOCH=2010.00 GEOID=GEOID18 ROLES=SYNTHETIC')
POINT_ID, POINT_RAW = 'SYN-1', ('870677.4322', '593234.5696', '820.2083')
NEGATIVES = {
    'missing_grid': 'verified_geoid18_grid_required',
    'wrong_grid': 'verified_geoid18_grid_required',
    'dem_wrong_grid': 'verified_geoid18_grid_required',
    'frame_horizontal_only': 'unsupported_frame',
    'frame_wgs84_3d': 'unsupported_frame',
    'epoch_unknown': 'unsupported_epoch',
    'height_not_a_number': 'invalid_coordinate',
    'height_out_of_range': 'height_out_of_range',
    'identity_operation': 'vertical_operation_identity',
    'ballpark_operation': 'vertical_operation_mismatch',
    'second_normalization': 'already_normalized',
    'relabelled_frame': 'working_frame_relabelled',
    'dem_unsupported_frame': 'unconverted_ellipsoid_source_required',
    'dem_utm_frame': 'explicit_epsg7541_horizontal_required',
    'dem_nonfinite_height': 'nonfinite_valid_height',
    'dem_second_conversion': 'second_conversion',
    'dem_output_as_source': 'unconverted_ellipsoid_source_required',
}
LEGACY_HEADERS = {
    'EPSG:7600': POINT_RAW,
    'EPSG:7600+6360': POINT_RAW,
    'EPSG:4979': ('-88.75', '43.1', '215.447'),
    'WGS84 UTM 16N': ('357590.0', '4773500.0', '215.447'),
}


def sha(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def survey_bytes(header=HEADER, raw=POINT_RAW):
    lines = [header] + [f'{raw[0]} {raw[1]} {raw[2]} 100 100 synthetic-{n}.jpg {POINT_ID}'
                        for n in (1, 2, 3)]
    return ('\n'.join(lines) + '\n').encode()


# ---------------------------------------------------------------- evaluate


def _near(observed, expected, limit):
    return (isinstance(observed, float) and math.isfinite(observed)
            and abs(observed - expected) <= limit)


def evaluate(receipt, launcher, grid):
    """Never reads an expected value or an error figure from the receipt."""
    checks = {}
    try:
        checks.update(_check_identity(receipt, launcher))
        checks.update(_check_runtime(receipt))
        checks.update(_check_normalization(receipt['normalization'], grid))
        checks.update(_check_dem(receipt['dem'], grid))
        checks['negative_cases'] = receipt['negatives'] == NEGATIVES
        checks['legacy_headers_unchanged'] = (
            receipt['legacy_compatibility'] == {header: True for header in LEGACY_HEADERS})
        crossing = receipt['reader_crossing']
        checks['reader_crossing'] = (
            crossing['status'] == 'preserved'
            and crossing['frame'] == dict(handoff.WORKING_FRAME)
            and _near(crossing['height_m'], _expected_working(grid)[2], WITNESS_M))
        refused = (crossing['status'] == 'refused' and crossing['code'] == handoff.REFUSED)
    except (KeyError, TypeError, IndexError, ValueError, AttributeError) as error:
        return {'verdict': 'fail', 'failed': ['malformed_receipt'],
                'error': type(error).__name__, 'admits_processing': False}
    failed = sorted(name for name, ok in checks.items() if ok is not True)
    verdict = 'fail'
    if not failed:
        verdict = 'synthetic_pass'
    elif failed == ['reader_crossing'] and refused:
        verdict = 'blocked_import_handoff'
    return {'verdict': verdict, 'failed': failed, 'checks': checks,
            'admits_processing': False}


def _check_identity(receipt, launcher):
    writer = receipt['writer']
    return {
        'schema': receipt['schema'] == SCHEMA,
        'image': launcher['image'] == IMAGE,
        'same_invocation': (len(writer['container']) >= 12
                            and launcher['container_id'].startswith(writer['container'])
                            and len(receipt['nonce']) >= 32
                            and launcher['nonce'] == receipt['nonce']
                            and launcher['exit_code'] == 0),
        'isolated': launcher['network_mode'] == 'none' and launcher['read_only'] is True,
        'receipt_bytes': launcher['receipt_sha256'] == receipt['_file_sha256'],
        'entrypoint': launcher['entrypoint'] == [writer['executable']],
        'engine_version': receipt['engine']['version'] == ENGINE_VERSION,
        'engine_files': receipt['engine']['files'] == ENGINE_FILES,
        'candidate_files': bool(receipt['candidate_files'])
        and receipt['candidate_files'] == launcher['candidate_files'],
        'no_workload': receipt['workload'] == {'photos': 0, 'jobs': 0},
    }


def _check_runtime(receipt):
    runtime = receipt['runtime']
    return {
        'network_off': runtime['pyproj_network'] is False and runtime['osr_network'] is False,
        'grid': runtime['grid_sha256'] == GRID_SHA256,
        'proj_db': runtime['system_proj_db_sha256'] == PROJ_DB_SHA256,
        'versions_recorded': all(isinstance(runtime[key], str) and runtime[key] for key in (
            'gdal', 'osr_proj', 'pyproj', 'pyproj_proj', 'python')),
        'operation': ('+proj=vgridshift +grids=' + runtime['grid_path'] + ' +multiplier=1'
                      in receipt['normalization']['operation']),
    }


def _expected_working(grid):
    east, north, height_ft = (float(v) for v in POINT_RAW)
    lon, lat = EPSG_7600.inverse(east, north)
    return lon, lat, height_ft * US_SURVEY_FOOT_M + grid.undulation(lon, lat)


def _check_normalization(observed, grid):
    lon, lat, h = _expected_working(grid)
    contract = handoff.verify_contract(observed['contract'])
    point, = contract['points']
    return {
        'normalization_raw_preserved': point['raw_xyz'] == list(POINT_RAW)
        and point['id'] == POINT_ID and contract['source']['raw_header'] == HEADER
        and contract['source']['raw_sha256'] == hashlib.sha256(survey_bytes()).hexdigest(),
        'normalization_height': _near(point['working'][2], h, WITNESS_M),
        'normalization_horizontal': _near(point['working'][0], lon, 1e-8)
        and _near(point['working'][1], lat, 1e-8),
        'normalization_round_trip': all(
            _near(back * US_SURVEY_FOOT_M, float(raw) * US_SURVEY_FOOT_M, ROUND_TRIP_M)
            for back, raw in zip(observed['round_trip_ftus'], POINT_RAW))
        and len(observed['round_trip_ftus']) == 3,
    }


def _check_dem(observed, grid):
    heights, trips = [], []
    for row, column in WITNESSES:
        x, y = column + 0.5, row + 0.5
        lon, lat = EPSG_7541.inverse(AFFINE[0] + x * AFFINE[1] + y * AFFINE[2],
                                     AFFINE[3] + x * AFFINE[4] + y * AFFINE[5])
        expected = SAMPLES[row][column] - grid.undulation(lon, lat)
        heights.append(_near(observed['values'][row][column], expected, WITNESS_M))
        trips.append(_near(observed['inverse_heights_m'][f'{row},{column}'],
                           SAMPLES[row][column], ROUND_TRIP_M))
    receipt = observed['receipt']
    return {
        'dem_heights': heights == [True] * 3,
        'dem_round_trip': trips == [True] * 3,
        'dem_affine': observed['affine'] == AFFINE,
        'dem_shape': observed['size'] == [2, 2] and observed['bands'] == 1
        and observed['data_type'] == 'Float64',
        'dem_mask': observed['mask'] == MASK,
        'dem_nodata': observed['nodata'] == NODATA and observed['values'][1][0] == NODATA,
        'dem_units': observed['unit'] == 'm',
        'dem_frame': observed['compound_7541_5703'] is True,
        'dem_metadata': observed['height_reference'] == 'NAVD88_GEOID18_METRE'
        and observed['transform_count'] == '1',
        'dem_source_unchanged': observed['source_sha256_before']
        == observed['source_sha256_after'] == receipt['source_sha256'],
        'dem_output_bound': observed['output_sha256'] == receipt['output_sha256']
        != receipt['source_sha256'],
        'dem_applied_once': receipt['vertical_application_count'] == 1
        and receipt['valid_pixels'] == 3 and receipt['grid_sha256'] == GRID_SHA256,
    }


# ----------------------------------------------------------------- collect


def _code(action):
    try:
        action()
    except SurveyRefused as error:
        return error.code
    except Exception as error:
        return f'unexpected:{type(error).__name__}'
    return 'accepted'


def _write_dem(path, *, epsg=7541, reference='NAD83(2011)_ELLIPSOID_METRE', samples=SAMPLES):
    import numpy as np
    from osgeo import gdal, osr
    gdal.SetThreadLocalConfigOption('GDAL_TIFF_INTERNAL_MASK', 'YES')
    raster = gdal.GetDriverByName('GTiff').Create(str(path), 2, 2, 1, gdal.GDT_Float64)
    frame = osr.SpatialReference()
    frame.ImportFromEPSG(epsg)
    raster.SetSpatialRef(frame)
    raster.SetGeoTransform(AFFINE)
    raster.SetMetadata({'HEIGHT_REFERENCE': reference, 'HEIGHT_TRANSFORM_COUNT': '0'})
    band = raster.GetRasterBand(1)
    band.SetUnitType('m')
    band.SetNoDataValue(NODATA)
    band.WriteArray(np.array(samples, dtype=np.float64))
    band.CreateMaskBand(gdal.GMF_PER_DATASET)
    band.GetMaskBand().WriteArray(np.array(MASK, dtype=np.uint8))
    band = raster = None
    gdal.SetThreadLocalConfigOption('GDAL_TIFF_INTERNAL_MASK', None)
    return path


def _collect_dem(work, grid_path, contract, bridge, ledger):
    from osgeo import gdal
    from pyproj import CRS
    source, output = _write_dem(work / 'ellipsoid.tif'), work / 'navd88.tif'
    before = sha(source)
    receipt = export_dem(source, output, source_sha256=before, grid_path=grid_path,
                         contract=contract, ledger=ledger, block_size=1)
    raster = gdal.Open(str(output), gdal.GA_ReadOnly)
    band = raster.GetRasterBand(1)
    values = band.ReadAsArray().tolist()
    inverse = {}
    for row, column in WITNESSES:
        x, y = column + 0.5, row + 0.5
        lon, lat = EPSG_7541.inverse(AFFINE[0] + x * AFFINE[1] + y * AFFINE[2],
                                     AFFINE[3] + x * AFFINE[4] + y * AFFINE[5])
        east, north = EPSG_7600.forward(lon, lat)
        restored = bridge.survey_to_ellipsoid((east, north, values[row][column] / US_SURVEY_FOOT_M))
        inverse[f'{row},{column}'] = restored[2]
    result = {
        'values': values, 'inverse_heights_m': inverse, 'receipt': receipt,
        'affine': list(raster.GetGeoTransform()), 'bands': raster.RasterCount,
        'size': [raster.RasterXSize, raster.RasterYSize],
        'data_type': gdal.GetDataTypeName(band.DataType),
        'mask': band.GetMaskBand().ReadAsArray().tolist(),
        'nodata': band.GetNoDataValue(), 'unit': band.GetUnitType(),
        'compound_7541_5703': CRS.from_wkt(raster.GetProjection()).equals(
            CRS.from_user_input('EPSG:7541+5703'), ignore_axis_order=True),
        'height_reference': raster.GetMetadataItem('HEIGHT_REFERENCE'),
        'transform_count': raster.GetMetadataItem('HEIGHT_TRANSFORM_COUNT'),
        'source_sha256_before': before, 'source_sha256_after': sha(source),
        'output_sha256': sha(output),
    }
    band = raster = None
    return result


class _Identity:
    """Stands in for an operation that drops the height step."""

    def __init__(self, bridge):
        self._bridge = bridge

    def survey_to_ellipsoid(self, xyz):
        lon, lat, _ = self._bridge.survey_to_ellipsoid(xyz)
        return lon, lat, xyz[2] * US_SURVEY_FOOT_M

    def ellipsoid_to_survey(self, working):
        return self._bridge.ellipsoid_to_survey(working)


class _Ballpark(_Identity):
    """Stands in for a constant shift where a grid lookup is required."""

    def survey_to_ellipsoid(self, xyz):
        lon, lat, height = super().survey_to_ellipsoid(xyz)
        return lon, lat, height - 34.0


def _collect_negatives(work, grid_path, contract, bridge, grid, ledger, source_sha256):
    wrong = work / 'wrong-grid.tif'
    data = bytearray(Path(grid_path).read_bytes())
    data[-1] ^= 1
    wrong.write_bytes(data)
    good = read_survey(survey_bytes())
    used = handoff.Ledger()
    handoff.normalize_once(good, bridge, grid, used)
    relabelled = {**contract, 'working_frame': {**contract['working_frame'], 'crs': 'EPSG:4979'}}
    relabelled['sha256'] = handoff.canonical_sha256(
        {key: value for key, value in relabelled.items() if key != 'sha256'})
    nan = float('nan')

    def normalize(header=HEADER, raw=POINT_RAW, with_bridge=bridge):
        handoff.normalize_once(read_survey(survey_bytes(header, raw)), with_bridge, grid,
                               handoff.Ledger())

    def crossing(path):
        target = work / 'negative.txt'
        target.write_bytes(survey_bytes())
        handoff.import_survey_gcp(target, work, grid_path=path)

    def dem(name, path=grid_path, **options):
        source = _write_dem(work / f'{name}.tif', **options)
        export_dem(source, work / f'{name}-out.tif', source_sha256=sha(source), grid_path=path,
                   contract=contract, ledger=ExportLedger())

    cases = {
        'missing_grid': lambda: crossing(work / 'absent.tif'),
        'wrong_grid': lambda: crossing(wrong),
        'dem_wrong_grid': lambda: dem('wrong-grid-dem', wrong),
        'frame_horizontal_only': lambda: normalize(HEADER.replace('7600+6360', '7600')),
        'frame_wgs84_3d': lambda: normalize(HEADER.replace('7600+6360', '4979')),
        'epoch_unknown': lambda: normalize(HEADER.replace('2010.00', 'UNKNOWN')),
        'height_not_a_number': lambda: normalize(raw=(*POINT_RAW[:2], 'nan')),
        'height_out_of_range': lambda: normalize(raw=(*POINT_RAW[:2], '99999')),
        'identity_operation': lambda: normalize(with_bridge=_Identity(bridge)),
        'ballpark_operation': lambda: normalize(with_bridge=_Ballpark(bridge)),
        'second_normalization': lambda: handoff.normalize_once(good, bridge, grid, used),
        'relabelled_frame': lambda: handoff.cross_import_handoff(relabelled, work),
        'dem_unsupported_frame': lambda: dem('wgs84', reference='WGS84_UNKNOWN'),
        'dem_utm_frame': lambda: dem('utm', epsg=32616),
        'dem_nonfinite_height': lambda: dem('nan', samples=[[nan, 245.5], [NODATA, 281.25]]),
        'dem_second_conversion': lambda: export_dem(
            work / 'ellipsoid.tif', work / 'again.tif', source_sha256=source_sha256,
            grid_path=grid_path, contract=contract, ledger=ledger),
        'dem_output_as_source': lambda: export_dem(
            work / 'navd88.tif', work / 'twice.tif', source_sha256=sha(work / 'navd88.tif'),
            grid_path=grid_path, contract=contract, ledger=ExportLedger()),
    }
    return {name: _code(action) for name, action in cases.items()}


def load_reader(modules_dir):
    """Import the reader from `modules_dir` with only its log module replaced."""
    import importlib
    import types
    for name in [n for n in sys.modules if n == 'modules' or n.startswith('modules.')]:
        del sys.modules[name]
    package = types.ModuleType('modules')
    package.__path__ = [str(modules_dir)]
    log = types.ModuleType('modules.log')
    log.INFO = log.WARNING = log.ERROR = lambda *args: None
    package.log = log
    sys.modules.update({'modules': package, 'modules.log': log})
    return importlib.import_module('modules.gcp')


def _read_through(reader, work, name, text):
    source, utm, out = (work / f'{name}{end}' for end in ('.txt', '.utm.txt', '.json'))
    source.write_bytes(text)
    converted = reader.GCPFile(reader.GCPFile(str(source)).create_utm_copy(str(utm)))
    photo = type('Photo', (), {'filename': 'synthetic-1.jpg', 'width': 4000, 'height': 3000})
    converted.export_opensfm_json(str(out), [photo])
    return utm.read_text(), json.loads(out.read_text())


def _collect_reader(work, engine_root, contract):
    """Cross the reader itself: the pinned one, then a copy with the opt-in hook."""
    modules = Path(engine_root) / 'modules'
    copy = work / 'engine' / 'modules'
    copy.mkdir(parents=True)
    (copy / 'location.py').write_bytes((modules / 'location.py').read_bytes())
    (copy / 'gcp.py').write_bytes(patch_gcp_source((modules / 'gcp.py').read_bytes()))
    legacy = {}
    for header, raw in LEGACY_HEADERS.items():
        text = (header + '\n' + f'{raw[0]} {raw[1]} {raw[2]} 100 100 synthetic-1.jpg A\n').encode()
        both = [_code_or_value(lambda: _read_through(
            load_reader(folder), work, f'legacy-{n}', text))
            for n, folder in enumerate((modules, copy))]
        legacy[header] = both[0] == both[1]
    reader = load_reader(modules)
    point = contract['points'][0]['working']
    rows = f'EPSG:6319\n{point[0]!r} {point[1]!r} {point[2]!r} 100 100 synthetic-1.jpg {POINT_ID}\n'
    utm, exported = _read_through(reader, work, 'declared', rows.encode())
    seen = exported['points'][0]['position']
    observed = {'crs': exported['crs'],
                'heights_m': {POINT_ID: seen.get('altitude')}}
    target = work / 'survey.txt'
    target.write_bytes(survey_bytes())
    crossing = {'declared_frame_as_engine_input': 'EPSG:6319',
                'utm_header_written': utm.splitlines()[0],
                'opensfm_crs_written': exported['crs'],
                'opensfm_altitude_m': seen.get('altitude'),
                'declared_frame_lost': _lost(contract, observed)}
    try:
        load_reader(copy).GCPFile(str(target))
        crossing.update(status='unexpected', code='accepted')
    except SurveyRefused as error:
        crossing.update(status='refused', code=error.code, details=error.details)
    return crossing, legacy


def _code_or_value(action):
    try:
        return action()
    except Exception as error:
        return type(error).__name__, str(error)


def _lost(contract, observed):
    try:
        handoff.require_preserved(contract, observed)
    except SurveyRefused as error:
        return error.details.get('lost', [error.code])
    return []


def collect(*, engine_root, grid_path, nonce):
    os.environ['PROJ_NETWORK'] = 'OFF'
    sys.dont_write_bytecode = True
    from osgeo import gdal, osr
    import pyproj
    from pyproj import datadir, network
    from .vertical_bridge import SurveyVerticalBridge
    network.set_network_enabled(False)
    osr.SetPROJEnableNetwork(False)
    root, grid_path = Path(engine_root), Path(grid_path).resolve()
    files = {name: sha(root / name) if (root / name).is_file() else None for name in ENGINE_FILES}
    version = (root / 'VERSION').read_text().strip()
    if version != ENGINE_VERSION or files != ENGINE_FILES:
        raise SurveyRefused('engine_identity_mismatch', {'version': version, 'files': files})
    grid, bridge = GeoidGrid(grid_path), SurveyVerticalBridge(grid_path)
    here = Path(__file__).parent
    with tempfile.TemporaryDirectory(prefix='survey-height-gate-') as folder:
        work = Path(folder)
        survey = read_survey(survey_bytes())
        contract = handoff.normalize_once(survey, bridge, grid, handoff.Ledger())
        ledger = ExportLedger()
        dem = _collect_dem(work, grid_path, contract, bridge, ledger)
        negatives = _collect_negatives(work, grid_path, contract, bridge, grid, ledger,
                                       dem['source_sha256_before'])
        # The hook looks for the grid beside the package; a gate run mounts it there.
        crossing, legacy = _collect_reader(work, root, contract)
    system_db = Path('/usr/share/proj/proj.db')
    return {
        'schema': SCHEMA, 'nonce': nonce, 'workload': {'photos': 0, 'jobs': 0},
        'engine': {'version': version, 'files': files},
        'runtime': {
            'python': sys.version.split()[0], 'gdal': gdal.VersionInfo('RELEASE_NAME'),
            'osr_proj': '.'.join(str(v()) for v in (
                osr.GetPROJVersionMajor, osr.GetPROJVersionMinor, osr.GetPROJVersionMicro)),
            'pyproj': pyproj.__version__, 'pyproj_proj': pyproj.proj_version_str,
            'pyproj_network': bool(network.is_network_enabled()),
            'osr_network': bool(osr.GetPROJEnableNetwork()),
            'osr_search_paths': osr.GetPROJSearchPaths(), 'pyproj_data': datadir.get_data_dir(),
            'system_proj_db_sha256': sha(system_db) if system_db.is_file() else None,
            'grid_path': grid_path.as_posix(), 'grid_sha256': sha(grid_path),
            'sys_path': list(sys.path),
            'proj_environment': sorted(n for n in os.environ if n.startswith(('PROJ', 'GDAL'))),
        },
        'candidate_files': {p.name: sha(p) for p in sorted(here.glob('*.py'))},
        'writer': {'container': platform.node(), 'executable': sys.executable,
                   'executable_sha256': sha(os.path.realpath(sys.executable)),
                   'argv': sys.argv, 'pid': os.getpid(),
                   'uid': getattr(os, 'geteuid', lambda: None)()},
        'normalization': {'contract': contract, 'operation': bridge.pipeline,
                          'round_trip_ftus': list(bridge.ellipsoid_to_survey(
                              contract['points'][0]['working']))},
        'dem': dem, 'negatives': negatives,
        'legacy_compatibility': legacy, 'reader_crossing': crossing,
    }


def main(argv=None):
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest='command', required=True)
    run = commands.add_parser('collect')
    for name in ('engine-root', 'grid', 'nonce', 'out'):
        run.add_argument(f'--{name}', required=True)
    judge = commands.add_parser('evaluate')
    for name in ('receipt', 'launcher', 'grid'):
        judge.add_argument(f'--{name}', required=True)
    options = parser.parse_args(argv)
    if options.command == 'collect':
        receipt = collect(engine_root=options.engine_root, grid_path=options.grid,
                          nonce=options.nonce)
        with open(options.out, 'x', encoding='utf-8', newline='\n') as stream:
            json.dump(receipt, stream, indent=1, sort_keys=True)
        print(sha(options.out))
        return 0
    receipt = json.loads(Path(options.receipt).read_text(encoding='utf-8'))
    receipt['_file_sha256'] = sha(options.receipt)
    launcher = json.loads(Path(options.launcher).read_text(encoding='utf-8'))
    result = evaluate(receipt, launcher, GeoidGrid(options.grid))
    print(json.dumps(result, indent=1, sort_keys=True))
    return {'synthetic_pass': 0, 'blocked_import_handoff': 3}.get(result['verdict'], 1)


if __name__ == '__main__':
    sys.exit(main())
