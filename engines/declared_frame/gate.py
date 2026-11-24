"""Judge one signed synthetic run. Runs away from the engine; needs no PROJ or GDAL.

Expected values come from the sealed profile and the pinned grid, never from the run.
A pass qualifies an invented fixture only and never admits processing.
"""
import math

from . import dem_frame, engine_hooks, provenance
from .contract import MAX_REFERENCE_DIFFERENCE_M, MAX_ROUND_TRIP_M, verify_contract
from .errors import FrameRefused
from .profile import load_profile

NEGATIVES = {
    'missing_grid': 'verified_geoid18_grid_required',
    'wrong_grid': 'verified_geoid18_grid_required',
    'unsupported_frame': 'unsupported_frame',
    'unknown_epoch': 'unsupported_epoch',
    'invalid_height': 'invalid_coordinate',
    'height_out_of_range': 'height_out_of_range',
    'identity_operation': 'vertical_operation_identity',
    'ballpark_operation': 'vertical_operation_mismatch',
    'second_normalization': 'already_normalized',
    'relabelled_frame': 'working_frame_relabelled',
    'withheld_point': 'withheld_point_before_render',
    'excluded_point': 'excluded_point',
    'sink_axis_order': 'sink_axis_order',
    'sink_missing_point': 'sink_count_mismatch',
    'dem_not_engine_derived': 'dem_not_engine_derived',
    'dem_second_conversion': 'second_conversion',
}


def _near(observed, expected, limit):
    return (isinstance(observed, float) and math.isfinite(observed)
            and abs(observed - expected) <= limit)


def evaluate(bundle, *, trust_sha256, sealer, grid, verify=provenance.verify_run):
    try:
        manifest, authorization, receipt = verify(
            bundle, trust_sha256=trust_sha256, sealer=sealer)
        profile = load_profile(bundle['profile'], manifest['profile_sha256'])
        if profile.kind != 'synthetic':
            raise FrameRefused('synthetic_run_only')
        checks = _checks(receipt, authorization, profile, grid)
    except FrameRefused as error:
        return _result('fail', [error.code])
    except (KeyError, TypeError, IndexError, ValueError, AttributeError) as error:
        return _result('fail', [f'malformed:{type(error).__name__}'])
    failed = sorted(name for name, ok in checks.items() if ok is not True)
    if failed:
        return _result('fail', failed)
    if engine_hooks.UNPORTED:
        return _result('blocked_unported', list(engine_hooks.UNPORTED))
    return _result('synthetic_pass', [])


def _result(verdict, failed):
    return {'verdict': verdict, 'failed': failed, 'admits_processing': False}


def _checks(receipt, authorization, profile, grid):
    chain = receipt['chain']
    dem_frame.verify_chain(chain, profile)
    contract = verify_contract(chain['contract'], profile)
    raw_sha, dem_sha = contract['source']['raw_sha256'], chain['dem']['dem_sha256']
    export = receipt['export']
    claims = [(entry['step'], entry['key']) for entry in receipt['ledger']]
    return {
        'once_only_ledger': sorted(claims) == sorted(
            [('normalize', raw_sha), ('export', dem_sha), ('export', export['output_sha256'])]),
        'ledger_custody': receipt['ledger_id'] == authorization['ledger_id'] != '',
        'sink_counts': (chain['sink']['points'] == len(profile.fit)
                        and chain['sink']['observations']
                        == len(profile.fit) * profile.marks_per_point),
        'normalization': all(_point_ok(point, profile, grid) for point in contract['points']),
        'cloud': chain['cloud']['count'] > 0,
        'negatives': receipt['negatives'] == NEGATIVES,
        'workload': receipt['workload'] == {'photos': 0, 'jobs': 0},
        'network_off': receipt['network'] == {'pyproj': False, 'osr': False},
        **_dem(receipt['dem'], export, chain, profile, grid),
    }


def _point_ok(point, profile, grid):
    east, north, height = (float(token) for token in point['raw_xyz'])
    lon, lat = profile.survey.inverse(east, north)
    expected = height * profile.survey_unit_m + grid.undulation(lon, lat)
    return (_near(point['working'][0], lon, 1e-8) and _near(point['working'][1], lat, 1e-8)
            and _near(point['working'][2], expected, MAX_REFERENCE_DIFFERENCE_M))


def _dem(dem, export, chain, profile, grid):
    affine = chain['dem']['affine']
    heights, trips = [], []
    for witness in dem['witnesses']:
        x, y = witness['column'] + 0.5, witness['row'] + 0.5
        lon, lat = profile.output.inverse(affine[0] + x * affine[1] + y * affine[2],
                                          affine[3] + x * affine[4] + y * affine[5])
        expected = witness['before'] - grid.undulation(lon, lat)
        heights.append(_near(witness['after'], expected, MAX_REFERENCE_DIFFERENCE_M))
        trips.append(_near(witness['inverse'], witness['before'], MAX_ROUND_TRIP_M))
    cells = {(w['row'], w['column']) for w in dem['witnesses']}
    return {
        'dem_witnesses': len(cells) == len(dem['witnesses']) == 3,
        'dem_heights': heights == [True] * 3,
        'dem_round_trip': trips == [True] * 3,
        'dem_grid_kept': dem['affine_after'] == affine and dem['size_after'] == dem['size_before'],
        'dem_mask_kept': dem['mask_after'] == dem['mask_before'] and dem['masked_cells'] >= 1,
        'dem_nodata_kept': dem['nodata_after'] == dem['nodata_before'],
        'dem_format': dem['data_type'] == 'Float64' and dem['unit'] == 'm',
        'dem_frame': dem['compound_epsg'] == [profile.output_epsg, 5703]
        and dem['height_reference'] == 'NAVD88_GEOID18_METRE' and dem['transform_count'] == '1',
        'dem_source_kept': dem['source_sha256_after'] == chain['dem']['dem_sha256'],
        'export_bound': export['dem_frame_sha256'] == chain['dem']['sha256']
        and export['source_sha256'] == chain['dem']['dem_sha256']
        and export['output_sha256'] == dem['output_sha256'] != export['source_sha256']
        and export['vertical_application_count'] == 1,
    }
