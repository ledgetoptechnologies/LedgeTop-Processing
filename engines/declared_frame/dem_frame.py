"""The engine-derived frame chain, from the solver's input to the DEM, and the export."""
import hashlib
import math

from .contract import check_seal, seal, verify_contract
from .errors import FrameRefused
from .geodesy import LocalFrame, apply_affine, invert_affine

GEOREFERENCE = 'declared-frame-georeference/v1'
CLOUD = 'declared-frame-cloud/v1'
DEM = 'declared-frame-dem/v1'
SINK = 'declared-frame-sink/v1'
MAX_POINT_DIFFERENCE_M = 0.010


def file_sha256(path):
    digest = hashlib.sha256()
    with open(path, 'rb') as stream:
        for block in iter(lambda: stream.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()


def georeference(contract, sink, profile, *, origin, affine, offset):
    """Record what the engine applied to its solved frame.

    `affine` is the engine's one matrix for cameras and points. It is linear, so it can
    be undone exactly. `offset` is the XY offset removed before dense processing.
    """
    verify_contract(contract, profile)
    check_seal(sink, SINK, 'sink')
    if sink['contract_sha256'] != contract['sha256']:
        raise FrameRefused('frame_chain_broken')
    matrix = [[float(v) for v in row[:4]] for row in affine[:3]]
    numbers = [*origin, *offset, *(v for row in matrix for v in row)]
    if len(matrix) != 3 or not all(math.isfinite(v) for v in numbers):
        raise FrameRefused('georeference_nonfinite')
    try:
        invert_affine(matrix)
    except ValueError as error:
        raise FrameRefused('affine_not_invertible') from error
    return seal({'schema': GEOREFERENCE, 'sink_sha256': sink['sha256'],
                 'contract_sha256': contract['sha256'],
                 'working_frame': contract['working_frame'],
                 'origin': [float(v) for v in origin], 'affine': matrix,
                 'offset': [float(v) for v in offset], 'output_epsg': profile.output_epsg})


def exact_point(record, profile, engine_xyz):
    """One engine point to output east, north and NAD83(2011) ellipsoidal height.

    Undoes the engine's matrix, then maps the point without any tangent plane. Cameras
    and the fit are not touched.
    """
    local = apply_affine(invert_affine(record['affine']), engine_xyz)
    lon, lat, h = LocalFrame(*record['origin']).to_geodetic(local)
    east, north = profile.output.forward(lon, lat)
    return east, north, h


def exact_pipeline(record, profile):
    """The same map as one PROJ pipeline, for the point cloud step."""
    inverse = invert_affine(record['affine'])
    lat, lon, height = record['origin']
    p = profile.output_parameters
    names = [f's{r + 1}{c + 1}' for r in range(3) for c in range(3)]
    values = [inverse[r][c] for r in range(3) for c in range(3)]
    ellipsoid = f'+a={LocalFrame.AXIS!r} +f={LocalFrame.FLATTENING!r}'
    return ' '.join([
        '+proj=pipeline',
        '+step +proj=affine', *(f'+{n}={v!r}' for n, v in zip(names, values)),
        *(f'+{n}={row[3]!r}' for n, row in zip(('xoff', 'yoff', 'zoff'), inverse)),
        f'+step +inv +proj=topocentric {ellipsoid} +lat_0={lat!r} +lon_0={lon!r} +h_0={height!r}',
        f'+step +inv +proj=cart {ellipsoid}',
        f"+step +proj=tmerc +ellps=GRS80 +lat_0={p['lat0']!r} +lon_0={p['lon0']!r}"
        f" +k={p['scale']!r} +x_0={p['false_east_m']!r} +y_0={p['false_north_m']!r}"])


def bind_cloud(record, profile, *, source_sha256, output_sha256, source_count, output_count,
               witnesses):
    """`witnesses` are (engine xyz, output xyz) pairs decoded from the two files."""
    check_seal(record, GEOREFERENCE, 'georeference')
    if source_count < 1 or source_count != output_count:
        raise FrameRefused('cloud_incomplete')
    if len(witnesses) < 3:
        raise FrameRefused('cloud_witnesses_required')
    for engine, output in witnesses:
        expected = exact_point(record, profile, engine)
        if not all(math.isfinite(v) and abs(v - e) <= MAX_POINT_DIFFERENCE_M
                   for v, e in zip(output, expected)) or len(output) != 3:
            raise FrameRefused('cloud_point_mismatch')
    return seal({'schema': CLOUD, 'georeference_sha256': record['sha256'],
                 'source_sha256': source_sha256, 'output_sha256': output_sha256,
                 'count': output_count})


def bind_dem(cloud, record, profile, *, cloud_sha256, dem_sha256, epsg, affine, data_type, unit):
    """Issued by the DEM step from the files it read and wrote."""
    check_seal(record, GEOREFERENCE, 'georeference')
    check_seal(cloud, CLOUD, 'cloud')
    if (cloud['georeference_sha256'] != record['sha256']
            or cloud['output_sha256'] != cloud_sha256):
        raise FrameRefused('frame_chain_broken')
    if epsg != profile.output_epsg or data_type != 'Float64' or unit != 'm':
        raise FrameRefused('dem_frame_mismatch')
    if len(affine) != 6 or not all(math.isfinite(v) for v in affine):
        raise FrameRefused('dem_frame_mismatch')
    return seal({'schema': DEM, 'cloud_sha256': cloud['sha256'], 'dem_sha256': dem_sha256,
                 'epsg': epsg, 'affine': list(affine), 'height': 'GRS80 ellipsoidal metre',
                 'working_frame': record['working_frame'], 'vertical_application_count': 0})


def verify_chain(chain, profile):
    """Every link, in order. A missing or foreign link refuses."""
    try:
        contract, sink, record, cloud, dem = (chain[key] for key in (
            'contract', 'sink', 'georeference', 'cloud', 'dem'))
    except (KeyError, TypeError) as error:
        raise FrameRefused('engine_derived_frame_required') from error
    verify_contract(contract, profile)
    for value, schema, code in ((sink, SINK, 'sink'), (record, GEOREFERENCE, 'georeference'),
                                (cloud, CLOUD, 'cloud'), (dem, DEM, 'dem')):
        check_seal(value, schema, code)
    if (sink['contract_sha256'] != contract['sha256']
            or record['sink_sha256'] != sink['sha256']
            or record['contract_sha256'] != contract['sha256']
            or record['working_frame'] != contract['working_frame']
            or record['output_epsg'] != profile.output_epsg
            or cloud['georeference_sha256'] != record['sha256']
            or dem['cloud_sha256'] != cloud['sha256']
            or dem['working_frame'] != contract['working_frame']
            or dem['epsg'] != profile.output_epsg or dem['vertical_application_count'] != 0):
        raise FrameRefused('frame_chain_broken')
    return dem


def export_dem(source, destination, *, chain, profile, grid_path, ledger, block_size=128,
               convert=None):
    """One NAVD88 conversion of one engine-derived DEM. Metadata alone is not enough."""
    dem = verify_chain(chain, profile)
    if profile.kind != 'synthetic':
        raise FrameRefused('synthetic_dem_only')
    source_sha256 = file_sha256(source)
    if source_sha256 != dem['dem_sha256']:
        raise FrameRefused('dem_not_engine_derived')
    ledger.claim('export', source_sha256)
    if convert is None:
        from .convert_dem import convert
    try:
        receipt = convert(source, destination, source_sha256=source_sha256, grid_path=grid_path,
                          horizontal_epsg=profile.output_epsg, block_size=block_size)
    except ValueError as error:
        raise FrameRefused(str(error)) from error
    # The output can never be a source, in this process or a later one.
    ledger.claim('export', receipt['output_sha256'])
    return {**receipt, 'dem_frame_sha256': dem['sha256'], 'vertical_application_count': 1}
