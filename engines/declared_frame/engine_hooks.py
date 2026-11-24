"""What the patched engine calls. Every unported path refuses instead of guessing."""
import json
import math
import os
from pathlib import Path

from . import dem_frame
from .contract import normalize_once, projection, verify_contract, verify_sink
from .errors import FrameRefused
from .geodesy import GRID_NAME, GeoidGrid, LocalFrame
from .ledger import Ledger
from .profile import load_profile
from .survey_frame import read_survey

# Engine code that reads or writes coordinates and is not ported, because its pinned
# source was not available. While this is not empty no declared run starts.
UNPORTED = (
    'modules/osfm.py: control export call, OpenSfM config and the transform step',
    'stages/run_opensfm.py: passes the PROJ string that this candidate refuses',
    'stages/odm_dem.py and modules/dem: rasterize and label the DEM',
    'modules/cropper.py, modules/boundary.py, modules/point_cloud.py: read the cloud CRS',
    'modules/photo.py and stages/dataset.py: camera GPS and control file discovery',
    'opensfm/io.py, opensfm/geo.py: control reader, local frame and projection matrix',
    'opensfm/reconstruction.py, opensfm/align.py: fit use of control and camera GPS',
)
RUN = Path(__file__).with_name('run')
MARKER = 'SURVEY3D'
COORDS_HEADER = 'SURVEY3D-FRAME/1'
CONTROL_FILE = 'ground_control_points.json'
MAX_LOCAL_DIFFERENCE_M = 1e-6


def _read(path):
    try:
        return json.loads(Path(path).read_text(encoding='utf-8'))
    except (OSError, ValueError) as error:
        raise FrameRefused('declared_state_missing', {'file': Path(path).name}) from error


def _create(path, value):
    """Exclusive, so an earlier record is never replaced; an equal one is accepted."""
    text = json.dumps(value, indent=1, sort_keys=True, allow_nan=False)
    try:
        with open(path, 'x', encoding='utf-8', newline='') as stream:
            stream.write(text)
    except FileExistsError as error:
        if Path(path).read_text(encoding='utf-8') != text:
            raise FrameRefused('declared_state_changed', {'file': Path(path).name}) from error


def run_profile():
    """The profile named by the manifest beside this package."""
    manifest = _read(RUN / 'manifest.json')
    try:
        raw = (RUN / 'profile.json').read_bytes()
    except OSError as error:
        raise FrameRefused('declared_state_missing', {'file': 'profile.json'}) from error
    return load_profile(raw, manifest.get('profile_sha256'))


def run_ledger():
    return Ledger(RUN / 'ledger', run_nonce=_read(RUN / 'authorization.json').get('run_nonce'))


def is_marker(line):
    return line.upper().startswith(MARKER)


def require_ported():
    if UNPORTED:
        raise FrameRefused('successor_incomplete', {'unported': list(UNPORTED)})


# ------------------------------------------------------- modules/gcp.py


class DeclaredSurvey:
    def __init__(self, survey, profile, contract):
        self.survey, self.profile, self.contract = survey, profile, contract

    def entries(self):
        """The surveyed text, so the engine's counts see the same marks."""
        return [' '.join((*m.raw_xyz, *m.raw_pixel, m.filename, m.id)) for m in self.survey.marks]

    def export(self, output_json, photos):
        sizes = {photo.filename: [photo.width, photo.height] for photo in photos}
        needed = {mark.filename for mark in self.survey.marks}
        if not needed <= set(sizes):
            raise FrameRefused('sink_image_missing')
        # No "points" and no "crs": a reader without the hooks cannot use this file.
        value = {'declared_frame': self.contract['sha256'], 'contract': self.contract,
                 'sizes': {name: sizes[name] for name in sorted(needed)}}
        with open(output_json, 'w', encoding='utf-8', newline='') as stream:
            json.dump(value, stream, indent=1, sort_keys=True, allow_nan=False)
        return output_json

    def georeference(self, photos, coords_file):
        names = {photo.filename for photo in photos}
        if not {mark.filename for mark in self.survey.marks} <= names:
            raise FrameRefused('sink_image_missing')
        places = [self.profile.output.forward(*point['working'][:2])
                  for point in self.contract['points']]
        offset = [int(round(math.fsum(axis) / len(places))) for axis in zip(*places)]
        with open(coords_file, 'w', encoding='utf-8', newline='') as stream:
            stream.write(f"{COORDS_HEADER} {self.contract['sha256']}\n{offset[0]} {offset[1]}\n")
        _create(Path(coords_file).with_name('declared_contract.json'), self.contract)
        return DeclaredGeoRef(self.contract, self.profile, *offset)


def open_survey(gcp_path, *, bridge=None):
    """Normalize on first open; later opens of the same bytes reuse that one result."""
    require_ported()
    profile = run_profile()
    survey = read_survey(Path(gcp_path).read_bytes(), profile)
    state = RUN / 'state' / f'contract-{survey.raw_sha256}.json'
    ledger = run_ledger()
    if state.exists():
        contract = verify_contract(_read(state), profile)
        mine = {'step': 'normalize', 'key': survey.raw_sha256}
        if contract['source']['raw_sha256'] != survey.raw_sha256 or mine not in ledger.entries():
            raise FrameRefused('declared_state_changed', {'file': state.name})
        return DeclaredSurvey(survey, profile, contract)
    try:
        grid = GeoidGrid(RUN / GRID_NAME)
    except ValueError as error:
        raise FrameRefused(str(error)) from error
    if bridge is None:
        from .vertical_bridge import VerticalBridge
        bridge = VerticalBridge(RUN / GRID_NAME, profile)
    contract = normalize_once(survey, profile, bridge, grid, ledger)
    _create(state, contract)
    return DeclaredSurvey(survey, profile, contract)


def unported(name):
    raise FrameRefused('declared_frame_unported', {'at': name})


# ------------------------------------- modules/location.py, modules/types.py


class DeclaredSrs:
    """Stands where the engine keeps a CRS. It cannot be flattened to PROJ.4 or a code."""

    declared_frame = True

    def __init__(self, epsg):
        self.horizontal_epsg = epsg

    def to_epsg(self):
        unported('CRS to EPSG code')

    def to_proj4(self):
        unported('CRS to PROJ string')


class DeclaredGeoRef:
    declared_frame = True

    def __init__(self, contract, profile, east_offset, north_offset):
        self.contract, self.profile = contract, profile
        self.srs = DeclaredSrs(profile.output_epsg)
        self.utm_east_offset, self.utm_north_offset = float(east_offset), float(north_offset)
        self.transform = []

    def proj4(self):
        return self.srs.to_proj4()

    def utm_offset(self):
        return self.utm_east_offset, self.utm_north_offset


def refuse_declared_crs(value):
    """The legacy conversion takes no declared frame: it would drop the datum."""
    if getattr(value, 'declared_frame', False):
        unported('legacy coordinate transformation')


def georef_from_coords(coords_file):
    require_ported()
    profile = run_profile()
    with open(coords_file, encoding='utf-8') as stream:
        header, offsets = stream.readline().split(), stream.readline().split()
    contract = verify_contract(
        _read(Path(coords_file).with_name('declared_contract.json')), profile)
    if header != [COORDS_HEADER, contract['sha256']] or len(offsets) != 2:
        raise FrameRefused('declared_state_changed', {'file': Path(coords_file).name})
    return DeclaredGeoRef(contract, profile, *map(float, offsets))


# --------------------------------------------------- opensfm/dataset.py


def _control(data):
    path = Path(data.data_path) / CONTROL_FILE
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding='utf-8'))
    except ValueError:
        return {}  # the engine's own reader then fails on it, as before
    return value if isinstance(value, dict) else {}


def is_declared(data):
    return 'declared_frame' in _control(data)


def _declared(data):
    profile = run_profile()
    control = _control(data)
    contract = verify_contract(control.get('contract'), profile)
    if control.get('declared_frame') != contract['sha256']:
        raise FrameRefused('declared_state_changed', {'file': CONTROL_FILE})
    sizes = {name: tuple(size) for name, size in control['sizes'].items()}
    return profile, contract, sizes


def load_points(data, pymap):
    """Fit points only, whatever the caller asks for. Checked as the solver sees them."""
    require_ported()
    profile, contract, sizes = _declared(data)
    # Camera GPS is in another frame and nothing here transforms it.
    if data.config.get('bundle_use_gps') is not False:
        raise FrameRefused('camera_gps_frame_unqualified')
    if not set(sizes) <= set(data.images()):
        raise FrameRefused('sink_image_missing')
    points = []
    for item in contract['points']:
        lon, lat, height = item['working']
        point = pymap.GroundControlPoint()
        point.id = item['id']
        point.lla = {'latitude': lat, 'longitude': lon, 'altitude': height}
        point.has_altitude = True
        point.role = pymap.GroundControlPointRole.GCP
        for mark in item['marks']:
            observation = pymap.GroundControlPointObservation()
            observation.shot_id = mark['filename']
            observation.projection = _vector(
                projection(mark['raw_pixel'], *sizes[mark['filename']]))
            point.add_observation(observation)
        points.append(point)
    sink = verify_sink(contract, profile, [_read_back(point) for point in points], sizes)
    _create(Path(data.data_path) / 'declared_sink.json', sink)
    return points


def _vector(values):
    try:
        import numpy
    except ImportError:
        return list(values)
    return numpy.array(values, dtype=float)


def _read_back(point):
    lla = point.lla
    return {'id': point.id, 'longitude': float(lla['longitude']),
            'latitude': float(lla['latitude']), 'height': float(lla['altitude']),
            'has_height': bool(point.has_altitude),
            'observations': [[o.shot_id, float(o.projection[0]), float(o.projection[1])]
                             for o in point.observations]}


def reference_frame(data):
    _, contract, _ = _declared(data)
    return contract['sha256']


def check_reference(data, stored):
    """A declared reference without its contract, or the reverse, is refused."""
    expected = reference_frame(data) if is_declared(data) else None
    if stored.get('declared_frame') != expected:
        raise FrameRefused('reference_frame_mismatch')


# --------------------------------------- opensfm/actions/export_geocoords.py


def declared_projection(data, proj):
    """Map projection inside one datum. The height is carried, never shifted."""
    require_ported()
    profile, _, _ = _declared(data)
    if proj != f'EPSG:{profile.output_epsg}':
        raise FrameRefused('output_frame_mismatch')
    from pyproj import Transformer
    flat = Transformer.from_crs(6318, profile.output_epsg, always_xy=True, allow_ballpark=False)

    def project(lon, lat, height):
        east, north = flat.transform(lon, lat, errcheck=True)
        return east, north, height

    return project


def record_affine(data, reference, matrix, offset):
    """Seal the matrix the engine is about to apply, after checking its local frame."""
    profile, contract, _ = _declared(data)
    frame = LocalFrame(reference.lat, reference.lon, reference.alt)
    for point in contract['points']:
        lon, lat, height = point['working']
        engine = reference.to_topocentric(lat, lon, height)
        if any(abs(a - b) > MAX_LOCAL_DIFFERENCE_M
               for a, b in zip(engine, frame.to_local(lon, lat, height))):
            raise FrameRefused('engine_local_frame_mismatch', {'id': point['id']})
    record = dem_frame.georeference(
        contract, _read(Path(data.data_path) / 'declared_sink.json'), profile,
        origin=(reference.lat, reference.lon, reference.alt),
        affine=[list(row) for row in matrix], offset=offset)
    _create(Path(data.data_path) / 'declared_georeference.json', record)
    return record


# ------------------------------------------- stages/odm_georeferencing.py


def georeference_cloud(source, output, georef, opensfm_path, *, run):
    """Exact per-point map of the cloud. Cameras, mesh and orthophoto are not handled.

    The output CRS states the horizontal system only. The height reference is carried by
    the sealed chain, because the LAS header cannot state this ellipsoidal height.
    """
    require_ported()
    record = _read(Path(opensfm_path) / 'declared_georeference.json')
    if record.get('contract_sha256') != georef.contract['sha256']:
        raise FrameRefused('frame_chain_broken')
    if list(georef.utm_offset()) != record['offset']:
        raise FrameRefused('georeference_offset_mismatch')
    if os.path.exists(output):
        raise FrameRefused('new_output_path_required')
    stages = [str(source),
              {'type': 'filters.ferry', 'dimensions': 'views => UserData'},
              {'type': 'filters.projpipeline',
               'coord_op': dem_frame.exact_pipeline(record, georef.profile),
               'out_srs': f'EPSG:{georef.profile.output_epsg}'},
              {'type': 'writers.las', 'filename': str(output), 'compression': True,
               'minor_version': 4, 'dataformat_id': 7,
               'a_srs': f'EPSG:{georef.profile.output_epsg}',
               **{f'scale_{axis}': 0.0001 for axis in 'xyz'},
               'offset_x': georef.utm_east_offset, 'offset_y': georef.utm_north_offset,
               'offset_z': 0}]
    run(['pdal', 'pipeline', '--stdin'], json.dumps(stages))
    # PDAL has returned success with points dropped, so counts come from decoded files.
    before, after = _inspect(source, run), _inspect(output, run)
    count = before['count']
    if count < 1 or after['count'] != count:
        raise FrameRefused('cloud_incomplete')
    index = sorted({0, count // 2, count - 1})
    witnesses = list(zip(_points(source, index, run), _points(output, index, run)))
    cloud = dem_frame.bind_cloud(
        record, georef.profile, source_sha256=dem_frame.file_sha256(source),
        output_sha256=dem_frame.file_sha256(output), source_count=count,
        output_count=after['count'], witnesses=witnesses)
    _create(Path(opensfm_path) / 'declared_cloud.json', cloud)
    return cloud


def _inspect(path, run):
    value = json.loads(run(['pdal', 'info', '--stats', '--dimensions', 'X,Y,Z', str(path)], None))
    counts = {item['count'] for item in value['stats']['statistic']}
    if len(value['stats']['statistic']) != 3 or len(counts) != 1:
        raise FrameRefused('cloud_incomplete')
    return {'count': counts.pop()}


def _points(path, index, run):
    value = json.loads(run(['pdal', 'info', '-p', ','.join(map(str, index)), str(path)], None))
    points = value['points']['point']
    points = points if isinstance(points, list) else [points]
    return [(p['X'], p['Y'], p['Z']) for p in points]
