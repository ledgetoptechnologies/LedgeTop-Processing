"""Cross the real pinned engine reader source, with the geodesy libraries stubbed.

Needs SURVEY_ENGINE_SOURCE: a folder holding modules/gcp.py and modules/location.py
with the pinned hashes. The engine source is not in this repository. The stubs return
fixed numbers, so these tests show what the reader can carry, not what it computes.
"""
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent))
sys.path.insert(0, str(HERE))
sys.dont_write_bytecode = True

from survey_height import admission_gate as gate  # noqa: E402
from survey_height import import_handoff as handoff  # noqa: E402
from survey_height import patch_engine  # noqa: E402
from survey_height.survey_frame import SurveyRefused  # noqa: E402
from test_survey_height import GRID_PATH, NEEDS_GRID, ReferenceBridge, declared_contract  # noqa: E402

SOURCE = os.environ.get('SURVEY_ENGINE_SOURCE')


class _Crs:
    parsed = []

    def __init__(self, text):
        self.text = text
        self.axis_info = [types.SimpleNamespace(unit_conversion_factor=1.0)] * 3

    @classmethod
    def from_epsg(cls, code):
        cls.parsed.append(f'epsg:{code}')
        return cls(f'epsg:{code}')

    @classmethod
    def from_proj4(cls, text):
        cls.parsed.append(text)
        return cls(text)

    def to_epsg(self):
        return None if '+' in self.text else 4979

    def to_proj4(self):
        return self.text


class _Transform:
    def __init__(self, source, target):
        pass

    def TransformPoint(self, x, y, z):  # noqa: N802
        return (-88.75, 43.1, z)


def _stub_geodesy():
    pyproj = types.ModuleType('pyproj')
    pyproj.CRS, pyproj.Proj, pyproj.Transformer = _Crs, object, object
    osr = types.ModuleType('osgeo.osr')

    class Reference:
        def __getattr__(self, name):
            return lambda *args: 0

    osr.SpatialReference, osr.CoordinateTransformation = Reference, _Transform
    osr.OAMS_TRADITIONAL_GIS_ORDER = 0
    osgeo = types.ModuleType('osgeo')
    osgeo.osr = osr
    return {'pyproj': pyproj, 'osgeo': osgeo, 'osgeo.osr': osr}


@unittest.skipUnless(SOURCE, 'needs the pinned engine reader: SURVEY_ENGINE_SOURCE')
class ReaderTests(unittest.TestCase):
    def setUp(self):
        self.work = Path(self.enterContext(tempfile.TemporaryDirectory()))
        saved = {name: sys.modules.get(name) for name in ('pyproj', 'osgeo', 'osgeo.osr')}
        sys.modules.update(_stub_geodesy())
        self.addCleanup(self._restore, saved)
        _Crs.parsed.clear()
        self.modules = Path(SOURCE) / 'modules'
        self.copy = self.work / 'engine' / 'modules'
        self.copy.mkdir(parents=True)
        original = (self.modules / 'gcp.py').read_bytes()
        (self.copy / 'gcp.py').write_bytes(patch_engine.patch_gcp_source(original))
        (self.copy / 'location.py').write_bytes((self.modules / 'location.py').read_bytes())

    @staticmethod
    def _restore(saved):
        for name, value in saved.items():
            if value is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = value

    def write(self, text):
        path = self.work / 'survey.txt'
        path.write_bytes(text)
        return str(path)

    def test_pins(self):
        self.assertEqual(handoff.engine_modules(SOURCE), {
            'modules/gcp.py': patch_engine.ORIGINAL_SHA256,
            'modules/location.py': patch_engine.LOCATION_SHA256})
        self.assertEqual(handoff.engine_modules(self.work / 'engine')['modules/gcp.py'],
                         patch_engine.PATCHED_SHA256)
        with self.assertRaisesRegex(ValueError, 'engine_reader_not_pinned'):
            patch_engine.patch_gcp_source(b'# other reader')

    def test_unpatched_reader_stops_on_the_marker_instead_of_reading_it(self):
        reader = gate.load_reader(self.modules)
        with self.assertRaises(RuntimeError) as caught:
            reader.GCPFile(self.write(gate.survey_bytes()))
        self.assertNotIsInstance(caught.exception, SurveyRefused)
        self.assertEqual(_Crs.parsed, [])

    def test_reader_writes_a_2d_wgs84_header_and_a_fixed_crs_for_any_input(self):
        reader = gate.load_reader(self.modules)
        for header in ('EPSG:6319', 'EPSG:7600+6360', 'EPSG:4979'):
            rows = header + '\n-88.75 43.1 215.447 10 10 synthetic-1.jpg SYN-1\n'
            utm, exported = gate._read_through(reader, self.work, 'loss', rows.encode())
            self.assertEqual(utm.splitlines()[0], 'WGS84 UTM 16N')
            self.assertEqual(exported['crs'], 'WGS84')
            self.assertEqual(sorted(exported['points'][0]['position']),
                             ['altitude', 'latitude', 'longitude'])
            observed = {'crs': exported['crs'], 'heights_m': {'SYN-1': 215.447}}
            self.assertEqual(gate._lost(declared_contract(), observed),
                             sorted(handoff.WORKING_FRAME))

    def test_legacy_headers_read_the_same_before_and_after(self):
        for header, raw in gate.LEGACY_HEADERS.items():
            text = f'{header}\n{raw[0]} {raw[1]} {raw[2]} 1 2 synthetic-1.jpg A\n'.encode()
            both = []
            for folder in (self.modules, self.copy):
                _Crs.parsed.clear()
                both.append((gate._read_through(gate.load_reader(folder), self.work, 'same', text),
                             list(_Crs.parsed)))
            self.assertEqual(both[0], both[1])
            self.assertTrue(both[0][1])

    @NEEDS_GRID
    def test_hook_refuses_through_the_reader_before_any_crs_is_parsed(self):
        original = handoff.import_survey_gcp
        self.addCleanup(setattr, handoff, 'import_survey_gcp', original)
        seen = []

        def with_test_inputs(path, root):
            seen.append(root)
            return original(path, root, grid_path=GRID_PATH,
                            bridge=ReferenceBridge(handoff.GeoidGrid(GRID_PATH)))

        handoff.import_survey_gcp = with_test_inputs
        with self.assertRaises(SurveyRefused) as caught:
            gate.load_reader(self.copy).GCPFile(self.write(gate.survey_bytes()))
        self.assertEqual(caught.exception.code, handoff.REFUSED)
        self.assertEqual(caught.exception.details['lost'], sorted(handoff.WORKING_FRAME))
        self.assertIn('modules/location.py:69-76', caught.exception.details['at'])
        self.assertEqual(_Crs.parsed, [])
        self.assertEqual([Path(root) for root in seen], [self.work / 'engine'])

    def test_a_handoff_that_returns_cannot_reach_the_legacy_path(self):
        original = handoff.import_survey_gcp
        self.addCleanup(setattr, handoff, 'import_survey_gcp', original)
        handoff.import_survey_gcp = lambda *args: None
        with self.assertRaisesRegex(RuntimeError, 'survey_import_returned_without_handoff'):
            gate.load_reader(self.copy).GCPFile(self.write(gate.survey_bytes()))
        self.assertEqual(_Crs.parsed, [])


if __name__ == '__main__':
    unittest.main(verbosity=2)
