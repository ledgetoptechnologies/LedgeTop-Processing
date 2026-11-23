"""PROJ and GDAL tests. Not runnable without pyproj, GDAL and SURVEY_GEOID18_GRID."""
import hashlib
import os
import sys
import tempfile
import unittest
from pathlib import Path

os.environ['PROJ_NETWORK'] = 'OFF'
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent))
sys.path.insert(0, str(HERE))
sys.dont_write_bytecode = True

try:
    import numpy  # noqa: F401
    from osgeo import gdal, osr  # noqa: F401
    from pyproj import network
    RUNTIME = True
except ImportError:
    RUNTIME = False

from survey_height import admission_gate as gate  # noqa: E402
from survey_height import import_handoff as handoff  # noqa: E402
from survey_height.export_once import ExportLedger  # noqa: E402
from survey_height.grid_reference import GeoidGrid  # noqa: E402
from survey_height.survey_frame import read_survey  # noqa: E402
from test_survey_height import GRID_PATH, NEEDS_GRID  # noqa: E402


@NEEDS_GRID
@unittest.skipUnless(RUNTIME, 'needs numpy, pyproj and GDAL')
class ProjRuntimeTests(unittest.TestCase):
    def setUp(self):
        from survey_height.vertical_bridge import SurveyVerticalBridge
        network.set_network_enabled(False)
        self.work = Path(self.enterContext(tempfile.TemporaryDirectory()))
        self.grid = GeoidGrid(GRID_PATH)
        self.bridge = SurveyVerticalBridge(GRID_PATH)
        self.contract = handoff.normalize_once(
            read_survey(gate.survey_bytes()), self.bridge, self.grid, handoff.Ledger())
        self.ledger = ExportLedger()
        self.dem = gate._collect_dem(self.work, GRID_PATH, self.contract, self.bridge, self.ledger)

    def test_normalization_and_dem_within_the_gate_limits(self):
        observed = {'contract': self.contract, 'operation': self.bridge.pipeline,
                    'round_trip_ftus': list(self.bridge.ellipsoid_to_survey(
                        self.contract['points'][0]['working']))}
        checks = {**gate._check_normalization(observed, self.grid),
                  **gate._check_dem(self.dem, self.grid)}
        self.assertEqual([name for name, ok in checks.items() if ok is not True], [])

    def test_every_negative_case_is_refused_for_its_own_reason(self):
        self.assertEqual(gate._collect_negatives(
            self.work, GRID_PATH, self.contract, self.bridge, self.grid, self.ledger,
            self.dem['source_sha256_before']), gate.NEGATIVES)

    def test_refused_dem_leaves_no_output(self):
        gate._collect_negatives(self.work, GRID_PATH, self.contract, self.bridge, self.grid,
                                self.ledger, self.dem['source_sha256_before'])
        left = sorted(p.name for p in self.work.glob('*')
                      if p.name.endswith('-out.tif') or p.name in ('again.tif', 'twice.tif')
                      or p.name.startswith('.navd88-'))
        self.assertEqual(left, [])

    def test_output_bytes_are_bound(self):
        actual = hashlib.sha256((self.work / 'navd88.tif').read_bytes()).hexdigest()
        self.assertEqual(actual, self.dem['receipt']['output_sha256'])


if __name__ == '__main__':
    unittest.main(verbosity=2)
