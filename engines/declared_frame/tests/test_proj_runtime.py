"""Four PROJ/GDAL tests. Run only in a digest-pinned container with no network.

Needs DECLARED_FRAME_GRID and DECLARED_FRAME_CONTAINER=1. See README.md.
"""
import math
import os
import sys
import tempfile
import unittest
from pathlib import Path

os.environ['PROJ_NETWORK'] = 'OFF'
sys.path.insert(0, str(Path(__file__).resolve().parent))
from support import (  # noqa: E402
    EAST, GRID_PATH, NEEDS_GRID, NONCE, NORTH, PROFILE, GeoidGrid, Ledger, Refusals, chain,
    dem_frame, frame_contract, normalize_once, seal, survey_bytes, survey_frame)

ALLOWED = os.environ.get('DECLARED_FRAME_CONTAINER') == '1'
NODATA = -9999.0
SAMPLES = [[220.0, 245.5], [NODATA, 281.25]]
MASK = [[255, 255], [0, 255]]
WITNESSES = ((0, 0), (0, 1), (1, 1))
AFFINE = (float(round(EAST)), 3200.0, 170.0, float(round(NORTH)), 230.0, -2700.0)


@NEEDS_GRID
@unittest.skipUnless(ALLOWED, 'runs only in the pinned container: DECLARED_FRAME_CONTAINER')
class ProjRuntimeTests(Refusals):
    def setUp(self):
        from osgeo import gdal, osr
        from pyproj import network
        from declared_frame.vertical_bridge import VerticalBridge
        network.set_network_enabled(False)
        osr.SetPROJEnableNetwork(False)
        self.gdal, self.osr = gdal, osr
        self.folder = Path(self.enterContext(tempfile.TemporaryDirectory()))
        (self.folder / 'ledger').mkdir()
        self.ledger = Ledger(self.folder / 'ledger', run_nonce=NONCE)
        self.grid = GeoidGrid(GRID_PATH)
        self.bridge = VerticalBridge(GRID_PATH, PROFILE)
        self.contract = normalize_once(survey_frame.read_survey(survey_bytes(), PROFILE), PROFILE,
                                       self.bridge, self.grid, self.ledger)

    def dem(self, name, *, epsg=None, reference='NAD83(2011)_ELLIPSOID_METRE', samples=SAMPLES):
        import numpy as np
        path = self.folder / name
        self.gdal.SetThreadLocalConfigOption('GDAL_TIFF_INTERNAL_MASK', 'YES')
        raster = self.gdal.GetDriverByName('GTiff').Create(
            str(path), 2, 2, 1, self.gdal.GDT_Float64)
        frame = self.osr.SpatialReference()
        frame.ImportFromEPSG(epsg or PROFILE.output_epsg)
        raster.SetSpatialRef(frame)
        raster.SetGeoTransform(AFFINE)
        raster.SetMetadata({'HEIGHT_REFERENCE': reference, 'HEIGHT_TRANSFORM_COUNT': '0'})
        band = raster.GetRasterBand(1)
        band.SetUnitType('m')
        band.SetNoDataValue(NODATA)
        band.WriteArray(np.array(samples, dtype=np.float64))
        band.CreateMaskBand(self.gdal.GMF_PER_DATASET)
        band.GetMaskBand().WriteArray(np.array(MASK, dtype=np.uint8))
        band = raster = None
        self.gdal.SetThreadLocalConfigOption('GDAL_TIFF_INTERNAL_MASK', None)
        return path

    def chain_for(self, path):
        value = chain(self.contract)
        value['dem'] = seal({**value['dem'], 'dem_sha256': dem_frame.file_sha256(path),
                             'affine': list(AFFINE)})
        return value

    def export(self, source, output, grid=GRID_PATH):
        return dem_frame.export_dem(source, output, chain=self.chain_for(source), profile=PROFILE,
                                    grid_path=grid, ledger=self.ledger, block_size=1)

    def test_normalization_matches_the_independent_reference(self):
        for point in frame_contract.verify_contract(self.contract, PROFILE)['points']:
            east, north, height = (float(v) for v in point['raw_xyz'])
            lon, lat = PROFILE.survey.inverse(east, north)
            expected = height * PROFILE.survey_unit_m + self.grid.undulation(lon, lat)
            self.assertAlmostEqual(point['working'][2], expected, delta=0.010)

    def test_dem_export_keeps_the_grid_and_converts_once(self):
        from pyproj import CRS
        source, output = self.dem('in.tif'), self.folder / 'out.tif'
        before = dem_frame.file_sha256(source)
        self.export(source, output)
        raster = self.gdal.Open(str(output))
        band = raster.GetRasterBand(1)
        values = band.ReadAsArray()
        for row, column in WITNESSES:
            x, y = column + 0.5, row + 0.5
            lon, lat = PROFILE.output.inverse(AFFINE[0] + x * AFFINE[1] + y * AFFINE[2],
                                              AFFINE[3] + x * AFFINE[4] + y * AFFINE[5])
            got = float(values[row, column])
            self.assertAlmostEqual(got, SAMPLES[row][column] - self.grid.undulation(lon, lat),
                                   delta=0.010)
            east, north = PROFILE.survey.forward(lon, lat)
            back = self.bridge.survey_to_ellipsoid((east, north, got / PROFILE.survey_unit_m))[2]
            self.assertAlmostEqual(back, SAMPLES[row][column], delta=0.001)
        self.assertEqual(raster.GetGeoTransform(), AFFINE)
        self.assertEqual(band.GetMaskBand().ReadAsArray().tolist(), MASK)
        self.assertEqual((band.GetNoDataValue(), float(values[1, 0])), (NODATA, NODATA))
        self.assertEqual(band.GetUnitType(), 'm')
        self.assertTrue(CRS.from_wkt(raster.GetProjection()).equals(
            CRS.from_user_input(f'EPSG:{PROFILE.output_epsg}+5703'), ignore_axis_order=True))
        self.assertEqual(raster.GetMetadataItem('HEIGHT_TRANSFORM_COUNT'), '1')
        self.assertEqual(dem_frame.file_sha256(source), before)
        band = raster = None
        self.refused('second_conversion', self.export, source, self.folder / 'again.tif')
        self.refused('second_conversion', self.export, output, self.folder / 'twice.tif')

    def test_refused_dems_leave_no_output(self):
        wrong = self.folder / 'wrong.tif'
        raw = bytearray(Path(GRID_PATH).read_bytes())
        raw[-1] ^= 1
        wrong.write_bytes(raw)
        cases = (('a', {'epsg': 32619}, GRID_PATH, 'declared_horizontal_required'),
                 ('b', {'reference': 'UNKNOWN'}, GRID_PATH, 'unconverted_ellipsoid_source_required'),
                 ('c', {'samples': [[math.nan, 1.0], [NODATA, 2.0]]}, GRID_PATH,
                  'nonfinite_valid_height'),
                 ('d', {'samples': [[221.0, 245.5], [NODATA, 281.25]]}, wrong,
                  'verified_geoid18_grid_required'))
        for name, options, grid, code in cases:
            output = self.folder / f'{name}-out.tif'
            self.refused(code, self.export, self.dem(f'{name}.tif', **options), output, grid)
            self.assertFalse(output.exists())
        self.assertEqual([p.name for p in self.folder.glob('.navd88-*')], [])

    def test_exact_cloud_pipeline_matches_the_reference(self):
        from pyproj import Transformer
        record = chain(self.contract)['georeference']
        pipeline = Transformer.from_pipeline(dem_frame.exact_pipeline(record, PROFILE))
        for point in ((10.0, 20.0, 230.0), (900.0, -700.0, 260.0), (-1200.0, 1100.0, 215.0)):
            for a, b in zip(pipeline.transform(*point, errcheck=True),
                            dem_frame.exact_point(record, PROFILE, point)):
                self.assertAlmostEqual(a, b, delta=1e-5)


if __name__ == '__main__':
    unittest.main(verbosity=2)
