"""Bounded synthetic real-engine qualification, intended for an offline container."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

from processing.treedar_admission import validate_lidar
from tests.test_treedar_admission import write_projected_forest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--synthetic', action='store_true', required=True)
    parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='treedar-qualification-') as temporary:
        root = Path(temporary)
        facts = write_projected_forest(root / 'synthetic.las')
        source = facts['path']
        sha = hashlib.sha256(source.read_bytes()).hexdigest()
        provenance = dict(schemaVersion=1,receiptId='synthetic-qualification',jobId='synthetic-job',
            generation=1,environment='staging',objectId='lidar-1',objectSha256=sha,
            byteSize=source.stat().st_size,sourceKind='synthetic_lidar',verticalUnits='metre',verticalDatum='synthetic_local')
        admitted = validate_lidar(source,job_id='synthetic-job',generation=1,environment='staging',
            object_id='lidar-1',expected_sha256=sha,expected_bytes=source.stat().st_size,
            provenance=provenance,verify_provenance=lambda receipt: receipt == provenance,allow_synthetic=True)
        output = root / 'output'
        result = subprocess.run(['treedar','pipeline',str(source),'--out',str(output),
            '--cell','0.5','--step','1','--preset','broadleaf',
            '--bbox','500000,4500000,500032,4500032'],capture_output=True,text=True,timeout=180)
        if result.returncode:
            print(json.dumps({'admissionPassed':True,'engineSucceeded':False,'exitCode':result.returncode,
                              'syntheticOnly':True,'diagnostic':result.stderr[-2000:]}),flush=True)
            raise SystemExit(result.returncode)
        with (output/'trees.csv').open(newline='') as stream:
            trees = list(csv.DictReader(stream))
        assert len(trees) == 3, 'known synthetic crowns were not recovered'
        assert all(0 < float(tree['height_m']) <= 19 for tree in trees)
        import numpy as np
        import rasterio
        rasters = {}
        for name in ('dtm.tif','chm.tif','crowns.tif'):
            with rasterio.open(output/name) as raster:
                assert raster.crs.to_epsg() == 32616
                assert raster.width <= 66 and raster.height <= 66
                values = raster.read(1,masked=True)
                assert np.isfinite(values.compressed()).all()
                rasters[name] = dict(width=raster.width,height=raster.height,epsg=32616)
        files = [path for path in output.rglob('*') if path.is_file()]
        print(json.dumps({'admissionPassed':True,'engineSucceeded':True,'syntheticOnly':True,
            'genuineAcquisition':False,'inputPoints':facts['pointCount'],'groundPoints':facts['groundPointCount'],
            'knownCrowns':3,'recoveredCrowns':len(trees),'heightsMetres':[float(tree['height_m']) for tree in trees],
            'rasters':rasters,'outputFiles':len(files),'outputBytes':sum(path.stat().st_size for path in files),
            'processingHostsTouched':False}),flush=True)


if __name__ == '__main__':
    main()
