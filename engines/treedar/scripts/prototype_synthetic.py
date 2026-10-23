"""Fixed in-container synthetic acceptance case, invoked by prototype_worker."""

import csv
import hashlib
import json
from pathlib import Path
import subprocess

import numpy as np
from PIL import Image
import rasterio
from synthetic_forest import TREES, write_forest
from treedar import __version__


def run():
    source = Path('/tmp/synthetic.laz')
    out = Path('/runs/synthetic')
    write_forest(source)
    before = hashlib.sha256(source.read_bytes()).hexdigest()
    subprocess.run(['treedar', 'pipeline', str(source), '--out', str(out)], check=True)
    assert hashlib.sha256(source.read_bytes()).hexdigest() == before
    manifest_path = out / 'manifest.json'
    manifest = json.loads(manifest_path.read_text())
    summary = json.loads((out / 'summary.json').read_text())
    assert manifest['command'] == 'pipeline'
    assert manifest['input'] == {'name': source.name, 'bytes': source.stat().st_size}
    assert manifest['treedar_version'] == summary['version'] == __version__
    assert summary['n_trees'] == len(TREES) == 3
    expected = {'dtm.tif', 'chm.tif', 'chm_raw.tif', 'crowns.tif', 'summary.json',
                'trees.csv', 'tree_features.csv', 'tree_map.geojson', 'report.html',
                'areas.png', 'tree_map.png'}
    listed = set()
    hashes = {}
    for item in manifest['outputs']:
        relative = Path(item['path'])
        assert not relative.is_absolute() and '..' not in relative.parts
        path = out / relative
        assert not path.is_symlink() and path.is_file()
        assert item['role'] and item['format']
        if 'bytes' in item:
            assert path.stat().st_size == item['bytes']
        listed.add(item['path'])
        hashes[item['path']] = hashlib.sha256(path.read_bytes()).hexdigest()
    assert expected <= listed
    for name in ('trees.csv', 'tree_features.csv'):
        with (out / name).open(newline='') as stream:
            assert len(list(csv.DictReader(stream))) == 3
    assert len(json.loads((out / 'tree_map.geojson').read_text())['features']) == 3
    assert 'TreeDar Run Report' in (out / 'report.html').read_text(encoding='utf-8')
    with rasterio.open(out / 'dtm.tif') as dtm:
        for name in ('dtm.tif', 'chm.tif', 'chm_raw.tif', 'crowns.tif'):
            with rasterio.open(out / name) as raster:
                assert raster.crs.to_epsg() == 32616
                assert raster.transform == dtm.transform and raster.shape == dtm.shape
                assert raster.profile['tiled']
                assert raster.tags(ns='IMAGE_STRUCTURE')['LAYOUT'] == 'COG'
                assert np.isfinite(raster.read(1)).all()
    for name in ('areas.png', 'tree_map.png'):
        with Image.open(out / name) as image:
            assert min(image.size) >= 100
            image.verify()
    # Store all output hashes locally; send just their aggregate and manifest hash.
    encoded = json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode()
    (Path('/runs') / 'output-sha256.json').write_bytes(encoded)
    print('PROTOTYPE_RESULT=' + json.dumps({
        'n_trees': 3, 'treedar_version': __version__,
        'manifest_sha256': hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
        'outputs_sha256': hashlib.sha256(encoded).hexdigest(),
    }, sort_keys=True))


if __name__ == '__main__':
    run()
