"""Execute the actual fixed TreeDar bootstrap in an ephemeral synthetic container.

Requires explicit --synthetic, a read-only source mount at /worker, and empty
ephemeral tmpfs mounts at /input, /control and /output. No Docker socket is used.
"""
import argparse
from dataclasses import asdict
import hashlib
import json
from pathlib import Path

from processing import reference_protocol as p
from processing.treedar_admission import AdmissionPolicy
from processing.treedar_engine import _inside, source_digest
from tests.test_treedar_admission import write_projected_forest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--synthetic', action='store_true', required=True)
    parser.parse_args()
    if not Path('/.dockerenv').is_file():
        raise ValueError('isolated synthetic container required')
    for name in ('/input', '/control', '/output'):
        directory = Path(name)
        if directory.is_symlink() or not directory.is_dir() or any(directory.iterdir()):
            raise ValueError('empty ephemeral qualification mounts required')
    facts = write_projected_forest(Path('/input/cloud.las'))
    source = facts['path']
    sha = hashlib.sha256(source.read_bytes()).hexdigest()
    job = dict(jobId='00000000-0000-4000-8000-000000000123',generation=1,
               environment='staging',manifestSha256=p.digest({'synthetic':sha}),
               pipelineDigest=p.digest({'syntheticTreeDar':True}))
    obj = dict(id='lidar-1',kind='lidar',name='synthetic.las',byteSize=source.stat().st_size,sha256=sha)
    provenance = dict(schemaVersion=1,receiptId='synthetic-bootstrap-provenance',jobId=job['jobId'],
        generation=1,environment='staging',objectId=obj['id'],objectSha256=sha,byteSize=obj['byteSize'],
        sourceKind='synthetic_lidar',verticalUnits='metre',verticalDatum='synthetic_local')
    from treedar import __version__
    task = dict(sourceSha256=source_digest('/worker'),engineVersion=__version__,
        policy=asdict(AdmissionPolicy()),inputObject=obj,job=job,provenance=provenance,
        allowSynthetic=True,outputBudget=64*1024**2,maxFiles=64,timeoutSeconds=180)
    Path('/control/task.json').write_bytes(p.canonical_json(task))
    status = _inside()
    if status != 0:
        raise RuntimeError('actual TreeDar bootstrap qualification failed')
    proof = p.decode_submission(Path('/output/proof.json').read_bytes())
    if proof['job'] != job or proof['admission']['genuineAcquisition'] is not False:
        raise RuntimeError('synthetic bootstrap proof binding changed')
    for obj in proof['objects']:
        output = Path('/output') / obj['relativePath']
        if output.stat().st_size != obj['byteSize'] or hashlib.sha256(output.read_bytes()).hexdigest() != obj['sha256']:
            raise RuntimeError('bootstrap output object verification failed')
    import csv
    with Path('/output/run/trees.csv').open(newline='') as stream:
        trees = list(csv.DictReader(stream))
    if len(trees) != 3 or sorted(float(tree['height_m']) for tree in trees) != [12.0,15.0,18.0]:
        raise RuntimeError('bootstrap synthetic crown geometry mismatch')
    print(json.dumps(dict(passed=True,actualBootstrap=True,actualTreeDarCli=True,
        syntheticOnly=True,genuineAcquisition=False,engineVersion=__version__,inputPoints=facts['pointCount'],
        knownCrowns=3,recoveredCrowns=len(trees),outputObjects=len(proof['objects']),
        outputHashesVerified=True,processingHostsTouched=False)),flush=True)


if __name__ == '__main__':
    main()
