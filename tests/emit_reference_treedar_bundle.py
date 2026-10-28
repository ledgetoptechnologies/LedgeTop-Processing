"""Emit an actual owned-descriptor Python ZIP into a synthetic artifact handoff.

This copies no production data, publishes no worker result, and creates no job.
--native-root accepts retained synthetic TreeDar CLI outputs, not acquisitions.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile

from processing import reference_protocol as p
from tests.test_reference_treedar_bundle import PartialWriterTests,FIXTURE


def main():
    assert sys.platform.startswith('linux'),'Linux synthetic handoff required'
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--handoff',required=True);parser.add_argument('--name',default='python-native-bundle')
    parser.add_argument('--native-root');parser.add_argument('--native-input');parser.add_argument('--expected');parser.add_argument('--submission')
    args=parser.parse_args()
    assert re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_-]{0,63}',args.name)
    destination=Path(args.handoff)
    assert destination.is_absolute() and destination.resolve()==destination and destination.is_dir() and not destination.is_symlink()
    case=PartialWriterTests('test_actual_streaming_archive_matches_shared_manifest_and_preserves_parent_and_sources')
    private_input=None
    expected=FIXTURE['expected']
    if args.native_root:
        assert args.native_input and args.expected and args.submission,'actual synthetic input and original submission binding required'
        expected=json.loads(Path(args.expected).read_bytes());raw=json.loads(Path(args.submission).read_bytes())
        checked=p.validate_submission(raw,environment=expected['binding']['environment'],capability_fingerprint=raw['capabilityFingerprint'],
            available_engines=('odm','treedar'),allowed_odm_options=set(raw['pipeline']['odm']['options']))
        assert (checked['manifestSha256'],checked['pipelineDigest'])==(expected['binding']['inputManifestSha256'],expected['binding']['pipelineDigest'])
        obj=next(item for item in checked['manifest']['objects'] if item['id']==expected['binding']['sourceLidar']['objectId'])
        assert obj['kind']=='lidar' and (obj['sha256'],obj['byteSize'])==(expected['binding']['sourceLidar']['sha256'],expected['binding']['sourceLidar']['byteSize'])
        assert raw['pipeline']['treedar']['provenanceReceiptId']==expected['binding']['sourceLidar']['provenanceReceiptId']
        private_input=tempfile.TemporaryDirectory(prefix='ltds-bundle-input-')
        try:
            input_path=Path(private_input.name)/'original-synthetic.las'
            with Path(args.native_input).open('rb') as source,input_path.open('xb') as destination_input:shutil.copyfileobj(source,destination_input,1024**2)
            input_path.chmod(0o600);digest=hashlib.sha256()
            with input_path.open('rb') as stream:
                while block:=stream.read(1024**2):digest.update(block)
            assert input_path.stat().st_size==obj['byteSize'] and digest.hexdigest()==obj['sha256']
        except BaseException:
            private_input.cleanup();raise
        case.native_source=Path(args.native_root);case.expected_override=expected;case.parent_input_bytes_override=checked['manifest']['totalBytes']
    try:
        case.setUp();case.persist_intent();result=case.writer.write_to(case.fd);case.assert_originals()
        archive_path=destination/(args.name+'.part')
        digest=hashlib.sha256();offset=0
        with archive_path.open('xb') as output:
            while block:=os.pread(case.fd,1024**2,offset):output.write(block);digest.update(block);offset+=len(block)
            output.flush();os.fsync(output.fileno())
        assert (offset,digest.hexdigest())==(result['archive']['byteSize'],result['archive']['sha256'])
        record=dict(scope='Synthetic owned partial construction and fixture handoff only; no actual job, genuine acquisition, publication or acceptance.',
            archiveName=archive_path.name,expected=expected,configuredLimits=FIXTURE['limits'],producer=result,
            sourceObjectsRetained=True,callerIntentState=json.loads(case.writer.intent_path.read_bytes())['state'])
        with (destination/(args.name+'.json')).open('x') as output:json.dump(record,output,sort_keys=True);output.write('\n');output.flush();os.fsync(output.fileno())
        fd=os.open(destination,os.O_RDONLY|os.O_DIRECTORY)
        try:os.fsync(fd)
        finally:os.close(fd)
        print(json.dumps(dict(archiveName=archive_path.name,byteSize=offset,sha256=digest.hexdigest(),fileCount=len(result['nativeFiles']),
            state=result['state'],sourceObjectsRetained=True,callerIntentState=record['callerIntentState'])))
    finally:
        case.doCleanups()
        if private_input is not None:private_input.cleanup()


if __name__=='__main__':main()
