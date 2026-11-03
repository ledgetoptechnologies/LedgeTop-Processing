"""Real Viewer in-memory repository → inactive Processing projection, synthetic only."""
import base64
import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import unittest

from processing import source_inventory_candidate as inv
from processing.viewer_source_snapshot_adapter_candidate import evaluate

ROOT=Path(__file__).resolve().parents[1]
VIEWER_COMMIT='5513f92b6cf45221475a3c38347e3ca1dbf0c32d'


class ViewerRepositorySourceSnapshotPairTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        root=os.environ.get('PINNED_VIEWER_ROOT')
        if not root:
            if os.environ.get('REQUIRE_PINNED_VIEWER_PAIR') == '1':
                raise RuntimeError('PINNED_VIEWER_ROOT required for release validation')
            raise unittest.SkipTest('PINNED_VIEWER_ROOT required; set REQUIRE_PINNED_VIEWER_PAIR=1 for release validation')
        cls.viewer=Path(root).resolve(strict=True)
        head=subprocess.run(['git','rev-parse','HEAD'],cwd=cls.viewer,text=True,capture_output=True,check=True).stdout.strip()
        status=subprocess.run(['git','status','--porcelain','--untracked-files=all'],cwd=cls.viewer,text=True,capture_output=True,check=True).stdout
        if head!=VIEWER_COMMIT or status:raise AssertionError('clean pinned Viewer checkout required')

    def fixture(self, mode='normal'):
        env=dict(os.environ,PINNED_VIEWER_ROOT=str(self.viewer))
        result=subprocess.run(['node',str(ROOT/'tests/support/viewer_source_snapshot_fixture.cjs'),mode],
            cwd=ROOT,env=env,text=True,encoding='utf8',capture_output=True,check=True,timeout=30)
        data=json.loads(result.stdout); dataset=data['dataset'];files=dataset['files'];submission=data['provenance']
        self.verify_synthetic_bytes(data)
        manifest=[{key:f[key] for key in ('relativePath','byteSize','sha256')} for f in files]
        manifest_wire=json.dumps(manifest,ensure_ascii=False,separators=(',',':')).encode('utf8')
        self.assertEqual(hashlib.sha256(manifest_wire).hexdigest(),dataset['manifestSha256'])
        expected=dict(principalId='viewer-synthetic',environment='staging',authorityId='30000000-0000-4000-8000-000000000001',
            datasetId=dataset['id'],manifestSha256=dataset['manifestSha256'],viewerTaskId=data['taskId'],viewerAttemptId=data['attemptId'],pipelineDigest='b'*64)
        base=lambda f:dict(datasetFileId=f['id'],relativePath=f['relativePath'],byteSize=f['byteSize'],sha256=f['sha256'],
                           processingRole=f['processingRole'],contentType=f['contentType'] or f['mimeType'])
        processing=sorted([base(f) for f in files if f['processingRole']=='image'],key=lambda x:x['datasetFileId'])
        retained=sorted([base(f) for f in files if f['processingRole']!='image'],key=lambda x:x['datasetFileId'])
        excluded=[dict(sourceDatasetFileId=f['datasetFileId'],sourceRelativePath=f['relativePath'],sourceSha256=f['sha256'],sourceProcessingRole='auto') for f in retained if f['processingRole']=='auto']
        value=dict(protocol=inv.EXCLUDED_AUTO_PROTOCOL,schemaVersion=3,dataset=dict(id=dataset['id'],manifestSha256=dataset['manifestSha256']),
            binding={k:expected[k] for k in ('viewerTaskId','viewerAttemptId','pipelineDigest')},processing=processing,retained=retained,
            generated=[data['evidence']['snapshot']],substitutions=[],excludedAuto=excluded)
        authoritative=dict(id=dataset['id'],manifestSha256=dataset['manifestSha256'],files=sorted([
            dict(datasetFileId=f['id'],relativePath=f['relativePath'],byteSize=f['byteSize'],sha256=f['sha256'],contentType=f['contentType'],
                 mimeType=f['mimeType'],imageWidth=f['width'],imageHeight=f['height']) for f in files],key=lambda x:x['datasetFileId']))
        selection={k:expected[k] for k in ('viewerTaskId','viewerAttemptId','pipelineDigest')}
        selection.update(gcpRequired=True,files=sorted([dict(datasetFileId=f['datasetFileId'],processingRole=f['processingRole'],disposition=disposition)
            for disposition,group in (('processing',processing),('retained',retained)) for f in group],key=lambda x:x['datasetFileId']))
        source_bytes=data['snapshot']['content'].encode('utf8')
        total=sum(f['byteSize'] for f in submission['inputFiles'])
        return dict(raw_inventory=json.dumps(value,ensure_ascii=False).encode(),expected=expected,authoritative_dataset=authoritative,
                    trusted_selection=selection,trusted_gcp_evidence=json.dumps(data['evidence'],ensure_ascii=False).encode(),
                    generated_gcp_bytes=source_bytes,viewer_submission=submission,
                    source_policy=dict(objectLimit=10000,byteLimit=10**7,transferByteBudget=3*total)), data

    @staticmethod
    def verify_synthetic_bytes(data):
        # This is fixture verification only, not an authenticated production
        # source reader or a durable file custody check.
        for f in data['dataset']['files']:
            try:b=base64.b64decode(data['sourceBytes'][f['id']],validate=True)
            except (KeyError,ValueError):raise ValueError('synthetic source missing or invalid') from None
            if (len(b),hashlib.sha256(b).hexdigest())!=(f['byteSize'],f['sha256']):
                raise ValueError('synthetic source changed')

    def test_persisted_viewer_provenance_matches_held_processing_projection(self):
        args,data=self.fixture();wire,_,state=evaluate(**args)
        self.assertEqual(state,'held_authenticated_source_authority_unavailable')
        self.assertEqual(json.loads(wire)['objectCount'],3)
        self.assertEqual(args['viewer_submission']['gcpSnapshotSha256'],data['snapshot']['sha256'])
        self.assertEqual(data['evidence'],data['evidenceAfterLiveEdit'])
        self.assertEqual([x['processingRole'] for x in data['dataset']['files']].count('auto'),1)

    def test_missing_changed_source_and_attempt_mismatch_hold(self):
        args,data=self.fixture()
        missing_bytes=copy.deepcopy(data);del missing_bytes['sourceBytes']['photo-1']
        with self.assertRaisesRegex(ValueError,'source missing'):self.verify_synthetic_bytes(missing_bytes)
        changed_bytes=copy.deepcopy(data);changed_bytes['sourceBytes']['photo-1']=base64.b64encode(b'altered').decode()
        with self.assertRaisesRegex(ValueError,'source changed'):self.verify_synthetic_bytes(changed_bytes)
        missing=copy.deepcopy(args);missing['authoritative_dataset']['files'].pop()
        with self.assertRaises(ValueError):evaluate(**missing)
        changed=copy.deepcopy(args);changed['authoritative_dataset']['files'][0]['sha256']='0'*64
        with self.assertRaises(ValueError):evaluate(**changed)
        wrong=copy.deepcopy(args);wrong['viewer_submission']['attemptId']='40000000-0000-4000-8000-000000000001'
        with self.assertRaises(ValueError):evaluate(**wrong)

    def test_duplicate_basename_holds_before_materialization(self):
        args,_=self.fixture('collision')
        with self.assertRaisesRegex(ValueError,'basename collision'):evaluate(**args)


if __name__=='__main__':unittest.main()
