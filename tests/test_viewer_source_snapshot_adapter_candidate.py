import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import unittest

from processing import reference_protocol as p
from processing.viewer_source_snapshot_adapter_candidate import evaluate
from tests.test_source_inventory_candidate import SourceInventoryCandidateTests


class ViewerSnapshotAdapterTests(unittest.TestCase):
    def setUp(self):
        self.fixture=SourceInventoryCandidateTests('test_exact_paths_bytes_and_immutable_binding');self.fixture.setUp()
        f=self.fixture
        self.expected=dict(principalId='viewer-prod',environment='production',authorityId='30000000-0000-4000-8000-000000000001',**f.expected)
        self.policy=dict(objectLimit=10000,byteLimit=100000,transferByteBudget=3+len(f.gcp))
        source=[dict(relativePath=x['relativePath'],byteSize=x['byteSize'],sha256=x['sha256'],processingRole=x['processingRole'])
                for x in f.inv['processing']+f.inv['retained']]
        sent=[{k:x[k] for k in ('relativePath','byteSize','sha256')} for x in source[:2]]
        sent.append(dict(relativePath='gcp_list.txt',byteSize=len(f.gcp),sha256=f.inv['generated'][0]['sha256']))
        from processing.viewer_source_snapshot_adapter_candidate import _viewer_input_bytes
        self.sub=dict(attemptId=f.expected['viewerAttemptId'],datasetId='dataset-1',datasetManifestSha256='a'*64,
            sourceFiles=source,sourceFilesSha256=p.digest(source),inputFiles=sent,
            inputManifestSha256=hashlib.sha256(_viewer_input_bytes(sent)).hexdigest(),gcpSnapshotSha256=f.inv['generated'][0]['sha256'])

    def run_adapter(self):
        f=self.fixture
        return evaluate(json.dumps(f.inv,ensure_ascii=False).encode(),expected=self.expected,
            authoritative_dataset=f.authoritative,trusted_selection=f.selection,
            trusted_gcp_evidence=json.dumps(f.evidence).encode(),generated_gcp_bytes=f.gcp,
            viewer_submission=self.sub,source_policy=self.policy)

    def test_source_projection_and_budget_are_held(self):
        wire,digest,hold=self.run_adapter();value=json.loads(wire)
        self.assertEqual((value['objectCount'],value['inputBytes']),(3,3+len(self.fixture.gcp)))
        self.assertEqual(hold,'held_authenticated_source_authority_unavailable')
        self.assertEqual(len(digest),64)
        self.policy['transferByteBudget']=1
        with self.assertRaisesRegex(ValueError,'budget'):self.run_adapter()

    def test_changed_order_role_or_attempt_is_rejected(self):
        self.sub['inputFiles'].reverse()
        with self.assertRaisesRegex(ValueError,'transmitted inventory'):self.run_adapter()
        self.sub['inputFiles'].reverse()
        self.sub['sourceFiles'][0]['processingRole']='administrative';self.sub['sourceFilesSha256']=p.digest(self.sub['sourceFiles'])
        with self.assertRaisesRegex(ValueError,'selection differs'):self.run_adapter()
        self.sub['sourceFiles'][0]['processingRole']='image';self.sub['sourceFilesSha256']=p.digest(self.sub['sourceFiles'])
        self.sub['attemptId']='40000000-0000-4000-8000-000000000001'
        with self.assertRaisesRegex(ValueError,'attempt provenance differs'):self.run_adapter()

    def test_actual_viewer_input_inventory_matches(self):
        source=os.environ.get('VIEWER_PROCESSING_PROVENANCE_JS')
        if not source:self.skipTest('pinned Viewer processingProvenance.js required')
        path=Path(source).resolve(strict=True)
        data=path.read_bytes().replace(b'\r\n',b'\n')
        self.assertEqual(hashlib.sha1(b'blob '+str(len(data)).encode()+b'\0'+data).hexdigest(),'f9adb05b62e1ffec2a90dc06038f77199f0c3436')
        script="const f=require(process.argv[1]).inputInventory;let s='';process.stdin.setEncoding('utf8');process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>{let x=JSON.parse(s);console.log(JSON.stringify(f(x.files,x.gcp)))})"
        value=dict(files=self.sub['sourceFiles'],gcp=dict(content=self.fixture.gcp.decode(),sha256=self.fixture.inv['generated'][0]['sha256']))
        node=subprocess.run(['node','-e',script,str(path)],input=json.dumps(value),text=True,encoding='utf8',capture_output=True,check=True)
        result=json.loads(node.stdout)
        for key in ('sourceFiles','sourceFilesSha256','inputFiles','inputManifestSha256'):
            self.assertEqual(result[key],self.sub[key])
        self.run_adapter()


if __name__=='__main__':unittest.main()
