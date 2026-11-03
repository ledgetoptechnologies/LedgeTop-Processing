import copy
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from processing import viewer_source_v2_reconcile_candidate as c


class ViewerSourceV2Tests(unittest.TestCase):
    def setUp(self):
        self.identity={'environment':'production','generation':1,'jobId':'job-1',
            'manifestSha256':'a'*64,'reservationId':'reservation-1',
            'sourceVersion':'dataset:synthetic-source:'+('a'*64),
            'viewerAttemptId':'123e4567-e89b-42d3-a456-426614174000'}
        self.objects=[{'id':'synthetic-1','byteSize':c.CHUNK+7,'sha256':'b'*64}]
        self.transfer={'protocol':'source-range-v2','viewerAttemptId':self.identity['viewerAttemptId'],
            'reservationId':self.identity['reservationId'],'generation':1,
            'expiresAt':'2035-01-01T00:10:00.000Z','manifestSha256':self.identity['manifestSha256'],
            'sourceVersion':self.identity['sourceVersion'],'objectIds':['synthetic-1'],
            'chunkBytes':c.CHUNK,'requestBudget':2,'byteBudget':2*(c.CHUNK+7),
            'nonce':'synthetic-nonce','attemptBudget':3}
        self.grant={**self.transfer,'expiresAt':'2035-01-01T00:05:00.000Z',
            'jobId':'job-1','environment':'production','token':'z'*43}
        self.now=2051222400
    def contract(self):
        return c.reconcile(binding=self.identity,transfer=self.transfer,grant=self.grant,objects=self.objects,now=self.now)

    def test_attempt_and_grant_bindings_are_exact_and_nonsecret(self):
        contract=self.contract()
        self.assertNotIn('token',str(contract));self.assertNotIn('nonce',str(contract))
        for kind in ('binding','transfer','grant'):
            binding,transfer,grant=copy.deepcopy(self.identity),copy.deepcopy(self.transfer),copy.deepcopy(self.grant)
            {'binding':binding,'transfer':transfer,'grant':grant}[kind]['viewerAttemptId']='123e4567-e89b-42d3-a456-426614174001'
            with self.subTest(kind=kind),self.assertRaises(ValueError):
                c.reconcile(binding=binding,transfer=transfer,grant=grant,objects=self.objects,now=self.now)

    def test_token_renewal_preserves_identity_but_closure_and_expiry_are_strict(self):
        original=self.contract();renewed=copy.deepcopy(self.grant)
        renewed['token']='y'*43;renewed['expiresAt']='2035-01-01T00:07:00.000Z'
        self.assertEqual(c.reconcile(binding=self.identity,transfer=self.transfer,grant=renewed,objects=self.objects,now=self.now),original)
        for key,value in [('expiresAt','2035-01-01T00:11:00.000Z'),('objectIds',['other']),('nonce','changed')]:
            bad=copy.deepcopy(renewed);bad[key]=value
            with self.subTest(key=key),self.assertRaises(ValueError):
                c.reconcile(binding=self.identity,transfer=self.transfer,grant=bad,objects=self.objects,now=self.now)
        bad_transfer=copy.deepcopy(self.transfer);bad_grant=copy.deepcopy(self.grant)
        bad_transfer['generation']=True;bad_grant['generation']=True
        with self.assertRaises(ValueError):
            c.reconcile(binding=self.identity,transfer=bad_transfer,grant=bad_grant,objects=self.objects,now=self.now)

    def test_actual_viewer_c92_canonical_functions_match(self):
        repo=os.environ.get('VIEWER_C92_REPO',r'C:\Projects\viewer-download-retention-20261031')
        if not Path(repo).exists():self.skipTest('Viewer c92 checkout not supplied')
        source=subprocess.run(['git','-C',repo,'show','c92deb5:apps/viewer/server/sourceRangeV2Candidate.js'],
                              capture_output=True,text=True,check=True,timeout=10).stdout
        script=r"""const crypto=require('node:crypto');let s='';process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>{let v=JSON.parse(s);const sha256=x=>crypto.createHash('sha256').update(x).digest('hex');let a=v.source.indexOf('function canonicalIdentity('),b=v.source.indexOf('function expectedChunk(',a);if(a<0||b<0)process.exit(2);eval(v.source.slice(a,b));process.stdout.write(JSON.stringify([canonicalIdentity(v.identity),requestIdFor(canonicalIdentity(v.identity),v.object,v.start,v.length)]));});"""
        # Printable sourceVersion may contain JSON escaping; the real Viewer code
        # must agree on bytes as well as on the resulting request digest.
        for suffix in ('','-quote"-slash\\'):
            identity=dict(self.identity,sourceVersion=self.identity['sourceVersion']+suffix)
            for start,length in ((0,c.CHUNK),(c.CHUNK,7)):
                inp={'source':source,'identity':identity,'object':self.objects[0],'start':start,'length':length}
                proc=subprocess.run(['node','-e',script],input=json.dumps(inp),text=True,capture_output=True,check=True,timeout=10)
                wire,digest=json.loads(proc.stdout)
                self.assertEqual(wire,c.canonical_identity(identity))
                self.assertEqual(digest,c.request_id(identity,self.objects[0],start,length))

    def test_v2_replay_counts_attempts_and_reopen_never_resets_lineage(self):
        contract=self.contract(); rid=c.request_id(self.identity,self.objects[0],0,c.CHUNK)
        with tempfile.TemporaryDirectory() as root:
            path=os.path.join(root,'ledger.sqlite');ledger=c.SyntheticViewerV2Ledger(path)
            a=ledger.record_attempt(contract,object_id='synthetic-1',start=0,length=c.CHUNK,claimed_request_id=rid)
            self.assertEqual(a,{'created':True,'requests':1,'bytes':c.CHUNK,'attempts':1})
            b=ledger.record_attempt(contract,object_id='synthetic-1',start=0,length=c.CHUNK,claimed_request_id=rid)
            self.assertEqual(b,{'created':False,'requests':1,'bytes':c.CHUNK,'attempts':2})
            ledger.close();ledger=c.SyntheticViewerV2Ledger(path)
            changed=copy.deepcopy(contract);changed['identity']['viewerAttemptId']='123e4567-e89b-42d3-a456-426614174001'
            other=c.request_id(changed['identity'],self.objects[0],0,c.CHUNK)
            with self.assertRaisesRegex(ValueError,'requires reconciliation'):
                ledger.record_attempt(changed,object_id='synthetic-1',start=0,length=c.CHUNK,claimed_request_id=other)
            z=ledger.record_attempt(contract,object_id='synthetic-1',start=0,length=c.CHUNK,claimed_request_id=rid)
            self.assertEqual(z['attempts'],3)
            with self.assertRaisesRegex(ValueError,'budget exhausted'):
                ledger.record_attempt(contract,object_id='synthetic-1',start=0,length=c.CHUNK,claimed_request_id=rid)
            ledger.close()

    def test_range_and_ledger_corruption_fail_closed(self):
        contract=self.contract()
        with self.assertRaises(ValueError):c.request_id(self.identity,self.objects[0],1,c.CHUNK)
        rid=c.request_id(self.identity,self.objects[0],0,c.CHUNK)
        with tempfile.TemporaryDirectory() as root:
            path=os.path.join(root,'ledger.sqlite');ledger=c.SyntheticViewerV2Ledger(path)
            with self.assertRaises(ValueError):ledger.record_attempt(contract,object_id='synthetic-1',start=0,length=c.CHUNK,claimed_request_id='0'*64)
            ledger.record_attempt(contract,object_id='synthetic-1',start=0,length=c.CHUNK,claimed_request_id=rid)
            ledger.db.execute('UPDATE identities SET attempts=0');ledger.close()
            with self.assertRaisesRegex(ValueError,'totals corrupt'):c.SyntheticViewerV2Ledger(path)

if __name__=='__main__':unittest.main()
