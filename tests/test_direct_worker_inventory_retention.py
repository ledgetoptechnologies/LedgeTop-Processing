import copy
import json
import os
import unittest
import subprocess
import sys
from unittest.mock import patch
from processing import direct_worker_inventory_retention as retention
from processing.scratch_policy import ScratchLedger, PolicyConflict
from tests.test_direct_worker_receiver_v2 import WorkerV2Tests

@unittest.skipIf(os.name=='nt','Linux scratch fixture required')
class InventoryRetentionTests(unittest.TestCase):
    def setUp(self):
        self.f=WorkerV2Tests();self.f.setUp();self.addCleanup(self.f.doCleanups)
        self.args=dict(payload=self.f.payload,item=self.f.item,worker=self.f.worker,
                       verification=self.f.options,coordinator_principal='coord-a')
    def test_replay_restart_and_principal_fence(self):
        first=retention.retain(self.f.ledger,**self.args)
        self.assertFalse(first['sourceCustodyAccepted'])
        self.assertEqual(first,retention.retain(self.f.ledger,**self.args))
        reopened=ScratchLedger(self.f.ledger.path,self.f.root,capacity_probe=lambda _:1000000,clock=lambda:100,safety_floor_bytes=100)
        args={k:v for k,v in self.args.items() if k!='payload'}
        self.assertEqual(retention.recover(reopened,**args),self.f.payload)
        with self.assertRaises(PolicyConflict):retention.recover(reopened,**dict(args,coordinator_principal='coord-b'))
        with self.assertRaises(PolicyConflict):retention.retain(reopened,**dict(self.args,coordinator_principal='coord-b'))
    def test_inventory_count_and_bytes_must_match_signed_manifests(self):
        for field in ('objectCount','inputBytes'):
            item=dict(self.f.item,**{field:self.f.item[field]+1})
            with self.subTest(field=field),self.assertRaises(PolicyConflict):
                retention.retain(self.f.ledger,**dict(self.args,item=item))
        with self.f.ledger._transaction() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM direct_inventory_blobs').fetchone()[0],0)

    def test_quota_and_rollback(self):
        with patch.object(self.f.ledger,'probe',return_value=200000):
            with self.assertRaises(PolicyConflict):retention.retain(self.f.ledger,**self.args)
        with self.f.ledger._transaction() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM direct_inventory_blobs').fetchone()[0],0)
        retention.retain(self.f.ledger,**self.args)
        with patch.object(self.f.ledger,'probe',return_value=300000):
            with self.assertRaises(PolicyConflict):
                self.f.ledger.reserve(job_id='other',generation=1,reservation_id='other',manifest_sha256='a'*64,
                    input_bytes=1,output_budget=1,environment='staging',ttl_seconds=10,worker_ready=True)
    def test_fault_rolls_back_and_process_reopens(self):
        with patch.object(self.f.ledger,'_latch',side_effect=OSError('injected commit failure')):
            with self.assertRaises(OSError):retention.retain(self.f.ledger,**self.args)
        with self.f.ledger._transaction() as db:
            self.assertEqual(db.execute('SELECT count(*) FROM direct_inventory_blobs').fetchone()[0],0)
        retention.retain(self.f.ledger,**self.args)
        code=("from processing.scratch_policy import ScratchLedger; "
              "import sys; x=ScratchLedger(sys.argv[1],sys.argv[2],capacity_probe=lambda _:1000000,"
              "clock=lambda:100,safety_floor_bytes=100); "
              "db=__import__('sqlite3').connect(x.path); print(db.execute('select count(*) from direct_inventory_blobs').fetchone()[0])")
        child=subprocess.run([sys.executable,'-c',code,str(self.f.ledger.path),str(self.f.root)],capture_output=True,text=True,timeout=10)
        self.assertEqual(child.returncode,0,child.stderr);self.assertEqual(child.stdout.strip(),'1')
        args={k:v for k,v in self.args.items() if k!='payload'}
        self.assertEqual(retention.recover(self.f.ledger,**args),self.f.payload)

    @unittest.skipUnless(os.environ.get('LTDS_OLD_SCRATCH_SOURCE'),'old scratch source not supplied')
    def test_actual_previous_scratch_code_refuses_latched_ledger(self):
        retention.retain(self.f.ledger,**self.args)
        import importlib.util
        spec=importlib.util.spec_from_file_location('previous_scratch',os.environ['LTDS_OLD_SCRATCH_SOURCE'])
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        with self.assertRaises(module.PolicyConflict):
            module.ScratchLedger(self.f.ledger.path,self.f.root,capacity_probe=lambda _:1000000,clock=lambda:100,safety_floor_bytes=100)

    def test_recover_signed_inventory_in_new_process(self):
        retention.retain(self.f.ledger,**self.args)
        args={k:v for k,v in self.args.items() if k!='payload'}
        args['verification']=dict(args['verification'],allowed_odm_options=list(args['verification']['allowed_odm_options']))
        script=("import json,sys; from processing.scratch_policy import ScratchLedger; "
            "from processing.direct_worker_inventory_retention import recover; "
            "x=json.load(sys.stdin); x['args']['verification']['allowed_odm_options']=set(x['args']['verification']['allowed_odm_options']); "
            "l=ScratchLedger(x['path'],x['root'],capacity_probe=lambda _:1000000,clock=lambda:100,safety_floor_bytes=100); "
            "print(recover(l,**x['args'])['submissionId'])")
        child=subprocess.run([sys.executable,'-c',script],input=json.dumps(dict(path=str(self.f.ledger.path),root=str(self.f.root),args=args)),
                             capture_output=True,text=True,timeout=10)
        self.assertEqual(child.returncode,0,child.stderr);self.assertEqual(child.stdout.strip(),self.f.payload['submissionId'])

    def test_missing_guard_refused_on_restart(self):
        retention.retain(self.f.ledger,**self.args)
        import sqlite3
        db=sqlite3.connect(self.f.ledger.path);db.execute('DROP TRIGGER direct_inventory_no_delete');db.commit();db.close()
        with self.assertRaises(PolicyConflict):
            ScratchLedger(self.f.ledger.path,self.f.root,capacity_probe=lambda _:1000000,clock=lambda:100,safety_floor_bytes=100)
