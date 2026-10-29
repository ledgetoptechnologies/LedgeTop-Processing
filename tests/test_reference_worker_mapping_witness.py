"""Prospective cold-controller mapping; real POSIX leases/TLS/SQLite, fake Docker only.

This does not reconcile uncertain backend execution or stale updater.lock.
"""
from dataclasses import asdict
import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import unittest
from unittest.mock import patch

from ops import reference_worker_maintenance as m
from ops import reference_worker_indexed_maintenance as indexed
from ops import bootstrap_worker_input_accounting as bootstrap
from ops import node_maintenance as managed
from ops.reference_worker_daemon import DaemonStateLock
from ops.node_ingress import save
from processing.node_gate import NodeGate
from tests import test_reference_worker_maintenance as fixture


@unittest.skipUnless(os.name=='posix','actual POSIX lease/SQLite/source qualification')
class MappingWitnessTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        fixture.WorkerMaintenanceTests.setUpClass()
        cls.addClassCleanup(fixture.WorkerMaintenanceTests.doClassCleanups)

    def setUp(self):
        self.f=fixture.WorkerMaintenanceTests();original=self.f.start_worker
        def start():
            self.f.config['inputCopyAccounting']={'schemaVersion':2,'fetchJournal':{'schemaVersion':1,'policyId':'indexed-worker-fetch-v1'}}
            if not self.f.worker_state.exists():
                save(self.f.config_path,self.f.config)
                bootstrap.bootstrap_input_copy_accounting(self.f.config)
            original()
        self.f.start_worker=start;self.addCleanup(self.f.doCleanups);self.f.setUp()

    def prepare(self):
        # Definite successful backend update, before the worker rebind/start.
        with patch.object(m.WorkerMaintenance,'after_backend',side_effect=RuntimeError('qualified synthetic checkpoint')):
            with self.assertRaisesRegex(RuntimeError,'qualified synthetic checkpoint'):self.f.run_update()
        self.folder=next(self.f.managed.glob('op-*'));self.state=m.read_json(self.folder/'managed-update.json')
        self.assertEqual(self.state['worker']['phase'],'quiesced')
        self.assertEqual(self.f.events,['stop','backend']);self.assertFalse(self.f.worker_running)
        self.assertEqual(m.read_json(self.folder/'update.json')['status'],'succeeded')
        indexed._PROOFS.clear();indexed._CONTEXTS.clear()
        return self.state

    def controller(self,folder=None):
        c=m.WorkerMaintenance(self.f.managed,self.f.policy,self.f.api,operator=self.f.operator,
            owned_schedule_request_id=self.f.owned_schedule_request_id,operation_receipt=folder or self.folder,runner=self.f.runner)
        self.addCleanup(c.close);return c

    def bytes(self):
        return {str(p.relative_to(self.f.worker_state)):hashlib.sha256(p.read_bytes()).hexdigest()
            for p in self.f.worker_state.rglob('*') if p.is_file()}

    def held(self,state=None,folder=None):
        before=self.bytes();events=list(self.f.events);c=self.controller(folder)
        try:
            with self.assertRaises((ValueError,OSError,KeyError)):c._stopped_owner(state or self.state)
        finally:c.close()
        self.assertEqual(self.bytes(),before);self.assertEqual(self.f.events,events)

    def test_fresh_controller_revalidates_full_history_without_writes(self):
        state=self.prepare();before=self.bytes();c=self.controller();c._stopped_owner(state)
        self.assertEqual(self.bytes(),before);self.assertEqual(self.f.events,['stop','backend'])
        self.assertIsNotNone(c._indexed_maintenance_context)

    def test_fresh_controller_owned_continuation_completes_without_replacement_replay(self):
        state=self.prepare()
        result=managed.run(self.f.managed,self.f.api,self.f.operator,self.folder,0,worker_aware=True,
            owned_schedule_request_id=self.f.owned_schedule_request_id,expected_plan_id=state['planId'],expected_target=fixture.NEW)
        self.assertEqual(result['status'],'succeeded')
        self.assertEqual(self.f.events,['stop','backend','start','open'])

    def test_genuine_new_process_revalidates_same_stopped_state(self):
        self.prepare();before=self.bytes()
        payload=dict(root=str(self.f.managed),policy=asdict(self.f.policy),receipt=str(self.folder),state=self.state,
            api=self.f.api.origin,token=self.f.api.token,operator=self.f.operator.token,
            request=self.f.owned_schedule_request_id,inspection=self.f.worker_inspect(),gate=str(self.f.gate_root))
        code=r'''import json,sys,subprocess,os
from pathlib import Path
from unittest.mock import patch
from ops import reference_worker_maintenance as m
from processing.odx_smoke_worker import Client
v=json.load(sys.stdin);v['policy']['legacy_maintenance_paths']=tuple(v['policy']['legacy_maintenance_paths'])
def runner(argv,**kwargs):
 a=argv[3:]
 if a[0]=='info':out=v['policy']['engine_id']
 elif a[0]=='inspect':out=json.dumps(v['inspection'])
 else:raise AssertionError('no mutation/exec allowed in cold proof')
 return subprocess.CompletedProcess(argv,0,(out+'\n').encode(),b'')
with patch('processing.node_gate.canonical_gate_path',return_value=Path(v['gate'])):
 c=m.WorkerMaintenance(v['root'],m.WorkerPolicy(**v['policy']),Client(v['api'],v['token']),operator=Client(v['api'],v['operator']),owned_schedule_request_id=v['request'],operation_receipt=v['receipt'],runner=runner)
 try:c._stopped_owner(v['state']);print(json.dumps(dict(pid=os.getpid(),validated=True)))
 finally:c.close()
'''
        child=subprocess.run([sys.executable,'-B','-c',code],input=json.dumps(payload),text=True,capture_output=True,timeout=30)
        self.assertEqual(child.returncode,0,child.stderr)
        result=json.loads(child.stdout);self.assertTrue(result['validated']);self.assertNotEqual(result['pid'],os.getpid())
        self.assertEqual(self.bytes(),before);self.assertEqual(self.f.events,['stop','backend'])

    def test_legacy_missing_witness_stays_held(self):
        self.prepare();del self.state['worker']['mappingWitness'];save(self.folder/'managed-update.json',self.state);self.held()

    def test_closed_typed_witness_and_all_pair_facts_are_bound(self):
        self.prepare();original=copy.deepcopy(self.state)
        changes=[lambda w:w.update(schemaVersion=True),lambda w:w.update(foreign=True),
            lambda w:w.update(planId='00000000-0000-4000-8000-000000000001'),
            lambda w:w['facts'][0].__setitem__(0,True),lambda w:w['pairs'][2].__setitem__(1,'/foreign')]
        changes += [lambda w,i=i:w['facts'][i].__setitem__(1,w['facts'][i][1]+1) for i in range(8)]
        for change in changes:
            with self.subTest(change=changes.index(change)):
                self.state=copy.deepcopy(original);change(self.state['worker']['mappingWitness'])
                save(self.folder/'managed-update.json',self.state);self.held()

    def test_copied_receipt_under_new_path_or_inode_is_not_authority(self):
        self.prepare();other=self.f.root/'copied-operation';shutil.copytree(self.folder,other);other.chmod(0o700)
        self.held(folder=other)
        held=self.folder.with_name(self.folder.name+'-retained');self.folder.rename(held);self.folder.mkdir(mode=0o700)
        for p in held.iterdir():
            if p.is_file():shutil.copyfile(p,self.folder/p.name);(self.folder/p.name).chmod(0o600)
        self.held()

    def test_same_config_bytes_new_inode_is_not_owned_rebind(self):
        self.prepare();raw=self.f.config_path.read_bytes();other=self.f.config_path.with_name('foreign-config.json')
        other.write_bytes(raw);other.chmod(0o600);os.replace(other,self.f.config_path);self.held()

    def test_changed_scratch_root_inode_stays_held(self):
        self.prepare();old=self.f.scratch.with_name('old-scratch');self.f.scratch.rename(old);self.f.scratch.mkdir(mode=0o700);self.held()

    def test_changed_engine_or_container_identity_stays_held(self):
        self.prepare();original=self.f.runner
        for kind in ('engine','container'):
            def runner(argv,**kwargs):
                result=original(argv,**kwargs)
                if argv[0]=='/usr/bin/docker' and argv[3]=='info' and kind=='engine':return subprocess.CompletedProcess(argv,0,b'foreign-engine\n',b'')
                if argv[0]=='/usr/bin/docker' and argv[3]=='inspect' and argv[-1]==fixture.WORKER_CID and kind=='container':
                    obj=json.loads(result.stdout);obj['id']='0'*64;return subprocess.CompletedProcess(argv,0,json.dumps(obj).encode(),b'')
                return result
            self.f.runner=runner
            with self.subTest(kind=kind):self.held()
        self.f.runner=original

    def test_lock_contention_does_not_bypass_daemon_owner(self):
        self.prepare()
        with DaemonStateLock(self.f.worker_state):self.held()

    def test_uncertain_backend_execution_is_not_reconciled_by_witness(self):
        self.prepare();u=m.read_json(self.folder/'update.json');u.update(stage='replace_pending');u.pop('status');save(self.folder/'update.json',u);self.held()

    def test_unknown_start_stays_held_and_never_issues_second_start(self):
        self.prepare();self.state['worker'].update(phase='start_pending',startIssued=True);save(self.folder/'managed-update.json',self.state)
        c=self.controller();before=self.bytes()
        with NodeGate('test-engine') as gate:
            with self.assertRaisesRegex(m.WorkerMaintenanceHeld,'worker_start_outcome_unresolved'):
                c.after_backend(self.state,self.folder,gate,self.f.adapter)
        self.assertEqual(self.f.events,['stop','backend']);self.assertEqual(self.bytes(),before)

    def test_oversize_and_duplicate_outer_receipts_refuse_before_engine_mutation(self):
        self.prepare();original=(self.folder/'managed-update.json').read_bytes()
        for raw in (b'{"stage":"first","stage":"second"}',b' '*65537):
            (self.folder/'managed-update.json').write_bytes(raw);events=list(self.f.events);before=self.bytes()
            with self.assertRaises(ValueError):
                managed.run(self.f.managed,self.f.api,self.f.operator,self.folder,0,worker_aware=True,
                    owned_schedule_request_id=self.f.owned_schedule_request_id,expected_plan_id=self.state['planId'],expected_target=fixture.NEW)
            self.assertEqual(self.f.events,events);self.assertEqual(self.bytes(),before)
        (self.folder/'managed-update.json').write_bytes(original)

    def test_update_receipt_replacement_during_authority_read_is_held(self):
        self.prepare();before=self.bytes();original=indexed._authority
        def replaced(*args,**kwargs):
            result=original(*args,**kwargs);save(self.folder/'update.json',m.read_json(self.folder/'update.json'));return result
        with patch.object(indexed,'_authority',replaced):self.held()
        self.assertEqual(self.bytes(),before)

    def test_capture_fsync_interruptions_never_issue_stop(self):
        original=m.save
        for after in (False,True):
            # One fresh operation is enough per subcase; a failed reserved
            # operation is never reused to manufacture a second pass.
            if after:
                break
            def failed(path,value,*args,**kwargs):
                if path.name=='managed-update.json' and value.get('worker',{}).get('phase')=='stop_pending':
                    raise OSError('synthetic before witness fsync')
                return original(path,value,*args,**kwargs)
            with patch.object(m,'save',failed):
                with self.assertRaisesRegex(OSError,'before witness fsync'):self.f.run_update()
            self.assertEqual(self.f.events,[]);self.assertTrue(self.f.worker_running)
            folder=next(self.f.managed.glob('op-*'));state=m.read_json(folder/'managed-update.json')
            self.assertNotIn('worker',state)

    def test_after_fsync_before_stop_holds_with_witness_but_no_stop(self):
        original=m.save
        def failed(path,value,*args,**kwargs):
            result=original(path,value,*args,**kwargs)
            if path.name=='managed-update.json' and value.get('worker',{}).get('phase')=='stop_pending':raise OSError('synthetic after witness fsync')
            return result
        with patch.object(m,'save',failed):
            with self.assertRaisesRegex(OSError,'after witness fsync'):self.f.run_update()
        self.assertEqual(self.f.events,[]);self.assertTrue(self.f.worker_running)
        folder=next(self.f.managed.glob('op-*'));state=m.read_json(folder/'managed-update.json')
        self.assertIn('mappingWitness',state['worker']);self.assertEqual(state['worker']['phase'],'stop_pending')

    def test_lost_stop_reply_retains_witness_without_backend_or_restart(self):
        self.f.loss='stop'
        with self.assertRaises(OSError):self.f.run_update()
        self.folder=next(self.f.managed.glob('op-*'));self.state=m.read_json(self.folder/'managed-update.json')
        self.assertIn('mappingWitness',self.state['worker']);self.assertEqual(self.f.events,['stop']);self.assertFalse(self.f.worker_running)
        indexed._PROOFS.clear();indexed._CONTEXTS.clear();self.held()

    def test_owned_atomic_rebind_pending_and_rebound_resume(self):
        self.prepare();c=self.controller();original=c.phase
        def interrupted(state,receipt,phase,**fields):
            if phase=='publication_pending':raise OSError('synthetic after owned rename')
            return original(state,receipt,phase,**fields)
        with patch.object(c,'phase',interrupted),NodeGate('test-engine') as gate:
            with self.assertRaisesRegex(OSError,'after owned rename'):c.rebind(self.state,self.folder,'synthetic-v2',gate)
        c.close();self.state=m.read_json(self.folder/'managed-update.json');self.assertEqual(self.state['worker']['phase'],'rebind_pending')
        indexed._PROOFS.clear();indexed._CONTEXTS.clear();fresh=self.controller();fresh._stopped_owner(self.state)
        with NodeGate('test-engine') as gate:fresh.rebind(self.state,self.folder,'synthetic-v2',gate)
        self.assertEqual(self.state['worker']['phase'],'rebound');fresh.close()
        indexed._PROOFS.clear();indexed._CONTEXTS.clear();self.controller()._stopped_owner(self.state)
        self.assertEqual(self.f.events,['stop','backend'])


    def test_oversize_and_duplicate_update_receipts_refuse_before_engine_mutation(self):
        self.prepare();path=self.folder/'update.json';original=path.read_bytes()
        for raw in (b'{"nodeId":"cluster","nodeId":"cluster"}',b' '*65537):
            path.write_bytes(raw);events=list(self.f.events);before=self.bytes()
            with self.assertRaises(ValueError):
                managed.run(self.f.managed,self.f.api,self.f.operator,self.folder,0,worker_aware=True,
                    owned_schedule_request_id=self.f.owned_schedule_request_id,expected_plan_id=self.state['planId'],expected_target=fixture.NEW)
            self.assertEqual(self.f.events,events);self.assertEqual(self.bytes(),before)
        path.write_bytes(original)

    def publication_interruption(self,after):
        self.prepare();c=self.controller();original=m.save
        def interrupted(path,value,*args,**kwargs):
            if path==c.publication:
                if after:original(path,value,*args,**kwargs)
                raise OSError('synthetic publication boundary')
            return original(path,value,*args,**kwargs)
        with patch.object(m,'save',interrupted),NodeGate('test-engine') as gate:
            with self.assertRaisesRegex(OSError,'publication boundary'):c.rebind(self.state,self.folder,'synthetic-v2',gate)
        c.close();self.state=m.read_json(self.folder/'managed-update.json')
        self.assertEqual(self.state['worker']['phase'],'publication_pending')
        indexed._PROOFS.clear();indexed._CONTEXTS.clear()

    def test_cold_publication_pending_after_exact_publication_write_resumes(self):
        self.publication_interruption(True);before=self.bytes();c=self.controller();c._stopped_owner(self.state)
        with NodeGate('test-engine') as gate:c.rebind(self.state,self.folder,'synthetic-v2',gate)
        self.assertEqual(self.state['worker']['phase'],'rebound');self.assertEqual(self.bytes(),before)
        self.assertEqual(self.f.events,['stop','backend'])

    def test_publication_pending_with_old_publication_is_held(self):
        self.publication_interruption(False);self.held()

if __name__=='__main__':unittest.main()
