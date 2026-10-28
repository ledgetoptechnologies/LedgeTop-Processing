from dataclasses import replace
from contextlib import contextmanager
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

from processing.update_registry import DockerRegistry,RegistryPolicy,RegistryChannel,_json
from processing.update_supervisor import (UpdateSupervisor,SupervisorPolicy,SupervisorError,SupervisorHeld,seal_receipt)
from tests import test_update_registry as registry_fixture


COMMON=('operationId','nodeId','engineId','authorityId','candidateId','manifestDigest','configDigest','localImageId','supervisorPolicySha256','hostGeneration')
OLD='sha256:'+'a'*64


class DurableAdapters:
    """Test dependencies persist their own receipts before losing responses."""
    def __init__(self,path,policy,engine):
        self.path,self.policy,self.engine=Path(path),policy,engine
        self.faults={};self.callback=None
        with self.db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS receipts(kind TEXT,request TEXT,value TEXT,PRIMARY KEY(kind,request))')
            db.execute('CREATE TABLE IF NOT EXISTS calls(kind TEXT)')
            db.execute('CREATE TABLE IF NOT EXISTS host(id INTEGER PRIMARY KEY,image TEXT)')
            db.execute('INSERT OR IGNORE INTO host VALUES(1,?)',(policy.current_image_id,))

    @contextmanager
    def db(self):
        db=sqlite3.connect(self.path)
        try:
            with db:yield db
        finally:db.close()

    def count(self,kind):
        with self.db() as db:return db.execute('SELECT COUNT(*) FROM calls WHERE kind=?',(kind,)).fetchone()[0]

    def lookup(self,kind,request):
        with self.db() as db:row=db.execute('SELECT value FROM receipts WHERE kind=? AND request=?',(kind,request['requestSha256'])).fetchone()
        return json.loads(row[0]) if row else None

    def mutate(self,kind,request):
        with self.db() as db:db.execute('INSERT INTO calls VALUES(?)',(kind,))
        fault=self.faults.get(kind)
        if fault=='before':raise OSError('private credentials must not escape')
        value=dict(schemaVersion=1,requestSha256=request['requestSha256'],**{key:request[key] for key in COMMON})
        if kind=='qualification':
            value.update(qualifierId=request['qualifierId'],qualificationPolicySha256=request['qualificationPolicySha256'],
                passed=True,engineVersion='fixture-1.0',evidenceSha256='e'*64)
            field='qualificationSha256'
        elif kind=='schedule':
            value.update(planId=str(uuid.uuid4()),previousImageId=request['previousImageId'],
                targetImageId=request['localImageId'],qualificationSha256=request['qualificationSha256'])
            field='planSha256'
        else:
            value.update(planId=request['plan']['planId'],planSha256=request['plan']['planSha256'],
                previousImageId=request['previousImageId'],targetImageId=request['localImageId'],
                qualificationSha256=request['qualificationSha256'],imageId=request['localImageId'],
                healthy=True,hostPinDurable=True,ingressOpen=True,coordinatorFinished=True)
            field='completionSha256'
        if fault=='mismatch':value['authorityId']=str(uuid.uuid4())
        if fault=='failed_qualification':value['passed']=False
        if fault=='not_finalized':value['coordinatorFinished']=False
        value=seal_receipt(value,field)
        with self.db() as db:
            db.execute('INSERT OR REPLACE INTO receipts VALUES(?,?,?)',(kind,request['requestSha256'],_json(value)))
            if kind=='run':db.execute('UPDATE host SET image=? WHERE id=1',(request['localImageId'],))
        if self.callback:self.callback(kind,request)
        if fault=='after':raise OSError('private response lost')
        return value

    def inspect_host(self):
        with self.db() as db:image=db.execute('SELECT image FROM host WHERE id=1').fetchone()[0]
        return dict(nodeId=self.policy.node_id,authorityId=self.policy.authority_id,engineId=self.engine,imageId=image)


class FixtureQualifier:
    def __init__(self,backend):
        self.backend=backend
        self.binding=dict(qualifierId=backend.policy.qualifier_id,policySha256=backend.policy.qualification_policy_sha256)
    def qualify(self,request):return self.backend.mutate('qualification',request)
    def lookup(self,request):return self.backend.lookup('qualification',request)


class FixtureMaintenance:
    def __init__(self,backend):
        self.backend=backend
        self.binding=dict(adapterId=backend.policy.maintenance_adapter_id,authorityId=backend.policy.authority_id,
            nodeId=backend.policy.node_id,engineId=backend.engine)
    def inspect_host(self):return self.backend.inspect_host()
    def schedule(self,request):return self.backend.mutate('schedule',request)
    def lookup_schedule(self,request):return self.backend.lookup('schedule',request)
    def ready_to_run(self,request):
        ready=self.backend.faults.get('readiness',True)
        if isinstance(ready,Exception):raise ready
        return ready
    def run(self,request):return self.backend.mutate('run',request)
    def lookup_completion(self,request):return self.backend.lookup('run',request)


def components(root,authority):
    root=Path(root);docker=registry_fixture.FakeDocker()
    registry_policy=RegistryPolicy(sys.executable,'unix:///var/run/docker.sock','test-engine',
        (RegistryChannel('test',registry_fixture.REPO,'approved-test'),),4096)
    registry=DockerRegistry(root/'registry.sqlite',registry_policy,capacity_admit=lambda *_:True,runner=docker)
    policy=SupervisorPolicy('main','test',OLD,authority,'synthetic-qualifier','d'*64,'managed-update',4096)
    backend=DurableAdapters(root/'adapters.sqlite',policy,'test-engine')
    supervisor=UpdateSupervisor(root/'supervisor.sqlite',policy,registry=registry,
        qualifier=FixtureQualifier(backend),maintenance=FixtureMaintenance(backend))
    return supervisor,backend,docker


class SupervisorTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.authority=str(uuid.uuid4())
        self.supervisor,self.backend,self.docker=components(self.root,self.authority)
        self.supervisor.__enter__()
        self.addCleanup(lambda:self.supervisor.close() if self.supervisor._lease is not None else None)

    def restart(self):
        self.supervisor.close()
        self.supervisor,self.backend,new_docker=components(self.root,self.authority)
        self.supervisor.registry.runner=self.docker
        self.supervisor.__enter__()

    def discover(self):return self.supervisor.discover()['operationId']

    def through(self,identifier,state):
        while self.supervisor.status(identifier)['state']!=state:self.supervisor.advance(identifier)

    def test_actual_registry_staging_qualification_schedule_run_order_and_restart(self):
        identifier=self.discover()
        states=[]
        for _ in range(4):states.append(self.supervisor.advance(identifier)['state'])
        self.assertEqual(states,['staged','qualified','scheduled','completed'])
        self.assertEqual([self.backend.count(kind) for kind in ('qualification','schedule','run')],[1,1,1])
        with self.supervisor._db() as db:self.assertEqual(db.execute('PRAGMA synchronous').fetchone()[0],2)
        result=self.supervisor.status(identifier)
        self.assertTrue(result['completion']['coordinatorFinished'])
        self.assertEqual(self.supervisor._current(),registry_fixture.CONFIG)
        self.restart()
        self.docker.first=self.docker.first[:1];self.docker.tags=0
        self.assertEqual(self.supervisor.advance(identifier),result)
        self.assertIsNone(self.supervisor.discover())
        self.assertEqual([self.backend.count(kind) for kind in ('qualification','schedule','run')],[1,1,1])
        self.assertEqual(len([call for call in self.docker.calls if call[0]=='pull']),1)

    def test_future_window_or_busy_host_waits_without_run_intent_across_restart(self):
        identifier=self.discover();self.through(identifier,'scheduled')
        self.backend.faults['readiness']=False
        for _ in range(2):
            self.assertEqual(self.supervisor.advance(identifier)['state'],'scheduled')
            self.assertIsNone(self.supervisor._row(identifier)['run_request'])
        self.assertEqual(self.backend.count('run'),0)
        self.restart();self.backend.faults['readiness']=False
        self.assertEqual(self.supervisor.advance(identifier)['state'],'scheduled')
        self.backend.faults['readiness']=True
        self.assertEqual(self.supervisor.advance(identifier)['state'],'completed')
        self.assertEqual(self.backend.count('run'),1)

    def test_readiness_error_or_non_boolean_never_issues_run_intent(self):
        identifier=self.discover();self.through(identifier,'scheduled')
        for ready in (OSError('private details'),None,1,{'ready':True}):
            self.backend.faults['readiness']=ready
            with self.subTest(ready=ready),self.assertRaises(SupervisorHeld):
                self.supervisor.advance(identifier)
            self.assertEqual(self.supervisor.status(identifier)['state'],'scheduled')
            self.assertIsNone(self.supervisor._row(identifier)['run_request'])
        self.assertEqual(self.backend.count('run'),0)

    def test_host_changed_during_readiness_is_rechecked_before_run_intent(self):
        identifier=self.discover();self.through(identifier,'scheduled')
        def changed(request):
            with self.backend.db() as db:
                db.execute('UPDATE host SET image=?',('sha256:'+'f'*64,))
            return True
        with patch.object(self.supervisor.maintenance,'ready_to_run',changed):
            with self.assertRaisesRegex(SupervisorHeld,'host_identity_or_image_changed'):
                self.supervisor.advance(identifier)
        self.assertIsNone(self.supervisor._row(identifier)['run_request'])
        self.assertEqual(self.backend.count('run'),0)

    def test_lost_qualification_receipt_readonly_lookup_no_reexecution(self):
        identifier=self.discover();self.supervisor.advance(identifier)
        self.backend.faults['qualification']='after'
        with self.assertRaisesRegex(SupervisorHeld,'qualification_outcome'):self.supervisor.advance(identifier)
        self.assertEqual(self.backend.count('schedule'),0)
        self.restart()
        with self.assertRaises(SupervisorHeld):self.supervisor.advance(identifier)
        self.assertEqual(self.supervisor.reconcile(identifier)['state'],'qualified')
        self.through(identifier,'completed')
        self.assertEqual(self.backend.count('qualification'),1)

    def test_unknown_qualification_without_receipt_stays_held_and_never_schedules(self):
        identifier=self.discover();self.supervisor.advance(identifier)
        self.backend.faults['qualification']='before'
        with self.assertRaises(SupervisorHeld) as error:self.supervisor.advance(identifier)
        self.assertNotIn('credentials',str(error.exception))
        self.restart()
        self.docker.selected=registry_fixture.entry(config='sha256:'+'f'*64)
        self.docker.first=[self.docker.selected];self.docker.tags=0
        before=len(self.docker.calls)
        for _ in range(2):
            with self.assertRaises(SupervisorHeld):self.supervisor.reconcile(identifier)
            with self.assertRaises(SupervisorHeld):self.supervisor.discover()
        self.assertEqual(self.backend.count('qualification'),1)
        self.assertEqual(self.backend.count('schedule'),0)
        self.assertEqual(len(self.docker.calls),before)

    def test_wrong_or_failed_qualification_cannot_schedule(self):
        for fault in ('mismatch','failed_qualification'):
            with self.subTest(fault=fault):
                root=self.root/fault;root.mkdir(mode=0o700)
                supervisor,backend,_=components(root,self.authority)
                with supervisor:
                    identifier=supervisor.discover()['operationId'];supervisor.advance(identifier)
                    backend.faults['qualification']=fault
                    with self.assertRaises(SupervisorHeld):supervisor.advance(identifier)
                    with self.assertRaises(SupervisorHeld):supervisor.reconcile(identifier)
                    self.assertEqual(backend.count('schedule'),0)

    def test_lost_schedule_uses_exact_owned_receipt_lookup_without_second_post(self):
        identifier=self.discover();self.through(identifier,'qualified')
        self.backend.faults['schedule']='after'
        with self.assertRaises(SupervisorHeld):self.supervisor.advance(identifier)
        self.restart()
        self.assertEqual(self.supervisor.reconcile(identifier)['state'],'scheduled')
        self.supervisor.advance(identifier)
        self.assertEqual(self.backend.count('schedule'),1)
        self.assertEqual(self.backend.count('run'),1)

    def test_peer_plan_or_missing_schedule_proof_never_adopted(self):
        identifier=self.discover();self.through(identifier,'qualified')
        self.backend.faults['schedule']='before'
        with self.assertRaises(SupervisorHeld):self.supervisor.advance(identifier)
        self.restart()
        with self.assertRaises(SupervisorHeld):self.supervisor.reconcile(identifier)
        row=self.supervisor._row(identifier);request=json.loads(row['schedule_request'])
        peer=dict(request,operationId='f'*64)
        peer_receipt=self.backend.mutate('schedule',peer)
        with patch.object(self.supervisor.maintenance,'lookup_schedule',return_value=peer_receipt):
            with self.assertRaises(SupervisorHeld):self.supervisor.reconcile(identifier)
        self.assertEqual(self.backend.count('run'),0)

    def test_lost_run_completion_restarts_inspection_only_with_exact_plan(self):
        identifier=self.discover();self.through(identifier,'scheduled')
        self.backend.faults['run']='after'
        with self.assertRaises(SupervisorHeld):self.supervisor.advance(identifier)
        old=self.supervisor._current();self.assertEqual(old,OLD)
        self.restart()
        with self.assertRaises(SupervisorHeld):self.supervisor.advance(identifier)
        result=self.supervisor.reconcile(identifier)
        self.assertEqual(result['state'],'completed')
        self.assertEqual(self.backend.count('run'),1)
        self.assertEqual(self.supervisor._current(),registry_fixture.CONFIG)

    def test_unfinalized_or_unknown_run_stays_held_without_second_run(self):
        identifier=self.discover();self.through(identifier,'scheduled')
        self.backend.faults['run']='not_finalized'
        with self.assertRaises(SupervisorHeld):self.supervisor.advance(identifier)
        self.restart()
        with self.assertRaises(SupervisorHeld):self.supervisor.reconcile(identifier)
        self.assertEqual(self.backend.count('run'),1)
        self.assertEqual(self.supervisor._current(),OLD)

    def test_full_intent_precedes_mutating_adapter_and_commit_fault_never_calls_it(self):
        identifier=self.discover();self.supervisor.advance(identifier)
        original=self.supervisor._commit
        def fail_intent(db):
            state=db.execute('SELECT state FROM supervisor_operations').fetchone()[0]
            if state=='qualification_intent':raise OSError('before commit')
            original(db)
        with patch.object(self.supervisor,'_commit',side_effect=fail_intent):
            with self.assertRaises(OSError):self.supervisor.advance(identifier)
        self.assertEqual(self.backend.count('qualification'),0)
        self.assertEqual(self.supervisor.status(identifier)['state'],'staged')
        def prove(kind,request):
            with self.supervisor._db() as db:
                self.assertEqual(db.execute('SELECT state FROM supervisor_operations').fetchone()[0],kind+'_intent')
        self.backend.callback=prove
        self.supervisor.advance(identifier)

    def test_lost_local_checkpoint_commit_reconciles_without_qualifier_replay(self):
        identifier=self.discover();self.supervisor.advance(identifier)
        original=self.supervisor._commit
        def lost(db):
            state=db.execute('SELECT state FROM supervisor_operations').fetchone()[0]
            original(db)
            if state=='qualified':raise OSError('lost commit ack')
        with patch.object(self.supervisor,'_commit',side_effect=lost):
            with self.assertRaises(SupervisorHeld):self.supervisor.advance(identifier)
        self.restart();self.assertEqual(self.supervisor.status(identifier)['state'],'qualified')
        self.through(identifier,'completed')
        self.assertEqual(self.backend.count('qualification'),1)

    def test_reentrant_mutation_and_close_refuse_while_original_adapter_running(self):
        identifier=self.discover();self.supervisor.advance(identifier)
        def reenter(kind,request):
            for action in (lambda:self.supervisor.advance(identifier),lambda:self.supervisor.reconcile(identifier),
                           self.supervisor.discover,self.supervisor.close):
                with self.assertRaisesRegex(SupervisorHeld,'operation_in_progress'):action()
            other,_,_=components(self.root,self.authority)
            with self.assertRaises(SupervisorError):other.__enter__()
        self.backend.callback=reenter
        self.through(identifier,'completed')
        self.assertIsNotNone(self.supervisor._lease)

    def test_persisted_qualification_tampering_blocks_schedule(self):
        identifier=self.discover();self.through(identifier,'qualified')
        with self.supervisor._db() as db:
            receipt=json.loads(db.execute('SELECT qualification_receipt FROM supervisor_operations').fetchone()[0])
            receipt['passed']=False
            db.execute('UPDATE supervisor_operations SET qualification_receipt=?',(_json(receipt),))
        with self.assertRaisesRegex(SupervisorHeld,'durable_update_evidence_changed'):self.supervisor.advance(identifier)
        self.assertEqual(self.backend.count('schedule'),0)

    def test_adapter_host_policy_and_lock_inode_changes_fail_closed(self):
        identifier=self.discover()
        self.supervisor.maintenance.binding['authorityId']=str(uuid.uuid4())
        with self.assertRaises(SupervisorError):self.supervisor.advance(identifier)
        self.supervisor.maintenance.binding['authorityId']=self.authority
        with self.backend.db() as db:db.execute('UPDATE host SET image=?',('sha256:'+'f'*64,))
        with self.assertRaises(SupervisorHeld):self.supervisor.advance(identifier)
        with self.backend.db() as db:db.execute('UPDATE host SET image=?',(OLD,))
        self.supervisor.close()
        with self.assertRaises(SupervisorError):
            UpdateSupervisor(self.supervisor.path,replace(self.supervisor.policy,qualifier_id='different'),
                registry=self.supervisor.registry,qualifier=FixtureQualifier(self.backend),maintenance=FixtureMaintenance(self.backend))
        new_policy=replace(self.supervisor.policy,current_image_id='sha256:'+'f'*64)
        changed=UpdateSupervisor(self.supervisor.path,new_policy,registry=self.supervisor.registry,
            qualifier=FixtureQualifier(self.backend),maintenance=FixtureMaintenance(self.backend))
        with self.assertRaises(SupervisorError):changed.__enter__()

    def test_replaced_lease_inode_cannot_be_adopted_after_restart(self):
        self.supervisor.close()
        self.supervisor.lock_path.rename(self.root/'old-supervisor.lock')
        fd=os.open(self.supervisor.lock_path,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600);os.close(fd)
        other,_,_=components(self.root,self.authority)
        with self.assertRaises(SupervisorError):other.__enter__()

    @unittest.skipUnless(os.name=='posix','forked owner process guard')
    def test_fork_child_cannot_use_or_close_parent_owner(self):
        identifier=self.discover();reader,writer=os.pipe()
        with self.supervisor._mutex:
            pid=os.fork()
            if pid==0:
                os.close(reader)
                try:
                    refused=0
                    for action in (self.supervisor.check,self.supervisor.close,lambda:self.supervisor.advance(identifier)):
                        try:action()
                        except SupervisorError:refused+=1
                    os.write(writer,str(refused).encode())
                finally:os._exit(0)
        os.close(writer);response=os.read(reader,10);os.close(reader);os.waitpid(pid,0)
        self.assertEqual(response,b'3');self.supervisor.check()
        other,_,_=components(self.root,self.authority)
        with self.assertRaises(SupervisorError):other.__enter__()

    @unittest.skipUnless(os.name=='posix','POSIX symlink and hardlink lease checks')
    def test_hardlinked_or_symlink_lease_refused_without_new_operations(self):
        self.supervisor.close()
        os.link(self.supervisor.lock_path,self.root/'duplicate.lock')
        other,_,_=components(self.root,self.authority)
        with self.assertRaises(SupervisorError):other.__enter__()
        # Separate synthetic root proves symlink rejection before any journal.
        nested=self.root/'symlink';nested.mkdir(mode=0o700)
        owner,_,_=components(nested,self.authority)
        owner.lock_path.symlink_to(self.supervisor.lock_path)
        with self.assertRaises(SupervisorError):owner.__enter__()
        self.assertFalse(owner.path.exists())

    def test_repeated_adoption_a_b_c_a_b_uses_new_generation_even_for_same_edge(self):
        first=self.discover();self.through(first,'completed')
        config_c='sha256:'+'c'*64
        self.docker.selected=registry_fixture.entry(config=config_c);self.docker.first=[self.docker.selected];self.docker.tags=0
        self.docker.image.update(Id=config_c,RepoDigests=[registry_fixture.REPO+'@'+self.docker.selected['Descriptor']['digest']])
        second=self.discover();self.through(second,'completed')
        self.restart()
        self.docker.selected=registry_fixture.entry(config=OLD);self.docker.first=[self.docker.selected];self.docker.tags=0
        self.docker.image.update(Id=OLD,RepoDigests=[registry_fixture.REPO+'@'+self.docker.selected['Descriptor']['digest']])
        third=self.discover();self.through(third,'completed')
        self.docker.selected=registry_fixture.entry();self.docker.first=[self.docker.selected];self.docker.tags=0
        self.docker.image.update(Id=registry_fixture.CONFIG,RepoDigests=[registry_fixture.REPO+'@'+self.docker.selected['Descriptor']['digest']])
        fourth=self.discover();self.assertNotEqual(first,fourth);self.through(fourth,'completed')
        row=self.supervisor._row(fourth);request=json.loads(row['run_request'])
        self.assertEqual(request['previousImageId'],OLD)
        self.assertEqual(request['hostGeneration'],3)
        self.assertEqual(self.backend.count('qualification'),4)
        self.assertEqual(self.backend.count('run'),4)
        self.assertEqual(self.supervisor._current_state(),(registry_fixture.CONFIG,4))
        # Immutable B image was staged only once; C and A each stage once.
        self.assertEqual(len([call for call in self.docker.calls if call[0]=='pull']),3)

    def test_containerd_manifest_local_id_flows_through_all_bound_receipts(self):
        descriptor=self.docker.selected['Descriptor']
        local=descriptor['digest']
        self.docker.image.update(Id=local,Descriptor={key:descriptor[key] for key in ('mediaType','digest','size')})
        identifier=self.discover();self.through(identifier,'completed')
        row=self.supervisor._row(identifier)
        metadata=json.loads(row['staged'])
        self.assertEqual((metadata['configDigest'],metadata['imageId'],metadata['imageIdKind']),
            (registry_fixture.CONFIG,local,'manifest'))
        self.assertNotEqual(local,registry_fixture.CONFIG)
        for name in ('qualification_request','qualification_receipt','schedule_request','plan','run_request','completion'):
            value=json.loads(row[name])
            self.assertEqual(value['configDigest'],registry_fixture.CONFIG)
            self.assertEqual(value['localImageId'],local)
            for key in ('targetImageId','imageId'):
                if key in value:self.assertEqual(value[key],local)
        self.assertEqual(self.supervisor._current(),local)
        self.restart()
        self.assertIsNone(self.supervisor.discover())
        self.assertEqual(self.backend.count('run'),1)

    def test_runtime_receipt_cannot_substitute_artifact_config_for_manifest_local_id(self):
        descriptor=self.docker.selected['Descriptor']
        self.docker.image.update(Id=descriptor['digest'],Descriptor={key:descriptor[key] for key in ('mediaType','digest','size')})
        identifier=self.discover();self.through(identifier,'scheduled')
        row=self.supervisor._row(identifier);request=self.supervisor._request(row,'run')
        value=self.backend.mutate('run',request)
        for key in ('localImageId','targetImageId','imageId'):
            changed=dict(value);changed.pop('completionSha256');changed[key]=registry_fixture.CONFIG
            changed=seal_receipt(changed,'completionSha256')
            with self.assertRaises(SupervisorHeld):self.supervisor._receipt(changed,request,'run')

    def test_config_digest_equal_to_current_does_not_skip_verified_manifest_runtime(self):
        # A config ID in current state cannot substitute for a verified cache
        # lookup whose actual runtime ID is the distinct manifest ID.
        with self.supervisor._db() as db:
            db.execute('UPDATE supervisor_current SET image=? WHERE id=1',(registry_fixture.CONFIG,))
        with self.backend.db() as db:db.execute('UPDATE host SET image=? WHERE id=1',(registry_fixture.CONFIG,))
        descriptor=self.docker.selected['Descriptor']
        self.docker.image.update(Id=descriptor['digest'],Descriptor={key:descriptor[key] for key in ('mediaType','digest','size')})
        identifier=self.discover()
        self.assertEqual(self.supervisor._row(identifier)['base_image'],registry_fixture.CONFIG)
        self.through(identifier,'completed')
        self.assertEqual(self.supervisor._current(),descriptor['digest'])
        self.assertEqual(self.backend.count('run'),1)

    def test_old_staged_metadata_without_runtime_identity_is_never_reinterpreted(self):
        identifier=self.discover();self.through(identifier,'staged')
        with self.supervisor._db() as db:
            staged=json.loads(db.execute('SELECT staged FROM supervisor_operations WHERE id=?',(identifier,)).fetchone()[0])
            staged.pop('configDigest');staged.pop('imageIdKind')
            db.execute('UPDATE supervisor_operations SET staged=? WHERE id=?',(_json(staged),identifier))
        self.restart()
        for call in (lambda:self.supervisor.status(identifier),lambda:self.supervisor.advance(identifier)):
            with self.assertRaises(SupervisorHeld):call()
        self.assertEqual(self.backend.count('qualification'),0)

    def test_staging_lost_response_reconciles_actual_registry_without_pull_replay(self):
        identifier=self.discover();self.docker.error=TimeoutError()
        with self.assertRaises(SupervisorHeld):self.supervisor.advance(identifier)
        self.restart();self.docker.error=None
        self.assertEqual(self.supervisor.reconcile(identifier)['state'],'staged')
        self.assertEqual(len([call for call in self.docker.calls if call[0]=='pull']),1)

    @unittest.skipUnless(os.name=='posix','real flock, fork and process death required')
    def test_actual_subprocess_owner_death_releases_lease_but_durable_unknown_stays(self):
        self.supervisor.close()
        ready=self.root/'ready'
        code='''import os,sys,time\nfrom pathlib import Path\nfrom tests.test_update_supervisor import components\ns,b,d=components(sys.argv[1],sys.argv[2])\ns.__enter__()\ni=s.discover()['operationId']\ns.advance(i)\nb.faults['qualification']='before'\ntry:s.advance(i)\nexcept Exception:pass\nPath(sys.argv[3]).write_text(i)\ntime.sleep(30)\n'''
        process=subprocess.Popen([sys.executable,'-B','-c',code,str(self.root),self.authority,str(ready)],stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
        self.addCleanup(lambda:process.kill() if process.poll() is None else None)
        deadline=time.monotonic()+5
        while not ready.exists() and process.poll() is None and time.monotonic()<deadline:time.sleep(.01)
        self.assertTrue(ready.exists(),process.stderr.read().decode() if process.poll() is not None else 'child owner unavailable')
        other,_,_=components(self.root,self.authority)
        with self.assertRaises(SupervisorError):other.__enter__()
        process.kill();process.wait(timeout=3);process.stderr.close()
        self.supervisor,self.backend,_=components(self.root,self.authority);self.supervisor.registry.runner=self.docker
        self.supervisor.__enter__();identifier=ready.read_text()
        with self.assertRaises(SupervisorHeld):self.supervisor.advance(identifier)
        with self.assertRaises(SupervisorHeld):self.supervisor.reconcile(identifier)
        self.assertEqual(self.backend.count('qualification'),1)


if __name__=='__main__':unittest.main()
