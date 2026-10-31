"""Real private SQLite/flocks and Registry proofs; no actual Docker or pulls."""
from dataclasses import asdict,replace
from contextlib import closing
import copy
import hashlib
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

from processing import update_capacity as module
from processing.update_registry import DockerRegistry,RegistryPolicy,RegistryChannel,RegistryHeld
from tests.test_update_registry import FakeDocker,REPO


@unittest.skipUnless(os.name=='posix','fixed Linux policy paths')
class CapacityPolicyTests(unittest.TestCase):
    def test_reviewed_floor_reservation_local_endpoint_and_legacy_inventory_are_closed(self):
        policy=module.CapacityPolicy('/usr/bin/docker','unix:///run/docker.sock','fixed-engine','/var/lib/docker')
        self.assertEqual((policy.min_free_bytes,policy.reservation_bytes),(100*module.GiB,64*module.GiB))
        for change in (dict(min_free_bytes=99*module.GiB),dict(reservation_bytes=32*module.GiB),
                dict(docker_endpoint='tcp://engine:2375'),dict(legacy_capacity_journals=('/z','/a')),
                dict(legacy_capacity_journals=('/a','/a'))):
            with self.subTest(change=change),self.assertRaises(module.CapacityError):replace(policy,**change)


@unittest.skipUnless(os.name=='posix','capacity ledger requires Linux UID/flock/statvfs')
class CapacityTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.root.chmod(0o700)
        self.docker_root=self.root/'docker-root';self.docker_root.mkdir(mode=0o700)
        self.policy=module.CapacityPolicy(sys.executable,'unix:///run/docker.sock','test-engine',str(self.docker_root))
        info=self.docker_root.stat()
        self.disk=dict(path=str(self.docker_root),device=info.st_dev,inode=info.st_ino,fsId=123,
            blockSize=4096,totalBytes=500*module.GiB,freeBytes=300*module.GiB)
        self.path_patch=patch.object(module,'canonical_capacity_path',side_effect=lambda engine:self.root/'shared'/hashlib.sha256(engine.encode()).hexdigest())
        self.path_patch.start();self.addCleanup(self.path_patch.stop)
        self.docker=FakeDocker();self.capacity=None;self.registry=None

    def open(self):
        return module.ImageStagingCapacity(self.policy,docker_probe=lambda:dict(engineId=self.policy.engine_id,dockerRootDir=self.policy.docker_root_dir),
            disk_probe=lambda path:dict(self.disk))

    def registry_for(self,admit=None,name='registry.sqlite3'):
        if admit is None:
            if self.capacity is None:self.capacity=self.open()
            admit=self.capacity
        policy=RegistryPolicy(self.policy.docker_executable,self.policy.docker_endpoint,self.policy.engine_id,
            (RegistryChannel('test',REPO,'approved-test'),),self.policy.reservation_bytes)
        return DockerRegistry(self.root/name,policy,capacity_admit=admit,runner=self.docker)

    def candidate(self):
        if self.registry is None:self.registry=self.registry_for()
        return self.registry.discover('test')

    def other(self,candidate):
        value=copy.deepcopy(candidate);value['manifestDigest']='sha256:'+'e'*64;value['manifestDescriptor']['digest']=value['manifestDigest']
        value['configDigest']='sha256:'+'f'*64;value.pop('candidateId');value['candidateId']=module.digest(value)
        return value

    def rows(self,capacity=None):
        capacity=capacity or self.capacity
        with capacity._locked(),capacity._db() as db:return [dict(row) for row in db.execute('SELECT * FROM reservations ORDER BY pull_id')]

    def test_exact_floor_aggregate_and_active_idempotency_share_one_canonical_book(self):
        candidate=self.candidate();other=self.other(candidate)
        self.disk['freeBytes']=164*module.GiB
        self.assertIs(self.capacity(candidate,64*module.GiB),True)
        second=self.open()
        self.assertIs(second(candidate,64*module.GiB),True)
        self.assertIs(second(other,64*module.GiB),False)
        self.assertEqual(len(self.rows()),1)
        self.disk['freeBytes']=228*module.GiB
        self.assertIs(second(other,64*module.GiB),True)
        self.disk['freeBytes']=228*module.GiB-1
        self.assertIs(self.capacity(candidate,64*module.GiB),False)
        self.assertEqual(sum(row['bytes'] for row in self.rows()),128*module.GiB)

    def test_candidate_tag_observation_cannot_duplicate_immutable_reservation(self):
        candidate=self.candidate();self.assertTrue(self.capacity(candidate,64*module.GiB))
        alias=copy.deepcopy(candidate);alias['tag']='new-observation';alias.pop('candidateId');alias['candidateId']=module.digest(alias)
        self.assertTrue(self.capacity(alias,64*module.GiB))
        self.assertEqual(len(self.rows()),1)

    def test_unknown_partial_pull_retains_budget_across_restart_and_cannot_release(self):
        candidate=self.candidate();self.docker.error=TimeoutError('synthetic partial pull')
        with self.assertRaises(RegistryHeld):self.registry.stage(candidate)
        self.capacity=self.open();self.disk['freeBytes']=150*module.GiB
        with self.assertRaises(module.CapacityError):self.capacity.release_staged(candidate,self.registry)
        self.assertEqual(self.rows()[0]['state'],'active')
        self.assertFalse(self.capacity(candidate,64*module.GiB))
        self.assertEqual(len([call for call in self.docker.calls if call[0]=='pull']),1)

    def test_actual_registry_cached_proof_releases_only_transient_budget_once(self):
        candidate=self.candidate();metadata=self.registry.stage(candidate)
        self.assertEqual(self.rows()[0]['state'],'active')
        self.assertTrue(self.capacity.release_staged(candidate,self.registry))
        row=self.rows()[0];self.assertEqual(row['state'],'released')
        self.assertEqual(json.loads(row['release_proof'])['metadata']['imageId'],metadata['imageId'])
        self.capacity=self.open();before=self.capacity.path.read_bytes()
        self.assertTrue(self.capacity.release_staged(candidate,self.registry))
        self.assertEqual(self.capacity.path.read_bytes(),before)
        with self.assertRaises(module.CapacityError):self.capacity(candidate,64*module.GiB)
        self.assertEqual(len([call for call in self.docker.calls if call[0]=='pull']),1)

    def test_cached_reuse_missing_reservation_is_verified_noop_not_manual_import(self):
        self.registry=self.registry_for(lambda *_:True);candidate=self.registry.discover('test');self.registry.stage(candidate)
        self.capacity=self.open()
        self.assertIs(self.capacity.release_staged(candidate,self.registry),False)
        self.assertEqual(self.rows(),[])
        self.docker.image['Id']='sha256:'+'0'*64
        with self.assertRaises(module.CapacityError):self.capacity.release_staged(candidate,self.registry)

    def test_staging_finished_before_release_crash_is_recovered_without_pull(self):
        candidate=self.candidate();self.registry.stage(candidate)
        self.capacity=self.open()
        self.assertTrue(self.capacity.release_staged(candidate,self.registry))
        self.assertEqual(self.rows()[0]['state'],'released')
        self.assertEqual(len([call for call in self.docker.calls if call[0]=='pull']),1)

    def test_new_candidate_observation_reuses_same_immutable_release_proof(self):
        candidate=self.candidate();self.registry.stage(candidate);self.capacity.release_staged(candidate,self.registry)
        self.docker.tags=0
        from tests.test_update_registry import entry
        self.docker.first=[self.docker.selected,entry(config='sha256:'+'e'*64,platform=dict(os='unknown',architecture='unknown'))]
        alias=self.registry.discover('test');self.assertNotEqual(alias['candidateId'],candidate['candidateId'])
        before=self.capacity.path.read_bytes()
        self.assertTrue(self.capacity.release_staged(alias,self.registry))
        self.assertEqual(self.capacity.path.read_bytes(),before)

    def test_lost_reserve_commit_reply_is_idempotent_and_remains_accounted(self):
        candidate=self.candidate();commit=self.capacity._commit
        def lost(db):commit(db);raise OSError('synthetic lost reserve reply')
        with patch.object(self.capacity,'_commit',side_effect=lost):
            with self.assertRaises(OSError):self.capacity(candidate,64*module.GiB)
        self.capacity=self.open();self.assertTrue(self.capacity(candidate,64*module.GiB))
        self.assertEqual(len(self.rows()),1)

    def test_lost_release_commit_reply_preserves_immutable_proof_without_double_release(self):
        candidate=self.candidate();self.registry.stage(candidate);commit=self.capacity._commit
        def lost(db):commit(db);raise OSError('synthetic lost release reply')
        with patch.object(self.capacity,'_commit',side_effect=lost):
            with self.assertRaises(OSError):self.capacity.release_staged(candidate,self.registry)
        self.capacity=self.open();self.assertTrue(self.capacity.release_staged(candidate,self.registry))
        self.assertEqual(self.rows()[0]['state'],'released')

    def test_wrong_engine_root_device_fs_and_probe_exception_never_allow(self):
        candidate=self.candidate()
        for key,value in (('device',self.disk['device']+1),('inode',self.disk['inode']+1),('fsId',124)):
            original=self.disk[key];self.disk[key]=value
            with self.subTest(key=key),self.assertRaises(module.CapacityError):self.capacity(candidate,64*module.GiB)
            self.disk[key]=original
        for probe in (lambda:dict(engineId='wrong',dockerRootDir=self.policy.docker_root_dir),
                lambda:dict(engineId=self.policy.engine_id,dockerRootDir='/other-root')):
            with patch.object(self.capacity,'docker_probe',probe),self.assertRaises(module.CapacityError):self.capacity(candidate,64*module.GiB)
        with patch.object(self.capacity,'disk_probe',side_effect=OSError('private failure')),self.assertRaises(module.CapacityError):
            self.capacity(candidate,64*module.GiB)
        self.assertEqual(self.rows(),[])

    def test_wrong_registry_or_forged_cached_metadata_cannot_free_reservation(self):
        candidate=self.candidate();metadata=self.registry.stage(candidate)
        for key,value in (('engineId','wrong-engine'),('configDigest','sha256:'+'0'*64),('imageId','sha256:'+'0'*64)):
            with patch.object(self.registry,'inspect_staged_artifact',return_value=dict(metadata,**{key:value})),self.assertRaises(module.CapacityError):
                self.capacity.release_staged(candidate,self.registry)
        with self.assertRaises(module.CapacityError):self.capacity.release_staged(candidate,object())
        self.assertEqual(self.rows()[0]['state'],'active')

    def test_foreign_schema_policy_symlink_hardlink_and_replaced_inode_refused(self):
        candidate=self.candidate()
        with self.capacity._locked(),self.capacity._db() as db:db.execute('CREATE TABLE foreign_progress(value TEXT)')
        with self.assertRaises(module.CapacityError):self.capacity(candidate,64*module.GiB)
        with closing(sqlite3.connect(self.capacity.path)) as db,db:db.execute('DROP TABLE foreign_progress')
        self.policy=replace(self.policy,min_free_bytes=101*module.GiB)
        with self.assertRaises(module.CapacityError):self.open()
        self.policy=replace(self.policy,min_free_bytes=100*module.GiB)
        link=self.root/'hardlink';os.link(self.capacity.path,link)
        with self.assertRaises(module.CapacityError):self.capacity(candidate,64*module.GiB)
        link.unlink()
        original=self.capacity.lock_path;retained=original.with_name('retained.lock');original.rename(retained)
        original.symlink_to(retained)
        with self.assertRaises(module.CapacityError):self.capacity(candidate,64*module.GiB)

    def test_real_default_statvfs_and_bounded_info_command_use_only_fixed_root(self):
        snapshot=module.disk_snapshot(self.docker_root)
        self.assertEqual((snapshot['device'],snapshot['inode']),(self.docker_root.stat().st_dev,self.docker_root.stat().st_ino))
        self.assertEqual(snapshot['freeBytes'],os.statvfs(self.docker_root).f_bavail*os.statvfs(self.docker_root).f_frsize)
        def run(command,**kwargs):
            self.assertEqual(command[:5],[self.policy.docker_executable,'--host',self.policy.docker_endpoint,'info','--format'])
            self.assertEqual(kwargs,dict(timeout=10,max_bytes=8192))
            return subprocess.CompletedProcess(command,0,json.dumps(dict(engineId=self.policy.engine_id,dockerRootDir=self.policy.docker_root_dir)).encode(),b'')
        with patch.object(module,'_run',side_effect=run):
            capacity=module.ImageStagingCapacity(self.policy)
            capacity.verify_legacy()

    def test_default_disk_probe_rejects_a_symlinked_docker_root(self):
        path=self.root/'docker-alias';path.symlink_to(self.docker_root,target_is_directory=True)
        with self.assertRaises(module.CapacityError):module.disk_snapshot(path)

    def test_foreign_manual_schema_is_not_silently_imported_even_if_empty(self):
        root=module.canonical_capacity_path(self.policy.engine_id);root.mkdir(parents=True,mode=0o700)
        path=root/'capacity.sqlite3'
        with closing(sqlite3.connect(path)) as db,db:db.execute('CREATE TABLE reservations(candidate TEXT PRIMARY KEY,bytes INTEGER NOT NULL)')
        path.chmod(0o600);before=path.read_bytes()
        with self.assertRaises(module.CapacityError):self.open()
        self.assertEqual(path.read_bytes(),before)
        with closing(sqlite3.connect(path)) as db,db:
            self.assertEqual([row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")],['reservations'])

    def legacy_fixture(self):
        directory=self.root/'old-manual';directory.mkdir(mode=0o700)
        path=directory/'capacity.sqlite'
        self.policy=replace(self.policy,legacy_capacity_journals=(str(path),))
        self.registry=self.registry_for(lambda *_:True)
        current=self.registry.discover('test');self.registry.stage(current)
        old=copy.deepcopy(current);old.pop('manifestDescriptor');old.update(schemaVersion=1,localImageId=old['configDigest']);old.pop('candidateId');old['candidateId']=module.digest(old)
        with self.registry._db() as db:db.execute('INSERT INTO registry_candidates VALUES(?,?)',(old['candidateId'],module.canonical(old)))
        with closing(sqlite3.connect(path)) as db,db:
            db.execute('CREATE TABLE reservations(candidate TEXT PRIMARY KEY,bytes INTEGER NOT NULL)')
            db.execute('INSERT INTO reservations VALUES(?,?)',(old['candidateId'],64*module.GiB))
        path.chmod(0o600);self.capacity=self.open()
        return path,old,current

    def test_explicit_legacy_v1_to_v2_definite_alias_closes_without_changing_old_books(self):
        path,old,current=self.legacy_fixture()
        with self.assertRaises(module.CapacityError):self.capacity.verify_legacy()
        before=(path.read_bytes(),self.registry.path.read_bytes())
        with patch.object(self.registry,'reconcile',side_effect=AssertionError('legacy metadata must never be rewritten')):
            self.assertTrue(self.capacity.reconcile_legacy(str(path),self.registry))
            self.assertTrue(self.capacity.reconcile_legacy(str(path),self.registry))
        self.assertEqual((path.read_bytes(),self.registry.path.read_bytes()),before)
        self.capacity.verify_legacy()
        with self.capacity._locked(),self.capacity._db() as db:
            proof=json.loads(db.execute('SELECT proof FROM legacy_closures').fetchone()[0])
        self.assertEqual((proof['aliases'][0]['legacyCandidateId'],proof['aliases'][0]['canonicalCandidateId']),
            (old['candidateId'],current['candidateId']))
        self.assertTrue(self.capacity(self.other(current),64*module.GiB))

    def test_changed_legacy_rows_or_inode_after_closure_block_future_admission(self):
        path,old,current=self.legacy_fixture();self.capacity.reconcile_legacy(str(path),self.registry)
        with closing(sqlite3.connect(path)) as db,db:db.execute('UPDATE reservations SET bytes=bytes-1')
        with self.assertRaises(module.CapacityError):self.capacity(self.other(current),64*module.GiB)
        with self.assertRaises(module.CapacityError):self.capacity.verify_legacy()

    def test_replaced_legacy_inode_cannot_reuse_the_old_closure(self):
        path,old,current=self.legacy_fixture();self.capacity.reconcile_legacy(str(path),self.registry)
        replacement=path.with_name('replacement.sqlite');replacement.write_bytes(path.read_bytes());replacement.chmod(0o600)
        os.replace(replacement,path)
        with self.assertRaises(module.CapacityError):self.capacity.verify_legacy()
        with self.assertRaises(module.CapacityError):self.capacity(self.other(current),64*module.GiB)

    def test_unknown_or_orphan_legacy_pull_never_closes_or_reconciles_registry(self):
        path,old,current=self.legacy_fixture()
        with self.registry._db() as db:db.execute("UPDATE registry_staging SET state='held'")
        with patch.object(self.registry,'reconcile',side_effect=AssertionError('must not promote unknown pull')):
            with self.assertRaises(module.CapacityError):self.capacity.reconcile_legacy(str(path),self.registry)
        with self.capacity._locked(),self.capacity._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM legacy_closures').fetchone()[0],0)

    def test_mismatched_legacy_candidate_hash_or_schema_cannot_be_adopted(self):
        path,old,current=self.legacy_fixture()
        changed=dict(old,configDigest='sha256:'+'0'*64)
        with self.registry._db() as db:db.execute('UPDATE registry_candidates SET value=? WHERE id=?',(module.canonical(changed),old['candidateId']))
        with self.assertRaises(module.CapacityError):self.capacity.reconcile_legacy(str(path),self.registry)
        with closing(sqlite3.connect(path)) as db,db:db.execute('CREATE TABLE unknown(value TEXT)')
        with self.assertRaises(module.CapacityError):self.capacity.reconcile_legacy(str(path),self.registry)

    def test_legacy_alias_cannot_choose_v1_metadata_or_another_artifact(self):
        path,old,current=self.legacy_fixture()
        with self.registry._db() as db:
            metadata=json.loads(db.execute('SELECT metadata FROM registry_staging').fetchone()[0])
            metadata['candidateId']=old['candidateId']
            db.execute('UPDATE registry_staging SET metadata=?',(module.canonical(metadata),))
        with self.assertRaises(module.CapacityError):self.capacity.reconcile_legacy(str(path),self.registry)

    def test_inherited_object_rejects_child_before_its_parent_mutex(self):
        candidate=self.candidate();reader,writer=os.pipe()
        self.capacity._mutex.acquire()
        child=os.fork()
        if child==0:
            os.close(reader)
            try:self.capacity(candidate,64*module.GiB)
            except module.CapacityError:os.write(writer,b'held')
            finally:os._exit(0)
        os.close(writer)
        try:
            import select
            ready,_,_=select.select([reader],[],[],2)
            self.assertTrue(ready);self.assertEqual(os.read(reader,20),b'held')
        finally:
            self.capacity._mutex.release();os.close(reader);os.waitpid(child,0)

    def test_actual_process_competitors_share_aggregate_reservation(self):
        candidate=self.candidate();other=self.other(candidate);self.disk['freeBytes']=164*module.GiB
        go=self.root/'go'
        code='''import json,sys,time\nfrom pathlib import Path\nfrom processing import update_capacity as m\np=json.loads(sys.argv[2]);p['legacy_capacity_journals']=tuple(p['legacy_capacity_journals']);p=m.CapacityPolicy(**p)\nm.canonical_capacity_path=lambda _:Path(sys.argv[1])\ndisk=json.loads(sys.argv[4])\nfor _ in range(100):\n try:c=m.ImageStagingCapacity(p,docker_probe=lambda:dict(engineId=p.engine_id,dockerRootDir=p.docker_root_dir),disk_probe=lambda _:dict(disk));break\n except m.CapacityBusy:time.sleep(.02)\nPath(sys.argv[5]+sys.argv[6]).write_text('ready')\nwhile not Path(sys.argv[5]).exists():time.sleep(.01)\nfor _ in range(100):\n try:print(json.dumps(c(json.loads(sys.argv[3]),p.reservation_bytes)));break\n except m.CapacityBusy:time.sleep(.02)\n'''
        processes=[]
        try:
            for index,value in enumerate((candidate,other)):
                processes.append(subprocess.Popen([sys.executable,'-B','-c',code,str(self.capacity.root),json.dumps(asdict(self.policy)),
                    json.dumps(value),json.dumps(self.disk),str(go),str(index)],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True))
            deadline=time.monotonic()+10
            while not all(Path(str(go)+str(index)).exists() for index in range(2)):
                if time.monotonic()>deadline:self.fail('synthetic children did not become ready')
                time.sleep(.01)
            go.write_text('go')
            results=[]
            for process in processes:
                stdout,stderr=process.communicate(timeout=10);self.assertEqual(process.returncode,0,stderr);results.append(json.loads(stdout))
            self.assertEqual(sorted(results),[False,True]);self.assertEqual(len(self.rows()),1)
        finally:
            for process in processes:
                if process.poll() is None:process.kill();process.wait()
