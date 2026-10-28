"""Real pinned671→current→671 idle SQLite reopen; no Docker/SSH mutations."""
from dataclasses import replace
import copy
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from types import SimpleNamespace
import uuid

from ops.incremental_coordinator_binding import IncrementalBinding,IncrementalHeld,volume_census,compare_transition,digest
from processing.prototype import Queue
from processing.reference_store import ReferenceStore
from processing.reference_terminal import TerminalService
from processing.reference_delivery import DeliveryReceiver
from processing.reference_failure import FailureReceiver
from processing.viewer_tasks import TaskStore
from tests.fixtures.coordinator_671.prototype import Queue as LegacyQueue

AUTHORITY='7131882e-38ce-4d3e-99cc-4ba5a94d2ee8'
IMAGE='sha256:'+'a'*64


@unittest.skipUnless(os.name=='posix','Linux owned coordinator state')
class IncrementalCoordinatorTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.root.chmod(0o700);self.volume=self.root/'state'
        self.queue=LegacyQueue(self.volume,node_ids=('cluster','main'),reference_enabled=True)
        self.addCleanup(lambda:self.queue.close())
        self.store=ReferenceStore(self.queue);TerminalService(self.store);DeliveryReceiver(self.store);FailureReceiver(self.store);TaskStore(self.queue,self.volume/'viewer-tasks');(self.volume/'viewer-spool').mkdir(mode=0o700)
        with self.queue._db() as db:
            db.execute('UPDATE coordinator_identity SET authority_id=?',(AUTHORITY,))
            for index in range(12):db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,'staging','treedar-synthetic-v1',?)",(str(uuid.uuid4()),'synthetic-history-'+str(index),'failed' if index<2 else 'succeeded'))
        self.queue.authority_id=AUTHORITY
        for node in ('cluster','main'):
            receipt=self.queue.schedule_node_owned(node,self.payload(node))
            self.queue.cancel_node(node,dict(expectedRevision=receipt['revision'],planId=receipt['planId']))
        with self.queue._db() as db:
            db.execute("UPDATE nodes SET revision=9 WHERE id='cluster'");db.execute("UPDATE nodes SET revision=18 WHERE id='main'")
        self.baseline=self.census()
        self.binding=IncrementalBinding('1'*64,'sha256:'+'2'*64,'671fd2a6bde40088bdfad91ccf67eab45ee3d8d7','6bbd468e1177746cb695e43195e75355b0681f23',AUTHORITY,'3'*64,'4'*64,'5'*64,IMAGE,'6'*64,IMAGE,self.baseline['tables']['jobs']['sha256'],digest(self.baseline['ownedRows']),digest(self.baseline['schema']))

    def census(self):return volume_census(self.volume,authority=AUTHORITY,uid=os.geteuid())

    def payload(self,node):
        current=self.queue.get_node(node)
        return dict(schemaVersion=1,requestId=str(uuid.uuid4()),authorityId=AUTHORITY,expectedRevision=current['revision'],drainAt='2026-01-01T00:00:00Z',notBefore='2026-01-01T00:00:00Z',afterJobId=None,targetImage=IMAGE)

    def plan(self,node):
        payload=self.payload(node);receipt=self.queue.schedule_node_owned(node,payload)
        with self.queue._db() as db:row=dict(db.execute('SELECT * FROM owned_maintenance_schedules WHERE request_id=?',(payload['requestId'],)).fetchone())
        return row,receipt

    def compare(self,observed,**kwargs):return compare_transition(observed,self.baseline,binding=self.binding,**kwargs)

    def test_actual_archived_predecessor_bytes_and_new_old_reopen_preserve_full_history(self):
        fixture=Path(__file__).parent/'fixtures/coordinator_671';attrs=json.loads((fixture/'fixture.attrs').read_text())
        self.assertEqual(attrs['sourceCommit'],self.binding.predecessor_source)
        for name,sha in attrs['files'].items():self.assertEqual(hashlib.sha256((fixture/name).read_bytes()).hexdigest(),sha)
        self.assertEqual(digest(self.baseline['schema']),'1f4bc730fb0242e380f6eaeef1a2b197bd883c17377c082a87a18238c4f2b80e')
        self.binding.check_baseline(self.baseline);self.queue.close()
        self.queue=Queue(self.volume,node_ids=('cluster','main'),reference_enabled=True)
        self.compare(self.census());self.queue.close()
        self.queue=LegacyQueue(self.volume,node_ids=('cluster','main'),reference_enabled=True)
        self.compare(self.census())
        self.assertEqual(len(self.census()['ownedRows']),2)

    def test_exact_two_owned_drain_and_cancel_transitions_preserve_both_prior_rows(self):
        plans=[];receipts={}
        for node in ('cluster','main'):
            row,receipt=self.plan(node);plans.append(row);receipts[node]=receipt
            self.compare(self.census(),plans=tuple(plans))
        cancel_intents=[]
        for index,node in enumerate(('cluster','main')):
            receipt=receipts[node];self.queue.cancel_node(node,dict(expectedRevision=receipt['revision'],planId=receipt['planId']))
            cancel_intents.append(dict(nodeId=node,expectedRevision=receipt['revision'],planId=receipt['planId']))
            with self.assertRaises(IncrementalHeld):self.compare(self.census(),plans=tuple(plans),reopened=('cluster','main')[:index+1])
            self.compare(self.census(),plans=tuple(plans),cancel_intents=tuple(cancel_intents),reopened=('cluster','main')[:index+1])
        # Actual predecessor cold-open over the same newly extended history is
        # the narrow rollback qualification, not permission to restore a DB.
        self.queue.close();self.queue=LegacyQueue(self.volume,node_ids=('cluster','main'),reference_enabled=True)
        self.compare(self.census(),plans=tuple(plans),cancel_intents=tuple(cancel_intents),reopened=('cluster','main'))

    def test_prior_history_deletion_tamper_and_unowned_row_never_table_wide_ignored(self):
        old=self.baseline['ownedRows'][0]
        with self.queue._db() as db:db.execute('DELETE FROM owned_maintenance_schedules WHERE request_id=?',(old['request_id'],))
        with self.assertRaises(IncrementalHeld):self.compare(self.census())
        with self.queue._db() as db:db.execute('INSERT INTO owned_maintenance_schedules VALUES(?,?,?,?,?,?)',tuple(old[key] for key in ('request_id','node_id','authority_id','request_sha256','request_json','receipt_json')))
        row,_=self.plan('cluster')
        with self.assertRaises(IncrementalHeld):self.compare(self.census())
        observed=self.census();observed['ownedRows'][0]['request_json']='{}'
        with self.assertRaises(IncrementalHeld):self.compare(observed,plans=(row,))

    def test_generation_revision_plan_image_and_columns_drift_hold(self):
        row,_=self.plan('cluster');valid=self.census()
        for field,value in (('revision',999),('plan_id',str(uuid.uuid4())),('target_image','sha256:'+'f'*64)):
            observed=copy.deepcopy(valid);observed['nodeRows']['cluster'][field]=value
            with self.assertRaises(IncrementalHeld):self.compare(observed,plans=(row,))
        with self.assertRaises(IncrementalHeld):compare_transition(valid,self.baseline,binding=replace(self.binding,cluster_image='sha256:'+'f'*64),plans=(row,))
        forged=copy.deepcopy(valid);forged['tables']['owned_maintenance_schedules']['count']=0
        with self.assertRaises(IncrementalHeld):self.compare(forged,plans=(row,))

    def test_unknown_schema_capacity_and_symlinks_hold_without_writes(self):
        with self.queue._db() as db:db.execute('CREATE TABLE unexpected(value TEXT)')
        before=(self.volume/'jobs.sqlite3').read_bytes()
        with self.assertRaises(IncrementalHeld):self.census()
        self.assertEqual((self.volume/'jobs.sqlite3').read_bytes(),before)
        with self.queue._db() as db:db.execute('DROP TABLE unexpected')
        extra=self.volume/'viewer-spool/foreign';extra.write_bytes(b'not-owned-by-oracle')
        with self.assertRaises(IncrementalHeld):self.census()
        extra.unlink();(self.volume/'unexpected-link').symlink_to(self.volume/'jobs.sqlite3')
        with self.assertRaises(IncrementalHeld):self.census()

    def test_baseline_binding_is_exact_and_readonly(self):
        before={p.name:p.read_bytes() for p in self.volume.iterdir() if p.is_file()}
        self.compare(self.census());self.binding.check_baseline(self.census())
        self.assertEqual(before,{p.name:p.read_bytes() for p in self.volume.iterdir() if p.is_file()})
        with self.assertRaises(IncrementalHeld):replace(self.binding,owned_history_sha256='f'*64).check_baseline(self.baseline)

    def test_historical_strict_helper_still_refuses_nonempty_owned_history(self):
        from ops.update_reference_coordinator import volume_census as historical_census
        before=(self.volume/'jobs.sqlite3').read_bytes()
        with self.assertRaises(AssertionError):historical_census(self.volume,uid=os.geteuid())
        self.assertEqual((self.volume/'jobs.sqlite3').read_bytes(),before)

    def test_oversized_owned_record_holds_before_json_decode_without_census_mutation(self):
        with self.queue._db() as db:db.execute('UPDATE owned_maintenance_schedules SET receipt_json=?',('x'*9000,))
        before=(self.volume/'jobs.sqlite3').read_bytes()
        with self.assertRaises(IncrementalHeld):self.census()
        self.assertEqual((self.volume/'jobs.sqlite3').read_bytes(),before)

    def test_inventory_refuses_fifth_entry_before_materializing_directory(self):
        class Entries:
            count=0
            def __enter__(entries):return entries
            def __exit__(entries,*args):pass
            def __iter__(entries):return entries
            def __next__(entries):
                entries.count+=1
                if entries.count>5:raise AssertionError('unbounded directory enumeration')
                return SimpleNamespace(name='synthetic-'+str(entries.count))
        entries=Entries()
        with patch('ops.incremental_coordinator_binding.os.scandir',return_value=entries):
            with self.assertRaisesRegex(IncrementalHeld,'state_inventory_bound'):self.census()
        self.assertEqual(entries.count,5)

    def test_clean_closed_wal_header_refused_before_sqlite_open_without_sidecars(self):
        db=sqlite3.connect(self.queue.path)
        try:self.assertEqual(db.execute('PRAGMA journal_mode=WAL').fetchone()[0],'wal')
        finally:db.close()
        path=Path(self.queue.path);before=path.read_bytes();inventory=set(self.volume.iterdir())
        self.assertEqual(before[18:20],b'\x02\x02')
        self.assertFalse(Path(str(path)+'-wal').exists());self.assertFalse(Path(str(path)+'-shm').exists())
        with patch('ops.incremental_coordinator_binding.sqlite3.connect',side_effect=AssertionError('must not open SQLite WAL')):
            with self.assertRaisesRegex(IncrementalHeld,'delete_mode_state_header_required'):self.census()
        self.assertEqual(path.read_bytes(),before);self.assertEqual(set(self.volume.iterdir()),inventory)


if __name__=='__main__':unittest.main()
