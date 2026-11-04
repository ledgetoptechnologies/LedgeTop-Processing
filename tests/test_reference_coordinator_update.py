"""Temporary real SQLite/HTTP, synthetic Docker/SSH boundaries; no host changes."""
from contextlib import redirect_stdout
import ast
import copy
import inspect
import io
import http.client
import json
import os
from pathlib import Path
import shutil
import socket
import socketserver
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from ops import update_reference_coordinator as module
from processing.app import make_handler
from processing.config import Config
from processing.odx_smoke_worker import Client
from processing.reference_store import ReferenceStore
from processing.reference_terminal import TerminalService
from processing.viewer_tasks import TaskStore
from tests.test_service import TOKEN,start_server

TARGET='sha256:'+'b'*64
SOURCE='c'*40
NEW_ID='d'*64
MAIN='sha256:'+'e'*64
OPERATOR='synthetic-coordinator-update-operator'


class GuardOptimizationTests(unittest.TestCase):
    def test_direct_script_help_from_unrelated_directory_with_isolated_python(self):
        env={k:v for k,v in os.environ.items() if k not in ('PYTHONPATH','PYTHONHOME','PYTHONSTARTUP')}
        with tempfile.TemporaryDirectory() as cwd:
            result=subprocess.run([sys.executable,'-I',str(Path(module.__file__).resolve()),'--help'],cwd=cwd,env=env,capture_output=True,text=True,timeout=30)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('--receipt-dir',result.stdout)

    def test_generated_guard_survives_optimization(self):
        source=module.explicit_guard_source('assert False')
        with self.assertRaises(AssertionError):exec(compile(source,'<guard>','exec',optimize=2),{})
        sources=[inspect.getsource(module.volume_census),module.explicit_guard_source(module.TLS_CENSUS)]
        if os.name=='posix':sources.append(module.backup_script())
        for code in sources:
            self.assertFalse(any(isinstance(node,ast.Assert) for node in ast.walk(ast.parse(code))))


class Gates:
    binding={'cluster':{'generation':1},'main':{'generation':1}}
    def __init__(self):self.live=False;self.dead=False;self.checks=0
    def __call__(self):return self
    def __enter__(self):self.live=True;self.check();return self
    def __exit__(self,*args):self.live=False
    def check(self):
        self.checks+=1
        if not self.live or self.dead:raise module.Stopped('synthetic_gate_lost')


@unittest.skipUnless(os.name=='posix','Linux ownership and offline helper contract')
class ReferenceCoordinatorUpdateTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name);self.root.chmod(0o700)
        self.volume=self.root/'volume'
        self.handler=make_handler(Config(TOKEN,'','',1000,20000,prototype_state_dir=str(self.volume),
            prototype_node_ids=('cluster','main'),prototype_operator_token=OPERATOR))
        self.queue=self.handler.prototype_queue
        self.addCleanup(self.queue.close)
        self.queue.allowed_profiles.add('reference-pipeline-v1')
        self.store=ReferenceStore(self.queue)
        self.terminal=TerminalService(self.store)
        TaskStore(self.queue,self.volume/'viewer-tasks');(self.volume/'viewer-spool').mkdir(mode=0o700)
        with self.queue._db() as db:
            db.execute('UPDATE coordinator_identity SET authority_id=?',(module.AUTHORITY,))
            db.execute('DROP TABLE owned_maintenance_schedules')
            for index in range(module.REVIEWED_JOB_COUNT):
                db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,'staging','treedar-synthetic-v1',?)",
                    (str(__import__('uuid').uuid4()),'synthetic-'+str(index),'failed' if index<2 else 'succeeded'))
        self.queue.authority_id=module.AUTHORITY
        self.retain_viewer_tasks()
        self.server=start_server(self.handler);self.addCleanup(self.server.server_close);self.addCleanup(self.server.shutdown)
        origin='http://127.0.0.1:'+str(self.server.server_port)
        self.api,self.operator=Client(origin,TOKEN),Client(origin,OPERATOR)
        self.gates=Gates();self.transport=self.make_transport()
        self.operation=self.root/'operation';self.operation.mkdir(mode=0o700)
        self.updater=module.ReferenceCoordinatorUpdate(self.operation,gate_factory=self.gates,transport=self.transport,cluster_probe=lambda state:None)
        self.backup_patch=patch.object(module,'copy_backup',side_effect=self.backup)
        self.backup_patch.start();self.addCleanup(self.backup_patch.stop)
        self.probe=patch.object(module,'owned_api_probe',return_value={'operatorLookupVerified':True})
        self.probe.start();self.addCleanup(self.probe.stop)
        self.state=self.updater.prepare(TARGET,SOURCE,MAIN)

    def retain_viewer_tasks(self,indices=range(2),stages=()):
        """Completed Viewer tasks with nested inputs and results (client deliverables)."""
        if not indices.start:self.retained={}
        for index in indices:
            identifier=str(__import__('uuid').uuid4());directory='task-'+str(index)
            base=self.volume/'viewer-tasks'/directory;(base/'images').mkdir(parents=True,mode=0o700)
            for relative,content in (('images/IMG_0001.JPG',b'synthetic photo '*64+bytes([index])),
                                     ('all.zip',b'PK\x03\x04synthetic result '+bytes([index])),('.archive.json',b'{}')):
                (base/relative).write_bytes(content);(base/relative).chmod(0o600)
                self.retained['viewer-tasks/'+directory+'/'+relative]=content
            with self.queue._db() as db:
                db.execute("""INSERT INTO viewer_tasks(uuid,directory,name,options,outputs,environment,stage,phase,archive_path)
                    VALUES(?,?,?,'[]','[]','production',?,'complete',?)""",(identifier,directory,'retained-'+str(index),
                    dict(stages).get(index,'succeeded'),str(base/'all.zip')))
                db.execute("INSERT INTO viewer_files VALUES(?,'img_0001.jpg','IMG_0001.JPG',?,1024,?,1)",(identifier,str(base/'images/IMG_0001.JPG'),'a'*64))
                db.execute("INSERT INTO viewer_incarnations VALUES(?,1,NULL,'{}','[]')",(identifier,))

    def assert_retained_viewer_bytes(self):
        observed={path.relative_to(self.volume).as_posix():path.read_bytes()
                  for path in (self.volume/'viewer-tasks').rglob('*') if path.is_file()}
        self.assertEqual(observed,self.retained)

    def test_retained_terminal_viewer_tasks_are_hashed_and_survive_update(self):
        census=self.state['baseline']
        self.assertEqual(census['tables']['viewer_tasks']['count'],2)
        files={item['path']:item for item in census['files']}
        for relative,content in self.retained.items():
            self.assertEqual((files[relative]['kind'],files[relative]['mode'],files[relative]['bytes'],files[relative]['sha256']),
                             ('file',0o600,len(content),__import__('hashlib').sha256(content).hexdigest()))
        self.assertEqual(files['viewer-tasks/task-0/images']['kind'],'directory')
        self.assertEqual(self.activate()['phase'],'completed')
        self.assert_retained_viewer_bytes()

    def test_fresh_terminal_history_is_pinned_and_later_changes_block_update(self):
        with self.queue._db() as db:
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,'staging','treedar-synthetic-v1','succeeded')",
                       (str(__import__('uuid').uuid4()), 'additional-finished-job'))
        operation=self.root/'new-history-operation';operation.mkdir(mode=0o700)
        updater=module.ReferenceCoordinatorUpdate(operation,gate_factory=self.gates,transport=self.transport,
            cluster_probe=lambda state:None)
        state=updater.prepare(TARGET,SOURCE,MAIN)
        self.assertEqual(state['history']['count'],module.REVIEWED_JOB_COUNT+1)
        self.assertEqual(state['baseline']['tables']['jobs']['count'],state['history']['count'])
        with self.queue._db() as db:
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,'staging','treedar-synthetic-v1','succeeded')",
                       (str(__import__('uuid').uuid4()), 'changed-after-prepare'))
        with self.assertRaises(module.Stopped):
            updater.activate(state)
        self.assertTrue(self.transport.containers[module.OLD_ID]['State']['Running'])

    def test_retained_viewer_bytes_survive_rollback(self):
        self.transport.loss='start'
        with self.assertRaises(OSError):self.activate()
        self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back')
        self.assert_retained_viewer_bytes()
        # Nodes are reopened after rollback; every other row and file equals the baseline.
        module.compatible(module.volume_census(self.volume,uid=os.geteuid()),self.state['baseline'],allow_owned=True,ignore_nodes=True)

    def test_retained_viewer_file_changed_during_drain_blocks_before_stop(self):
        target=self.volume/'viewer-tasks/task-1/all.zip'
        original=self.operator.json
        def mutate(method,route,payload=None,**kwargs):
            value=original(method,route,payload,**kwargs)
            if route.endswith('/schedule'):target.write_bytes(target.read_bytes()+b'x')
            return value
        with patch.object(self.operator,'json',side_effect=mutate):
            with self.assertRaisesRegex(module.Stopped,'full_census_changed'):self.activate()
        self.assertFalse(any(call[0]=='stop' for call in self.transport.calls))
        self.assertTrue(self.transport.containers[module.OLD_ID]['State']['Running'])

    def test_retained_viewer_census_refusals(self):
        census=lambda:module.volume_census(self.volume,uid=os.geteuid())
        base=self.volume/'viewer-tasks/task-0'
        cases=[]
        def symlink():(base/'link').symlink_to(base/'all.zip');return lambda:(base/'link').unlink()
        def hardlink():os.link(base/'all.zip',base/'second');return lambda:(base/'second').unlink()
        def oversize():
            with open(base/'huge','wb') as stream:stream.truncate(21474836481)
            return lambda:(base/'huge').unlink()
        def nonterminal():
            with self.queue._db() as db:db.execute("UPDATE viewer_tasks SET stage='running',phase='processing' WHERE directory='task-0'")
            def undo():
                with self.queue._db() as db:db.execute("UPDATE viewer_tasks SET stage='succeeded',phase='complete' WHERE directory='task-0'")
            return undo
        def orphan():
            with self.queue._db() as db:db.execute("INSERT INTO viewer_files VALUES('orphan','x','x','x',1,'b',1)")
            def undo():
                with self.queue._db() as db:db.execute("DELETE FROM viewer_files WHERE uuid='orphan'")
            return undo
        def spool():(self.volume/'viewer-spool'/'stray').write_bytes(b'x');return lambda:(self.volume/'viewer-spool'/'stray').unlink()
        for make in (symlink,hardlink,oversize,nonterminal,orphan,spool):
            with self.subTest(case=make.__name__):
                undo=make()
                try:
                    with self.assertRaises(AssertionError):census()
                finally:undo()
                census()

    def test_failed_and_cancelled_complete_viewer_tasks_survive_update_and_rollback(self):
        with self.queue._db() as db:
            db.execute("UPDATE viewer_tasks SET stage='failed' WHERE directory='task-0'")
            db.execute("UPDATE viewer_tasks SET stage='cancelled' WHERE directory='task-1'")
        self.assertEqual(module.volume_census(self.volume,uid=os.geteuid())['tables']['viewer_tasks']['count'],2)
        operation=self.root/'terminal-operation';operation.mkdir(mode=0o700)
        updater=module.ReferenceCoordinatorUpdate(operation,gate_factory=self.gates,transport=self.transport,cluster_probe=lambda state:None)
        state=updater.prepare(TARGET,SOURCE,MAIN)
        self.state=state  # the synthetic transport labels candidates with this operation
        self.transport.loss='start'
        with self.assertRaises(OSError):updater.activate(state)
        self.assertEqual(updater.rollback(state)['phase'],'rolled_back')
        self.assert_retained_viewer_bytes()
        with self.queue._db() as db:
            stages=sorted(row[0] for row in db.execute('SELECT stage FROM viewer_tasks'))
        self.assertEqual(stages,['cancelled','failed'])

    def test_nonterminal_viewer_task_refuses_fresh_prepare(self):
        with self.queue._db() as db:db.execute("UPDATE viewer_tasks SET phase='processing' WHERE directory='task-1'")
        operation=self.root/'second-operation';operation.mkdir(mode=0o700)
        updater=module.ReferenceCoordinatorUpdate(operation,gate_factory=self.gates,transport=self.transport,cluster_probe=lambda state:None)
        with self.assertRaises(AssertionError):updater.prepare(TARGET,SOURCE,MAIN)
        self.assertEqual(self.transport.calls,[])

    def test_viewer_extra_tables_exact_empty_upgrade_and_copy(self):
        with self.queue._db() as db:
            db.execute('DROP TABLE viewer_option_evidence')
            db.execute('DROP TABLE viewer_reservations')
        before=module.volume_census(self.volume,uid=os.geteuid())
        TaskStore(self.queue,self.volume/'viewer-tasks')
        after=module.volume_census(self.volume,uid=os.geteuid())
        module.compatible(after,before,allow_owned=True,allow_viewer_additions=True)
        # Rollback compatibility must not silently permit a newer schema.
        with self.assertRaises(module.Stopped):module.compatible(after,before,allow_owned=True)
        nonempty=copy.deepcopy(after);nonempty['tables']['viewer_reservations']['count']=1
        with self.assertRaises(module.Stopped):module.compatible(nonempty,before,allow_owned=True,allow_viewer_additions=True)
        malformed=copy.deepcopy(after);malformed['schema']=[r for r in malformed['schema'] if r[1]!='sqlite_autoindex_viewer_reservations_1']
        with self.assertRaises(module.Stopped):module.compatible(malformed,before,allow_owned=True,allow_viewer_additions=True)
        copied=self.root/'viewer-schema-copy';shutil.copytree(self.volume,copied)
        module.compatible(module.volume_census(copied,uid=os.geteuid(),offline=True),after)

    def test_viewer_extra_tables_partial_schema_holds(self):
        with self.queue._db() as db:db.execute('DROP TABLE viewer_option_evidence')
        with self.assertRaises(AssertionError):module.volume_census(self.volume,uid=os.geteuid())

    def test_viewer_extra_tables_changed_schema_or_nonempty_hold(self):
        with self.queue._db() as db:
            db.execute('ALTER TABLE viewer_reservations ADD COLUMN unreviewed TEXT')
        with self.assertRaises(AssertionError):module.volume_census(self.volume,uid=os.geteuid())

    def test_viewer_extra_tables_nonempty_reservation_holds(self):
        with self.queue._db() as db:
            db.execute("INSERT INTO viewer_reservations VALUES('synthetic','staging','synthetic','reserved')")
        with self.assertRaises(AssertionError):module.volume_census(self.volume,uid=os.geteuid())

    def test_release_tombstones_and_changed_guard_schema_block_update(self):
        with self.queue._db() as db:
            db.execute("INSERT INTO reference_released_claims VALUES('r','j','n','w','i','digest')")
        with self.assertRaises(AssertionError):
            module.volume_census(self.volume,uid=os.geteuid())
        with self.queue._db() as db:
            db.execute('DELETE FROM reference_released_claims')
            db.execute('ALTER TABLE viewer_guarded_dispatch_history ADD COLUMN unreviewed TEXT')
        with self.assertRaises(AssertionError):
            module.volume_census(self.volume,uid=os.geteuid())

    def test_authority_lease_marker_is_censused_and_must_remain_valid(self):
        with self.queue._db() as db:
            db.execute('CREATE TABLE coordinator_authority_lease (singleton INTEGER PRIMARY KEY CHECK(singleton=1), required INTEGER NOT NULL CHECK(required=1))')
            db.execute('INSERT INTO coordinator_authority_lease VALUES(1,1)')
        before=module.volume_census(self.volume,uid=os.geteuid())
        self.assertEqual(before['tables']['coordinator_authority_lease']['count'],1)
        copied=self.root/'lease-copy';shutil.copytree(self.volume,copied)
        module.compatible(module.volume_census(copied,uid=os.geteuid(),offline=True),before)
        with self.queue._db() as db:
            db.execute('DELETE FROM coordinator_authority_lease')
        with self.assertRaises(AssertionError):
            module.volume_census(self.volume,uid=os.geteuid())

    def test_historical_owned_receipt_is_preserved_and_tampering_holds(self):
        from datetime import datetime, timezone
        with self.queue._db() as db: self.queue._init_maintenance(db)
        stamp = datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
        request = {'schemaVersion':1, 'requestId':str(__import__('uuid').uuid4()),
                   'authorityId':module.AUTHORITY, 'expectedRevision':self.queue.get_node('main')['revision'],
                   'drainAt':stamp, 'notBefore':stamp, 'afterJobId':None, 'targetImage':MAIN}
        receipt = self.queue.schedule_node_owned('main', request)
        self.queue.cancel_node('main', {'planId':receipt['planId'], 'expectedRevision':receipt['revision']})
        before = module.volume_census(self.volume,uid=os.geteuid())
        self.assertEqual(before['tables']['owned_maintenance_schedules']['count'],1)
        copied = self.root/'owned-history-copy';shutil.copytree(self.volume,copied)
        module.compatible(module.volume_census(copied,uid=os.geteuid(),offline=True), before)
        with self.queue._db() as db:
            db.execute("UPDATE owned_maintenance_schedules SET request_sha256=?", ('0'*64,))
        with self.assertRaisesRegex(AssertionError,'owned schedule evidence invalid'):
            module.volume_census(self.volume,uid=os.geteuid())
    def test_terminal_viewer_task_files_are_retained_and_censused(self):
        import hashlib,uuid
        identifier=str(uuid.uuid4())
        directory=self.volume/'viewer-tasks'/identifier
        directory.mkdir(mode=0o700)
        payload=b'synthetic retained viewer artifact'
        artifact=directory/'retained.bin'
        artifact.write_bytes(payload);artifact.chmod(0o600)
        digest=hashlib.sha256(payload).hexdigest()
        with self.queue._db() as db:
            db.execute("""INSERT INTO viewer_tasks
                (uuid,directory,name,options,outputs,environment,stage,phase)
                VALUES(?,?,?,?,?,?,?,?)""",(identifier,identifier,'synthetic','[]','null',
                'production','succeeded','complete'))
            db.execute("INSERT INTO viewer_files VALUES(?,?,?,?,?,?,?)",
                (identifier,'retained','retained.bin','retained.bin',len(payload),digest,0))
            db.execute("INSERT INTO viewer_option_evidence VALUES(?,?,?,?)",
                (identifier,1,'[]',hashlib.sha256(b'[]').hexdigest()))
            db.execute("INSERT INTO viewer_incarnations VALUES(?,?,?,?,?)",
                (identifier,1,None,'{}','[]'))
        before=module.volume_census(self.volume,uid=os.geteuid())
        copied=self.root/'retained-viewer-copy';shutil.copytree(self.volume,copied)
        copied_artifact=copied/'viewer-tasks'/identifier/'retained.bin'
        copied_artifact.write_bytes(payload+b'changed')
        changed=module.volume_census(copied,uid=os.geteuid(),offline=True)
        with self.assertRaises(module.Stopped):module.compatible(changed,before)
        copied_artifact.write_bytes(payload);copied_artifact.chmod(0o600)
        module.compatible(module.volume_census(copied,uid=os.geteuid(),offline=True),before)
        self.assertEqual(artifact.read_bytes(),payload)
        with self.queue._db() as db:
            db.execute("UPDATE viewer_tasks SET phase='queued',removed=1 WHERE uuid=?",(identifier,))
        with self.assertRaises(AssertionError):module.volume_census(self.volume,uid=os.geteuid())

    def test_nonempty_viewer_reservation_holds_under_optimized_python(self):
        with self.queue._db() as db:
            db.execute("INSERT INTO viewer_reservations VALUES('synthetic','staging','synthetic','reserved')")
        code="from ops.update_reference_coordinator import volume_census; import os,sys; volume_census(sys.argv[1],uid=os.geteuid())"
        result=subprocess.run([sys.executable,'-O','-c',code,str(self.volume)],capture_output=True,text=True,timeout=10)
        self.assertNotEqual(result.returncode,0);self.assertIn('nonempty guarded table',result.stderr)

    def test_rollback_retains_candidate_when_new_empty_viewer_schema_was_added(self):
        with self.queue._db() as db:
            db.execute('DROP TABLE viewer_option_evidence')
            db.execute('DROP TABLE viewer_reservations')
        self.state['baseline']=module.volume_census(self.volume,uid=os.geteuid())
        self.updater.save(self.state)
        def candidate_start(action):
            if action=='start':TaskStore(self.queue,self.volume/'viewer-tasks')
        self.transport.hook=candidate_start;self.transport.loss='start'
        with self.assertRaises(OSError):self.activate()
        with self.assertRaises(module.Stopped):self.updater.rollback(self.state)
        self.assertFalse(self.transport.containers[module.OLD_ID]['State']['Running'])
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])
        self.assertTrue(self.state.get('downgradeRefused'))

    def old_volume(self,*,extra=()):
        """Live predecessor schema; only the first start runs the real migration."""
        with self.queue._db() as db:
            for name in ('reference_viewer_receipts','reference_viewer_transfers','viewer_full_option_evidence',
                         'reference_released_claims','viewer_guarded_dispatch_checkpoint','viewer_guarded_dispatch_history'):
                db.execute('DROP TABLE '+name)
            for table,column,_statement in module.MIGRATION_COLUMNS:db.execute('ALTER TABLE '+table+' DROP COLUMN '+column)
        self.state['baseline']=module.volume_census(self.volume,uid=os.geteuid());self.updater.save(self.state)
        started=[]
        def migrate(action):
            if action=='start' and not started:
                started.append(action);self.migrate()
                with self.queue._db() as db:
                    for statement in extra:db.execute(statement)
        self.transport.hook=migrate
        return self.state['baseline']

    def migrate(self):
        ReferenceStore(self.queue);TaskStore(self.queue,self.volume/'viewer-tasks')

    def columns(self,table):
        with self.queue._db() as db:return {row[1]:list(row) for row in db.execute('PRAGMA table_info('+table+')')}

    def test_pinned_migration_is_exactly_what_the_candidate_creates(self):
        fresh=module.volume_census(self.volume,uid=os.geteuid())
        pinned=module.normalized_schema(module.migration_schema())
        self.assertEqual({row[2] for row in pinned},{'reference_viewer_receipts','reference_viewer_transfers',
            'viewer_full_option_evidence','reference_released_claims','viewer_guarded_dispatch_checkpoint',
            'viewer_guarded_dispatch_history'})
        self.assertEqual(module.normalized_schema([row for row in fresh['schema'] if row[2] in {row[2] for row in pinned}]),pinned)
        source=''.join((Path(module.__file__).resolve().parents[1]/'processing'/name).read_text() for name in ('reference_store.py','viewer_tasks.py'))
        for table,column,statement in module.MIGRATION_COLUMNS:
            self.assertIn(statement,source)
            self.assertIn(self.columns(table)[column],fresh['tables'][table]['columns'])
        for statement in module.MIGRATION_TABLES:
            self.assertIn(statement.replace('CREATE TABLE ','CREATE TABLE IF NOT EXISTS ',1) if statement.startswith('CREATE') else statement,source)
        self.assertEqual([column[1:] for column in fresh['tables']['viewer_tasks']['columns'][-2:]],
            [[ 'full_option_evidence_required','INTEGER',1,'0',0],['node_guarded','INTEGER',1,'0',0]])
        self.assertEqual(fresh['tables']['reference_jobs']['columns'][-1][1:],['viewer_receipt_required','INTEGER',0,None,0])
        # Replaying pinned DDL over the live-like old table equals the real migration.
        before=self.old_volume();self.migrate();after=module.volume_census(self.volume,uid=os.geteuid())
        for table in ('viewer_tasks','reference_jobs'):
            original=[row for row in before['schema'] if row[0]=='table' and row[1]==table][0]
            statements=[statement for name,_column,statement in module.MIGRATION_COLUMNS if name==table]
            self.assertEqual(module.migrated_table(original,statements),
                ([row for row in after['schema'] if row[0]=='table' and row[1]==table],after['tables'][table]['columns']))
        module.compatible(after,before,allow_migration=True)
        with self.assertRaises(module.Stopped):module.compatible(after,before,allow_owned=True,allow_viewer_additions=True)
        with self.assertRaises(module.Stopped):module.compatible(before,after,allow_migration=True)

    def test_old_volume_reviewed_migration_completes_and_verifies(self):
        self.old_volume()
        self.assertNotIn('node_guarded',self.columns('viewer_tasks'))
        self.assertEqual(self.activate()['phase'],'completed')
        self.assertIn('verification',self.state)
        self.assertTrue({'full_option_evidence_required','node_guarded'}<=set(self.columns('viewer_tasks')))
        self.assertIn('viewer_receipt_required',self.columns('reference_jobs'))
        self.assertEqual(self.transport.inspected(module.NAME)['Id'],NEW_ID)
        self.assertEqual({self.queue.get_node(node)['state'] for node in ('cluster','main')},{'active'})

    def test_old_volume_reviewed_migration_allows_exact_rollback(self):
        self.old_volume();self.fail_after_start()
        self.assertIn('node_guarded',self.columns('viewer_tasks'))
        self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back')
        restored=self.transport.inspected(module.NAME)
        self.assertEqual((restored['Id'],restored['Image']),(module.OLD_ID,module.OLD_IMAGE))
        self.assertFalse(self.transport.containers[NEW_ID]['State']['Running'])
        self.assertFalse(self.state['databaseRestored'])

    def test_old_volume_unreviewed_migration_stops_update_and_refuses_rollback(self):
        self.old_volume(extra=('ALTER TABLE reference_jobs ADD COLUMN unreviewed INTEGER',))
        with self.assertRaises(module.Stopped):self.activate()
        self.assertEqual(self.state['phase'],'needs_inspection')
        calls=len(self.transport.calls)
        with self.assertRaises(module.Stopped):self.updater.rollback(self.state)
        self.assertEqual(len(self.transport.calls),calls)
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])
        self.assertFalse(self.transport.containers[module.OLD_ID]['State']['Running'])

    def test_migration_negative_cases_stop(self):
        old=self.root/'old-volume-copy'
        before=self.old_volume();shutil.copytree(self.volume,old)
        self.migrate();after=module.volume_census(self.volume,uid=os.geteuid())
        allow=dict(allow_owned=True,allow_viewer_additions=True,allow_migration=True)
        module.compatible(after,before,**allow)
        def census(name,statements,base):
            target=self.root/name;shutil.copytree(base,target)
            with sqlite3.connect(target/'jobs.sqlite3') as db:
                for statement in statements:db.execute(statement)
            db.close()
            return module.volume_census(target,uid=os.geteuid(),offline=True)
        pinned=[statement for _table,_column,statement in module.MIGRATION_COLUMNS]
        real=[*(statement.replace('CREATE TABLE ','CREATE TABLE IF NOT EXISTS ',1) for statement in module.MIGRATION_TABLES)]
        module.compatible(census('replayed',real+pinned,old),before,**allow)
        variants={
            'unreviewed-column':(self.volume,['ALTER TABLE viewer_tasks ADD COLUMN unreviewed INTEGER NOT NULL DEFAULT 0']),
            'wrong-default':(old,real+[pinned[0],pinned[1].replace('DEFAULT 0','DEFAULT 1'),pinned[2]]),
            'wrong-null-default':(old,real+pinned[:2]+[pinned[2]+' DEFAULT 0']),
            'wrong-declaration':(old,real+[pinned[0].replace(' CHECK(full_option_evidence_required IN (0,1))',''),*pinned[1:]]),
            'wrong-table-ddl':(old,[real[0].replace('receipt_json TEXT NOT NULL','receipt_json TEXT'),*real[1:],*pinned]),
            'missing-table-column':(old,[*real[:2],real[3],*pinned]),
        }
        for name,(base,statements) in variants.items():
            with self.subTest(case=name):
                with self.assertRaises(module.Stopped):module.compatible(census(name,statements,base),before,**allow)
        # The read-only census itself fences rows, so pinned defaults hold without reading values.
        for name,statement in (('nonempty-table',"INSERT INTO reference_viewer_transfers(job_id,receipt_id,acceptance_json) VALUES('synthetic','receipt','{}')"),
                ('nondefault-value',"INSERT INTO viewer_tasks(uuid,directory,name,options,outputs,environment,node_guarded) VALUES('synthetic','d','n','{}','[]','staging',1)"),
                ('unpinned-table','CREATE TABLE reference_unreviewed (id TEXT PRIMARY KEY)')):
            with self.subTest(case=name):
                with self.assertRaises(AssertionError):census(name,[statement],self.volume)
        crafted={}
        crafted['nonempty']=copy.deepcopy(after);crafted['nonempty']['tables']['reference_viewer_receipts']['count']=1
        crafted['nondefault']=copy.deepcopy(after);crafted['nondefault']['tables']['viewer_tasks'].update(count=1,sha256='0'*64)
        crafted['unpinned']=copy.deepcopy(after);crafted['unpinned']['tables']['reference_unreviewed']=dict(count=0,sha256=module.digest([]),columns=[])
        crafted['unpinned']['schema'].append(['table','reference_unreviewed','reference_unreviewed','CREATE TABLE reference_unreviewed (id TEXT)'])
        for name,value in crafted.items():
            with self.subTest(case=name):
                with self.assertRaises(module.Stopped):module.compatible(value,before,**allow)
        # Additions already in the baseline are no longer additions: exact match only.
        changed=copy.deepcopy(after)
        changed['tables']['viewer_tasks']['columns'][-1][4]='1'
        with self.assertRaises(module.Stopped):module.compatible(changed,after,**allow)
        with self.assertRaises(module.Stopped):module.compatible(after,before,allow_owned=True,allow_viewer_additions=True)

    def five_retained(self):
        """Five live-like terminal tasks (succeeded, failed, cancelled) on the old schema."""
        self.retain_viewer_tasks(range(2,5),stages=((2,'failed'),(3,'cancelled')))
        with self.queue._db() as db:
            identifier=db.execute("SELECT uuid FROM viewer_tasks WHERE directory='task-4'").fetchone()[0]
            db.execute("INSERT INTO viewer_option_evidence VALUES(?,1,'[]',?)",(identifier,__import__('hashlib').sha256(b'[]').hexdigest()))
        before=self.old_volume()
        self.assertEqual(before['tables']['viewer_tasks']['count'],5)
        self.assertEqual((before['tables']['viewer_tasks']['projectedSha256'],before['tables']['viewer_tasks']['nonDefault']),
                         (before['tables']['viewer_tasks']['sha256'],{}))
        return before

    def assert_projected_rows(self,before):
        after=module.volume_census(self.volume,uid=os.geteuid())
        for table,count in (('viewer_tasks',5),('reference_jobs',0)):
            self.assertEqual((after['tables'][table]['count'],after['tables'][table]['projectedSha256']),
                             (count,before['tables'][table]['sha256']))
            self.assertEqual(set(after['tables'][table]['nonDefault'].values()),{0})
        for table in ('viewer_files','viewer_incarnations','viewer_option_evidence'):
            self.assertEqual(after['tables'][table],before['tables'][table])
        self.assert_retained_viewer_bytes()
        return after

    def test_census_pinned_defaults_equal_the_pinned_migration(self):
        tree=ast.parse(inspect.getsource(module.volume_census))
        pinned=[ast.literal_eval(node.value) for node in ast.walk(tree)
                if isinstance(node,ast.Assign) and getattr(node.targets[0],'id',None)=='pinned']
        self.assertEqual(len(pinned),1)
        self.assertEqual({(table,column) for table,columns in pinned[0].items() for column in columns},
                         {(table,column) for table,column,_statement in module.MIGRATION_COLUMNS})
        for table,column,statement in module.MIGRATION_COLUMNS:
            db=sqlite3.connect(':memory:')
            try:
                db.execute('CREATE TABLE '+table+' (id INTEGER PRIMARY KEY)');db.execute('INSERT INTO '+table+'(id) VALUES(1)')
                db.execute(statement)
                self.assertEqual(db.execute('SELECT '+column+' FROM '+table).fetchone()[0],pinned[0][table][column])
            finally:db.close()

    def test_old_volume_five_retained_tasks_migrate_through_update(self):
        before=self.five_retained()
        self.assertEqual(self.activate()['phase'],'completed')
        self.assertTrue({'full_option_evidence_required','node_guarded'}<=set(self.columns('viewer_tasks')))
        self.assert_projected_rows(before)

    def test_old_volume_five_retained_tasks_rollback_accepts_exact_migration(self):
        before=self.five_retained();self.fail_after_start()
        self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back')
        restored=self.transport.inspected(module.NAME)
        self.assertEqual((restored['Id'],restored['Image']),(module.OLD_ID,module.OLD_IMAGE))
        # The additive schema stays; every retained row and byte is unchanged.
        self.assertIn('node_guarded',self.columns('viewer_tasks'))
        self.assert_projected_rows(before)
        self.assertFalse(self.state['databaseRestored'])

    def test_retained_row_migration_negative_cases(self):
        before=self.five_retained();old=self.root/'five-old-copy';shutil.copytree(self.volume,old)
        self.migrate();after=module.volume_census(self.volume,uid=os.geteuid())
        allow=dict(allow_owned=True,allow_viewer_additions=True,allow_migration=True)
        module.compatible(after,before,**allow)
        def variant(name,statements,base=self.volume):
            target=self.root/name;shutil.copytree(base,target)
            db=sqlite3.connect(target/'jobs.sqlite3')
            try:
                with db:
                    for statement in statements:db.execute(statement)
            finally:db.close()
            return module.volume_census(target,uid=os.geteuid(),offline=True)
        for name,statements in (
                ('nondefault-pinned',["UPDATE viewer_tasks SET node_guarded=1 WHERE directory='task-0'"]),
                ('nondefault-evidence',["UPDATE viewer_tasks SET full_option_evidence_required=1 WHERE directory='task-2'"]),
                ('old-column-drift',["UPDATE viewer_tasks SET name='altered' WHERE directory='task-3'"]),
                ('row-removed',[*("DELETE FROM "+table+" WHERE uuid IN (SELECT uuid FROM viewer_tasks WHERE directory='task-4')"
                    for table in ('viewer_files','viewer_incarnations','viewer_option_evidence')),"DELETE FROM viewer_tasks WHERE directory='task-4'"]),
                ('extra-column',['ALTER TABLE viewer_tasks ADD COLUMN unreviewed INTEGER NOT NULL DEFAULT 0'])):
            with self.subTest(case=name):
                with self.assertRaisesRegex(module.Stopped,'unreviewed_migration|full_census_changed'):
                    module.compatible(variant(name,statements),before,**allow)
        for name,statements in (
                ('nonterminal',["UPDATE viewer_tasks SET phase='processing' WHERE directory='task-1'"]),
                ('unreviewed-table',['CREATE TABLE viewer_unreviewed (uuid TEXT PRIMARY KEY)']),
                ('nonempty-pinned-table',["INSERT INTO viewer_full_option_evidence VALUES((SELECT uuid FROM viewer_tasks WHERE directory='task-0'),1,'[]','x')"])):
            with self.subTest(case=name):
                with self.assertRaises(AssertionError):variant(name,statements)
        # A base holding only part of a table's pinned columns is not reviewed.
        partial=variant('partial-base',['ALTER TABLE viewer_tasks ADD COLUMN node_guarded INTEGER NOT NULL DEFAULT 0 CHECK(node_guarded IN (0,1))'],old)
        with self.assertRaisesRegex(module.Stopped,'unreviewed_migration_base'):module.compatible(after,partial,**allow)
        # NULL-aware non-default counts: NULL differs from 0 and 0 from NULL.
        for table,column,value in (('viewer_tasks','node_guarded',1),('reference_jobs','viewer_receipt_required',1)):
            with self.subTest(crafted=column):
                crafted=copy.deepcopy(after);crafted['tables'][table]['nonDefault'][column]=value
                with self.assertRaises(module.Stopped):module.compatible(crafted,before,**allow)
        db=sqlite3.connect(':memory:')
        try:
            db.execute('CREATE TABLE t (v INTEGER)');db.executemany('INSERT INTO t VALUES(?)',[(None,),(0,),(1,)])
            self.assertEqual([db.execute('SELECT COUNT(*) FROM t WHERE v IS NOT ?',(default,)).fetchone()[0] for default in (None,0)],[2,2])
        finally:db.close()

    def test_legacy_null_viewer_receipt_policy_is_held(self):
        """Rows the predecessor inserts on migrated state get the legacy policy."""
        from processing.maintenance import Conflict
        self.five_retained();self.migrate()
        job_id=str(__import__('uuid').uuid4())
        with self.queue._db() as db:
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES(?,?,'staging','reference-pipeline-v1','succeeded')",
                       (job_id,'reference:synthetic-legacy'))
            # Exact 693cb7e column lists: neither names a pinned column.
            db.execute("""INSERT INTO reference_jobs(job_id,submission_id,request_digest,environment,input_json,
              manifest_sha,pipeline_digest,phase) VALUES(?,?,?,?,?,?,?,'queued')""",
              (job_id,'synthetic-legacy','a'*64,'staging','{}','b'*64,'c'*64))
            db.execute('''INSERT OR REPLACE INTO viewer_tasks
                (uuid,generation,directory,name,options,outputs,environment,option_evidence_required)
                VALUES(?,?,?,?,?,?,?,?)''',('synthetic-legacy-task',1,'synthetic-legacy-task','n','[]','[]','staging',0))
            self.assertIsNone(db.execute('SELECT viewer_receipt_required FROM reference_jobs WHERE job_id=?',(job_id,)).fetchone()[0])
            self.assertEqual(tuple(db.execute("SELECT full_option_evidence_required,node_guarded FROM viewer_tasks WHERE uuid='synthetic-legacy-task'").fetchone()),(0,0))
            db.execute("UPDATE reference_jobs SET phase='completed',result_json='{}' WHERE job_id=?",(job_id,))
        store=ReferenceStore(self.queue,viewer_required_environments=('staging',))
        self.assertEqual(store.audit_viewer_acceptance_page(),{'state':'reconcile'})
        self.assertEqual(store.pending_viewer_transfer_jobs(),[])
        with self.assertRaisesRegex(Conflict,'legacy Viewer acceptance policy requires reconciliation'):store.accept(job_id,{})
        # A later update census holds on any predecessor-admitted row.
        with self.assertRaises(AssertionError):module.volume_census(self.volume,uid=os.geteuid())

    def test_prepare_refuses_predecessor_config_it_cannot_read(self):
        for version in (3,4,5,None):
            with self.subTest(version=version):
                self.transport.config=dict(self.transport.config,schemaVersion=version)
                operation=self.root/('config-'+str(version));operation.mkdir(mode=0o700)
                updater=module.ReferenceCoordinatorUpdate(operation,gate_factory=self.gates,transport=self.transport,cluster_probe=lambda state:None)
                with self.assertRaisesRegex(module.Stopped,'predecessor_config_schema_unsupported'):updater.prepare(TARGET,SOURCE,MAIN)
        self.assertEqual(self.transport.calls,[])

    def test_rollback_refused_when_predecessor_config_changes(self):
        self.five_retained()
        self.assertEqual(self.state['predecessorConfig'],{'path':'runtime.json','bytes':512,'sha256':'a'*64,'schemaVersion':1})
        self.fail_after_start()
        calls=len(self.transport.calls)
        for name,change in (('v2-new-bytes',dict(schemaVersion=2,sha256='2'*64)),('v3',dict(schemaVersion=3)),
                            ('v4',dict(schemaVersion=4,sha256='4'*64)),('v5',dict(schemaVersion=5)),
                            ('same-version-new-bytes',dict(sha256='f'*64,bytes=513)),('unparsed',dict(schemaVersion=None))):
            with self.subTest(case=name):
                self.transport.config=dict(self.state['predecessorConfig'],**change)
                with self.assertRaisesRegex(module.Stopped,'predecessor_config'):self.updater.rollback(self.state)
                self.assertEqual(len(self.transport.calls),calls)
                self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])
                self.assertFalse(self.transport.containers[module.OLD_ID]['State']['Running'])
        # A checkpoint without the captured config never starts the predecessor.
        self.transport.config=dict(self.state['predecessorConfig'])
        captured=self.state.pop('predecessorConfig')
        with self.assertRaisesRegex(module.Stopped,'predecessor_config'):self.updater.rollback(self.state)
        self.state['predecessorConfig']=captured
        self.assertEqual(len(self.transport.calls),calls)
        self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back')
        self.assert_retained_viewer_bytes()

    def make_transport(self):
        owner=self
        class Transport:
            save=staticmethod(module.legacy.save);private_file=staticmethod(module.legacy.private_file)
            def __init__(self):
                self.calls=[];self.loss=None;self.hook=None
                self.config={'path':'runtime.json','bytes':512,'sha256':'a'*64,'schemaVersion':1}
                self.application={'PROCESSING_API_TOKEN':TOKEN,'PROCESSING_PROTOTYPE_OPERATOR_TOKEN':OPERATOR,
                    'PROCESSING_REFERENCE_CONFIG':module.legacy.CONFIG_PATH}
                self.old=self.container(module.OLD_ID,module.OLD_IMAGE,module.NAME,True)
                self.containers={module.OLD_ID:self.old,
                    '6'*64:self.container('6'*64,'sha256:'+'6'*64,'historical-native-untouched',False)}
            def image(self,image):
                version=module.OLD_SOURCE if image==module.OLD_IMAGE else SOURCE
                defaults=['PATH=/old/default'] if image==module.OLD_IMAGE else ['PATH=/new/default']
                return dict(Id=image,Config=dict(Env=defaults,Labels={'org.opencontainers.image.revision':version},
                    User='10001:10001',WorkingDir='/app',Cmd=['python','-m','processing.app'],Entrypoint=None,Healthcheck={'Test':['CMD','health']}))
            def container(self,identifier,image,name,running):
                config=self.image(image)['Config'].copy()
                defaults=dict(item.split('=',1) for item in config['Env'])
                config['Env']=[key+'='+value for key,value in dict(defaults,**self.application).items()]
                config['Labels']={}
                return dict(Id=identifier,Image=image,Name='/'+name,Config=config,
                    State={'Running':running,'Health':{'Status':'healthy'}},
                    HostConfig=dict(Init=True,NetworkMode='bridge',ReadonlyRootfs=True,Privileged=False,PidsLimit=64,NanoCpus=500000000,
                        Memory=268435456,MemorySwap=536870912,IpcMode='private',ShmSize=67108864,AutoRemove=False,
                        CapDrop=['ALL'],CapAdd=None,SecurityOpt=['no-new-privileges'],RestartPolicy={'Name':'unless-stopped'},
                        LogConfig={'Type':'json-file','Config':{}},PortBindings=module.legacy.REFERENCE_PORTS),
                    Mounts=[{'Type':'volume','Name':module.VOLUME,'Destination':'/prototype-state','RW':True},
                        {'Type':'bind','Source':str(module.legacy.TLS_MOUNT),'Destination':'/run/processing-reference','RW':False}])
            def facts(self,item,**kwargs):return module.legacy.facts(item,**kwargs)
            def environment(self):return {key:value for key,value in self.application.items() if key!='PROCESSING_REFERENCE_CONFIG'}
            def inspected(self,identifier):
                if identifier==module.CLUSTER_NAME:return {'Id':'f'*64,'Image':'sha256:'+'f'*64,'State':{'Running':True}}
                matches=[item for key,item in self.containers.items() if key.startswith(identifier) or item['Name']=='/'+identifier]
                if len(matches)!=1:raise module.Stopped('synthetic_identity_unavailable')
                return copy.deepcopy(matches[0])
            def local_clients(self):return self.environment(),owner.api,owner.operator
            def sole_owner(self,identifier):
                if [item['Id'] for item in self.containers.values() if item['State']['Running']]!=[identifier]:raise module.Stopped('synthetic_other_owner')
            def activity(self,api,*,retained_viewer=False):
                assert retained_viewer
                assert api.json('GET','/v1/prototype/authority')['authorityId']==module.AUTHORITY
                jobs=api.json('GET','/v1/prototype/jobs')['jobs']
                assert all(job['status'] in ('succeeded','failed') for job in jobs)
                return dict(count=len(jobs),sha256=module.digest(sorted(jobs,key=lambda row:row['id'])))
            def candidate_loader(self,*args):pass
            def health(self,*args,**kwargs):pass
            def verify_reference(self):return {'tlsVerified':True,'nativeProductionCredentialsRejected':True}
            def docker(self,*args,**kwargs):
                action=args[0]
                if action=='ps':
                    if any(str(value).startswith('volume=') for value in args):return '\n'.join(item['Id'] for item in self.containers.values() if item['State']['Running'])
                    if any(str(value).startswith('label=') for value in args):return '\n'.join(item['Id'] for item in self.containers.values() if item['Config']['Labels'].get(module.LABEL)==owner.state['operation'])
                    return ''
                if action in ('run','exec'):
                    if args[-1]==module.explicit_guard_source(module.TLS_CENSUS):return json.dumps([{'path':'runtime.json','uid':10001,'mode':0o600,'sha256':'a'*64}])
                    if args[-1]==module.explicit_guard_source(module.CONFIG_CENSUS):return json.dumps(self.config)
                    if action=='run':
                        assert '--network' in args and args[args.index('--network')+1]=='none'
                        assert 'type=volume,src='+module.VOLUME+',dst=/source,readonly' in args
                    return json.dumps(module.volume_census(owner.volume,uid=os.geteuid(),offline=action=='run'))
                assert owner.gates.live and not owner.gates.dead
                self.calls.append(args)
                if self.hook:self.hook(action)
                if action=='create':
                    item=self.container(NEW_ID,TARGET,module.NAME,False)
                    mounts=[args[index+1] for index,value in enumerate(args) if value=='--tmpfs']
                    if mounts:item['HostConfig']['Tmpfs']=dict(mount.split(':',1) for mount in mounts)
                    item['Config']['Labels'][module.LABEL]=owner.state['operation'];self.containers[NEW_ID]=item
                    result=NEW_ID
                elif action=='rename':self.containers[args[1]]['Name']='/'+args[2];result=''
                elif action in ('start','stop'):
                    self.containers[args[-1]]['State']['Running']=action=='start';result=args[-1]
                    if action=='start' and args[-1]==NEW_ID:
                        with owner.queue._db() as db:
                            db.execute('''CREATE TABLE IF NOT EXISTS owned_maintenance_schedules (
                                request_id TEXT PRIMARY KEY,node_id TEXT NOT NULL,authority_id TEXT NOT NULL,
                                request_sha256 TEXT NOT NULL,request_json TEXT NOT NULL,receipt_json TEXT NOT NULL)''')
                else:raise AssertionError(args)
                if self.loss==action:self.loss=None;raise OSError('synthetic lost mutation response')
                return result
        return Transport()

    def backup(self,transport,root,state):
        destination=root/'copied-volume';shutil.copytree(self.volume,destination)
        before=module.volume_census(self.volume,uid=os.geteuid(),offline=True)
        after=module.volume_census(destination,uid=os.geteuid(),offline=True)
        self.assertEqual(before,after)
        return dict(fullCensusSha256=module.digest(before),treeSha256=module.digest(before['files']),sqliteQuickCheck='ok')

    def activate(self):return self.updater.activate(self.state)

    def test_full_reference_recipe_application_defaults_and_tls_ports_preserved(self):
        self.assertEqual(self.activate()['phase'],'completed')
        current=self.transport.inspected(module.NAME)
        self.assertEqual(current['Id'],NEW_ID)
        self.assertEqual(current['HostConfig']['PortBindings'],module.legacy.REFERENCE_PORTS)
        self.assertEqual(current['HostConfig']['Tmpfs'],module.TMPFS)
        self.assertNotIn('Tmpfs',self.transport.inspected(self.state['rollbackName'])['HostConfig'])
        self.assertEqual(current['Mounts'],self.transport.old['Mounts'])
        self.assertIn('PATH=/new/default',current['Config']['Env'])
        self.assertEqual(sum(call[0]=='create' for call in self.transport.calls),1)
        self.assertFalse(any('6'*64 in call for call in self.transport.calls))
        self.assertEqual({self.queue.get_node(node)['state'] for node in ('cluster','main')}, {'active'})
        self.assertFalse(self.state['databaseRestored'])

    def test_create_adds_only_the_reviewed_private_tmp_within_unchanged_limits(self):
        self.assertFalse(module.ACTIVATION_REVIEWED)
        self.assertEqual(module.TMPFS,{'/tmp':'rw,nosuid,nodev,noexec,size=64m,mode=1777'})
        args=self.updater.create_args(self.state)
        self.assertEqual([args[index+1] for index,value in enumerate(args) if value=='--tmpfs'],
                         ['/tmp:rw,nosuid,nodev,noexec,size=64m,mode=1777'])
        for flag,value in (('--memory','256m'),('--memory-swap','512m'),('--shm-size','64m'),('--pids-limit','64')):
            self.assertEqual(args[args.index(flag)+1],value)
        self.assertIn('--read-only',args)

    def test_authority_lease_recipe_is_exact_and_binding_is_pinned(self):
        lease='/private/authority'; binding={'path':lease,'device':1,'inode':2,'lockDevice':1,'lockInode':3}
        self.updater.lease_dir=lease
        state=dict(self.state,leaseBinding=binding)
        with patch.object(module,'lease_binding',return_value=binding):
            args=self.updater.create_args(state)
            self.assertIn('PROCESSING_AUTHORITY_LEASE_DIR=/run/ledgetop-authority-lease',args)
            self.assertIn('type=bind,src='+lease+',dst=/run/ledgetop-authority-lease',args)
            item=copy.deepcopy(self.transport.inspected(module.NAME))
            item['Mounts'].append(dict(Type='bind',Source=lease,Destination='/run/ledgetop-authority-lease',RW=True))
            self.updater.facts(item)
            item['Mounts'][-1]['RW']=False
            with self.assertRaisesRegex(module.Stopped,'authority_lease_mount_changed'):self.updater.facts(item)
        with patch.object(module,'lease_binding',return_value={**binding,'lockInode':4}):
            with self.assertRaisesRegex(module.Stopped,'lease_binding_changed'):self.updater.create_args(state)

    def test_candidate_without_exact_tmpfs_is_refused_before_start_and_held(self):
        real_docker=self.transport.docker
        def create_with_wrong_tmpfs(*args,**kwargs):
            result=real_docker(*args,**kwargs)
            if args[0]=='create':self.transport.containers[NEW_ID]['HostConfig']['Tmpfs']={'/tmp':'rw,size=1g'}
            return result
        with patch.object(self.transport,'docker',side_effect=create_with_wrong_tmpfs):
            with self.assertRaisesRegex(module.Stopped,'candidate_tmpfs_changed'):self.activate()
        self.assertEqual(self.state['phase'],'needs_inspection')
        self.assertFalse(any(call[0]=='start' for call in self.transport.calls))
        before=len(self.transport.calls)
        with self.assertRaisesRegex(module.Stopped,'candidate_tmpfs_changed'):self.updater.rollback(self.state)
        self.assertEqual(len(self.transport.calls),before)
        self.assert_retained_viewer_bytes()

    def test_predecessor_with_tmpfs_is_not_the_reviewed_primary(self):
        self.transport.containers[module.OLD_ID]['HostConfig']['Tmpfs']=dict(module.TMPFS)
        before=len(self.transport.calls)
        with self.assertRaisesRegex(module.Stopped,'exact_current_reference_primary_required'):self.activate()
        self.assertEqual(len(self.transport.calls),before)

    def test_lost_legacy_schedule_ack_holds_no_adoption_or_repost(self):
        original=self.operator.json;posts=[]
        def lose(method,route,payload=None,**kwargs):
            value=original(method,route,payload,**kwargs)
            if route.endswith('/schedule'):posts.append(payload);raise OSError('lost schedule ACK')
            return value
        with patch.object(self.operator,'json',side_effect=lose):
            with self.assertRaises(OSError):self.activate()
            with self.assertRaises(module.Stopped):self.activate()
            with self.assertRaises(module.Stopped):self.updater.rollback(self.state)
        self.assertEqual(len(posts),1);self.assertIn('pendingPlan',self.state)
        self.assertEqual(self.transport.calls,[])
        self.assertTrue(self.transport.old['State']['Running'])

    def test_lost_create_response_recovers_exact_label_never_recreates(self):
        self.transport.loss='create'
        with self.assertRaises(OSError):self.activate()
        self.assertEqual(self.updater.candidate(self.state)['Id'],NEW_ID)
        self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back')
        self.assertTrue(self.transport.containers[module.OLD_ID]['State']['Running'])
        self.assertEqual(sum(call[0]=='create' for call in self.transport.calls),1)

    def test_lost_old_stop_response_is_read_back_on_explicit_rollback_without_second_stop(self):
        self.transport.loss='stop'
        with self.assertRaises(OSError):self.activate()
        self.assertFalse(self.transport.old['State']['Running'])
        with self.assertRaises(module.Stopped):self.activate()
        self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back')
        self.assertEqual([call[0] for call in self.transport.calls],['stop','start'])

    def test_lease_backed_predecessor_rollback_preserves_original_mount(self):
        lease='/private/authority';binding={'path':lease,'device':1,'inode':2,'lockDevice':1,'lockInode':3}
        self.updater.lease_dir=lease;self.state['leaseBinding']=binding
        self.transport.old['Mounts'].append(dict(Type='bind',Source=lease,Destination='/run/ledgetop-authority-lease',RW=True))
        self.transport.old['Config']['Env'].append('PROCESSING_AUTHORITY_LEASE_DIR=/run/ledgetop-authority-lease')
        self.transport.loss='stop'
        with patch.object(module,'lease_binding',return_value=binding):
            with self.assertRaises(OSError):self.activate()
            self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back')
        self.assertTrue(self.transport.old['State']['Running'])
        self.assertEqual([call[0] for call in self.transport.calls],['stop','start'])

    def test_lost_old_rename_response_never_repeats_rename_or_creates_candidate(self):
        self.transport.loss='rename'
        with self.assertRaises(OSError):self.activate()
        self.assertEqual(self.transport.old['Name'],'/'+self.state['rollbackName'])
        self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back')
        self.assertEqual([call[0] for call in self.transport.calls],['stop','rename','rename','start'])

    def test_gate_loss_before_stop_leaves_original_running_and_no_following_mutation(self):
        original=self.updater.save
        def lost(state,phase=None,**kwargs):
            original(state,phase,**kwargs)
            if phase=='stop_old_pending':self.gates.dead=True
        with patch.object(self.updater,'save',side_effect=lost):
            with self.assertRaises(module.Stopped):self.activate()
        self.assertEqual(self.transport.calls,[])
        self.assertTrue(self.transport.old['State']['Running'])

    def test_private_content_admitted_during_old_stop_blocks_before_backup_or_create(self):
        def late(action):
            if action=='stop':
                with self.queue._db() as db:db.execute("UPDATE jobs SET claim_hash='new-private-content' WHERE seq=1")
        self.transport.hook=late
        with self.assertRaises(module.Stopped):self.activate()
        self.assertEqual([call[0] for call in self.transport.calls],['stop'])
        self.assertFalse((self.operation/'copied-volume').exists())

    def fail_after_start(self):
        self.transport.loss='start'
        with self.assertRaises(OSError):self.activate()
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])

    def test_compatible_empty_owned_table_allows_exact_reference_tls_rollback(self):
        self.fail_after_start()
        self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back')
        restored=self.transport.inspected(module.NAME)
        self.assertEqual((restored['Id'],restored['Image']),(module.OLD_ID,module.OLD_IMAGE))
        self.assertEqual(restored['HostConfig']['PortBindings'],module.legacy.REFERENCE_PORTS)

    def test_receipt_admitted_while_new_stops_refuses_old_and_restarts_only_new(self):
        self.fail_after_start()
        def late(action):
            if action=='stop':
                with self.queue._db() as db:db.execute("INSERT INTO owned_maintenance_schedules VALUES('new','main','a','b','{}','{}')")
        self.transport.hook=late
        calls=len(self.transport.calls)
        with self.assertRaises(module.Stopped):self.updater.rollback(self.state)
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])
        self.assertFalse(self.transport.containers[module.OLD_ID]['State']['Running'])
        self.assertEqual([row[0] for row in self.transport.calls[calls:]],['stop','start'])
        self.assertTrue(self.state['downgradeRefused'])

    def test_partial_reopen_unknown_second_ack_cannot_rollback_or_cancel_peer(self):
        original=self.operator.json;cancels=[]
        def lose(method,route,payload=None,**kwargs):
            if route.endswith('/cancel'):
                cancels.append(route)
                if len(cancels)==2:raise OSError('second reopen unknown before send')
            return original(method,route,payload,**kwargs)
        with patch.object(self.operator,'json',side_effect=lose):
            with self.assertRaises(module.Stopped):self.activate()
        before=len(self.transport.calls)
        with self.assertRaises(module.Stopped):self.updater.rollback(self.state)
        self.assertEqual(len(self.transport.calls),before)
        self.assertTrue(self.state['reopeningStarted'])
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])

    def test_gate_dies_between_reopens_no_second_cancel_or_old_restart(self):
        original=self.operator.json;cancels=[]
        def lose(method,route,payload=None,**kwargs):
            value=original(method,route,payload,**kwargs)
            if route.endswith('/cancel'):cancels.append(route);self.gates.dead=True
            return value
        with patch.object(self.operator,'json',side_effect=lose):
            with self.assertRaises(module.Stopped):self.activate()
        self.assertEqual(cancels,['/v1/prototype/nodes/cluster/cancel'])
        self.assertEqual(self.queue.get_node('main')['state'],'draining')
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])

    def test_unavailable_stopped_census_restarts_exact_candidate_never_old(self):
        self.fail_after_start();original=self.updater.census
        def unavailable(state,**kwargs):
            if kwargs.get('offline'):raise OSError('synthetic offline read unavailable')
            return original(state,**kwargs)
        before=len(self.transport.calls)
        with patch.object(self.updater,'census',side_effect=unavailable):
            with self.assertRaises(module.Stopped):self.updater.rollback(self.state)
        self.assertEqual([call[0] for call in self.transport.calls[before:]],['stop','start'])
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])
        self.assertFalse(self.transport.containers[module.OLD_ID]['State']['Running'])

    def test_online_new_owned_receipt_refuses_before_any_candidate_stop(self):
        self.fail_after_start()
        with self.queue._db() as db:db.execute("INSERT INTO owned_maintenance_schedules VALUES('new','main','a','b','{}','{}')")
        before=len(self.transport.calls)
        with self.assertRaises(AssertionError):self.updater.rollback(self.state)
        self.assertEqual(len(self.transport.calls),before)
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])

    def terminal_receipt_row(self):
        # Populate the actual service-owned schema. Even an isolated receipt
        # row is forbidden by this updater's empty-reference baseline policy.
        with self.queue._db() as db:
            db.execute('INSERT INTO reference_terminal_acceptances VALUES(?,?,?,?,?,?)',
                ('terminal-receipt','storage-receipt','synthetic-job',1,'a'*64,'{}'))

    def test_real_terminal_service_empty_schema_is_hashed_and_preserved(self):
        baseline=module.volume_census(self.volume,uid=os.geteuid())
        self.assertEqual(baseline['tables']['reference_terminal_acceptances']['count'],0)
        names=[row[1] for row in baseline['schema'] if row[2]=='reference_terminal_acceptances']
        self.assertEqual(len(names),4)  # Table plus three actual UNIQUE indexes.
        self.assertIn('reference_terminal_acceptances',names)
        self.assertEqual(self.activate()['phase'],'completed')

    def test_viewer_receipt_tables_are_censused_and_nonempty_blocks_prepare_and_rollback(self):
        baseline=module.volume_census(self.volume,uid=os.geteuid())
        for name in ('reference_viewer_receipts','reference_viewer_transfers'):
            self.assertEqual(baseline['tables'][name]['count'],0)
            self.assertTrue(any(row[0]=='table' and row[1]==name for row in baseline['schema']))
            missing=copy.deepcopy(baseline)
            missing['tables'].pop(name)
            missing['schema']=[row for row in missing['schema'] if row[2]!=name]
            with self.assertRaises(module.Stopped):module.compatible(baseline,missing)
        def with_row(table):
                # A partial or orphaned row must still block a downgrade.
                with sqlite3.connect(self.queue.path) as db:
                    if table=='reference_viewer_receipts':
                        db.execute("INSERT INTO reference_viewer_receipts(receipt_id,storage_receipt_id,acceptance_receipt_id,job_id,digest,receipt_json) VALUES('receipt','storage','acceptance','synthetic',?, '{}')",('a'*64,))
                    else:
                        db.execute("INSERT INTO reference_viewer_transfers(job_id,receipt_id,acceptance_json) VALUES('synthetic','receipt','{}')")
        for table in ('reference_viewer_receipts','reference_viewer_transfers'):
            with self.subTest(stage='prepare',table=table):
                with_row(table);before=len(self.transport.calls)
                fresh=self.root/('fresh-'+table);fresh.mkdir(mode=0o700)
                updater=module.ReferenceCoordinatorUpdate(fresh,gate_factory=self.gates,transport=self.transport,cluster_probe=lambda state:None)
                with self.assertRaises(AssertionError):updater.prepare(TARGET,SOURCE,MAIN)
                self.assertEqual(len(self.transport.calls),before)
                with sqlite3.connect(self.queue.path) as db:db.execute('DELETE FROM '+table)
        self.fail_after_start()
        for table in ('reference_viewer_receipts','reference_viewer_transfers'):
            with self.subTest(stage='rollback',table=table):
                with_row(table);before=len(self.transport.calls)
                with self.assertRaises(AssertionError):self.updater.rollback(self.state)
                self.assertEqual(len(self.transport.calls),before)
                self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])
                with sqlite3.connect(self.queue.path) as db:db.execute('DELETE FROM '+table)

    def test_terminal_receipt_refuses_fresh_prepare_before_any_drain(self):
        self.terminal_receipt_row()
        fresh=self.root/'fresh-operation';fresh.mkdir(mode=0o700)
        updater=module.ReferenceCoordinatorUpdate(fresh,gate_factory=self.gates,transport=self.transport,cluster_probe=lambda state:None)
        with self.assertRaises(AssertionError):updater.prepare(TARGET,SOURCE,MAIN)
        checkpoint=json.loads((fresh/'checkpoint.json').read_text())
        self.assertEqual((checkpoint['phase'],checkpoint['plans']),('preparing',{}))
        self.assertEqual(self.transport.calls,[])
        self.assertTrue(self.transport.old['State']['Running'])

    def test_terminal_receipt_refuses_downgrade_before_any_candidate_stop(self):
        self.fail_after_start();self.terminal_receipt_row()
        before=len(self.transport.calls)
        with self.assertRaises(AssertionError):self.updater.rollback(self.state)
        self.assertEqual(len(self.transport.calls),before)
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])
        self.assertFalse(self.transport.containers[module.OLD_ID]['State']['Running'])

    def test_foreign_candidate_and_legacy_checkpoint_refused_without_mutation(self):
        self.fail_after_start();self.transport.containers[NEW_ID]['Config']['Labels'][module.LABEL]='foreign'
        before=len(self.transport.calls)
        with self.assertRaises(module.Stopped):self.updater.rollback(self.state)
        self.assertEqual(len(self.transport.calls),before)
        changed=dict(self.state,schemaVersion=1)
        with self.assertRaises(module.Stopped):self.updater.rollback(changed)

    def test_census_all_private_rows_schema_and_files_are_bound(self):
        baseline=module.volume_census(self.volume,uid=os.geteuid())
        with self.queue._db() as db:db.execute("UPDATE jobs SET claim_hash='changed' WHERE seq=1")
        with self.assertRaises(module.Stopped):module.compatible(module.volume_census(self.volume,uid=os.geteuid()),baseline)
        with self.queue._db() as db:db.execute('CREATE TABLE unknown_progress(value TEXT)')
        with self.assertRaises(AssertionError):module.volume_census(self.volume,uid=os.geteuid())

    def test_actual_copy_script_hashes_copied_full_database_and_retains_all_bytes(self):
        large=self.volume/'viewer-tasks/task-0/large-synthetic.bin'
        with large.open('wb') as stream:stream.truncate(64*1024*1024+1)
        large.chmod(0o600)
        destination=self.root/'actual-backup';destination.mkdir(mode=0o700)
        code=module.backup_script().replace("Path('/source')",'Path('+repr(str(self.volume))+')').replace("Path('/backup')",'Path('+repr(str(destination))+')')
        code=code.replace('10001',str(os.geteuid())).replace('uid=10001','uid='+str(os.geteuid()))
        output=io.StringIO()
        with redirect_stdout(output):exec(compile(code,'<synthetic-offline-backup>','exec',optimize=2),{})
        proof=json.loads(output.getvalue())
        self.assertEqual(proof['fullCensusSha256'],module.digest(module.volume_census(self.volume,uid=os.geteuid(),offline=True)))
        self.assertEqual((destination/'jobs.sqlite3').read_bytes(),(self.volume/'jobs.sqlite3').read_bytes())
        for relative,content in self.retained.items():self.assertEqual((destination/relative).read_bytes(),content)
        self.assertEqual((destination/'viewer-tasks/task-0/large-synthetic.bin').stat().st_size,large.stat().st_size)
        restored=self.root/'synthetic-restore';shutil.copytree(destination,restored)
        before=module.volume_census(self.volume,uid=os.geteuid(),offline=True)
        after=module.volume_census(restored,uid=os.geteuid(),offline=True)
        before.pop('rootMode');after.pop('rootMode')
        self.assertEqual(after,before)

    def run_actual_backup(self,destination):
        code=module.backup_script().replace("Path('/source')",'Path('+repr(str(self.volume))+')').replace("Path('/backup')",'Path('+repr(str(destination))+')')
        code=code.replace('10001',str(os.geteuid())).replace('uid=10001','uid='+str(os.geteuid()))
        output=io.StringIO()
        with redirect_stdout(output):exec(compile(code,'<synthetic-offline-backup>','exec',optimize=2),{})
        return json.loads(output.getvalue())

    def test_actual_backup_and_restore_keep_each_terminal_stage_with_its_own_bytes(self):
        # A third task, so succeeded, failed and cancelled each own distinct files.
        identifier=str(__import__('uuid').uuid4());base=self.volume/'viewer-tasks/task-2';(base/'images').mkdir(parents=True,mode=0o700)
        for relative,content in (('images/IMG_0001.JPG',b'synthetic cancelled photo'),('all.zip',b'PKsynthetic cancelled partial')):
            (base/relative).write_bytes(content);(base/relative).chmod(0o600);self.retained['viewer-tasks/task-2/'+relative]=content
        with self.queue._db() as db:
            db.execute("""INSERT INTO viewer_tasks(uuid,directory,name,options,outputs,environment,stage,phase,archive_path)
                VALUES(?,'task-2','retained-2','[]','[]','production','cancelled','complete',?)""",(identifier,str(base/'all.zip')))
            db.execute("UPDATE viewer_tasks SET stage='failed' WHERE directory='task-1'")
        expected={'task-0':'succeeded','task-1':'failed','task-2':'cancelled'}
        destination=self.root/'stage-backup';destination.mkdir(mode=0o700)
        proof=self.run_actual_backup(destination)
        before=module.volume_census(self.volume,uid=os.geteuid(),offline=True)
        self.assertEqual(proof['fullCensusSha256'],module.digest(before))
        self.assertEqual(before['tables']['viewer_tasks']['count'],3)
        restored=self.root/'stage-restore';shutil.copytree(destination,restored)
        after=module.volume_census(restored,uid=os.geteuid(),offline=True)
        before.pop('rootMode');after.pop('rootMode')
        self.assertEqual(after,before)
        with sqlite3.connect('file:'+str(restored/'jobs.sqlite3')+'?mode=ro',uri=True) as db:
            stages=dict(db.execute("SELECT directory,stage FROM viewer_tasks WHERE phase='complete'"))
        self.assertEqual(stages,expected)
        for relative,content in self.retained.items():self.assertEqual((restored/relative).read_bytes(),content)
        self.assertEqual(len({(restored/'viewer-tasks'/directory/'all.zip').read_bytes() for directory in expected}),3)

    def test_actual_backup_refuses_a_nonterminal_viewer_task_under_python_optimize(self):
        with self.queue._db() as db:db.execute("UPDATE viewer_tasks SET stage='running',phase='processing' WHERE directory='task-1'")
        destination=self.root/'refused-backup';destination.mkdir(mode=0o700)
        with self.assertRaises(AssertionError):self.run_actual_backup(destination)

    def test_actual_authenticated_owned_get_requires_exact_new_missing_receipt_body(self):
        self.probe.stop()
        with self.queue._db() as db:
            db.execute(next(row[3] for row in module.owned_schema() if row[0]=='table'))
        actual=http.client.HTTPConnection
        def connection(host,port,**kwargs):
            self.assertEqual((host,port),('127.0.0.1',18080))
            return actual('127.0.0.1',self.server.server_port,**kwargs)
        operator=Client('http://127.0.0.1:18080',OPERATOR)
        with patch.object(module.http.client,'HTTPConnection',side_effect=connection):
            self.assertEqual(module.owned_api_probe(operator),{'operatorLookupVerified':True})
            with self.assertRaises(module.Stopped):module.owned_api_probe(Client(operator.origin,TOKEN))
            reply=self.handler._reply
            def old_reply(handler,status,value):
                if '/schedule-owned/' in handler.path:value={'error':'not_found'}
                reply(handler,status,value)
            with patch.object(self.handler,'_reply',old_reply):
                with self.assertRaises(module.Stopped):module.owned_api_probe(operator)

    def test_unowned_private_node_column_change_during_drain_is_not_normalized_away(self):
        original=self.operator.json
        def tamper(method,route,payload=None,**kwargs):
            value=original(method,route,payload,**kwargs)
            if route.endswith('/main/schedule'):
                with self.queue._db() as db:db.execute("UPDATE nodes SET finish_status='foreign' WHERE id='cluster'")
            return value
        with patch.object(self.operator,'json',side_effect=tamper):
            with self.assertRaises(module.Stopped):self.activate()
        self.assertEqual(self.transport.calls,[])

    def test_gate_loss_after_candidate_health_cannot_reopen_nodes(self):
        def health(*args,**kwargs):self.gates.dead=True
        with patch.object(self.transport,'health',side_effect=health):
            with self.assertRaises(module.Stopped):self.activate()
        self.assertFalse(self.state.get('reopeningStarted',False))
        self.assertEqual({self.queue.get_node(node)['state'] for node in ('cluster','main')},{'draining'})
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])

    def test_lost_cancel_ack_reconciles_exact_revision_without_another_cancel(self):
        original=self.operator.json;cancels=[]
        def lose(method,route,payload=None,**kwargs):
            value=original(method,route,payload,**kwargs)
            if route.endswith('/cancel'):
                cancels.append(route);raise OSError('synthetic ACK loss after exact cancel')
            return value
        with patch.object(self.operator,'json',side_effect=lose):self.assertEqual(self.activate()['phase'],'completed')
        self.assertEqual(len(cancels),2)
        self.assertEqual(len(set(cancels)),2)

    def test_gate_loss_during_final_cancel_never_marks_completed(self):
        original=self.operator.json
        def lose(method,route,payload=None,**kwargs):
            value=original(method,route,payload,**kwargs)
            if route.endswith('/main/cancel'):self.gates.dead=True
            return value
        with patch.object(self.operator,'json',side_effect=lose):
            with self.assertRaises(module.Stopped):self.activate()
        self.assertEqual(self.state['phase'],'needs_inspection')
        self.assertTrue(self.state['reopeningStarted'])
        self.assertTrue(self.transport.containers[NEW_ID]['State']['Running'])

    def test_new_admission_after_first_reopen_is_retained_and_does_not_block_second_own_reopen(self):
        original=self.operator.json
        def admit(method,route,payload=None,**kwargs):
            value=original(method,route,payload,**kwargs)
            if route.endswith('/cluster/cancel'):
                self.queue.submit(dict(requestId='admitted-after-reopen',environment='staging',profile='treedar-synthetic-v1'))
            return value
        with patch.object(self.operator,'json',side_effect=admit):self.assertEqual(self.activate()['phase'],'completed')
        self.assertEqual(self.queue.get_node('main')['state'],'active')
        self.assertEqual(len(self.queue.list()),module.REVIEWED_JOB_COUNT+1)
        self.assertEqual(self.queue.list()[-1]['status'],'queued')
        calls=len(self.transport.calls)
        with self.assertRaises(module.Stopped):self.updater.rollback(self.state)
        self.assertEqual(len(self.transport.calls),calls)

    def test_lost_old_start_ack_retry_keeps_restored_old_sole_owner(self):
        self.fail_after_start()
        self.transport.loss='start'
        with self.assertRaises(OSError):self.updater.rollback(self.state)
        self.assertEqual(self.state['phase'],'rollback_start_old_pending')
        self.assertTrue(self.transport.containers[module.OLD_ID]['State']['Running'])
        before=len(self.transport.calls)
        self.assertEqual(self.updater.rollback(self.state)['phase'],'rolled_back')
        self.assertEqual(len(self.transport.calls),before)
        self.assertFalse(self.transport.containers[NEW_ID]['State']['Running'])

    def test_foreign_volume_owner_prevents_retention_start(self):
        self.fail_after_start()
        self.transport.containers[NEW_ID]['State']['Running']=False
        foreign=self.transport.containers['6'*64];foreign['State']['Running']=True
        calls=len(self.transport.calls)
        with self.gates:
            with self.assertRaises(module.Stopped):self.updater.retain(self.state,self.gates,self.transport.inspected(NEW_ID))
        self.assertEqual(len(self.transport.calls),calls)

    def test_holder_loss_during_slow_read_is_rechecked_before_any_schedule(self):
        original=self.transport.docker
        def lose(*args,**kwargs):
            value=original(*args,**kwargs)
            if args[-1]==module.explicit_guard_source(module.TLS_CENSUS) and self.gates.live:self.gates.dead=True
            return value
        with patch.object(self.transport,'docker',side_effect=lose):
            with self.assertRaises(module.Stopped):self.activate()
        self.assertEqual(self.transport.calls,[])
        self.assertEqual(self.state['plans'],{})

    def test_live_factory_binds_exact_bridge_helper_without_exporting_session_nonce(self):
        from ops.coordinator_gate_bridge import helper_digest
        def binding(node):return dict(engineId='fixed-'+node,nodeId=node,authorityId=module.AUTHORITY,
            coordinatorOrigin='http://127.0.0.1:18080',generation=1,helperSha256=helper_digest())
        config=dict(schemaVersion=1,main=binding('main'),cluster=binding('cluster'),sessionNonce='a'*64,timeoutSeconds=10)
        factory=module.GateFactory(config,operation_id=self.state['operation'],transport=self.transport)
        self.assertEqual(factory.binding,{'main':config['main'],'cluster':config['cluster']})
        self.assertNotIn(config['sessionNonce'],json.dumps(factory.binding))
        config['main']['helperSha256']='b'*64
        with self.assertRaises(module.Stopped):module.GateFactory(config,operation_id=self.state['operation'])

    def test_cluster_probe_actual_node_http_and_fixed_upload_process_census(self):
        from ops.native_node_fixture import NativeNodeFixture
        fixture=NativeNodeFixture(self.root/'native-fixture.sqlite',synthetic_enabled=True)
        fixture.start();self.addCleanup(fixture.close)
        actual_connection=socket.create_connection
        def connection(address,**kwargs):
            self.assertEqual(address,('127.0.0.1',13001))
            return actual_connection(('127.0.0.1',int(fixture.origin.rsplit(':',1)[1])),**kwargs)
        container=dict(Id='f'*64,Name='/'+module.CLUSTER_NAME,Image=self.state['engineImages']['cluster'],State={'Running':True},
            Config={'Labels':{'com.docker.compose.project':module.CLUSTER_PROJECT,'com.docker.compose.service':'nodeodm'}},
            NetworkSettings={'Ports':{'3000/tcp':[{'HostIp':'127.0.0.1','HostPort':'13001'}]}})
        def docker(*args,**kwargs):
            if args[0]=='top':return 'PID COMMAND\n123 /usr/bin/node /var/www/index.js\n'
            if args[2:4]==('/bin/ls','-A'):return '.gitignore\n'
            if args[2]=='/usr/bin/stat':return 'regular file 1\n'
            if args[2]=='/bin/cat':return '*'
            raise AssertionError(args)
        with patch.object(module.socket,'create_connection',side_effect=connection), \
                patch.object(self.transport,'environment',return_value={'LOCAL_NODE_TOKEN':'private-existing-local-token'}), \
                patch.object(self.transport,'inspected',return_value=container),patch.object(self.transport,'docker',side_effect=docker):
            self.updater.cluster_idle(self.state)
            for field, replacement in (('Name', '/node-odx-gpu'), ('Id', '0' * 64),
                                       ('Image', 'sha256:' + '0' * 64)):
                with self.subTest(changed=field):
                    original = container[field]
                    container[field] = replacement
                    try:
                        with self.assertRaises(module.Stopped): self.updater.cluster_idle(self.state)
                    finally:
                        container[field] = original
            with fixture._db() as db:
                db.execute("INSERT INTO tasks(uuid,name,options,committed,status) VALUES('00000000-0000-4000-8000-000000000001','synthetic','[]',1,40)")
            # Zero queue count alone cannot hide a retained completed task.
            with self.assertRaises(module.Stopped):self.updater.cluster_idle(self.state)


class ClusterReadDeadlineTests(unittest.TestCase):
    def trickle(self,mode):
        stopped=threading.Event();finished=threading.Event();requests=[]
        class Handler(socketserver.BaseRequestHandler):
            def handle(self):
                try:
                    request=b''
                    while b'\r\n\r\n' not in request:request+=self.request.recv(4096)
                    requests.append(request)
                    if mode=='headers':prefix=b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nX-Trickle: '
                    else:prefix=b'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 128\r\n\r\n'
                    self.request.sendall(prefix)
                    for _ in range(128):
                        if stopped.wait(.02):break
                        self.request.sendall(b' ')
                except (OSError,ConnectionError):pass
                finally:finished.set()
        server=socketserver.ThreadingTCPServer(('127.0.0.1',0),Handler)
        server.daemon_threads=False;server.block_on_close=True
        thread=threading.Thread(target=server.serve_forever);thread.start()
        actual_connection=socket.create_connection;connections=[]
        def connect(address,**kwargs):
            self.assertEqual(address,('127.0.0.1',13001))
            value=actual_connection(server.server_address,**kwargs);connections.append(value);return value
        try:
            start=time.monotonic()
            with patch.object(module.socket,'create_connection',side_effect=connect):
                with self.assertRaises(module.Stopped) as caught:
                    module.cluster_http_json('/info','private+?&%token',start+.15)
            self.assertLess(time.monotonic()-start,1)
            self.assertNotIn('private',str(caught.exception))
            self.assertEqual([item.fileno() for item in connections],[-1])
            self.assertIn(b'/info?token=private%2B%3F%26%25token HTTP/1.1',requests[0])
        finally:
            stopped.set();server.shutdown();server.server_close();thread.join(2)
        self.assertTrue(finished.is_set());self.assertFalse(thread.is_alive())

    def test_actual_trickled_headers_obey_total_deadline_and_close_socket(self):self.trickle('headers')
    def test_actual_trickled_body_obeys_total_deadline_and_close_socket(self):self.trickle('body')
