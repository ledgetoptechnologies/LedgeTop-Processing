"""Small synthetic tests for disabled per-input physical reservation."""
import errno
import hashlib
import io
import http.client
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from processing import input_space
from processing import config as runtime_config
from processing.app import make_handler
from processing.config import Config
from processing.input_space import InputSpaceUnavailable
from processing.policy import plan
from processing.prototype import Queue
from processing.viewer_http import ViewerFacade
from processing.viewer_tasks import TaskStore, InputLimitExceeded, InputRecoveryRequired
from tests.test_viewer_http import MemoryHandler, multipart, TOKEN, BOUNDARY

@unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'POSIX fallocate required')
class ViewerInputSpaceTests(unittest.TestCase):
    def setUp(self):
        self.root=Path(tempfile.mkdtemp(prefix='lt-input-reservation-')).resolve()
        self.addCleanup(lambda:print('retained synthetic input root:',self.root,flush=True))
        self.q=Queue(self.root/'queue');self.addCleanup(self.q.close)
        self.store=TaskStore(self.q,self.root/'queue'/'viewer-tasks',input_free_floor=0)
        self.identifier=str(uuid.uuid4());self.store.initialize(self.identifier,'synthetic',[],None,'production')
        self.spool=self.root/'spool';self.spool.mkdir(mode=0o700)
        store=self.store
        class Service:
            def __init__(self):self.store=store
            def upload(self,identifier,filename,stream,size):return self.store.upload(identifier,filename,stream,size)
        self.facade=ViewerFacade(Service(),TOKEN,temp_dir=self.spool,input_free_floor=0)
    def request(self,body):
        return MemoryHandler('/task/new/upload/'+self.identifier+'?token='+TOKEN,body,
            Content_Type='multipart/form-data; boundary='+BOUNDARY,Content_Length=str(len(body)))
    def test_real_http_boundary_reserves_spool_and_exact_destination(self):
        payload=b'small-synthetic-image';body=multipart([('images','image.jpg',payload)])
        req=self.request(body)
        with patch('processing.viewer_http.reserve_input',wraps=input_space.reserve_input) as spool, \
             patch('processing.input_space.reserve_input',wraps=input_space.reserve_input) as dest:
            self.assertTrue(self.facade.handle(req))
        self.assertEqual(req.status,200);self.assertEqual(spool.call_count,1);self.assertEqual(dest.call_count,1)
        self.assertEqual(dest.call_args.args[1:],(len(payload),0))
        self.assertEqual(spool.call_args.args[1],len(payload))
        files=self.store.task(self.identifier)['files'];self.assertEqual(len(files),1)
        self.assertEqual(Path(files[0]['path']).read_bytes(),payload)
    def test_real_local_http_upload_uses_reserved_paths(self):
        facade=self.facade
        class Handler(BaseHTTPRequestHandler):
            def log_message(self,*_):pass
            def do_POST(self):
                if not facade.handle(self):self.send_error(404)
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever);thread.start()
        connection=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=3)
        try:
            body=multipart([('images','real-http.jpg',b'synthetic-http')])
            with patch('processing.viewer_http.reserve_input',wraps=input_space.reserve_input) as spool, \
                 patch('processing.input_space.reserve_input',wraps=input_space.reserve_input) as dest:
                connection.request('POST','/task/new/upload/'+self.identifier+'?token='+TOKEN,body,
                                   {'Content-Type':'multipart/form-data; boundary='+BOUNDARY})
                response=connection.getresponse();self.assertEqual(response.status,200);response.read()
            self.assertEqual(spool.call_count,1);self.assertEqual(dest.call_count,1)
            files=self.store.task(self.identifier)['files'];self.assertEqual(Path(files[0]['path']).read_bytes(),b'synthetic-http')
        finally:
            connection.close();server.shutdown();server.server_close();thread.join(5)
            self.assertFalse(thread.is_alive())

    def test_multipart_reserves_each_part_not_remaining_request(self):
        body=multipart([('images','a.jpg',b'a'),('images','b.jpg',b'bb')]);req=self.request(body)
        with patch('processing.viewer_http.reserve_input',wraps=input_space.reserve_input) as spool:
            self.assertTrue(self.facade.handle(req))
        self.assertEqual(req.status,200)
        self.assertEqual([call.args[1] for call in spool.call_args_list],[1,2])
        self.assertEqual(len(self.store.task(self.identifier)['files']),2)

    def test_real_upload_enospc_after_reservation_returns_space_error(self):
        body=multipart([('images','image.jpg',b'payload')]);req=self.request(body)
        with patch.object(self.store,'upload',side_effect=OSError(errno.ENOSPC,'synthetic')):
            self.assertTrue(self.facade.handle(req))
        self.assertEqual(req.status,503)
        self.assertEqual(json.loads(req.wfile.getvalue())['error'],'insufficient_input_space')
        self.assertEqual(self.store.task(self.identifier)['files'],[])

    def test_spool_low_space_returns_503_without_destination_write(self):
        body=multipart([('images','image.jpg',b'x')]);req=self.request(body)
        with patch('processing.input_space.os.fstatvfs',return_value=SimpleNamespace(f_bavail=0,f_frsize=4096)):
            self.assertTrue(self.facade.handle(req))
        self.assertEqual(req.status,503);self.assertEqual(json.loads(req.wfile.getvalue())['error'],'insufficient_input_space')
        self.assertEqual(self.store.task(self.identifier)['files'],[])
    def test_destination_reservation_failure_retains_task_and_removes_only_owned_temp(self):
        body=multipart([('images','image.jpg',b'payload')]);req=self.request(body)
        with patch('processing.input_space.reserve_input',side_effect=InputSpaceUnavailable('synthetic')):
            self.assertTrue(self.facade.handle(req))
        self.assertEqual(req.status,503);self.assertEqual(self.store.task(self.identifier)['files'],[])
        self.assertEqual(list(self.root.rglob('.incoming-*')),[])
    def test_fallocate_failure_is_not_treated_as_success(self):
        with tempfile.TemporaryFile(dir=self.spool) as output:
            with patch('processing.input_space.os.posix_fallocate',side_effect=OSError(errno.ENOSPC,'synthetic')):
                with self.assertRaises(InputSpaceUnavailable):input_space.reserve_input(output.fileno(),1,0)
    def journal_store(self):
        from processing import viewer_tasks, intake_offline_migration
        first=patch.object(viewer_tasks,'INTAKE_JOURNAL_QUALIFIED',True);first.start();self.addCleanup(first.stop)
        second=patch.object(intake_offline_migration,'QUALIFIED',True);second.start();self.addCleanup(second.stop)
        self.q=Queue(self.root/'journal-queue');self.addCleanup(self.q.close)
        directory=self.root/'journal-queue'/'viewer-tasks'
        TaskStore(self.q,directory,input_free_floor=0)
        with self.q._db() as db:intake_offline_migration.install_empty_offline(db)
        self.q._intake_guard_enabled=True
        store=TaskStore(self.q,directory,input_free_floor=0,intake_journal=True)
        store.initialize(self.identifier,'synthetic',[],None,'production')
        return store

    def test_ten_thousand_synthetic_files_first_census_and_cached_check(self):
        import time
        store=self.journal_store()
        with self.q._db() as db:
            row=store._row(db,self.identifier);directory=store._directory(row)
        seed=self.root/'synthetic-seed';seed.write_bytes(b'x')
        rows=[]
        for index in range(10000):
            name='input-'+format(index,'032x');os.link(seed,directory/name)
            rows.append((self.identifier,'photo-%05d.jpg'%index,'photo-%05d.jpg'%index,name,1,hashlib.sha256(b'x').hexdigest(),1))
        with self.q._db() as db:
            db.executemany('INSERT INTO viewer_files(uuid,name_key,name,path,bytes,sha256,is_photo) VALUES(?,?,?,?,?,?,?)',rows)
        start=time.monotonic()
        store.startup_intake_census()
        first=time.monotonic()-start
        start=time.monotonic()
        with self.q._db() as db:store._check_input_recovery(db,store._row(db,self.identifier))
        cached=time.monotonic()-start
        print('synthetic 10000 input census seconds:',round(first,4),'cached:',round(cached,4),flush=True)
        self.assertIn((self.identifier,1),store._intake_reconciled)
        store.remove(self.identifier)
        start=time.monotonic();store.initialize(self.identifier,'next',[],None,'production')
        elapsed=time.monotonic()-start
        with self.q._db() as db:
            self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_intake_snapshot_files_v1 WHERE uuid=? AND generation=1',(self.identifier,)).fetchone()[0],10000)
        print('synthetic 10000 file generation snapshot seconds:',round(elapsed,4),flush=True)
        self.assertEqual((directory/('input-'+format(0,'032x'))).read_bytes(),b'x')
        start=time.monotonic();store.startup_intake_census()
        print('synthetic 10000 historical hash census seconds:',round(time.monotonic()-start,4),flush=True)

    def test_census_over_bound_fails_closed_preserving_files(self):
        from processing import viewer_tasks
        store=self.journal_store()
        with self.q._db() as db:directory=store._directory(store._row(db,self.identifier))
        for index in range(3):(directory/('input-'+format(index,'032x'))).write_bytes(b'x')
        with patch.object(viewer_tasks,'INTAKE_CENSUS_MAX_ENTRIES',2):
            with self.q._db() as db:
                with self.assertRaises(InputRecoveryRequired):store._check_input_recovery(db,store._row(db,self.identifier))
        self.assertEqual(len(list(directory.glob('input-*'))),3)

    def test_actual_queue_rejects_existing_replace_with_recursive_modes(self):
        import sqlite3
        store=self.journal_store();self.facade.service.store=store
        req=self.request(multipart([('images','a.jpg',b'original')]))
        self.assertTrue(self.facade.handle(req));self.assertEqual(req.status,200)
        for recursive in (0,1):
            with self.q._db() as db:
                db.execute('PRAGMA recursive_triggers='+str(recursive))
                with self.assertRaises(sqlite3.IntegrityError):
                    db.execute('INSERT OR REPLACE INTO viewer_tasks SELECT * FROM viewer_tasks WHERE uuid=?',(self.identifier,))
                with self.assertRaises(sqlite3.IntegrityError):
                    db.execute('INSERT OR REPLACE INTO viewer_files SELECT * FROM viewer_files WHERE uuid=?',(self.identifier,))
        self.assertEqual(Path(store.task(self.identifier)['files'][0]['path']).read_bytes(),b'original')

    def test_guarded_remove_reinitialize_archives_generation_and_keeps_bytes(self):
        store=self.journal_store();self.facade.service.store=store
        req=self.request(multipart([('images','a.jpg',b'first-generation')]))
        self.assertTrue(self.facade.handle(req));self.assertEqual(req.status,200)
        old_path=Path(store.task(self.identifier)['files'][0]['path'])
        store.remove(self.identifier)
        store.initialize(self.identifier,'second',[],None,'production')
        with self.q._db() as db:
            row=store._row(db,self.identifier)
            self.assertEqual((row['generation'],row['removed'],row['stage']),(2,0,'uploading'))
            self.assertEqual(db.execute('SELECT COUNT(*) FROM viewer_incarnations WHERE uuid=? AND generation=1',(self.identifier,)).fetchone()[0],1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM viewer_intake_journal_v1 WHERE uuid=? AND generation=1 AND state='committed'",(self.identifier,)).fetchone()[0],1)
        self.assertEqual(old_path.read_bytes(),b'first-generation')
        req=self.request(multipart([('images','b.jpg',b'second-generation')]))
        self.assertTrue(self.facade.handle(req));self.assertEqual(req.status,200)
        self.assertEqual(old_path.read_bytes(),b'first-generation')

    def test_authorized_sql_rejects_forged_snapshot_and_invalid_directory(self):
        import sqlite3
        from processing.viewer_tasks import canonical
        store=self.journal_store();self.facade.service.store=store
        req=self.request(multipart([('images','a.jpg',b'first')]))
        self.assertTrue(self.facade.handle(req));self.assertEqual(req.status,200)
        original=Path(store.task(self.identifier)['files'][0]['path'])
        store.remove(self.identifier)
        with self.q._db() as db:
            db.execute('BEGIN IMMEDIATE')
            row=store._row(db,self.identifier,removed=True)
            metadata=dict(row);files=[dict(item) for item in db.execute('SELECT * FROM viewer_files WHERE uuid=?',(self.identifier,))]
            altered=dict(metadata);altered['name']='forged'
            sql='INSERT INTO viewer_incarnations(uuid,generation,job_id,metadata,files) VALUES(?,?,?,?,?)'
            with self.assertRaises(sqlite3.IntegrityError):db.execute(sql,(self.identifier,1,None,canonical(altered),canonical(files)))
            fake=[{**files[0],'sha256':'0'*64}]
            with self.assertRaises(sqlite3.IntegrityError):db.execute(sql,(self.identifier,1,None,canonical(metadata),canonical(fake)))
            db.execute(sql,(self.identifier,1,None,canonical(metadata),canonical(files)))
            with self.assertRaises(sqlite3.IntegrityError):db.execute('UPDATE viewer_incarnations SET files=?',(canonical([]),))
            with self.assertRaises(sqlite3.IntegrityError):db.execute("UPDATE viewer_files SET uuid='other' WHERE uuid=?",(self.identifier,))
            with self.assertRaises(sqlite3.IntegrityError):db.execute("UPDATE viewer_tasks SET uuid='other' WHERE uuid=?",(self.identifier,))
            for recursive in (0,1):
                db.execute('PRAGMA recursive_triggers='+str(recursive))
                with self.assertRaises(sqlite3.IntegrityError):db.execute('INSERT OR REPLACE INTO viewer_intake_snapshot_files_v1 SELECT * FROM viewer_intake_snapshot_files_v1 WHERE uuid=?',(self.identifier,))
            db.execute('DELETE FROM viewer_files WHERE uuid=?',(self.identifier,))
            for directory in (None,'not-canonical',self.identifier+'-2-xyz'):
                with self.assertRaises(sqlite3.IntegrityError):
                    db.execute("UPDATE viewer_tasks SET generation=2,directory=?,removed=0,stage='uploading' WHERE uuid=?",(directory,self.identifier))
            db.rollback()
        self.assertEqual(original.read_bytes(),b'first')

    def test_guarded_unfinished_generation_cannot_be_removed_or_reset(self):
        import sqlite3
        store=self.journal_store();self.facade.service.store=store
        body=multipart([('images','a.jpg',b'first')]);req=self.request(body)
        with patch('pathlib.Path.unlink',side_effect=OSError('synthetic unlink failure')):
            self.assertTrue(self.facade.handle(req))
        self.assertEqual(req.status,503)
        with self.assertRaises(InputRecoveryRequired):store.remove(self.identifier)
        with self.q._db() as db:
            with self.assertRaises(sqlite3.IntegrityError):
                db.execute("UPDATE viewer_tasks SET generation=2,directory='changed' WHERE uuid=?",(self.identifier,))
        self.assertEqual([p.read_bytes() for p in self.root.rglob('input-*')],[b'first'])

    def test_opt_in_journal_upload_and_exact_retry(self):
        store=self.journal_store();self.facade.service.store=store
        body=multipart([('images','a.jpg',b'first')]);req=self.request(body)
        self.assertTrue(self.facade.handle(req));self.assertEqual(req.status,200)
        with self.q._db() as db:
            self.assertEqual(db.execute('SELECT state FROM viewer_intake_journal_v1').fetchone()[0],'committed')
        retry=self.request(body);self.assertTrue(self.facade.handle(retry));self.assertEqual(retry.status,200)
        self.assertEqual(len(store.task(self.identifier)['files']),1)

    def test_post_commit_unlink_failure_holds_same_live_store(self):
        store=self.journal_store();self.facade.service.store=store
        body=multipart([('images','a.jpg',b'first')]);req=self.request(body)
        with patch('pathlib.Path.unlink',side_effect=OSError('synthetic unlink failure')):
            self.assertTrue(self.facade.handle(req))
        self.assertEqual(req.status,503)
        with self.q._db() as db:
            self.assertEqual(db.execute('SELECT state FROM viewer_intake_journal_v1').fetchone()[0],'cleanup')
        self.assertEqual([p.read_bytes() for p in self.root.rglob('.incoming-*')],[b'first'])
        retry=self.request(body);self.assertTrue(self.facade.handle(retry))
        self.assertEqual(retry.status,503)
        self.assertEqual(json.loads(retry.wfile.getvalue())['error'],'input_recovery_required')
        with self.assertRaises(InputRecoveryRequired):store.commit(self.identifier)

    def test_post_commit_directory_sync_failure_holds_same_live_store(self):
        from processing import viewer_tasks
        store=self.journal_store();self.facade.service.store=store
        body=multipart([('images','a.jpg',b'first')]);req=self.request(body)
        original=viewer_tasks.sync_directory;calls=[0]
        def fail_third(path):
            calls[0]+=1
            if calls[0]==3:raise OSError('synthetic fsync failure')
            return original(path)
        with patch('processing.viewer_tasks.sync_directory',side_effect=fail_third):
            self.assertTrue(self.facade.handle(req))
        self.assertEqual(req.status,503);self.assertEqual(calls[0],3)
        with self.q._db() as db:
            self.assertEqual(db.execute('SELECT state FROM viewer_intake_journal_v1').fetchone()[0],'cleanup')
        retry=self.request(body);self.assertTrue(self.facade.handle(retry))
        self.assertEqual(retry.status,503)
        self.assertEqual(json.loads(retry.wfile.getvalue())['error'],'input_recovery_required')
        self.assertEqual([p.read_bytes() for p in self.root.rglob('input-*')],[b'first'])

    def test_committed_null_digest_holds_after_reopen(self):
        from processing import viewer_tasks
        store=self.journal_store();self.facade.service.store=store
        req=self.request(multipart([('images','a.jpg',b'first')]))
        self.assertTrue(self.facade.handle(req));self.assertEqual(req.status,200)
        from processing import intake_offline_migration
        with self.q._db() as db:db.execute('DROP TRIGGER viewer_intake_guard_update_v1')
        with self.assertRaises(intake_offline_migration.MigrationUnavailable):
            with self.q._db():pass

    def test_final_collision_preserves_both_files_and_holds(self):
        store=self.journal_store();self.facade.service.store=store
        real_link=os.link; collision=[]
        def collide(source,dest,**kwargs):
            Path(dest).write_bytes(b'existing');collision.append(Path(dest))
            return real_link(source,dest,**kwargs)
        req=self.request(multipart([('images','a.jpg',b'new-data')]))
        with patch('processing.viewer_tasks.os.link',side_effect=collide):
            self.assertTrue(self.facade.handle(req))
        self.assertEqual(req.status,503);self.assertEqual(collision[0].read_bytes(),b'existing')
        incoming=list(self.root.rglob('.incoming-*'));self.assertEqual(len(incoming),1)
        self.assertEqual(incoming[0].read_bytes(),b'new-data')
        with self.assertRaises(InputRecoveryRequired):store.commit(self.identifier)

    def test_retry_holds_changed_stored_file_even_same_length(self):
        store=self.journal_store();self.facade.service.store=store
        body=multipart([('images','a.jpg',b'first')]);req=self.request(body)
        self.assertTrue(self.facade.handle(req));self.assertEqual(req.status,200)
        Path(store.task(self.identifier)['files'][0]['path']).write_bytes(b'wrong')
        retry=self.request(body);self.assertTrue(self.facade.handle(retry))
        self.assertEqual(retry.status,503)
        self.assertEqual(json.loads(retry.wfile.getvalue())['error'],'input_recovery_required')

    def test_opt_in_journal_rename_failure_holds_after_reopen(self):
        from processing import viewer_tasks
        store=self.journal_store();self.facade.service.store=store
        original=viewer_tasks.sync_directory
        def fail_after_rename(path):
            if list(Path(path).glob('input-*')):raise OSError('synthetic after rename')
            return original(path)
        req=self.request(multipart([('images','a.jpg',b'payload')]))
        with patch('processing.viewer_tasks.sync_directory',side_effect=fail_after_rename):
            self.assertTrue(self.facade.handle(req))
        self.assertEqual(req.status,503)
        with self.q._db() as db:
            self.assertEqual(db.execute('SELECT state FROM viewer_intake_journal_v1').fetchone()[0],'sealed')
        with patch.object(viewer_tasks,'INTAKE_JOURNAL_QUALIFIED',True):
            restarted=TaskStore(self.q,self.root/'journal-queue'/'viewer-tasks',input_free_floor=0,intake_journal=True)
        self.facade.service.store=restarted
        retry=self.request(multipart([('images','a.jpg',b'payload')]))
        self.assertTrue(self.facade.handle(retry));self.assertEqual(retry.status,503)
        self.assertEqual(json.loads(retry.wfile.getvalue())['error'],'input_recovery_required')
        self.assertEqual([p.read_bytes() for p in self.root.rglob('input-*')],[b'payload'])

    def test_restart_after_rename_failure_holds_and_retains_unregistered_bytes(self):
        body=multipart([('images','image.jpg',b'crash-window-bytes')]); req=self.request(body)
        with patch('processing.viewer_tasks.sync_directory',side_effect=OSError('synthetic crash window')):
            self.assertTrue(self.facade.handle(req))
        self.assertEqual(req.status,503)
        orphan=list(self.root.rglob('input-*')); self.assertEqual(len(orphan),1)
        self.assertEqual(orphan[0].read_bytes(),b'crash-window-bytes')
        restarted=TaskStore(self.q,self.root/'queue'/'viewer-tasks',input_free_floor=0)
        self.facade.service.store=restarted
        retry=self.request(body);self.assertTrue(self.facade.handle(retry))
        self.assertEqual(retry.status,503)
        self.assertEqual(json.loads(retry.wfile.getvalue())['error'],'input_recovery_required')
        with self.assertRaises(InputRecoveryRequired):restarted.commit(self.identifier)
        self.assertEqual(orphan[0].read_bytes(),b'crash-window-bytes')
        self.assertEqual(restarted.task(self.identifier)['files'],[])

    def test_restart_with_incoming_file_holds_without_deleting(self):
        with self.q._db() as db:
            directory=self.store._directory(self.store._row(db,self.identifier))
        incoming=directory/'.incoming-synthetic';incoming.write_bytes(b'partial')
        restarted=TaskStore(self.q,self.root/'queue'/'viewer-tasks',input_free_floor=0)
        with self.assertRaises(InputRecoveryRequired):
            restarted.upload(self.identifier,'a.jpg',io.BytesIO(b'a'),1)
        self.assertEqual(incoming.read_bytes(),b'partial')

    def test_partial_multipart_retry_accepts_exact_existing_part(self):
        body=multipart([('images','a.jpg',b'first'),('images','b.jpg',b'second')]); req=self.request(body)
        original=self.store.upload; calls=[0]
        def fail_second(*args,**kwargs):
            calls[0]+=1
            if calls[0]==2:raise InputSpaceUnavailable('synthetic')
            return original(*args,**kwargs)
        with patch.object(self.store,'upload',side_effect=fail_second):
            self.assertTrue(self.facade.handle(req))
        self.assertEqual(req.status,503)
        self.assertEqual(len(self.store.task(self.identifier)['files']),1)
        retry=self.request(body);self.assertTrue(self.facade.handle(retry))
        self.assertEqual(retry.status,200)
        files=self.store.task(self.identifier)['files'];self.assertEqual(len(files),2)
        self.assertEqual({Path(f['path']).read_bytes() for f in files},{b'first',b'second'})

    def test_concurrent_reservations_serialize_check_and_allocation(self):
        available=[10]; entered=threading.Event(); release=threading.Event(); results=[]
        def stat(_): return SimpleNamespace(f_bavail=available[0],f_frsize=1)
        def allocate(_fd,_offset,size):
            entered.set()
            if not release.wait(2): raise RuntimeError('test allocation timed out')
            available[0]-=size
        def attempt():
            try: input_space.reserve_input(0,6,4); results.append('ok')
            except InputSpaceUnavailable: results.append('held')
        with patch('processing.input_space.os.fstatvfs',side_effect=stat), \
             patch('processing.input_space.os.posix_fallocate',side_effect=allocate):
            first=threading.Thread(target=attempt); second=threading.Thread(target=attempt)
            first.start(); self.assertTrue(entered.wait(2)); second.start(); release.set()
            first.join(2); second.join(2)
            self.assertFalse(first.is_alive()); self.assertFalse(second.is_alive())
        self.assertCountEqual(results,['ok','held'])

    def test_default_task_limit_rejects_50_gib_before_reading(self):
        with self.assertRaises(InputLimitExceeded):self.store.upload(self.identifier,'image.jpg',io.BytesIO(b''),50*1024**3)
    def test_mismatched_facade_store_guard_is_rejected(self):
        with self.assertRaises(ValueError):ViewerFacade(SimpleNamespace(store=SimpleNamespace(input_free_floor=None)),TOKEN,temp_dir=self.spool,input_free_floor=0)

class InputGuardConfigurationTests(unittest.TestCase):
    def test_runtime_guard_cannot_be_enabled_without_qualification(self):
        config=Config('synthetic','http://127.0.0.1:1','http://127.0.0.1:2',1000,20*1024**3,
            prototype_state_dir='/synthetic/not-used',viewer_enabled=True,authority_lease_dir='/synthetic/not-used',
            viewer_input_space_guard=True)
        with self.assertRaisesRegex(RuntimeError,'viewer_input_space_guard_not_qualified'):make_handler(config)
    def test_environment_mode_is_explicit_and_disabled(self):
        env={'PROCESSING_API_TOKEN':'synthetic-api-'+'a'*32,'PROCESSING_VIEWER_ENABLED':'1',
             'PROCESSING_VIEWER_INPUT_SPACE_GUARD':'1','PROCESSING_PROTOTYPE_STATE_DIR':'/synthetic/state',
             'PROCESSING_AUTHORITY_LEASE_DIR':'/synthetic/lease'}
        with patch.dict(os.environ,env,clear=True):
            with self.assertRaisesRegex(ValueError,'not qualified'):runtime_config.from_environment()
        with patch.dict(os.environ,{**env,'PROCESSING_AUTHORITY_LEASE_DIR':''},clear=True):
            with self.assertRaisesRegex(ValueError,'authority lease'):runtime_config.from_environment()

@unittest.skipUnless(os.name=='posix' and hasattr(os,'posix_fallocate'),'POSIX fallocate required')
class IntakeRuntimeStartupTests(unittest.TestCase):
    def setUp(self):
        self.root=Path(tempfile.mkdtemp(prefix='lt-intake-runtime-')).resolve()
        self.addCleanup(lambda:print('retained synthetic runtime root:',self.root,flush=True))
        self.state=self.root/'state';self.leases=self.root/'leases';self.leases.mkdir()
        queue=Queue(self.state,viewer_enabled=True)
        TaskStore(queue,self.state/'viewer-tasks')
        from processing import intake_offline_migration
        with patch.object(intake_offline_migration,'QUALIFIED',True):
            with queue._db() as db:intake_offline_migration.install_empty_offline(db)
        queue.close()
    def settings(self,enabled=True):
        return Config('synthetic','http://127.0.0.1:1','http://127.0.0.1:2',1000,20*1024**3,
            prototype_state_dir=str(self.state),viewer_enabled=True,viewer_token='v'*32,
            authority_lease_dir=str(self.leases),viewer_input_space_guard=enabled,
            viewer_input_free_floor=0,viewer_intake_journal=enabled)
    def qualify(self):
        from contextlib import ExitStack
        from processing import viewer_tasks,intake_offline_migration
        stack=ExitStack()
        for module,name in ((runtime_config,'VIEWER_INPUT_SPACE_GUARD_QUALIFIED'),
                            (runtime_config,'VIEWER_INTAKE_JOURNAL_QUALIFIED'),
                            (viewer_tasks,'INTAKE_JOURNAL_QUALIFIED'),(intake_offline_migration,'QUALIFIED')):
            stack.enter_context(patch.object(module,name,True))
        return stack
    def test_disabled_runtime_rejects_migrated_database(self):
        with self.assertRaisesRegex(RuntimeError,'intake_journal_configuration_mismatch'):
            make_handler(self.settings(False))
        probe=Queue(self.state,viewer_enabled=True);probe.close()
    def test_startup_cleanup_attempts_every_close_and_preserves_original(self):
        from processing.app import _release_failed_startup
        class Owned:
            def __init__(self,name):self.name=name
            def close(self):
                closed.append(self.name)
                if self.name!='lease':raise OSError('synthetic close failure')
        closed=[];original=RuntimeError('original startup failure')
        _release_failed_startup(original,Owned('service'),Owned('queue'),Owned('lease'))
        self.assertEqual(closed,['service','queue','lease'])
        self.assertEqual(str(original),'original startup failure')
        self.assertEqual(len(original.__notes__),2)

    def test_startup_db_failure_closes_queue_and_lease(self):
        from contextlib import contextmanager
        from processing import authority_lease
        real_record=authority_lease.record_required
        for failure_index in (1,2):
            @contextmanager
            def fail_after_record(queue,call_count):
                call_count[0]+=1
                if call_count[0]==failure_index:raise OSError('synthetic startup db failure')
                with original_db() as db:yield db
            def install_failure(queue,lease):
                nonlocal original_db
                real_record(queue,lease);original_db=queue._db;counts=[0]
                queue._db=lambda:fail_after_record(queue,counts)
            original_db=None
            with self.qualify(), patch.object(authority_lease,'record_required',side_effect=install_failure):
                with self.assertRaisesRegex(OSError,'synthetic startup db failure'):make_handler(self.settings())
            probe=Queue(self.state,viewer_enabled=True);probe.close()
            lease=authority_lease.acquire(self.leases,self.state);lease.close()

    def historical_fixture(self):
        handler=make_handler(self.settings());queue=handler.prototype_queue;store=handler.viewer_service.store
        identifier=str(uuid.uuid4());store.initialize(identifier,'old',[],None,'production')
        store.upload(identifier,'a.jpg',io.BytesIO(b'first'),5)
        old=Path(store.task(identifier)['files'][0]['path'])
        store.remove(identifier);store.initialize(identifier,'new',[],None,'production')
        handler.viewer_service.close();queue.close();queue.authority_lease.close()
        return old

    def test_startup_audits_valid_historical_generation(self):
        with self.qualify():
            old=self.historical_fixture();handler=make_handler(self.settings())
            self.assertEqual(old.read_bytes(),b'first')
            queue=handler.prototype_queue;handler.viewer_service.close();queue.close();queue.authority_lease.close()

    def test_startup_holds_changed_missing_or_extra_historical_inputs(self):
        with self.qualify():
            old=self.historical_fixture()
            old.write_bytes(b'wrong')
            with self.assertRaisesRegex(InputRecoveryRequired,'historical input bytes'):make_handler(self.settings())
            old.write_bytes(b'first');extra=old.parent/('input-'+'f'*32);extra.write_bytes(b'extra')
            with self.assertRaisesRegex(InputRecoveryRequired,'historical input paths'):make_handler(self.settings())
            # Preserve the extra and simulate a missing expected file by renaming
            # only this test-owned fixture; no user data is touched.
            moved=old.with_name('held-synthetic');old.rename(moved)
            with self.assertRaisesRegex(InputRecoveryRequired,'historical input paths'):make_handler(self.settings())
            self.assertEqual(moved.read_bytes(),b'first');self.assertEqual(extra.read_bytes(),b'extra')

    def test_startup_holds_oversized_historical_json(self):
        from processing import viewer_tasks
        with self.qualify():
            old=self.historical_fixture()
            for key in ('HISTORY_METADATA_MAX_BYTES','HISTORY_FILES_JSON_MAX_BYTES'):
                with patch.object(viewer_tasks,key,4):
                    with self.assertRaises(InputRecoveryRequired):make_handler(self.settings())
            self.assertEqual(old.read_bytes(),b'first')

    def test_startup_holds_duplicate_and_wrong_type_historical_json(self):
        import sqlite3
        from processing import intake_offline_migration as migration
        with self.qualify():
            old=self.historical_fixture()
            db=sqlite3.connect(self.state/'jobs.sqlite3')
            try:
                original=db.execute('SELECT metadata,files FROM viewer_incarnations').fetchone()
                meta=json.loads(original[0]);files=json.loads(original[1])
                wrong=[dict(files[0],is_photo=True)]
                duplicate=original[0][:-1]+',"generation":1}'
                for metadata,content in ((duplicate,original[1]),(original[0],json.dumps(wrong))):
                    db.execute('DROP TRIGGER viewer_intake_guard_incarnation_update_v1')
                    db.execute('UPDATE viewer_incarnations SET metadata=?,files=?',(metadata,content))
                    db.execute(migration.TRIGGERS['viewer_intake_guard_incarnation_update_v1']);db.commit()
                    with self.assertRaises(InputRecoveryRequired):make_handler(self.settings())
                db.execute('DROP TRIGGER viewer_intake_guard_incarnation_update_v1')
                db.execute('UPDATE viewer_incarnations SET metadata=?,files=?',original)
                db.execute(migration.TRIGGERS['viewer_intake_guard_incarnation_update_v1']);db.commit()
            finally:db.close()
            self.assertEqual(old.read_bytes(),b'first')

    def test_sqlite_progress_interrupt_releases_connection_and_queue_lock(self):
        import sqlite3
        from processing import viewer_tasks
        with self.qualify():
            handler=make_handler(self.settings());queue=handler.prototype_queue;store=handler.viewer_service.store
            rows=[(str(uuid.uuid4()),1,'synthetic-'+str(n),'name','[]','null','production') for n in range(1001)]
            with queue._db() as db:
                db.executemany('INSERT INTO viewer_tasks(uuid,generation,directory,name,options,outputs,environment) VALUES(?,?,?,?,?,?,?)',rows)
            calls=[0]
            def clock():
                calls[0]+=1
                return 100 if calls[0]<=2 else 200
            with patch.object(viewer_tasks.time,'monotonic',side_effect=clock):
                with self.assertRaisesRegex(sqlite3.OperationalError,'interrupted'):
                    store.startup_intake_census()
            self.assertGreaterEqual(calls[0],3)
            with queue._db() as db:self.assertEqual(db.execute('SELECT 1').fetchone()[0],1)
            handler.viewer_service.close();queue.close();queue.authority_lease.close()
            probe=Queue(self.state,viewer_enabled=True);probe.close()

    def test_final_deadline_check_rejects_expired_census(self):
        from processing import viewer_tasks
        with self.qualify():
            handler=make_handler(self.settings());queue=handler.prototype_queue;store=handler.viewer_service.store
            calls=[0]
            def clock():
                calls[0]+=1
                return 100 if calls[0]<=2 else 200
            with patch.object(viewer_tasks.time,'monotonic',side_effect=clock):
                with self.assertRaisesRegex(InputRecoveryRequired,'exceeds bound'):
                    store.startup_intake_census()
            handler.viewer_service.close();queue.close();queue.authority_lease.close()

    def test_startup_holds_at_zero_audit_deadline(self):
        from processing import viewer_tasks
        with self.qualify():
            old=self.historical_fixture()
            with patch.object(viewer_tasks,'HISTORY_CENSUS_SECONDS',0):
                with self.assertRaisesRegex(InputRecoveryRequired,'exceeds bound'):make_handler(self.settings())
            self.assertEqual(old.read_bytes(),b'first')

    def test_startup_holds_over_existing_task_ceiling(self):
        with self.qualify():
            handler=make_handler(self.settings());queue=handler.prototype_queue
            rows=[(str(uuid.uuid4()),1,'synthetic-'+str(n),'name','[]','null','production') for n in range(1001)]
            with queue._db() as db:
                db.executemany('INSERT INTO viewer_tasks(uuid,generation,directory,name,options,outputs,environment) VALUES(?,?,?,?,?,?,?)',rows)
            handler.viewer_service.close();queue.close();queue.authority_lease.close()
            with self.assertRaisesRegex(InputRecoveryRequired,'task census exceeds bound'):make_handler(self.settings())

    def test_startup_holds_when_historical_byte_budget_exceeded(self):
        from processing import viewer_tasks
        with self.qualify():
            old=self.historical_fixture()
            with patch.object(viewer_tasks,'HISTORY_CENSUS_MAX_BYTES',4):
                with self.assertRaisesRegex(InputRecoveryRequired,'exceeds bound'):make_handler(self.settings())
            self.assertEqual(old.read_bytes(),b'first')

    def test_startup_holds_orphan_viewer_files(self):
        with self.qualify():
            handler=make_handler(self.settings());queue=handler.prototype_queue
            with queue._db() as db:
                db.execute('INSERT INTO viewer_files VALUES(?,?,?,?,?,?,?)',
                    (str(uuid.uuid4()),'x.jpg','x.jpg','input-'+'a'*32,1,'a'*64,1))
            handler.viewer_service.close();queue.close();queue.authority_lease.close()
            with self.assertRaisesRegex(InputRecoveryRequired,'orphan input metadata'):make_handler(self.settings())

    def test_actual_startup_holds_orphan_directory(self):
        with self.qualify():
            handler=make_handler(self.settings())
            orphan=self.state/'viewer-tasks'/'unregistered-task';orphan.mkdir();(orphan/'data').write_bytes(b'preserve')
            queue=handler.prototype_queue;handler.viewer_service.close();queue.close();queue.authority_lease.close()
            with self.assertRaises(InputRecoveryRequired):make_handler(self.settings())
            self.assertEqual((orphan/'data').read_bytes(),b'preserve')

    def test_actual_startup_holds_orphan_journal(self):
        with self.qualify():
            handler=make_handler(self.settings());queue=handler.prototype_queue
            with queue._db() as db:
                db.execute("INSERT INTO viewer_intake_journal_v1 VALUES(?,?,?,?,?,?,?,?,NULL,'prepared')",
                           ('a'*32,str(uuid.uuid4()),1,'x.jpg','unregistered','temp','final',1))
            handler.viewer_service.close();queue.close();queue.authority_lease.close()
            with self.assertRaises(InputRecoveryRequired):make_handler(self.settings())

    def test_actual_startup_holds_unfinished_intake_and_releases_owned_locks(self):
        from processing import authority_lease
        with self.qualify():
            handler=make_handler(self.settings())
            queue=handler.prototype_queue;store=handler.viewer_service.store
            identifier=str(uuid.uuid4());store.initialize(identifier,'synthetic',[],None,'production')
            with patch('pathlib.Path.unlink',side_effect=OSError('synthetic failure')):
                with self.assertRaises(OSError):store.upload(identifier,'a.jpg',io.BytesIO(b'abc'),3)
            handler.viewer_service.close();queue.close();queue.authority_lease.close()
            with self.assertRaises(InputRecoveryRequired):make_handler(self.settings())
            probe=Queue(self.state,viewer_enabled=True);probe.close()
            lease=authority_lease.acquire(self.leases,self.state);lease.close()
            self.assertEqual([p.read_bytes() for p in self.root.rglob('input-*')],[b'abc'])

class LargeRoutingTests(unittest.TestCase):
    def test_ten_thousand_photos_route_main_but_config_limit_remains_20_gib(self):
        ready={'ready':True,'taskQueueCount':0,'state':'active','maxImages':None}
        config=Config('synthetic','http://127.0.0.1:1','http://127.0.0.1:2',1000,20*1024**3)
        self.assertEqual(config.viewer_max_input_bytes,20*1024**3)
        self.assertEqual(plan(10000,50*1024**3,ready,ready,1000,20*1024**3)['decision'],'main')

if __name__=='__main__':unittest.main()
