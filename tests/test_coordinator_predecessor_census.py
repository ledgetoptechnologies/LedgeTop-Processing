"""Exact frozen-693 SQL, synthetic retained data; no engine/NAS/runtime mutation."""
import base64
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch
import zipfile

from ops import coordinator_predecessor_census as c


@unittest.skipUnless(os.name == 'posix', 'exact Linux ownership/modes')
class ClosedPredecessorCensusTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / 'state'
        self.root.mkdir(mode=0o755); self.root.chmod(0o755)
        for name in ('viewer-tasks', 'viewer-spool'):
            (self.root / name).mkdir(mode=0o700)
        self.path = self.root / 'jobs.sqlite3'
        self.authority = str(uuid.uuid4())
        self.key = b'synthetic-private-census-key-000'
        self.schema = json.loads(c.SCHEMA_PATH.read_text())
        with self.db() as db:
            for kind, name, _, sql in self.schema['objects']:
                if kind == 'table' and name != 'sqlite_sequence': db.execute(sql)
            for kind, _, _, sql in self.schema['objects']:
                if kind == 'index' and sql: db.execute(sql)
            db.execute('INSERT INTO coordinator_identity VALUES(1,?)', (self.authority,))
            db.execute('INSERT INTO coordinator_authority_lease VALUES(1,1)')
            for node in ('cluster','main'): db.execute('INSERT INTO nodes(id) VALUES(?)', (node,))
            self.tasks = []
            for index in range(22):
                job, task = str(uuid.uuid4()), str(uuid.uuid4())
                environment = 'production' if index < 12 else 'staging'
                if index >= 7:
                    db.execute("INSERT INTO jobs(id,request_id,environment,profile,status,result) VALUES(?,?,?,'treedar-synthetic-v1',?,'{}')",
                               (job, 'synthetic-' + str(index), environment, 'failed' if index >= 20 else 'succeeded'))
                    continue
                node = 'cluster' if index % 2 else 'main'
                token = 'synthetic-claim-' + str(index)
                db.execute("INSERT INTO jobs(id,request_id,environment,profile,status,node_id,claim_hash,result) VALUES(?,?,?,'odm-viewer-v1','succeeded',?,?,'{}')",
                           (job, task, environment, node, hashlib.sha256(token.encode()).hexdigest()))
                directory = self.root / 'viewer-tasks' / (task + '-1')
                directory.mkdir(mode=0o700)
                engine = str(uuid.uuid4())
                db.execute("INSERT INTO viewer_tasks(uuid,directory,name,options,outputs,environment,stage,phase,job_id,node_id,engine_uuid,claim_token,option_evidence_required) VALUES(?,?,?,'{}','[]',?,'succeeded','complete',?,?,?,?,1)",
                           (task, directory.name, 'synthetic', environment, job, node, engine, token))
                definitions = '[{"name":"pc-ept","type":"bool"}]'
                db.execute('INSERT INTO viewer_option_evidence VALUES(?,1,?,?)',
                           (task, definitions, hashlib.sha256(definitions.encode()).hexdigest()))
                for number in range(23 if index == 0 else 22):
                    name = 'photo_' + str(number) + '.jpg'
                    path = 'input-' + uuid.uuid4().hex
                    data = ('synthetic-photo-' + str(index) + '-' + str(number)).encode()
                    (directory / path).write_bytes(data)
                    (directory / path).chmod(0o600)
                    db.execute('INSERT INTO viewer_files VALUES(?,?,?,?,?,?,1)',
                               (task, name.casefold(), name, path, len(data), hashlib.sha256(data).hexdigest()))
                archive = directory / 'all.zip'
                with zipfile.ZipFile(archive, 'w') as zip:
                    zip.writestr('synthetic/result.txt', b'synthetic-engine-result')
                archive.chmod(0o600)
                data = archive.read_bytes()
                receipt = dict(version=1, task=task, generation=1, job=job, node=node, engine=engine,
                               bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
                (directory / 'all.zip.receipt').write_text(json.dumps(receipt))
                (directory / 'all.zip.receipt').chmod(0o600)
                self.tasks.append((task, job, directory))
        self.path.chmod(0o600)
        (self.root / 'coordinator.lock').write_bytes(b'')
        (self.root / 'coordinator.lock').chmod(0o644)

    def db(self):
        from contextlib import closing, contextmanager
        @contextmanager
        def connection():
            with closing(sqlite3.connect(self.path)) as db:
                db.row_factory = sqlite3.Row
                with db: yield db
        return connection()

    def census(self, **kwargs):
        args = dict(key=self.key, authority_id=self.authority, node_ids=('cluster','main'), uid=os.geteuid(),
                    max_artifact_bytes=16*1024*1024,
                    expected_counts=dict(jobs=22,viewer_tasks=7,viewer_reservations=0,viewer_option_evidence=7,viewer_files=155))
        args.update(kwargs)
        return c.census(self.root, **args)

    def mutate(self, sql, args=()):
        with self.db() as db: db.execute(sql, args)

    def test_exact_22_job_7_task_155_file_fixture_is_stable_and_read_only(self):
        before = {str(p): (p.read_bytes(),p.stat().st_mode,p.stat().st_mtime_ns) for p in self.root.rglob('*') if p.is_file()}
        result = self.census()
        self.assertEqual(result, self.census())
        self.assertEqual(result['archiveReceipts'], 7)
        self.assertFalse(result['writerExclusionProven'])
        self.assertFalse(result['signedViewerAcceptanceProven'])
        self.assertEqual(result['rootMode'], 0o755)
        self.assertEqual(before, {str(p): (p.read_bytes(),p.stat().st_mode,p.stat().st_mtime_ns) for p in self.root.rglob('*') if p.is_file()})
        self.assertNotIn(self.tasks[0][0], json.dumps(result))

    def test_historical_incarnation_files_and_receipts_remain_in_census(self):
        import shutil
        identifier, old_job, directory = self.tasks[0]
        with self.db() as db:
            old = dict(db.execute('SELECT * FROM viewer_tasks WHERE uuid=?',(identifier,)).fetchone())
            files = [dict(r) for r in db.execute('SELECT * FROM viewer_files WHERE uuid=?',(identifier,))]
            db.execute('INSERT INTO viewer_incarnations VALUES(?,1,?,?,?)',
                       (identifier,old_job,json.dumps(old),json.dumps(files)))
            new_job = str(uuid.uuid4()); token = 'synthetic-generation-two'
            request = str(uuid.uuid5(uuid.UUID(identifier),'generation:2'))
            db.execute("INSERT INTO jobs(id,request_id,environment,profile,status,node_id,claim_hash,result) VALUES(?,?,?,'odm-viewer-v1','succeeded',?,?,'{}')",
                       (new_job,request,old['environment'],old['node_id'],hashlib.sha256(token.encode()).hexdigest()))
            new_dir = directory.parent / (identifier+'-2')
            shutil.copytree(directory,new_dir)
            receipt = json.loads((new_dir/'all.zip.receipt').read_text())
            receipt.update(generation=2,job=new_job)
            (new_dir/'all.zip.receipt').write_text(json.dumps(receipt))
            db.execute('UPDATE viewer_tasks SET generation=2,job_id=?,directory=?,claim_token=? WHERE uuid=?',
                       (new_job,new_dir.name,token,identifier))
            item=db.execute('SELECT * FROM viewer_option_evidence WHERE uuid=?',(identifier,)).fetchone()
            db.execute('INSERT INTO viewer_option_evidence VALUES(?,2,?,?)',(identifier,item['definitions'],item['sha256']))
        result=self.census(expected_counts=None)
        self.assertEqual(result['archiveReceipts'],8)
        self.assertEqual(result['tables']['viewer_incarnations']['count'],1)
        self.assertEqual(result['tables']['jobs']['count'],23)
        old_input=next(directory.glob('input-*'));old_input.write_bytes(b'changed-historical-source')
        with self.assertRaisesRegex(c.CensusRefused,'retained_artifact_digest_mismatch'):
            self.census(expected_counts=None)

    def test_receipt_symlink_refused_before_receipt_open(self):
        target=self.tasks[0][2]/'all.zip.receipt'
        outside=Path(self.temp.name)/'outside';outside.write_bytes(target.read_bytes())
        target.unlink();target.symlink_to(outside)
        with patch.object(c.os,'open',side_effect=AssertionError('must not open symlink')):
            with self.assertRaisesRegex(c.CensusRefused,'original_path_required'):
                c._read_small_json(target,os.geteuid())

    def add_closed_maintenance_history(self):
        latest = {}
        with self.db() as db:
            for index in range(16):
                node = 'cluster' if index % 2 else 'main'
                expected_revision = (index//2)*2
                request = dict(schemaVersion=1,requestId=str(uuid.uuid4()),authorityId=self.authority,
                    expectedRevision=expected_revision,drainAt='2026-11-01T00:00:00Z',notBefore='2026-11-01T00:00:00Z',
                    afterJobId=None,targetImage='sha256:'+str(index%10)*64)
                digest=hashlib.sha256(c._canonical(dict(nodeId=node,request=request))).hexdigest()
                receipt=dict(schemaVersion=1,requestId=request['requestId'],authorityId=self.authority,nodeId=node,
                    requestSha256=digest,expectedRevision=expected_revision,revision=expected_revision+1,
                    planId=str(uuid.uuid4()),state='draining',drainAt=request['drainAt'],notBefore=request['notBefore'],
                    afterJobId=None,targetImage=request['targetImage'])
                receipt['receiptSha256']=hashlib.sha256(c._canonical(receipt)).hexdigest()
                db.execute('INSERT INTO owned_maintenance_schedules VALUES(?,?,?,?,?,?)',
                    (request['requestId'],node,self.authority,digest,c._canonical(request).decode(),c._canonical(receipt).decode()))
                db.execute("UPDATE nodes SET state='active',revision=?,plan_id=?,drain_at=?,not_before=?,target_image=? WHERE id=?",
                    (expected_revision+2,receipt['planId'],receipt['drainAt'],receipt['notBefore'],receipt['targetImage'],node))
                latest[node] = request['requestId']
        return latest

    def test_sixteen_owned_receipts_and_active_residual_plans_are_preserved(self):
        self.add_closed_maintenance_history()
        self.mutate('UPDATE nodes SET result=? WHERE id=?',
                    (json.dumps(dict(operatorAction='resume',note='synthetic healthy resume',workerStopped=True,healthy=True)),'main'))
        result=self.census()
        self.assertEqual(result['tables']['owned_maintenance_schedules']['count'],16)
        self.assertEqual(result,self.census())

    def test_corrupt_old_owned_receipt_is_not_ignored(self):
        self.add_closed_maintenance_history()
        self.mutate("UPDATE owned_maintenance_schedules SET request_sha256=? WHERE request_id=(SELECT request_id FROM owned_maintenance_schedules ORDER BY rowid LIMIT 1)",('0'*64,))
        with self.assertRaisesRegex(c.CensusRefused,'owned_schedule_receipt_mismatch'): self.census()

    def test_current_residual_plan_must_match_latest_owned_receipt(self):
        self.add_closed_maintenance_history()
        self.mutate("UPDATE nodes SET target_image=? WHERE id='main'",('sha256:'+'f'*64,))
        with self.assertRaisesRegex(c.CensusRefused,'current_node_plan_binding_mismatch'): self.census()

    def test_receipted_plan_still_draining_or_updating_is_refused(self):
        self.add_closed_maintenance_history()
        for state in ('scheduled','draining','updating','held','failed'):
            with self.subTest(state=state):
                self.mutate("UPDATE nodes SET state=? WHERE id='main'",(state,))
                with self.assertRaisesRegex(c.CensusRefused,'foreign_node_maintenance_pending'): self.census()
        self.mutate("UPDATE nodes SET state='active',revision=revision-1 WHERE id='main'")
        with self.assertRaisesRegex(c.CensusRefused,'current_node_plan_not_closed'): self.census()

    def test_unbound_foreign_residual_plan_refused(self):
        self.mutate("UPDATE nodes SET plan_id=?,revision=2,target_image=? WHERE id='main'",(str(uuid.uuid4()),'sha256:'+'a'*64))
        with self.assertRaisesRegex(c.CensusRefused,'current_node_plan_not_closed'): self.census()

    def probe(self, **kwargs):
        args=dict(key=self.key,authority_id=self.authority,node_ids=('cluster','main'),uid=os.geteuid(),
                  max_artifact_bytes=16*1024*1024,reviewed_schema_bytes=c.SCHEMA_PATH.read_bytes())
        args.update(kwargs)
        return c.metadata_probe(self.root,**args)

    def test_incremental_canonical_size_matches_escaped_unicode(self):
        value=dict(text='quote" backslash\\ controls\n\t\x00 del\x7f unicode\u00e9 astral\U0001f600',integer=42,real=2.5,empty=None)
        expected=c._canonical(value)
        self.assertEqual(c._row_bytes(value,len(expected),c._ReadBudget(None,None)),expected)
        with self.assertRaisesRegex(c.CensusRefused,'predecessor_metadata_budget_exceeded'):
            c._row_bytes(value,len(expected)-1,c._ReadBudget(None,None))

    def test_diagnostic_metadata_probe_never_hashes_retained_content(self):
        with patch.object(c,'_hash_file',side_effect=AssertionError('bulk content must not be opened')):
            result=self.probe()
        self.assertEqual(result['profile'],c.PROBE_PROFILE)
        self.assertFalse(result['retainedFilesValidated'])
        self.assertFalse(result['writerExclusionProven'])
        self.assertLess(result['directFileBytesRead'],65536)
        self.assertTrue(all('content' not in item for item in result['files'].values()))
        full=self.census()
        self.assertEqual(full['profile'],c.PROFILE)
        self.assertTrue(full['retainedFilesValidated'])
        self.assertGreater(full['directFileBytesRead'],result['directFileBytesRead'])

    def test_diagnostic_does_not_claim_same_size_content_integrity(self):
        target=next(self.tasks[0][2].glob('input-*'));data=target.read_bytes();target.write_bytes(b'x'*len(data))
        result=self.probe()
        self.assertFalse(result['retainedFilesValidated'])
        with self.assertRaisesRegex(c.CensusRefused,'retained_artifact_digest_mismatch'): self.census()

    def test_diagnostic_rejects_schema_bytes_mutation(self):
        changed=json.loads(c.SCHEMA_PATH.read_text());changed['sourceCommit']='0'*40
        with self.assertRaisesRegex(c.CensusRefused,'reviewed_schema_profile_changed'):
            self.probe(reviewed_schema_bytes=json.dumps(changed).encode())

    def test_coarse_database_budget_refuses_before_connect(self):
        maximum=1024
        coarse=8*maximum+100000*1024+1024*1024
        with self.path.open('r+b') as stream: stream.truncate(coarse+4096)
        with patch.object(c.sqlite3,'connect',side_effect=AssertionError('must refuse before SQLite')):
            with self.assertRaisesRegex(c.CensusRefused,'sqlite_coarse_physical_budget_exceeded'):
                self.census(max_metadata_bytes=maximum)
            with self.assertRaisesRegex(c.CensusRefused,'sqlite_coarse_physical_budget_exceeded'):
                self.probe(max_metadata_bytes=maximum,max_database_bytes=coarse+8192)

    def test_huge_cell_refuses_before_full_row_materialization(self):
        self.mutate('UPDATE jobs SET result=? WHERE id=?',('x'*100000,self.tasks[0][1]))
        trace=[];original=c.sqlite3.connect
        def tracked(*args,**kwargs):
            connection=original(*args,**kwargs);connection.set_trace_callback(trace.append);return connection
        for action in (self.census,self.probe):
            with self.subTest(action=action.__name__):
                trace.clear()
                with patch.object(c.sqlite3,'connect',side_effect=tracked):
                    with self.assertRaisesRegex(c.CensusRefused,'predecessor_metadata_budget_exceeded'):
                        action(max_metadata_bytes=32768)
                self.assertFalse(any('SELECT * FROM "jobs" WHERE rowid' in sql for sql in trace))
                self.assertFalse(any('quick_check' in sql for sql in trace))

    def test_full_hash_direct_read_budget_and_duration_are_cooperative(self):
        with self.assertRaisesRegex(c.CensusRefused,'census_read_budget_exceeded'):
            self.census(max_read_bytes=100)
        with patch.object(c.time,'monotonic',side_effect=[0,0,2]):
            with self.assertRaisesRegex(c.CensusRefused,'census_duration_exceeded'):
                self.census(max_duration_seconds=1)
        with self.assertRaisesRegex(c.CensusRefused,'diagnostic_database_size_exceeded'):
            self.probe(max_database_bytes=1)

    def test_serialized_stdin_probe_needs_no_contract_file_or_package(self):
        request=dict(schemaBase64=base64.b64encode(c.SCHEMA_PATH.read_bytes()).decode(),keyHex=self.key.hex(),
                     root=str(self.root),authorityId=self.authority,nodeIds=['cluster','main'],uid=os.geteuid(),
                     maxArtifactBytes=16*1024*1024,maxMetadataBytes=64*1024*1024,expectedCounts=None,
                     logicalRoot='/prototype-state',maxDurationSeconds=5,maxReadBytes=1024*1024,
                     maxDatabaseBytes=8*1024*1024)
        result=subprocess.run([sys.executable,'-I','-'],input=c.serializable_probe_source(request),
                              cwd=self.temp.name,capture_output=True,text=True,timeout=10,check=False)
        self.assertEqual(result.returncode,0,result.stderr)
        answer=json.loads(result.stdout)
        self.assertTrue(answer['ok'])
        self.assertEqual(answer['result']['profile'],c.PROBE_PROFILE)
        self.assertFalse(answer['result']['retainedFilesValidated'])
        self.assertNotIn('files',answer['result'])
        self.assertGreater(answer['result']['fileCount'],155)
        self.assertNotIn(self.key.hex(),result.stdout)
        self.assertNotIn(self.tasks[0][0],result.stdout)
        request['schemaBase64']=base64.b64encode(b'{}').decode()
        rejected=subprocess.run([sys.executable,'-I','-'],input=c.serializable_probe_source(request),
                                cwd=self.temp.name,capture_output=True,text=True,timeout=10,check=False)
        self.assertEqual(rejected.returncode,2)
        self.assertEqual(json.loads(rejected.stdout),dict(ok=False,error='reviewed_schema_profile_changed'))

    def test_unknown_table_refused(self):
        self.mutate('CREATE TABLE future_state(x TEXT)')
        with self.assertRaisesRegex(c.CensusRefused,'unreviewed_predecessor_schema'): self.census()

    def test_same_columns_weakened_schema_refused(self):
        self.mutate('DROP TABLE viewer_reservations')
        self.mutate('CREATE TABLE viewer_reservations(uuid TEXT PRIMARY KEY,environment TEXT NOT NULL,capability_hash TEXT NOT NULL,state TEXT NOT NULL)')
        with self.assertRaisesRegex(c.CensusRefused,'unreviewed_predecessor_schema'): self.census()

    def test_trigger_refused(self):
        self.mutate('CREATE TRIGGER future_writer AFTER INSERT ON jobs BEGIN UPDATE jobs SET result=NULL; END')
        with self.assertRaisesRegex(c.CensusRefused,'unreviewed_predecessor_schema'): self.census()

    def test_sidecars_refused_without_recovery_or_deletion(self):
        for suffix in ('-journal','-wal','-shm'):
            with self.subTest(suffix=suffix):
                target=Path(str(self.path)+suffix); target.write_bytes(b'synthetic-not-a-journal')
                try:
                    with self.assertRaisesRegex(c.CensusRefused,'sqlite_io_not_quiescent'): self.census()
                    self.assertEqual(target.read_bytes(),b'synthetic-not-a-journal')
                finally: target.unlink()

    def test_unresolved_reservation_refused(self):
        self.mutate("INSERT INTO viewer_reservations VALUES(?,?,?,'reserved')", (str(uuid.uuid4()),'production','x'*64))
        with self.assertRaises(c.CensusRefused): self.census(expected_counts=None)

    def test_nonterminal_task_refused(self):
        self.mutate("UPDATE viewer_tasks SET stage='running',phase='processing' WHERE uuid=?", (self.tasks[0][0],))
        with self.assertRaisesRegex(c.CensusRefused,'viewer_execution_binding_invalid'): self.census()

    def test_claim_digest_mismatch_refused(self):
        self.mutate("UPDATE jobs SET claim_hash=? WHERE id=?", ('0'*64,self.tasks[0][1]))
        with self.assertRaisesRegex(c.CensusRefused,'viewer_execution_binding_invalid'): self.census()

    def test_unknown_file_and_orphan_directory_refused(self):
        for target, directory in ((self.tasks[0][2]/'unknown.bin',False), (self.root/'viewer-tasks'/'orphan',True)):
            with self.subTest(directory=directory):
                if directory: target.mkdir(mode=0o700)
                else: target.write_bytes(b'synthetic');target.chmod(0o600)
                try:
                    with self.assertRaisesRegex(c.CensusRefused,'unknown_predecessor_path'): self.census()
                finally:
                    if directory: target.rmdir()
                    else: target.unlink()

    def test_input_digest_mutation_refused(self):
        target=next(self.tasks[0][2].glob('input-*'));target.write_bytes(b'changed-input')
        with self.assertRaisesRegex(c.CensusRefused,'retained_artifact_digest_mismatch'): self.census()

    def test_archive_receipt_wrong_engine_refused(self):
        target=self.tasks[0][2]/'all.zip.receipt';value=json.loads(target.read_text());value['engine']=str(uuid.uuid4());target.write_text(json.dumps(value))
        with self.assertRaisesRegex(c.CensusRefused,'archive_receipt_binding_invalid'): self.census()

    def test_option_evidence_mutation_refused(self):
        self.mutate('UPDATE viewer_option_evidence SET definitions=? WHERE uuid=?', ('[]',self.tasks[0][0]))
        with self.assertRaisesRegex(c.CensusRefused,'invalid_option_evidence'): self.census()

    def test_callback_flags_and_output_history_are_not_ignored(self):
        before=self.census()
        self.mutate('UPDATE viewer_tasks SET removed=1,output_lines=? WHERE uuid=?', ('["synthetic-retained-log"]',self.tasks[0][0]))
        after=self.census()
        self.assertNotEqual(before['tables']['viewer_tasks'],after['tables']['viewer_tasks'])

    def test_artifact_and_metadata_budgets_independent(self):
        with self.assertRaises(c.CensusRefused): self.census(max_artifact_bytes=1)
        with self.assertRaisesRegex(c.CensusRefused,'metadata_budget'): self.census(max_metadata_bytes=1)

    def test_foreign_node_update_and_wrong_authority_refused(self):
        with self.assertRaisesRegex(c.CensusRefused,'authority_changed'): self.census(authority_id=str(uuid.uuid4()))
        self.mutate("UPDATE nodes SET state='draining',plan_id=? WHERE id='main'",(str(uuid.uuid4()),))
        with self.assertRaisesRegex(c.CensusRefused,'foreign_node_maintenance_pending'): self.census()

    def test_unknown_nested_symlink_and_hardlink_refused(self):
        source=next(self.tasks[0][2].glob('input-*'));link=self.tasks[1][2]/('input-'+uuid.uuid4().hex)
        os.link(source,link)
        with self.assertRaises(c.CensusRefused): self.census()
        link.unlink()
        link.symlink_to(source)
        with self.assertRaises(c.CensusRefused): self.census()


if __name__ == '__main__': unittest.main()
