"""Exact frozen-693 SQL, synthetic retained data; no engine/NAS/runtime mutation."""
import hashlib
import json
import os
from pathlib import Path
import sqlite3
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
