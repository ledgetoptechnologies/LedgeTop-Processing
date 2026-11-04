import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from tests.inspect_real_viewer_pilot import inspect


class InspectPilotTests(unittest.TestCase):
    def test_inspection_is_read_only_and_rejects_wrong_owner(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'root';registry=Path(tmp)/'registry';root.mkdir();registry.mkdir()
            owner='a'*32;attempt='d78ee527-b8c3-40aa-a1b7-0ff44669f51e'
            task='42e87ef6-28a3-4711-a328-ce0230e9ec6e';provider='d12854e8-ece0-4d48-9a9a-c81d6bb12941'
            (root/'owner.json').write_text(json.dumps({'owner':owner}))
            (registry/'active.json').write_text(json.dumps({'owner':owner}))
            (root/'pilot-receipt.json').write_text(json.dumps({'owner':owner,'attemptId':attempt,'taskId':task,'providerTaskId':provider}))
            dbpath=root/'viewer.sqlite';db=sqlite3.connect(dbpath)
            db.execute('CREATE TABLE processing_attempts(id,task_id,provider_task_id,status,submission_phase,uploaded_file_count,error_code)')
            db.execute('CREATE TABLE processing_jobs(id,attempt_id,job_type,status,created_at)')
            db.execute('INSERT INTO processing_attempts VALUES(?,?,?,?,?,?,?)',(attempt,task,provider,'running','committed',25,'private token with spaces'))
            db.execute('INSERT INTO processing_jobs VALUES(?,?,?,?,?)',('1',attempt,'reconcile','pending','0'));db.commit();db.close()
            before=dbpath.read_bytes();result=inspect(root,registry)
            self.assertEqual(result['errorCode'],'unrecognized')
            self.assertEqual(result['submissionPhase'],'committed')
            self.assertEqual(result['jobs'],[{'type':'reconcile','status':'pending'}])
            self.assertEqual(dbpath.read_bytes(),before)
            (registry/'active.json').write_text(json.dumps({'owner':'b'*32}))
            with self.assertRaises(ValueError):inspect(root,registry)


if __name__=='__main__':unittest.main()
