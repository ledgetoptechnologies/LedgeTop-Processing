import sqlite3
import json
import types
import unittest
from unittest.mock import patch
from processing.reference_copy_hold_integrity import held_copy_ids
from processing import reference_protocol as p

class BoundedHeldCopyScanTests(unittest.TestCase):
 def test_oversized_and_nul_fields_hold_before_json_decoder(self):
  for table,column in (("reference_viewer_envelopes_v2","envelope_json"),("reference_viewer_activation","binding_json")):
   for bad in ("x"*(p.MAX_BODY_BYTES+1),"x\x00"+"y"*1000000,"["*30+"0"+"]"*30):
    with self.subTest(table=table,case=bad[:3]),sqlite3.connect(":memory:") as db:
     db.row_factory=sqlite3.Row
     db.executescript("""
CREATE TABLE reference_viewer_v3_copy_hold(job_id TEXT,envelope_sha256 TEXT,payload_sha256 TEXT,payload_json TEXT);
CREATE TABLE reference_jobs(job_id TEXT,submission_id TEXT,generation INTEGER,environment TEXT,manifest_sha TEXT,pipeline_digest TEXT,request_digest TEXT,input_json TEXT);
CREATE TABLE reference_viewer_submission_owners(job_id TEXT,protocol TEXT,principal_id TEXT,environment TEXT,request_digest TEXT);
CREATE TABLE reference_viewer_envelopes_v2(job_id TEXT,activation_id TEXT,viewer_task_id TEXT,viewer_attempt_id TEXT,envelope_sha256 TEXT,envelope_json TEXT);
CREATE TABLE reference_viewer_activation(job_id TEXT,binding_sha256 TEXT,binding_json TEXT);
CREATE TABLE reference_viewer_v3_policy(job_id TEXT,policy_json TEXT,policy_sha256 TEXT);
INSERT INTO reference_viewer_v3_copy_hold VALUES ('j','x','x','{}');
INSERT INTO reference_jobs VALUES ('j','x',1,'staging','x','x','x','{}');
INSERT INTO reference_viewer_submission_owners VALUES ('j','x','x','staging','x');
INSERT INTO reference_viewer_envelopes_v2 VALUES ('j','x','x','x','x','{}');
INSERT INTO reference_viewer_activation VALUES ('j','x','{}');
INSERT INTO reference_viewer_v3_policy VALUES ('j','{}','x');
""")
     db.execute('UPDATE '+table+' SET '+column+'=?',(bad,))
     actual=json.loads
     def checked(value,*args,**kwargs):
      if value==bad: raise AssertionError('malformed JSON reached decoder')
      return actual(value,*args,**kwargs)
     with patch.dict('sys.modules',{'rfc8785':types.SimpleNamespace()}), patch('processing.reference_copy_hold_integrity.json.loads',side_effect=checked):
      with self.assertRaises(ValueError):held_copy_ids(db)

if __name__=='__main__':unittest.main()
