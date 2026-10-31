import io
import json
import os
import subprocess
import sys
import unittest
from ops.gateway_health_probe import probe,Refused

class Response:
    status=200
    def __init__(self,body):self.body=body
    def __enter__(self):return self
    def __exit__(self,*_):pass
    def read(self,n):return self.body[:n]
class Opener:
    def __init__(self,info=b'{"engine":"odm","taskQueueCount":0}',tasks=b'[]'):
        self.values=[info,tasks];self.calls=[]
    def open(self,request,timeout):
        self.calls.append((request.full_url,request.get_method(),timeout))
        return Response(self.values.pop(0))

class Tests(unittest.TestCase):
    def test_only_fixed_gets_and_no_credential_in_output(self):
        opener=Opener();result=probe(b'{"token":"secret-123","engine":"odm"}',opener=opener)
        self.assertEqual([r[0].split('?')[0] for r in opener.calls],['http://nodeodm:3000/info','http://nodeodm:3000/task/list'])
        self.assertTrue(all(r[1]=='GET' for r in opener.calls))
        self.assertNotIn('secret-123',json.dumps(result));self.assertFalse(result['maintenanceAuthorized'])
    @unittest.skipUnless(os.name=='posix','POSIX alarm required')
    def test_stalled_stdin_exits_without_credentials(self):
        process=subprocess.Popen([sys.executable,'-m','ops.gateway_health_probe'],stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        try:
            process.wait(timeout=8)
            output=process.stdout.read()
            self.assertEqual(process.returncode,1)
            self.assertNotIn(b'token',output)
            self.assertEqual(process.stderr.read(),b'')
        finally:
            if process.poll() is None:process.kill();process.wait(timeout=2)
            for stream in (process.stdin,process.stdout,process.stderr):stream.close()

    def test_bad_input_ambiguous_json_and_busy_hold(self):
        for raw in (b'{"token":"a","token":"b","engine":"odm"}',b'{"token":"x","engine":"odm","url":"x"}',b'x'*8193):
            with self.subTest(raw=raw[:40]),self.assertRaises(Refused):probe(raw,opener=Opener())
        for info,tasks in ((b'{"engine":"odm","taskQueueCount":true}',b'[]'),
                           (b'{"engine":"odm","taskQueueCount":0}',b'[{}]'),
                           (b'{"engine":"odm","taskQueueCount":0,"engine":"odm"}',b'[]'),
                           (b'{"engine":"odm","taskQueueCount":0}',b'x'*65537)):
            with self.subTest(info=info[:50],tasks=tasks[:5]),self.assertRaises(Refused):
                probe(b'{"token":"","engine":"odm"}',opener=Opener(info,tasks))
