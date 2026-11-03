"""Synthetic paired test. Viewer root must be exact clean c92deb5 checkout."""
import json, os, queue, sqlite3, subprocess, tempfile, threading, time, unittest
from pathlib import Path
from contextlib import closing
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from processing.viewer_source_v2_reconcile_candidate import reconcile, request_id, SyntheticViewerV2Ledger

VIEWER=os.environ.get('VIEWER_C92_ROOT')
@unittest.skipUnless(VIEWER, 'VIEWER_C92_ROOT must name exact Viewer c92deb5 checkout')
class PairedBrokerTest(unittest.TestCase):
    def test_actual_viewer_broker_and_processing_reconciliation(self):
        self.assertEqual(subprocess.check_output(['git','rev-parse','HEAD'],cwd=VIEWER,text=True).strip(), 'c92deb5ba143fd302bdf787ba832e0ac1bd0e2f0')
        with tempfile.TemporaryDirectory() as root:
            script=Path(__file__).resolve().parent/'fixtures/viewer_source_broker_process.cjs'
            process=subprocess.Popen(['node',str(script),VIEWER,root],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,bufsize=1)
            ledger=None
            def line_with_deadline():
                result=queue.Queue(maxsize=1)
                reader=threading.Thread(target=lambda: result.put(process.stdout.readline()),daemon=True)
                reader.start()
                try: return result.get(timeout=5)
                except queue.Empty: self.fail('Viewer synthetic broker timed out')
            try:
                line=line_with_deadline()
                if not line: self.fail('Viewer synthetic broker did not start')
                fixture=json.loads(line); manifest=fixture['manifest'];binding=fixture['binding'];transfer=fixture['transfer'];grant=fixture['grant']
                objects=[{k:o[k] for k in ('id','byteSize','sha256')} for o in manifest['objects']]
                policy={k:transfer[k] for k in ('chunkBytes','requestBudget','byteBudget','attemptBudget')}
                contract=reconcile(binding=binding,transfer=transfer,grant=grant,objects=objects,trusted_manifest=manifest,trusted_policy=policy,now=time.time())
                ledger=SyntheticViewerV2Ledger(str(Path(root)/'processing.sqlite'))
                obj=objects[0];rid=request_id(binding,obj,0,obj['byteSize'])
                def fetch():
                    headers={'Authorization':'Bearer '+grant['token'],'Range':'bytes=0-%d'%(obj['byteSize']-1),'If-Match':'"sha256-%s"'%obj['sha256'],'X-Processing-Request-ID':rid}
                    request=Request('http://127.0.0.1:%d/api/processing/source-range/v2/objects/%s'%(fixture['port'],obj['id']),headers=headers)
                    try:
                        with urlopen(request,timeout=5) as response:return response.status,response.read(),response.headers.get('X-Processing-Request-ID')
                    except HTTPError as e:return e.code,e.read(),None
                for n in (1,2):
                    charged=ledger.record_attempt(contract,object_id=obj['id'],start=0,length=obj['byteSize'],claimed_request_id=rid)
                    self.assertEqual((charged['requests'],charged['bytes'],charged['attempts']),(1,obj['byteSize'],n))
                    status,data,returned=fetch();self.assertEqual((status,data,returned),(206,b'synthetic-photo-123',rid))
                process.stdin.write('revoke\n');process.stdin.flush();self.assertEqual(line_with_deadline().strip(),'revoked')
                charged=ledger.record_attempt(contract,object_id=obj['id'],start=0,length=obj['byteSize'],claimed_request_id=rid)
                self.assertEqual(charged['attempts'],3)
                self.assertIn(fetch()[0],(401,403))
                with self.assertRaises(ValueError):ledger.record_attempt(contract,object_id=obj['id'],start=0,length=obj['byteSize'],claimed_request_id=rid)
                with closing(sqlite3.connect(str(Path(root)/'viewer-broker.sqlite'))) as db:
                    self.assertEqual(db.execute('SELECT requests_used,bytes_used,attempts_used FROM source_range_usage').fetchone(),(1,obj['byteSize'],2))
            finally:
                if ledger is not None: ledger.close()
                if process.poll() is None:
                    process.stdin.close()
                    try: process.wait(timeout=5)
                    except subprocess.TimeoutExpired: process.kill();process.wait(timeout=5)
                process.stdout.close();process.stderr.close()
                self.assertIsNotNone(process.returncode)

if __name__=='__main__':unittest.main()
