import copy, hashlib, http.client, json, os, subprocess, sys, tempfile, threading, unittest, uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from processing.prototype import Queue
from processing.reference_store import ReferenceStore, PROFILE, JCSRuntimeUnavailable
from processing.maintenance import Conflict
from processing.reference_prepared_api_candidate import PreparedSubmissionAPI
from processing.prepared_envelope_jcs_candidate import PROTOCOL

class JCSHTTPIntegrationTests(unittest.TestCase):
    def test_http_store_restart_replay_digest_and_policy(self):
        temp=tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        queue=Queue(temp.name); queue.allowed_profiles.add(PROFILE); self.addCleanup(queue.close)
        disabled_store=ReferenceStore(queue)
        store=ReferenceStore(queue,jcs_runtime_enabled=True)
        fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
        legacy=dict(capabilityFingerprint=fixture['capabilities']['capabilityFingerprint'],optionsFingerprint=fixture['capabilities']['engines']['odm']['optionsFingerprint'],allowedOdmOptions=['resolution'],availableEngines=['odm'])
        policy=dict(capabilityFingerprint=legacy['capabilityFingerprint'],optionsFingerprint=legacy['optionsFingerprint'],availableEngines=['odm'],optionDefinitions={'resolution':{'type':'float','domain':{'min':-100,'max':100}},'quality':{'type':'enum','domain':['high','ultra']}})
        token='synthetic-jcs-viewer-token-0001'
        def make_api(store,policy,authority=None): return PreparedSubmissionAPI(store,credentials=[dict(token=token,principalId='viewer-a',environment='production'),dict(token='synthetic-jcs-other-token-0001',principalId='viewer-b',environment='production'),dict(token='synthetic-jcs-stage-token-0001',principalId='viewer-a',environment='staging')],legacy_bearer_tokens=[],policies={'production':legacy,'staging':legacy},jcs_policies={'production':policy,'staging':policy},expected_authority_id=queue.authority_id if authority is None else authority)
        with self.assertRaises(ValueError):
            PreparedSubmissionAPI(store,credentials=[dict(token=token,principalId='viewer-a',environment='production')],legacy_bearer_tokens=[],policies={'production':legacy,'staging':legacy},jcs_policies={'production':policy,'staging':policy})
        api=make_api(store,policy)
        disabled_api=PreparedSubmissionAPI(disabled_store,credentials=[dict(token=token,principalId='viewer-a',environment='production')],legacy_bearer_tokens=[],policies={'production':legacy,'staging':legacy})
        env=dict(schemaVersion=3,protocol=PROTOCOL,submission=copy.deepcopy(fixture['submission']),viewerTaskId=str(uuid.uuid4()),viewerAttemptId=str(uuid.uuid4()),activationId=str(uuid.uuid4()))
        env['submission']['pipeline']['odm']['options']={'resolution':-1.25,'quality':'ultra'}
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                raw=self.rfile.read(int(self.headers.get('content-length','0'))); result=self.server.api.handle('POST',self.path,self.headers,raw,protected_transport=True)
                out=json.dumps(result.body).encode();self.send_response(result.status);self.send_header('Content-Length',str(len(out)));self.end_headers();self.wfile.write(out)
            def do_GET(self):
                result=self.server.api.handle('GET',self.path,self.headers,protected_transport=True)
                out=json.dumps(result.body).encode();self.send_response(result.status);self.send_header('Content-Length',str(len(out)));self.end_headers();self.wfile.write(out)
            def log_message(self,*args): pass
        server=ThreadingHTTPServer(('127.0.0.1',0),Handler);server.api=api
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        def post(obj, path='/ltds/prepared/v3/submissions', caller=token):
            request=Request('http://127.0.0.1:%d%s'%(server.server_port,path),data=json.dumps(obj).encode(),headers={'Authorization':'Bearer '+caller,'X-LTDS-Expected-Coordinator-Authority':queue.authority_id},method='POST')
            try:
                with urlopen(request,timeout=5) as response: return response.status,json.load(response)
            except HTTPError as error: return error.code,json.load(error)
        def get(path,caller=token):
            request=Request('http://127.0.0.1:%d%s'%(server.server_port,path),headers={'Authorization':'Bearer '+caller,'X-LTDS-Expected-Coordinator-Authority':queue.authority_id})
            try:
                with urlopen(request,timeout=5) as response: return response.status,json.load(response)
            except HTTPError as error: return error.code,json.load(error)
        try:
            missing=Request('http://127.0.0.1:%d/ltds/prepared/v3/submissions'%server.server_port,data=json.dumps(env).encode(),headers={'Authorization':'Bearer '+token},method='POST')
            with self.assertRaises(HTTPError) as missing_pin: urlopen(missing,timeout=5)
            self.assertEqual(missing_pin.exception.code,400)
            conn=http.client.HTTPConnection('127.0.0.1',server.server_port,timeout=5)
            try:
                payload=json.dumps(env).encode();conn.putrequest('POST','/ltds/prepared/v3/submissions')
                conn.putheader('Authorization','Bearer '+token)
                conn.putheader('X-LTDS-Expected-Coordinator-Authority',queue.authority_id)
                conn.putheader('X-LTDS-Expected-Coordinator-Authority',queue.authority_id)
                conn.putheader('Content-Length',str(len(payload)));conn.endheaders(payload)
                response=conn.getresponse();self.assertEqual(response.status,400);response.read()
            finally: conn.close()

            wrong=Request('http://127.0.0.1:%d/ltds/prepared/v3/submissions'%server.server_port,data=json.dumps(env).encode(),headers={'Authorization':'Bearer '+token,'X-LTDS-Expected-Coordinator-Authority':str(uuid.uuid4())},method='POST')
            with self.assertRaises(HTTPError) as rejected: urlopen(wrong,timeout=5)
            self.assertEqual(rejected.exception.code,503)
            self.assertEqual(queue.list(),[])
            status,job=post(env); self.assertEqual(status,201)
            self.assertEqual(job['authorityId'],queue.authority_id)
            delivery='/ltds/prepared/v3/submissions/'+env['submission']['submissionId']
            for route in ('/source-grant','/accepted-copies'):
                self.assertEqual(post({'synthetic':'payload'},delivery+route)[0],503)
                self.assertEqual(post({'synthetic':'payload'},delivery+route,'synthetic-jcs-other-token-0001')[0],404)
            with queue._db() as db:
                self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_transfer_grants').fetchone()[0],0)
                self.assertEqual(db.execute('SELECT COUNT(*) FROM reference_acceptances').fetchone()[0],0)

            # A separately configured wrong installation pin must not use the DB's value.
            server.api=make_api(store,policy,str(uuid.uuid4()))
            self.assertEqual(post(env)[0],503)
            server.api=make_api(store,policy)

            recovery='/ltds/prepared/v3/submissions/'+env['submission']['submissionId']
            self.assertEqual(get(recovery)[1]['authorityId'],queue.authority_id)
            with queue._db() as db: db.execute('UPDATE coordinator_identity SET authority_id=? WHERE singleton=1',(str(uuid.uuid4()),))
            self.assertEqual(post(env)[0],503)
            self.assertEqual(get(recovery)[0],503)
            with queue._db() as db: db.execute('DELETE FROM coordinator_identity')
            self.assertEqual(get(recovery)[0],503)
            with queue._db() as db: db.execute('INSERT INTO coordinator_identity VALUES(1,?)',(queue.authority_id,))
            with queue._db() as db: db.execute('DROP TABLE coordinator_identity')
            self.assertEqual(get(recovery)[0],503)
            with queue._db() as db: db.execute('CREATE TABLE coordinator_identity (singleton INTEGER)')
            self.assertEqual(get(recovery)[0],503)
            with queue._db() as db: db.execute('DROP TABLE coordinator_identity')
            with queue._db() as db:
                db.execute('CREATE TABLE coordinator_identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), authority_id TEXT NOT NULL)')
                db.execute('INSERT INTO coordinator_identity VALUES(1,?)',(queue.authority_id,))


            with self.assertRaises(JCSRuntimeUnavailable): disabled_store.next_for_node('cluster')
            with self.assertRaises(JCSRuntimeUnavailable): disabled_store.list()
            failure=disabled_api.handle('GET','/ltds/prepared/v2/submissions/'+env['submission']['submissionId'],{'Authorization':'Bearer '+token,'X-LTDS-Expected-Coordinator-Authority':queue.authority_id},protected_transport=True)
            self.assertEqual((failure.status,failure.body),(503,{'error':'prepared_runtime_unavailable'}))
            with self.assertRaises(ValueError): make_api(disabled_store,policy)

            persisted=store.get(job['jobId'],private=True)['submission']
            self.assertEqual(persisted['pipeline']['odm']['options']['resolution'],-1.25)
            from processing import reference_protocol as rp
            normalized_raw = {k:v for k,v in persisted.items() if k not in ('environment','requestDigest','manifestSha256','pipelineDigest')}
            self.assertEqual(persisted['requestDigest'],rp.digest(normalized_raw))
            self.assertEqual(persisted['pipelineDigest'],rp.digest(normalized_raw['pipeline']))
            module=os.environ.get('NODE_JCS_MODULE')
            self.assertTrue(module, 'pinned Node canonicalize module required')
            script="const j=require(process.argv[1]);let s='';process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>process.stdout.write(j(JSON.parse(s))))"
            node=subprocess.run(['node','-e',script,module],input=json.dumps(env,ensure_ascii=False),text=True,capture_output=True,check=True).stdout
            with queue._db() as db:
                record=db.execute('SELECT envelope_json,envelope_sha256 FROM reference_viewer_envelopes_v2 WHERE job_id=?',(job['jobId'],)).fetchone()
            self.assertEqual(record['envelope_json'],node)
            self.assertEqual(record['envelope_sha256'],hashlib.sha256((PROTOCOL+'\x00').encode()+node.encode()).hexdigest())

            # A reopened store and changed capability snapshot recover existing identity.
            reopened=ReferenceStore(queue,jcs_runtime_enabled=True); changed=copy.deepcopy(policy);changed['capabilityFingerprint']='0'*64
            server.api=make_api(reopened,changed)
            self.assertEqual(post(env),(200,job))
            mutated=copy.deepcopy(env);mutated['submission']['pipeline']['odm']['options']['resolution']=-1.5
            self.assertEqual(post(mutated)[0],409)
            self.assertEqual(len(queue.list()),1)
            # New request under original policy: JCS treats -0 and 0 as equal.
            server.api=make_api(reopened,policy)
            zero=copy.deepcopy(env);zero['submission']['submissionId']=str(uuid.uuid4());zero['viewerAttemptId']=str(uuid.uuid4());zero['activationId']=str(uuid.uuid4())
            zero['submission']['pipeline']['odm']['options']['resolution']=-0.0
            status, zero_job = post(zero); self.assertEqual(status,201)
            zero['submission']['pipeline']['odm']['options']['resolution']=0
            self.assertEqual(post(zero),(200,zero_job))
            zero['submission']['pipeline']['odm']['options']['resolution']=0.25
            self.assertEqual(post(zero)[0],409)
            with self.assertRaisesRegex(JCSRuntimeUnavailable, 'enabled capable runtime'):
                ReferenceStore(queue)
            v2=copy.deepcopy(zero);v2['schemaVersion']=2;v2['protocol']='ltds-viewer-prepared-envelope-v2'
            with self.assertRaises(Conflict):
                reopened.prepare_viewer_envelope_v2(v2,authenticated_principal='viewer-a',authenticated_environment='production',trusted_policy=legacy)
            competing=copy.deepcopy(env);competing['submission']['submissionId']=str(uuid.uuid4());competing['viewerAttemptId']=str(uuid.uuid4());competing['activationId']=str(uuid.uuid4())
            raw=json.dumps(competing).encode()
            def claim(principal):
                try: return reopened.prepare_viewer_envelope_jcs_v3(raw,authenticated_principal=principal,authenticated_environment='production',trusted_policy=policy)[0]['jobId']
                except Conflict: return None
            with ThreadPoolExecutor(max_workers=2) as pool:
                outcomes=list(pool.map(claim,['viewer-a','viewer-b']))
            self.assertEqual(sum(x is not None for x in outcomes),1)
            competing['submission']['submissionId']=str(uuid.uuid4());competing['viewerAttemptId']=str(uuid.uuid4());competing['activationId']=str(uuid.uuid4())
            raw=json.dumps(competing).encode()
            def claim_env(environment):
                try: return reopened.prepare_viewer_envelope_jcs_v3(raw,authenticated_principal='viewer-a',authenticated_environment=environment,trusted_policy=policy)[0]['jobId']
                except Conflict: return None
            with ThreadPoolExecutor(max_workers=2) as pool:
                outcomes=list(pool.map(claim_env,['production','staging']))
            self.assertEqual(sum(x is not None for x in outcomes),1)

            found=reopened.find_viewer_envelope_v2(env['submission']['submissionId'],authenticated_principal='viewer-a',authenticated_environment='production',expected_protocol=PROTOCOL)
            self.assertEqual(found['viewerAttemptId'],env['viewerAttemptId'])
            activation=dict(activationId=env['activationId'],viewerTaskId=env['viewerTaskId'],viewerAttemptId=env['viewerAttemptId'],envelopeSha256=found['envelopeSha256'])
            path='/ltds/prepared/v3/submissions/'+env['submission']['submissionId']+'/activation'
            # Real production behavior must hold until source provenance exists.
            self.assertEqual(post(activation,path)[0],503)
            self.assertEqual(post(activation,path,'synthetic-jcs-other-token-0001')[0],404)
            self.assertEqual(post(activation,path,'synthetic-jcs-stage-token-0001')[0],404)
            altered=dict(activation,viewerTaskId=str(uuid.uuid4()))
            self.assertEqual(post(altered,path)[0],409)
            with patch.object(ReferenceStore,'_v3_source_contract_ready',return_value=True):
                with ThreadPoolExecutor(max_workers=2) as pool:
                    results=list(pool.map(lambda _: post(activation,path),range(2)))
                self.assertEqual([code for code,_ in results],[200,200])
                self.assertCountEqual([body['created'] for _,body in results],[True,False])
                self.assertEqual(results[0][1]['bindingSha256'],results[1][1]['bindingSha256'])
                with queue._db() as db:
                    original=db.execute('SELECT policy_json FROM reference_viewer_v3_policy WHERE job_id=?',(job['jobId'],)).fetchone()[0]
                    db.execute("UPDATE reference_viewer_v3_policy SET policy_json='{}' WHERE job_id=?",(job['jobId'],))
                self.assertEqual(post(activation,path)[0],409)
                server.api=make_api(reopened,policy)
                prefix='/ltds/prepared/v3/submissions/'+env['submission']['submissionId']
                status_code,status=get(prefix+'/status');self.assertEqual(status_code,200);self.assertTrue(status['activationRecorded']);self.assertFalse(status['dispatchReady']);self.assertEqual((status['schemaVersion'],status['view']),(1,'status'))
                self.assertEqual(status['viewerAttemptId'],env['viewerAttemptId']);self.assertEqual(status['authorityId'],queue.authority_id)
                self.assertEqual(get(prefix+'/status','synthetic-jcs-other-token-0001')[0],404)
                # Corrupt private evidence must not distinguish another owner.
                with queue._db() as db:
                    original_digest=db.execute('SELECT envelope_sha256 FROM reference_viewer_envelopes_v2 WHERE job_id=?',(job['jobId'],)).fetchone()[0]
                    db.execute('UPDATE reference_viewer_envelopes_v2 SET envelope_sha256=? WHERE job_id=?',('f'*64,job['jobId']))
                for suffix in ('','/status','/result-manifest','/accepted-copies'):
                    self.assertEqual(get(prefix+suffix,'synthetic-jcs-other-token-0001')[0],404)
                self.assertEqual(get(prefix+'/status')[0],409)
                with queue._db() as db:
                    db.execute('UPDATE reference_viewer_envelopes_v2 SET envelope_sha256=? WHERE job_id=?',(original_digest,job['jobId']))
                    db.execute("UPDATE reference_viewer_v3_policy SET policy_json='{}' WHERE job_id=?",(job['jobId'],))
                code,held=get(prefix+'/status');self.assertEqual(code,200);self.assertTrue(held['activationRecorded']);self.assertFalse(held['dispatchReady'])
                with queue._db() as db:
                    db.execute('UPDATE reference_viewer_v3_policy SET policy_json=? WHERE job_id=?',(original,job['jobId']))

                self.assertEqual(get(prefix+'/result-manifest')[0],503)
                self.assertEqual(get(prefix+'/accepted-copies')[0],503)
                # Synthetic DB injection only: no claim or live processing is performed.
                result=copy.deepcopy(fixture['resultManifest'])
                result.update(jobId=job['jobId'],generation=job['generation'],environment=job['environment'],pipelineDigest=job['pipelineDigest'])
                result['resultManifestSha256']=rp.digest({k:v for k,v in result.items() if k!='resultManifestSha256'})
                with queue._db() as db:
                    db.execute("UPDATE jobs SET status='succeeded' WHERE id=?",(job['jobId'],))
                    db.execute("UPDATE reference_jobs SET phase='awaiting_acceptance',result_json=? WHERE job_id=?",(rp.canonical_json(result).decode(),job['jobId']))
                self.assertEqual(get(prefix+'/result-manifest')[0],503)
                with patch.object(ReferenceStore,'_v3_result_contract_ready',return_value=True):
                    code,body=get(prefix+'/result-manifest');self.assertEqual(code,200)
                    self.assertEqual((body['schemaVersion'],body['view']),(1,'result-manifest'));self.assertEqual(body['resultManifest'],result)
                    self.assertEqual(body['envelopeSha256'],found['envelopeSha256'])
                    self.assertEqual(get(prefix+'/result-manifest','synthetic-jcs-other-token-0001')[0],404)
                    server.api=make_api(ReferenceStore(queue,jcs_runtime_enabled=True),policy)
                    self.assertEqual(get(prefix+'/result-manifest')[1],body)

                with queue._db() as db:
                    db.execute('UPDATE reference_viewer_v3_policy SET policy_json=? WHERE job_id=?',(original,job['jobId']))
                # A reopened store retains the same evidence and does not dispatch.
                server.api=make_api(ReferenceStore(queue,jcs_runtime_enabled=True),policy)
                self.assertFalse(post(activation,path)[1]['created'])
                self.assertIsNone(reopened.next_for_node('cluster'))
                server.api=make_api(reopened,changed)
                self.assertEqual(post(activation,path)[0],409)

            self.assertIsNone(reopened.next_for_node('cluster'))
        finally:
            server.shutdown();server.server_close();thread.join(5)
        self.assertFalse(thread.is_alive())

    def test_missing_dependency_fails_before_reference_schema_mutation(self):
        temp=tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        queue=Queue(temp.name); queue.allowed_profiles.add(PROFILE); self.addCleanup(queue.close)
        with queue._db() as db:
            before=list(db.execute("SELECT name,sql FROM sqlite_master ORDER BY name"))
        with patch.dict(sys.modules, {'rfc8785':None}):
            with self.assertRaises(JCSRuntimeUnavailable): ReferenceStore(queue,jcs_runtime_enabled=True)
        with queue._db() as db:
            self.assertEqual(before,list(db.execute("SELECT name,sql FROM sqlite_master ORDER BY name")))

    def test_process_death_before_sqlite_commit_rolls_back_queue_insert(self):
        temp=tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        queue=Queue(temp.name);queue.allowed_profiles.add(PROFILE);self.addCleanup(queue.close)
        ReferenceStore(queue,jcs_runtime_enabled=True)
        script = """import os,sqlite3,sys
con=sqlite3.connect(sys.argv[1]);con.execute('BEGIN IMMEDIATE')
con.execute("INSERT INTO jobs(id,request_id,environment,profile,status) VALUES('uncommitted','reference:uncommitted','production','reference-pipeline-v1','queued')")
os._exit(42)
"""
        result=subprocess.run([sys.executable,'-c',script,queue.path],timeout=15)
        self.assertEqual(result.returncode,42)
        with queue._db() as db:
            self.assertIsNone(db.execute("SELECT 1 FROM jobs WHERE id='uncommitted'").fetchone())

    def test_post_commit_process_death_restart_recovers_accepted_prepare(self):
        temp=tempfile.TemporaryDirectory(); self.addCleanup(temp.cleanup)
        fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
        env=dict(schemaVersion=3,protocol=PROTOCOL,submission=copy.deepcopy(fixture['submission']),viewerTaskId=str(uuid.uuid4()),viewerAttemptId=str(uuid.uuid4()),activationId=str(uuid.uuid4()))
        env['submission']['pipeline']['odm']['options']={'resolution':-1.25}
        policy=dict(capabilityFingerprint=fixture['capabilities']['capabilityFingerprint'],optionsFingerprint=fixture['capabilities']['engines']['odm']['optionsFingerprint'],availableEngines=['odm'],optionDefinitions={'resolution':{'type':'float','domain':{'min':-100,'max':100}}})
        script = """import json,os,sys
from processing.prototype import Queue
from processing.reference_store import ReferenceStore, PROFILE, JCSRuntimeUnavailable
from processing.maintenance import Conflict
arg=json.load(sys.stdin);q=Queue(arg['path']);q.allowed_profiles.add(PROFILE)
s=ReferenceStore(q,jcs_runtime_enabled=True)
s.prepare_viewer_envelope_jcs_v3(json.dumps(arg['envelope']).encode(),authenticated_principal='viewer-a',authenticated_environment='production',trusted_policy=arg['policy'])
os._exit(23)
"""
        result=subprocess.run([sys.executable,'-c',script],input=json.dumps(dict(path=temp.name,envelope=env,policy=policy)),text=True,timeout=15)
        self.assertEqual(result.returncode,23)
        queue=Queue(temp.name);queue.allowed_profiles.add(PROFILE);self.addCleanup(queue.close)
        with self.assertRaisesRegex(JCSRuntimeUnavailable,'enabled capable runtime'): ReferenceStore(queue)
        store=ReferenceStore(queue,jcs_runtime_enabled=True)
        changed=copy.deepcopy(policy);changed['capabilityFingerprint']='0'*64
        job,created=store.prepare_viewer_envelope_jcs_v3(json.dumps(env).encode(),authenticated_principal='viewer-a',authenticated_environment='production',trusted_policy=changed)
        self.assertFalse(created)
        self.assertEqual(len(queue.list()),1)
        self.assertIsNone(store.next_for_node('cluster'))
        # A missing identity on restart is regenerated by legacy Queue; the
        # separately provisioned old pin must nevertheless refuse it.
        old_authority=queue.authority_id
        queue.close()
        import sqlite3
        with sqlite3.connect(queue.path) as db: db.execute('DELETE FROM coordinator_identity')
        new_queue=Queue(temp.name);new_queue.allowed_profiles.add(PROFILE);self.addCleanup(new_queue.close)
        self.assertNotEqual(old_authority,new_queue.authority_id)
        new_store=ReferenceStore(new_queue,jcs_runtime_enabled=True)
        with self.assertRaises(JCSRuntimeUnavailable):
            new_store.find_viewer_envelope_v2(env['submission']['submissionId'],authenticated_principal='viewer-a',authenticated_environment='production',expected_protocol=PROTOCOL,expected_authority_id=old_authority)

if __name__=='__main__': unittest.main()
