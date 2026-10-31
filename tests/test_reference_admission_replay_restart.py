"""Lost submission response survives actual coordinator process exit."""
import copy
import json
import multiprocessing
import os
from pathlib import Path
import tempfile
import unittest
import uuid

from processing.prototype import Queue
from processing.reference_api import ReferenceAPI
from processing.reference_store import ReferenceStore,PROFILE

STAGING='synthetic-staging-token-0001'
PRODUCTION='synthetic-production-token-0002'
FIXTURE=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())

def api(store,capability,options):
    return ReferenceAPI(store,bearer_tokens={STAGING:'staging',PRODUCTION:'production'},
        capability_fingerprint=capability,options_fingerprint=options,allowed_odm_options={'pc-quality','projection'})

def request(adapter,payload,token=STAGING):
    return adapter.handle('POST','/ltds/v1/jobs',{'Authorization':'Bearer '+token},
        json.dumps(payload).encode(),protected_transport=True)

def commit_then_die(directory):
    queue=Queue(directory);queue.allowed_profiles.add(PROFILE);store=ReferenceStore(queue)
    adapter=api(store,FIXTURE['capabilities']['capabilityFingerprint'],
        FIXTURE['capabilities']['engines']['odm']['optionsFingerprint'])
    response=request(adapter,FIXTURE['submission'])
    os._exit(0 if response.status==201 else 2)


class AdmissionReplayRestartTests(unittest.TestCase):
    def test_exact_replay_after_process_death_and_capability_change(self):
        context=multiprocessing.get_context('spawn')
        with tempfile.TemporaryDirectory() as directory:
            child=context.Process(target=commit_then_die,args=(directory,));child.start();child.join(10)
            if child.is_alive():child.terminate();child.join(5);self.fail('fixture child did not exit')
            self.assertEqual(child.exitcode,0)
            queue=Queue(directory);queue.allowed_profiles.add(PROFILE);store=ReferenceStore(queue)
            try:
                adapter=api(store,'c'*64,'d'*64)
                replay=request(adapter,FIXTURE['submission']);self.assertEqual(replay.status,200)
                found=store.find_submission(FIXTURE['submission']['submissionId'],environment='staging')
                self.assertEqual(replay.body['jobId'],found['jobId'])
                self.assertEqual(len(store.list()),1)
                changed=copy.deepcopy(FIXTURE['submission']);changed['manifest']['revision']+=1
                self.assertNotEqual(request(adapter,changed).status,200)
                fresh=copy.deepcopy(FIXTURE['submission']);fresh['submissionId']=str(uuid.uuid4())
                self.assertEqual(request(adapter,fresh).status,400)
                self.assertNotEqual(request(adapter,FIXTURE['submission'],token=PRODUCTION).status,200)
                self.assertEqual(len(store.list()),1)
            finally:queue.close()
