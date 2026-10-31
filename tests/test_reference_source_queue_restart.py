"""Exercise metadata-only admission; this does not qualify source retrieval."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
import uuid

from processing import reference_protocol as protocol
from processing.maintenance import Conflict
from processing.prototype import Queue
from processing.reference_store import ReferenceStore,PROFILE


class SourceQueueRestartTest(unittest.TestCase):
    def test_reference_metadata_survives_restart_without_source_and_prioritizes_production(self):
        fixture=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-v1.contract.json').read_text())
        def submission(environment):
            raw=copy.deepcopy(fixture['submission']);raw['submissionId']=str(uuid.uuid4())
            obj=raw['manifest']['objects'][0];obj['byteSize']=50*1024**3
            raw['manifest']['totalBytes']=sum(item['byteSize'] for item in raw['manifest']['objects'])
            raw['manifest']['sha256']=protocol.manifest_sha256(raw['manifest'])
            return protocol.validate_submission(raw,environment=environment,capability_fingerprint=raw['capabilityFingerprint'])
        staging,production=submission('staging'),submission('production')
        with tempfile.TemporaryDirectory() as state:
            queue=Queue(state,node_ids=('cluster','main'));queue.allowed_profiles.add(PROFILE);store=ReferenceStore(queue)
            try:
                stage_job,created=store.submit(staging);self.assertTrue(created)
                prod_job,created=store.submit(production);self.assertTrue(created)
                self.assertEqual(store.get(prod_job['jobId'],private=True)['submission']['manifest'],production['manifest'])
                self.assertFalse(any(path.is_file() and path.suffix.lower() in ('.jpg','.jpeg','.tif') for path in Path(state).rglob('*')))
            finally:queue.close()
            queue=Queue(state,node_ids=('cluster','main'));queue.allowed_profiles.add(PROFILE);store=ReferenceStore(queue)
            try:
                replay,created=store.submit(production);self.assertFalse(created);self.assertEqual(replay['jobId'],prod_job['jobId'])
                self.assertEqual(store.get(stage_job['jobId'],private=True)['submission']['manifest'],staging['manifest'])
                changed=copy.deepcopy(production);changed['manifest']['objects'][0]['sha256']='a'*64
                changed['manifest']['sha256']=protocol.manifest_sha256(changed['manifest'])
                raw={k:changed[k] for k in ('schemaVersion','submissionId','manifest','pipeline','capabilityFingerprint')}
                changed=protocol.validate_submission(raw,environment='production',capability_fingerprint=raw['capabilityFingerprint'])
                with self.assertRaises(Conflict):store.submit(changed)
                same_id_other_environment=protocol.validate_submission({k:production[k] for k in ('schemaVersion','submissionId','manifest','pipeline','capabilityFingerprint')},environment='staging',capability_fingerprint=production['capabilityFingerprint'])
                with self.assertRaises(Conflict):store.submit(same_id_other_environment)
                chosen=store.next_for_node('main');self.assertEqual(chosen['jobId'],prod_job['jobId'])
                self.assertIsNone(store.next_for_node('cluster'))
            finally:queue.close()
