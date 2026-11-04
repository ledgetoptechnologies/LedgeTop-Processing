"""Held-only prepared-v3 equivalents of legacy scheduler regressions."""
import copy, json, threading, unittest, uuid
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from processing import reference_protocol as p
from processing.prototype import VIEWER_PROFILE
from processing.reference_store import ReferenceStore
from processing.reference_prepared_api_candidate import PreparedSubmissionAPI
from processing.prepared_envelope_jcs_candidate import PROTOCOL
import tests.test_reference_store as original

class PreparedSchedulerTests(unittest.TestCase):
    setUp = original.ReferenceStoreTests.setUp
    tearDown = original.ReferenceStoreTests.tearDown
    new_submission = original.ReferenceStoreTests.new_submission

    def prepared(self, store, validated):
        env=validated['environment']
        raw={key:copy.deepcopy(validated[key]) for key in ('schemaVersion','submissionId','manifest','pipeline','capabilityFingerprint')}
        caps=self.fixture['capabilities']
        legacy=dict(capabilityFingerprint=caps['capabilityFingerprint'], optionsFingerprint=caps['engines']['odm']['optionsFingerprint'], allowedOdmOptions=['resolution'], availableEngines=['odm'])
        policy=dict(capabilityFingerprint=legacy['capabilityFingerprint'], optionsFingerprint=legacy['optionsFingerprint'], availableEngines=['odm'], optionDefinitions={'resolution': {'type':'float','domain':{'min':-100,'max':100}}})
        token='synthetic-prepared-test-token-001'
        api=PreparedSubmissionAPI(store, credentials=[dict(token=token,principalId='viewer-a',environment=env)], legacy_bearer_tokens=[], policies={'staging':legacy,'production':legacy}, jcs_policies={'staging':policy,'production':policy}, expected_authority_id=self.queue.authority_id)
        def post(path, body):
            return api.handle('POST',path,{'Authorization':'Bearer '+token,'X-LTDS-Expected-Coordinator-Authority':self.queue.authority_id},json.dumps(body).encode(),protected_transport=True)
        envelope=dict(schemaVersion=3,protocol=PROTOCOL,submission=raw,viewerTaskId=str(uuid.uuid4()),viewerAttemptId=str(uuid.uuid4()),activationId=str(uuid.uuid4()))
        response=post('/ltds/prepared/v3/submissions',envelope)
        self.assertEqual(response.status,201,response.body)
        found=store.find_viewer_envelope_v2(raw['submissionId'],authenticated_principal='viewer-a',authenticated_environment=env,expected_protocol=PROTOCOL)
        activation=dict(activationId=envelope['activationId'],viewerTaskId=envelope['viewerTaskId'],viewerAttemptId=envelope['viewerAttemptId'],envelopeSha256=found['envelopeSha256'])
        with patch.object(ReferenceStore,'_v3_source_contract_ready',return_value=True):
            result=post('/ltds/prepared/v3/submissions/'+raw['submissionId']+'/activation',activation)
        self.assertEqual(result.status,200,result.body)
        return response.body,True


    def test_candidate_inventory_atomic_discovery_and_claims(self):
            self.queue.per_node_slots = True
            inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                         dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
            ready = {node['id']: {'ready': True, 'taskQueueCount': 0, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
                     for node in inventory}
            store = ReferenceStore(self.queue, jcs_runtime_enabled=True, node_inventory=inventory, node_readiness=lambda n: ready[n])
            first, _ = self.prepared(store, self.new_submission())
            second, _ = self.prepared(store, self.new_submission())
            self.assertEqual(store.next_for_node('cluster')['jobId'], first['jobId'])
            self.assertIsNone(store.next_for_node('node-2'))
            claim = lambda job, node: store.claim(job, node_id=node, worker_id=node,
                worker_instance_id='instance', claim_request_id='claim-' + job)
            self.assertIsNone(claim(first['jobId'], 'node-2'))
            self.assertIsNotNone(claim(first['jobId'], 'cluster'))
            self.assertEqual(store.next_for_node('node-2')['jobId'], second['jobId'])
            self.assertIsNotNone(claim(second['jobId'], 'node-2'))
            self.assertEqual([self.queue.get(job['jobId'])['nodeId'] for job in (first, second)],
                             ['cluster', 'node-2'])

    def test_candidate_inventory_bad_readiness_fails_closed(self):
            self.queue.per_node_slots = True
            inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                         dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
            observations = {'cluster': None, 'node-2': None}
            store = ReferenceStore(self.queue, jcs_runtime_enabled=True, node_inventory=inventory,
                                   node_readiness=lambda name: observations[name])
            job, _ = self.prepared(store, self.new_submission())
            malformed = (None, {}, {'ready': True, 'taskQueueCount': False, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024},
                         {'ready': True, 'taskQueueCount': 0, 'maxImages': True},
                         {'ready': True, 'taskQueueCount': 1, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024},
                         {'ready': True, 'taskQueueCount': 0},
                         {'ready': False, 'taskQueueCount': 0, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024})
            for report in malformed:
                with self.subTest(report=report):
                    observations['cluster'] = report
                    self.assertIsNone(store.next_for_node('cluster'))
                    self.assertIsNone(store.claim(job['jobId'], node_id='cluster', worker_id='worker',
                        worker_instance_id='instance', claim_request_id='bad-report'))
                    self.assertEqual(self.queue.get(job['jobId'])['status'], 'queued')
            observations['cluster'] = {'ready': True, 'taskQueueCount': 0, 'maxImages': 1, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
            self.assertEqual(store.next_for_node('cluster')['jobId'], job['jobId'])

    def test_candidate_inventory_rechecks_maintenance_after_discovery(self):
            self.queue.per_node_slots = True
            inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                         dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
            report = {'ready': True, 'taskQueueCount': 0, 'maxImages': None,
                      'availableScratchBytes': 1000000, 'outputBudgetBytes': 1024}
            store = ReferenceStore(self.queue, jcs_runtime_enabled=True, node_inventory=inventory, node_readiness=lambda _: report)
            job, _ = self.prepared(store, self.new_submission())
            self.assertEqual(store.next_for_node('cluster')['jobId'], job['jobId'])
            now = datetime.now(timezone.utc).isoformat()
            self.queue.schedule_node('cluster', {'expectedRevision': 0, 'drainAt': now,
                'notBefore': now, 'afterJobId': None, 'targetImage': 'sha256:' + 'a' * 64})
            self.assertIsNone(store.claim(job['jobId'], node_id='cluster', worker_id='worker',
                worker_instance_id='instance', claim_request_id='maintenance-race'))
            self.assertEqual(store.next_for_node('node-2')['jobId'], job['jobId'])
            self.assertEqual(self.queue.get(job['jobId'])['status'], 'queued')

    def test_candidate_inventory_rechecks_node_readiness_on_claim(self):
            self.queue.per_node_slots = True
            inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                         dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
            ready = {n['id']: {'ready': True, 'taskQueueCount': 0, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
                     for n in inventory}
            store = ReferenceStore(self.queue, jcs_runtime_enabled=True, node_inventory=inventory, node_readiness=lambda n: ready[n])
            job, _ = self.prepared(store, self.new_submission())
            self.assertEqual(store.next_for_node('cluster')['jobId'], job['jobId'])
            ready['cluster'] = {'ready': False, 'taskQueueCount': 0, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
            def claim(node):
                return store.claim(job['jobId'], node_id=node, worker_id=node,
                    worker_instance_id='instance', claim_request_id='claim-' + node)
            self.assertIsNone(claim('cluster'))
            self.assertEqual(self.queue.get(job['jobId'])['status'], 'queued')
            self.assertEqual(store.next_for_node('node-2')['jobId'], job['jobId'])
            self.assertIsNotNone(claim('node-2'))

    def test_candidate_inventory_requires_capacity_for_inputs_and_outputs(self):
            self.queue.per_node_slots = True
            inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                         dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
            reports = {'cluster': {'ready': True, 'taskQueueCount': 0, 'maxImages': None,
                        'availableScratchBytes': 1026, 'outputBudgetBytes': 1024},
                       'node-2': {'ready': True, 'taskQueueCount': 0, 'maxImages': None,
                        'availableScratchBytes': 1027, 'outputBudgetBytes': 1024}}
            store = ReferenceStore(self.queue, jcs_runtime_enabled=True, node_inventory=inventory, node_readiness=lambda n: reports[n])
            job, _ = self.prepared(store, self.new_submission())
            self.assertIsNone(store.next_for_node('cluster'))
            self.assertEqual(store.next_for_node('node-2')['jobId'], job['jobId'])
            del reports['node-2']['availableScratchBytes']
            self.assertIsNone(store.next_for_node('node-2'))
            self.assertEqual(self.queue.get(job['jobId'])['status'], 'queued')

    def test_reference_can_pass_viewer_job_only_on_photo_ineligible_node(self):
            self.queue.per_node_slots = True
            self.queue.allowed_profiles.add(VIEWER_PROFILE)
            inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                         dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
            ready = {'ready': True, 'taskQueueCount': 0, 'maxImages': None,
                     'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
            store = ReferenceStore(self.queue, jcs_runtime_enabled=True, node_inventory=inventory, node_readiness=lambda _: ready)
            viewer, _ = self.queue.submit({'requestId': 'viewer-first', 'environment': 'staging',
                                           'profile': VIEWER_PROFILE})
            with self.queue._db() as db:
                db.execute('CREATE TABLE viewer_tasks(uuid TEXT, job_id TEXT, removed INTEGER)')
                db.execute('CREATE TABLE viewer_files(uuid TEXT, is_photo INTEGER)')
                db.execute('INSERT INTO viewer_tasks VALUES(?,?,0)', ('viewer-task', viewer['id']))
                db.executemany('INSERT INTO viewer_files VALUES(?,1)', [('viewer-task',), ('viewer-task',)])
            reference, _ = self.prepared(store, self.new_submission())
            self.assertEqual(store.next_for_node('cluster')['jobId'], reference['jobId'])
            self.assertIsNone(store.next_for_node('node-2'))
            self.assertIsNone(store.claim(reference['jobId'], node_id='node-2', worker_id='main',
                                          worker_instance_id='instance', claim_request_id='wrong-node'))
            self.assertIsNotNone(store.claim(reference['jobId'], node_id='cluster', worker_id='local',
                                             worker_instance_id='instance', claim_request_id='small-node'))
            self.assertEqual(self.queue.get(viewer['id'])['status'], 'queued')

    def test_candidate_inventory_racing_claims_do_not_share_a_slot(self):
            self.queue.per_node_slots = True
            inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                         dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
            store = ReferenceStore(self.queue, jcs_runtime_enabled=True, node_inventory=inventory,
                node_readiness=lambda _: {'ready': True, 'taskQueueCount': 0, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024})
            job, _ = self.prepared(store, self.new_submission())
            def claim(i):
                return store.claim(job['jobId'], node_id='cluster', worker_id='worker',
                    worker_instance_id='instance', claim_request_id='race-' + str(i))
            with ThreadPoolExecutor(max_workers=2) as pool:
                claims = list(pool.map(claim, range(2)))
            self.assertEqual(sum(value is not None for value in claims), 1)

    def test_candidate_inventory_skips_ineligible_head_without_bypassing_production(self):
            self.queue.per_node_slots = True
            inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                         dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
            ready = {node['id']: {'ready': True, 'taskQueueCount': 0, 'maxImages': None, 'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
                     for node in inventory}
            store = ReferenceStore(self.queue, jcs_runtime_enabled=True, node_inventory=inventory, node_readiness=lambda n: ready[n])
            def submission(images, environment):
                raw = copy.deepcopy(self.fixture['submission'])
                raw['submissionId'] = str(uuid.uuid4())
                m = raw['manifest']
                m['objects'] = [{**m['objects'][0], 'id': 'image-' + str(i),
                                 'name': 'image-' + str(i) + '.jpg'} for i in range(images)]
                m['objectCount'] = images
                m['totalBytes'] = images * 3
                m['inputCounts']['images'] = images
                m['sha256'] = p.manifest_sha256(m)
                return p.validate_submission(raw, environment=environment,
                    capability_fingerprint=raw['capabilityFingerprint'])
            def claim(job, node):
                return store.claim(job['jobId'], node_id=node, worker_id=node,
                    worker_instance_id='instance', claim_request_id='claim-' + job['jobId'])
            running, _ = self.prepared(store, submission(2, 'production'))
            self.assertIsNotNone(claim(running, 'node-2'))
            large, _ = self.prepared(store, submission(2, 'production'))
            staging, _ = self.prepared(store, submission(1, 'staging'))
            small, _ = self.prepared(store, submission(1, 'production'))
            self.assertEqual(store.next_for_node('cluster')['jobId'], small['jobId'])
            self.assertIsNone(claim(staging, 'cluster'))
            self.assertIsNotNone(claim(small, 'cluster'))
            self.assertEqual(self.queue.get(large['jobId'])['status'], 'queued')
            self.assertEqual(self.queue.get(staging['jobId'])['status'], 'queued')

    def test_mixed_profile_concurrent_claims_take_distinct_eligible_nodes(self):
            self.queue.per_node_slots = True
            self.queue.allowed_profiles.add(VIEWER_PROFILE)
            inventory = [dict(id='cluster', maxPhotos=1, capabilities=['odm']),
                         dict(id='node-2', maxPhotos=None, capabilities=['odm'])]
            ready = {'ready': True, 'taskQueueCount': 0, 'maxImages': None,
                     'availableScratchBytes': 1000000000, 'outputBudgetBytes': 1024}
            store = ReferenceStore(self.queue, jcs_runtime_enabled=True, node_inventory=inventory, node_readiness=lambda _: ready)
            viewer, _ = self.queue.submit({'requestId': 'mixed-viewer', 'environment': 'staging',
                                           'profile': VIEWER_PROFILE})
            with self.queue._db() as db:
                db.execute('CREATE TABLE viewer_tasks(uuid TEXT, job_id TEXT, removed INTEGER)')
                db.execute('CREATE TABLE viewer_files(uuid TEXT, is_photo INTEGER)')
                db.execute('INSERT INTO viewer_tasks VALUES(?,?,0)', ('viewer-task', viewer['id']))
                db.executemany('INSERT INTO viewer_files VALUES(?,1)', [('viewer-task',), ('viewer-task',)])
            reference, _ = self.prepared(store, self.new_submission())
            barrier = threading.Barrier(2)
            def take_viewer():
                barrier.wait(timeout=5)
                return self.queue.claim({'workerId': 'viewer', 'nodeId': 'node-2',
                    'profiles': [VIEWER_PROFILE], 'expectedJobId': viewer['id']},
                    allow_viewer_same_priority_skip=True)
            def take_reference():
                barrier.wait(timeout=5)
                return store.claim(reference['jobId'], node_id='cluster', worker_id='reference',
                                   worker_instance_id='instance', claim_request_id='mixed-claim')
            with ThreadPoolExecutor(max_workers=2) as pool:
                futures = (pool.submit(take_viewer), pool.submit(take_reference))
                results = [future.result(timeout=10) for future in futures]
            self.assertTrue(all(results))
            self.assertEqual(self.queue.get(viewer['id'])['nodeId'], 'node-2')
            self.assertEqual(self.queue.get(reference['jobId'])['nodeId'], 'cluster')
