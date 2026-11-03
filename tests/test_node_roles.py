from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import os
import json
import tempfile
import unittest
from unittest.mock import patch

from processing.config import from_environment
from processing.policy import plan
from processing.prototype import PHOTO_PROFILE, PROFILE, Queue
from tests.test_service import TOKEN


class NodeRoleTests(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.TemporaryDirectory()
        self.queue = Queue(self.root.name, True, node_ids=('cluster', 'main'),
                           treedar_node_ids=('main',))

    def tearDown(self):
        self.queue.close()
        self.root.cleanup()

    def submit(self, name, profile, environment='staging'):
        return self.queue.submit({'requestId': name, 'profile': profile,
                                  'environment': environment})[0]

    def test_inventory_migration_preserves_legacy_local_photo_limit(self):
        node = {'id':'small', 'physicalHostId':'cluster-pc', 'origin':'http://192.0.2.1:3000',
                'tokenEnv':'SMALL_TOKEN', 'maxPhotos':1000, 'capabilities':['odm']}
        env = {'PROCESSING_API_TOKEN':TOKEN, 'PROCESSING_PER_NODE_SLOTS':'1',
               'LOCAL_NODE_URL':'http://192.0.2.1:3001', 'LOCAL_MAX_PHOTOS':'1000'}
        with patch.dict(os.environ, env, clear=True):
            for limit in (None, 1001):
                with self.subTest(limit=limit), patch.dict(os.environ, {'PROCESSING_NODE_INVENTORY':json.dumps([{**node, 'maxPhotos':limit}])}):
                    with self.assertRaisesRegex(ValueError, 'preserve existing local photo limit'):
                        from_environment()
            with patch.dict(os.environ, {'PROCESSING_NODE_INVENTORY':json.dumps([node])}):
                self.assertEqual(from_environment().node_inventory[0]['maxPhotos'], 1000)

    def claim(self, node, profile):
        return self.queue.claim({'workerId': node + '-worker', 'nodeId': node,
                                 'profiles': [profile]})

    def complete(self, job):
        self.queue.complete(job['id'], {'claimToken': job['claimToken'],
                                      'status': 'succeeded', 'result': {}})

    def test_capabilities_and_ineligible_claim_cannot_bypass_production(self):
        self.assertEqual(self.queue.get_node('cluster')['allowedProfiles'], [PHOTO_PROFILE])
        self.assertEqual(self.queue.get_node('main')['allowedProfiles'], sorted([PHOTO_PROFILE, PROFILE]))
        photo = self.submit('photo', PHOTO_PROFILE)
        trees = self.submit('trees', PROFILE, 'production')
        self.assertIsNone(self.claim('cluster', PROFILE))
        self.assertIsNone(self.claim('cluster', PHOTO_PROFILE))
        self.assertEqual(self.queue.get(trees['id'])['status'], 'queued')
        claimed = self.claim('main', PROFILE)
        self.assertEqual(claimed['id'], trees['id'])
        self.complete(claimed)
        self.assertEqual(self.claim('cluster', PHOTO_PROFILE)['id'], photo['id'])

    def test_both_engine_directions_share_global_slot_and_hold(self):
        for first_profile, second_profile in ((PROFILE, PHOTO_PROFILE), (PHOTO_PROFILE, PROFILE)):
            with self.subTest(first=first_profile):
                self.submit('first-' + first_profile, first_profile)
                self.submit('second-' + first_profile, second_profile)
                first_node = 'main' if first_profile == PROFILE else 'cluster'
                second_node = 'main' if second_profile == PROFILE else 'cluster'
                first = self.claim(first_node, first_profile)
                self.assertIsNone(self.claim(second_node, second_profile))
                self.queue.operator(first['id'], 'hold', {'note': 'inspect worker'})
                self.assertIsNone(self.claim(second_node, second_profile))
                self.queue.operator(first['id'], 'resolve', {'note': 'worker stopped', 'workerStopped': True})
                self.complete(self.claim(second_node, second_profile))

    def test_concurrent_cross_node_claims_have_one_global_winner(self):
        self.submit('photo', PHOTO_PROFILE)
        self.submit('trees', PROFILE)
        with ThreadPoolExecutor(max_workers=8) as pool:
            claims = list(pool.map(lambda i: self.claim('cluster' if i % 2 else 'main',
                                PHOTO_PROFILE if i % 2 else PROFILE), range(8)))
        self.assertEqual(sum(job is not None for job in claims), 1)

    def test_main_maintenance_preserves_priority_and_node_eligibility(self):
        self.submit('production-trees', PROFILE, 'production')
        self.submit('staging-photo', PHOTO_PROFILE)
        stamp = datetime.now(timezone.utc).isoformat()
        self.queue.schedule_node('main', {'expectedRevision': 0, 'drainAt': stamp,
            'notBefore': stamp, 'afterJobId': None, 'targetImage': 'sha256:' + 'a' * 64})
        self.assertIsNone(self.claim('main', PROFILE))
        self.assertIsNone(self.claim('cluster', PHOTO_PROFILE))
        self.assertIsNone(self.claim('cluster', PROFILE))
        self.assertEqual([job['status'] for job in self.queue.list()], ['queued', 'queued'])

    def test_invalid_role_configuration_rejected_before_state_is_created(self):
        for nodes in ((), ('main', 'main'), ('unknown',), 'main', ([],)):
            with self.subTest(nodes=nodes), tempfile.TemporaryDirectory() as directory:
                with self.assertRaises(ValueError):
                    Queue(directory, node_ids=('cluster', 'main'), treedar_node_ids=nodes)


class NodeRoleConfigTests(unittest.TestCase):
    def environment(self, **values):
        return patch.dict(os.environ, {'PROCESSING_API_TOKEN': TOKEN, **values}, clear=True)

    def test_default_cap_includes_1000_and_routes_1001_to_main(self):
        with self.environment():
            config = from_environment()
        self.assertEqual(config.local_max_photos, 1000)
        nodes = {'ready': True, 'maxImages': None}
        for count, expected in ((1000, 'local'), (1001, 'main')):
            self.assertEqual(plan(count, 1, nodes, nodes, config.local_max_photos,
                                  config.local_max_input_bytes)['decision'], expected)

    def test_main_role_and_invalid_environment_settings(self):
        with self.environment(PROCESSING_PROTOTYPE_NODE_IDS='cluster,main', PROCESSING_TREEDAR_NODE_IDS='main'):
            self.assertEqual(from_environment().treedar_node_ids, ('main',))
        for value in ('', 'main,main', 'missing', 'main, cluster'):
            with self.subTest(value=value), self.environment(PROCESSING_PROTOTYPE_NODE_IDS='cluster,main',
                                                            PROCESSING_TREEDAR_NODE_IDS=value):
                with self.assertRaises(ValueError):
                    from_environment()
