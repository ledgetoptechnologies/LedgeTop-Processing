from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import os
import tempfile
import unittest
from unittest.mock import patch

from processing.config import from_environment
from processing.config import ProcessingNode
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
        # The smaller cluster is first in the configured node order.
        self.assertIsNone(self.claim('main', PHOTO_PROFILE))
        self.assertEqual(self.claim('cluster', PHOTO_PROFILE)['id'], photo['id'])

    def test_later_node_can_take_small_profile_when_preferred_node_is_busy(self):
        first = self.submit('small-first', PHOTO_PROFILE)
        second = self.submit('small-overflow', PHOTO_PROFILE)
        cluster = self.claim('cluster', PHOTO_PROFILE)
        self.assertEqual(cluster['id'], first['id'])
        main = self.claim('main', PHOTO_PROFILE)
        self.assertEqual(main['id'], second['id'])
        self.complete(cluster)
        self.complete(main)

    def test_each_node_has_one_slot_and_other_nodes_can_work_concurrently(self):
        for first_profile, second_profile in ((PROFILE, PHOTO_PROFILE), (PHOTO_PROFILE, PROFILE)):
            with self.subTest(first=first_profile):
                self.submit('first-' + first_profile, first_profile)
                self.submit('second-' + first_profile, second_profile)
                self.submit('third-' + first_profile, first_profile)
                first_node = 'main' if first_profile == PROFILE else 'cluster'
                second_node = 'main' if second_profile == PROFILE else 'cluster'
                first = self.claim(first_node, first_profile)
                # One job per node; a job on another node may run at the same time.
                second = self.claim(second_node, second_profile)
                self.assertIsNotNone(second)
                self.assertEqual(second['nodeId'], second_node)
                # The same node stays occupied while its first job is active.
                self.assertIsNone(self.claim(first_node, first_profile))
                self.queue.operator(first['id'], 'hold', {'note': 'inspect worker'})
                self.assertIsNone(self.claim(first_node, first_profile))
                self.queue.operator(first['id'], 'resolve', {'note': 'worker stopped', 'workerStopped': True})
                reopened = self.claim(first_node, first_profile)
                self.assertIsNotNone(reopened)
                self.complete(reopened)
                self.complete(second)

    def test_distinct_jobs_can_run_on_both_nodes(self):
        photo = self.submit('photo', PHOTO_PROFILE)
        trees = self.submit('trees', PROFILE)
        cluster = self.claim('cluster', PHOTO_PROFILE)
        self.assertEqual(cluster['id'], photo['id'])
        with ThreadPoolExecutor(max_workers=2) as pool:
            claims = list(pool.map(lambda pair: self.claim(*pair),
                                   (('cluster', PHOTO_PROFILE), ('main', PROFILE))))
        self.assertEqual({job['id'] for job in claims if job}, {trees['id']})
        self.assertEqual({job['nodeId'] for job in claims if job}, {'main'})

    def test_concurrent_cross_node_claims_take_distinct_heads_once_each(self):
        photo = self.submit('photo', PHOTO_PROFILE)
        trees = self.submit('trees', PROFILE)
        cluster = self.claim('cluster', PHOTO_PROFILE)
        self.assertEqual(cluster['id'], photo['id'])
        with ThreadPoolExecutor(max_workers=8) as pool:
            claims = list(pool.map(lambda i: self.claim('cluster' if i % 2 else 'main',
                                PHOTO_PROFILE if i % 2 else PROFILE), range(8)))
        claimed = [job for job in claims if job is not None]
        self.assertEqual({job['id'] for job in claimed}, {trees['id']})
        self.assertEqual({job['nodeId'] for job in claimed}, {'main'})

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

    def test_per_node_photo_cap_absent_null_and_explicit(self):
        from processing.config import node_image_limit
        with self.environment(PROCESSING_PROTOTYPE_NODE_IDS='cluster,middle,main',
                PROCESSING_NODE_MIDDLE_MAX_IMAGES='2500'):
            config = from_environment()
        self.assertEqual([node_image_limit(n, config.local_max_photos)
                          for n in config.node_endpoints], [1000, 2500, None])
        with self.environment(PROCESSING_NODE_CLUSTER_MAX_IMAGES='null'):
            config = from_environment()
        self.assertIsNone(node_image_limit(config.node_endpoints[0], 1000))
        for value in ('0', '-1', 'true', '1000001', ''):
            with self.subTest(value=value), self.environment(PROCESSING_NODE_CLUSTER_MAX_IMAGES=value):
                with self.assertRaises(ValueError): from_environment()

    def test_ordinary_inventory_has_no_required_role_name(self):
        with self.environment(PROCESSING_PROTOTYPE_NODE_IDS='small,large',
                PROCESSING_PROTOTYPE_STATE_DIR='C:/state', PROCESSING_VIEWER_ENABLED='1',
                PROCESSING_VIEWER_TOKEN='viewer-production-secret-000000000000',
                PROCESSING_NODE_SMALL_URL='http://small.invalid:3000',
                PROCESSING_NODE_LARGE_URL='http://large.invalid:3000'):
            config = from_environment()
        self.assertEqual(tuple(n.id for n in config.node_endpoints), ('small', 'large'))
        self.assertEqual(config.local_node_url, '')
        self.assertEqual(config.main_node_url, '')

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

    def test_ordered_config_accepts_distinct_dynamic_endpoints_and_hides_tokens(self):
        values = {
            'PROCESSING_PROTOTYPE_NODE_IDS': 'cluster,middle,main',
            'PROCESSING_PROTOTYPE_STATE_DIR': 'C:/state',
            'PROCESSING_VIEWER_ENABLED': '1',
            'PROCESSING_VIEWER_TOKEN': 'viewer-production-secret-000000000000',
            'PROCESSING_NODE_CLUSTER_URL': 'http://cluster.invalid:3000',
            'PROCESSING_NODE_CLUSTER_TOKEN': 'cluster-private-secret-000000000000',
            'PROCESSING_NODE_CLUSTER_MAX_INPUT_BYTES': '2000',
            'PROCESSING_NODE_MIDDLE_URL': 'http://middle.invalid:3000',
            'PROCESSING_NODE_MIDDLE_TOKEN': 'middle-private-secret-000000000000',
            'PROCESSING_NODE_MAIN_URL': 'http://main.invalid:3000',
            'PROCESSING_NODE_MAIN_TOKEN': 'main-private-secret-000000000000',
        }
        with self.environment(**values):
            config = from_environment()
        self.assertEqual([node.id for node in config.node_endpoints], ['cluster', 'middle', 'main'])
        self.assertEqual([node.max_input_bytes for node in config.node_endpoints], [2000, None, None])
        self.assertNotIn('private-secret', repr(config.node_endpoints))

    def test_dynamic_node_environment_names_must_not_collide(self):
        with self.environment(PROCESSING_PROTOTYPE_NODE_IDS='cluster,a-b,a_b'):
            with self.assertRaisesRegex(ValueError, 'unique environment'):
                from_environment()

    def test_node_engine_origins_reject_normalized_aliases(self):
        values = {
            'PROCESSING_PROTOTYPE_NODE_IDS': 'cluster,main',
            'PROCESSING_NODE_CLUSTER_URL': 'http://NodeODM.Example:80/',
            'PROCESSING_NODE_MAIN_URL': 'http://nodeodm.example',
        }
        with self.environment(**values), self.assertRaisesRegex(ValueError, 'unique normalized engine origins'):
            from_environment()

    def test_multiple_nodes_may_share_engine_token_but_not_application_token(self):
        shared = 'shared-engine-secret-000000000000000000'
        values = {
            'PROCESSING_PROTOTYPE_NODE_IDS': 'cluster,main',
            'PROCESSING_PROTOTYPE_STATE_DIR': 'C:/state',
            'PROCESSING_VIEWER_ENABLED': '1',
            'PROCESSING_VIEWER_TOKEN': 'viewer-production-secret-000000000000',
            'PROCESSING_NODE_CLUSTER_URL': 'http://cluster.invalid:3000',
            'PROCESSING_NODE_CLUSTER_TOKEN': shared,
            'PROCESSING_NODE_MAIN_URL': 'http://main.invalid:3000',
            'PROCESSING_NODE_MAIN_TOKEN': shared,
        }
        with self.environment(**values):
            config = from_environment()
        self.assertEqual([node.token for node in config.node_endpoints], [shared, shared])
        values['PROCESSING_VIEWER_TOKEN'] = shared
        with self.environment(**values), self.assertRaisesRegex(ValueError, 'credentials must be distinct'):
            from_environment()
