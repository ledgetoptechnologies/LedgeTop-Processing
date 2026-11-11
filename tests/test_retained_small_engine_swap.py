import copy
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stdout
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import stat
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import uuid

from ops.retained_small_engine_swap import (
    BASE, TARGET, FLAG, Held, Runtime, apply, candidate_config, canonical, command_with_retention, main,
    digest, no_cluster_claims, snapshot_tree, terminal_inventory, verify_owned_receipt, verify_retained,
    host_config_equivalent,
    BASE_EXPORT_BUDGET, RESERVE, require_backup_headroom,
)
from processing.maintenance import Conflict
from processing.prototype import Queue, VIEWER_PROFILE
from processing.viewer_tasks import TaskStore


class FakeRuntime:
    def __init__(self, fail=None):
        self.fail = fail
        self.calls = []
        self.closed = False
        self.current = dict(id='cluster', state='active', revision=0, planId=None,
                            targetImage=None, result=None)
        self.prepared = dict(requestId=str(uuid.uuid4()), authorityId=str(uuid.uuid4()),
                             node=copy.deepcopy(self.current), command=['--max-images', '1000', '--token', 'synthetic'],
                             engine={'tasks': {str(uuid.uuid4()): 40}})
        self.operator = SimpleNamespace(json=self.operator_json)

    def step(self, name):
        self.calls.append(name)
        if self.fail == name:
            raise Held('synthetic uncertain operation')

    def authority(self):
        return self.prepared['authorityId']

    def node(self):
        return copy.deepcopy(self.current)

    def operator_json(self, method, route, payload):
        action = route.rsplit('/', 1)[-1]
        self.step(action)
        if action == 'schedule-owned':
            self.current.update(state='draining', revision=1, planId=str(uuid.uuid4()), targetImage=TARGET)
            receipt = dict(payload, nodeId='cluster', revision=1, state='draining', planId=self.current['planId'],
                           requestSha256=digest(canonical({'nodeId': 'cluster', 'request': payload})))
            receipt['receiptSha256'] = digest(canonical(receipt))
            return {'receipt': receipt}
        if action == 'begin':
            self.current.update(state='updating', revision=2)
            return {'node': self.node(), 'updateToken': 'synthetic-token'}
        if action == 'finish':
            self.current.update(state='active', revision=3, result=payload['result'])
            return {'node': self.node()}
        raise AssertionError('unexpected API mutation')

    def revalidate(self, _):
        self.step('revalidate')

    def engine_idle(self, *_):
        self.step('engine_idle')

    def claims(self):
        return [{'nodeId': 'main', 'status': 'running'}]

    def change_ingress(self, _, *, closed):
        self.step('close' if closed else 'open')
        self.closed = closed

    def stop_and_backup(self, *_):
        self.step('stop_and_backup')

    def recreate_and_verify(self, *_):
        self.step('recreate_and_verify')

    def persist_pin(self, _):
        self.step('persist_pin')

    def hold_closed(self, *_):
        self.calls.append('hold_closed')
        self.closed = True
        if self.current['state'] == 'updating':
            self.current['state'] = 'held'


class RetainedSwapTests(unittest.TestCase):
    def test_pinned_base_export_budget_does_not_use_underreported_image_size(self):
        measured_export, daemon_image_size = 6365878272, 3952627133
        self.assertGreater(measured_export, daemon_image_size)
        self.assertEqual(BASE_EXPORT_BUDGET, 8 * 1024 ** 3)
        self.assertGreater(BASE_EXPORT_BUDGET, measured_export)
        writable, data = 1024 ** 2, 2 * 1024 ** 2
        minimum = BASE_EXPORT_BUDGET + writable + data + RESERVE
        self.assertEqual(require_backup_headroom(minimum + 1, writable, data), minimum)
        for free in (minimum, minimum - 1):
            with self.subTest(free=free), self.assertRaises(Held):
                require_backup_headroom(free, writable, data)
        for free, rw, files in ((True, 0, 0), (minimum + 1, -1, 0),
                                (minimum + 1, 0, -1), (minimum + 1, 0.0, 0)):
            with self.subTest(accounting=(free, rw, files)), self.assertRaises(Held):
                require_backup_headroom(free, rw, files)

    def test_only_observed_oom_default_representation_is_equivalent(self):
        base = {'OomKillDisable': None, 'Memory': 805306368, 'NanoCpus': 500000000,
                'ReadonlyRootfs': False, 'Binds': ['owned-volume:/var/www/data']}
        for before, after in ((None, False), (False, None), (None, None), (False, False), (True, True)):
            with self.subTest(before=before, after=after):
                self.assertTrue(host_config_equivalent(dict(base, OomKillDisable=before),
                                                       dict(base, OomKillDisable=after)))
        for before, after in ((None, True), (True, None), (False, True), (True, False),
                              (0, False), (False, 0), (0, 0), (0.0, False), (1, True), ('false', False)):
            with self.subTest(before=before, after=after):
                self.assertFalse(host_config_equivalent(dict(base, OomKillDisable=before),
                                                        dict(base, OomKillDisable=after)))
        missing = {k: v for k, v in base.items() if k != 'OomKillDisable'}
        for before, after in ((missing, base), (base, missing), (missing, missing)):
            with self.subTest(missing_before='OomKillDisable' not in before,
                              missing_after='OomKillDisable' not in after):
                self.assertFalse(host_config_equivalent(before, after))

    def test_oom_default_exception_preserves_every_other_host_field_exactly(self):
        base = {'OomKillDisable': None, 'Memory': 805306368, 'MemorySwap': 805306368,
                'NanoCpus': 500000000, 'ReadonlyRootfs': False,
                'RestartPolicy': {'Name': 'no', 'MaximumRetryCount': 0}}
        for patch_value in ({'Memory': 123}, {'MemorySwap': -1}, {'NanoCpus': 1000000000},
                            {'ReadonlyRootfs': True}, {'ReadonlyRootfs': 0},
                            {'RestartPolicy': {'Name': 'no', 'MaximumRetryCount': False}},
                            {'NetworkMode': 'another-network'}):
            with self.subTest(patch=patch_value):
                self.assertFalse(host_config_equivalent(base, dict(base, OomKillDisable=False, **patch_value)))
        self.assertIsNone(base['OomKillDisable'])

    def test_command_and_all_other_configuration_are_preserved(self):
        command = ['--max-images', '1000', '--token', 'synthetic-private']
        current = {'services': {'nodeodm': {'image': BASE, 'command': command,
                    'volumes': [{'source': 'owned-volume', 'target': '/var/www/data'}],
                    'networks': ['existing'], 'restart': 'unless-stopped', 'mem_limit': 123}},
                   'networks': {'existing': {'external': True}}}
        before = copy.deepcopy(current)
        candidate = candidate_config(current, command)
        self.assertEqual(candidate['services']['nodeodm']['image'], TARGET)
        self.assertEqual(candidate['services']['nodeodm']['command'], command + [FLAG])
        candidate['services']['nodeodm']['image'] = BASE
        candidate['services']['nodeodm']['command'] = command
        self.assertEqual(candidate, before)
        self.assertEqual(current, before)
        for bad in (command + [FLAG], ['--max-images', '1001', '--token', 'x'],
                    ['--max-images', '1000', '--token', ''], 'shell token'):
            with self.subTest(bad=bad), self.assertRaises(Held):
                command_with_retention(bad)

    def test_retained_terminal_tasks_allowed_but_active_unknown_or_duplicate_rejected(self):
        ids = [str(uuid.uuid4()) for _ in range(3)]
        codes = dict(zip(ids, (30, 40, 50)))
        records = [{'uuid': identifier} for identifier in ids]
        self.assertEqual(terminal_inventory(records, lambda i: {'status': {'code': codes[i]}}), codes)
        for code in (None, 0, 10, 20, 99, '40'):
            with self.subTest(code=code), self.assertRaises(Held):
                terminal_inventory(records[:1], lambda i: {'status': {'code': code}})
        with self.assertRaises(Held):
            terminal_inventory(records + records[:1], lambda i: {'status': {'code': codes[i]}})

    def test_main_claim_does_not_block_small_swap_and_small_claims_do(self):
        no_cluster_claims([{'nodeId': 'main', 'status': 'running'}, {'nodeId': None, 'status': 'queued'}])
        for state in ('queued', 'running', 'held'):
            with self.subTest(state=state), self.assertRaises(Held):
                no_cluster_claims([{'nodeId': 'cluster', 'status': state}])

    def test_success_orders_backup_before_recreate_and_finish_after_verified_open(self):
        runtime, checkpoints = FakeRuntime(), []
        with tempfile.TemporaryDirectory() as temporary:
            result = apply(runtime, Path(temporary), runtime.prepared,
                           lambda state: checkpoints.append(copy.deepcopy(state)))
        self.assertEqual(result['status'], 'succeeded')
        ordered = ['schedule-owned', 'begin', 'close', 'stop_and_backup',
                   'recreate_and_verify', 'persist_pin', 'open', 'finish']
        self.assertEqual([x for x in runtime.calls if x in ordered], ordered)
        self.assertEqual(checkpoints[-1]['stage'], 'completed')
        self.assertFalse(runtime.closed)

    def test_each_mutation_failure_stays_closed_never_boots_old_or_replays(self):
        for stage in ('schedule-owned', 'begin', 'close', 'stop_and_backup',
                      'recreate_and_verify', 'persist_pin', 'open', 'finish'):
            runtime, checkpoints = FakeRuntime(fail=stage), []
            with self.subTest(stage=stage), tempfile.TemporaryDirectory() as temporary:
                with self.assertRaises(Held):
                    apply(runtime, Path(temporary), runtime.prepared,
                          lambda state: checkpoints.append(copy.deepcopy(state)))
                self.assertTrue(runtime.closed)
                self.assertEqual(runtime.calls.count(stage), 1)
                self.assertNotIn('rollback', runtime.calls)
                self.assertNotIn('start_old', runtime.calls)
                self.assertEqual(checkpoints[-1]['stage'], 'held_for_inspection')
                if stage in ('close', 'stop_and_backup'):
                    self.assertNotIn('recreate_and_verify', runtime.calls)

    def test_existing_operation_and_changed_authority_never_schedule(self):
        runtime = FakeRuntime()
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'operation.json').write_text('{}')
            with self.assertRaises(Held):
                apply(runtime, root, runtime.prepared, lambda _: None)
        self.assertEqual(runtime.calls, [])
        runtime.prepared['authorityId'] = 'different'
        runtime.authority = lambda: 'actual-authority'
        with tempfile.TemporaryDirectory() as temporary, self.assertRaises(Held):
            apply(runtime, Path(temporary), runtime.prepared, lambda _: None)
        self.assertEqual(runtime.calls, [])

    def test_owned_schedule_digest_mismatch_cannot_be_adopted(self):
        runtime = FakeRuntime()
        request = dict(schemaVersion=1, requestId=runtime.prepared['requestId'],
                       authorityId=runtime.authority(), expectedRevision=0,
                       drainAt='2026-11-11T00:00:00Z', notBefore='2026-11-11T00:00:00Z',
                       afterJobId=None, targetImage=TARGET)
        receipt = runtime.operator_json('POST', '/schedule-owned', request)['receipt']
        verify_owned_receipt(receipt, request)
        receipt['targetImage'] = BASE
        with self.assertRaises(Held):
            verify_owned_receipt(receipt, request)

    def test_retained_file_inventory_detects_changes_and_permits_index_reserialization(self):
        import time
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / 'source.jpg').write_bytes(b'synthetic retained original')
            (root / 'output.zip').write_bytes(b'synthetic retained result')
            (root / 'tasks.json').write_text('[{}]')
            before = snapshot_tree(root, deadline=time.monotonic() + 5)
            (root / 'tasks.json').write_text('[ { } ]')
            verify_retained(before, snapshot_tree(root, deadline=time.monotonic() + 5))
            (root / 'source.jpg').write_bytes(b'changed')
            with self.assertRaises(Held):
                verify_retained(before, snapshot_tree(root, deadline=time.monotonic() + 5))

    def test_failed_unowned_schedule_never_closes_somebody_elses_ingress(self):
        runtime = Runtime.__new__(Runtime)
        runtime.operator = Mock()
        runtime.operator.json.side_effect = Held('no owned historical schedule')
        runtime.change_ingress = Mock()
        outcome = runtime.hold_closed({'requestId': str(uuid.uuid4())}, {})
        self.assertFalse(outcome['ingressClosedVerified'])
        runtime.change_ingress.assert_not_called()

    def test_default_cli_inspects_only_and_does_not_print_private_configuration(self):
        prepared = FakeRuntime().prepared
        prepared['command'][-1] = 'private-not-for-output'
        runtime = Mock()
        runtime.snapshot.return_value = prepared
        with patch('sys.argv', ['swap', '--credentials', 'synthetic-private.env']), \
                patch('ops.retained_small_engine_swap.Runtime', return_value=runtime), \
                redirect_stdout(io.StringIO()) as output:
            self.assertEqual(main(), 0)
        report = json.loads(output.getvalue())
        self.assertEqual(report['mode'], 'inspect')
        self.assertFalse(report['writes'])
        self.assertNotIn('private-not-for-output', output.getvalue())
        self.assertEqual(runtime.method_calls, [('snapshot', (), {})])

    @unittest.skipUnless(os.name == 'posix', 'POSIX fchmod/fchown and umask semantics')
    def test_ingress_atomic_replace_preserves_mode_under_private_umask(self):
        from ops import retained_small_engine_swap as swap
        with tempfile.TemporaryDirectory() as temporary:
            nginx = Path(temporary) / 'nginx.conf'
            source = b'location / { ' + swap.PASS + b' }\n'
            nginx.write_bytes(source)
            nginx.chmod(0o644)
            before = nginx.stat()
            runtime = Runtime.__new__(Runtime)
            runtime.nginx_workers = Mock(side_effect=[(123, {124}), (123, {125})])
            runtime.engine_client = Mock(return_value=SimpleNamespace(token='synthetic'))
            runtime.docker = Mock(return_value='')
            runtime.run = Mock(return_value='503')
            client = Mock()
            client.request.side_effect = OSError('synthetic closed ingress')
            old_umask = os.umask(0o077)
            try:
                with patch.object(swap, 'NGINX', nginx), patch.object(swap, 'Client', return_value=client):
                    runtime.change_ingress({'nginxHex': source.hex(), 'requestId': str(uuid.uuid4()),
                        'command': ['--max-images', '1000', '--token', 'synthetic']}, closed=True)
            finally:
                os.umask(old_umask)
            after = nginx.stat()
            self.assertEqual(stat.S_IMODE(after.st_mode), 0o644)
            self.assertEqual((after.st_uid, after.st_gid), (before.st_uid, before.st_gid))
            self.assertEqual(nginx.read_bytes(), source.replace(swap.PASS, b'return 503;'))


class RealQueueRetentionFenceTests(unittest.TestCase):
    """Real SQLite queue/claim transactions, not a copy of the swap checks."""
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.now = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)
        self.queue = Queue(Path(self.temp.name) / 'queue', node_ids=('cluster', 'main'),
                           viewer_enabled=True, clock=lambda: self.now)

    def tearDown(self):
        self.queue.close()
        self.temp.cleanup()

    def submit(self, name):
        return self.queue.submit({'requestId': name, 'profile': VIEWER_PROFILE,
                                  'environment': 'production'})[0]

    def claim(self, worker, node='cluster', **bounds):
        return self.queue.claim({'workerId': worker, 'nodeId': node,
                                 'profiles': [VIEWER_PROFILE]}, **bounds)

    def schedule_owned(self):
        node = self.queue.get_node('cluster')
        stamp = self.now.isoformat().replace('+00:00', 'Z')
        request = dict(schemaVersion=1, requestId=str(uuid.uuid4()), authorityId=self.queue.authority_id,
                       expectedRevision=node['revision'], drainAt=stamp, notBefore=stamp,
                       afterJobId=None, targetImage=TARGET)
        receipt = self.queue.schedule_node_owned('cluster', request)
        verify_owned_receipt(receipt, request)
        return receipt

    def begin(self):
        node = self.queue.get_node('cluster')
        return self.queue.begin_node_update('cluster', {'expectedRevision': node['revision'],
                                                        'planId': node['planId']})

    def test_stale_eligible_snapshot_cannot_claim_draining_or_updating_small_node(self):
        first, second = self.submit('first-photo'), self.submit('second-photo')
        snapshot = self.queue.dispatch_snapshot()
        bounds = dict(eligible_job_ids=[first['id'], second['id']],
                      eligible_node_ids=['cluster', 'main'], expected_queue_sequence=snapshot['sequence'])
        receipt = self.schedule_owned()
        self.assertEqual(receipt['state'], 'draining')
        # Supply the pre-maintenance eligibility snapshot deliberately. The
        # queue's own transaction must still reject every new cluster claim.
        with ThreadPoolExecutor(max_workers=2) as workers:
            claims = list(workers.map(lambda i: self.claim('draining-worker-' + str(i), **bounds), range(4)))
        self.assertEqual(claims, [None] * 4)
        self.assertEqual([job['status'] for job in self.queue.list()], ['queued', 'queued'])
        self.assertEqual(self.begin()['node']['state'], 'updating')
        self.assertIsNone(self.claim('updating-worker', **bounds))
        self.assertIsNone(self.queue.get(first['id'])['nodeId'])
        self.assertIsNone(self.queue.get(second['id'])['nodeId'])
        # The independent main node is neither fenced nor restarted.
        main = self.claim('main-worker', 'main', **bounds)
        self.assertEqual(main['nodeId'], 'main')
        self.assertEqual(main['id'], first['id'])
        self.assertEqual(self.queue.get(second['id'])['status'], 'queued')

    def test_linked_running_job_blocks_begin_before_viewer_assignment_then_unrelated_main_does_not(self):
        store = TaskStore(self.queue, Path(self.temp.name) / 'viewer-inputs')
        identifier = str(uuid.uuid4())
        store.initialize(identifier, 'Synthetic retained-fence source', [], None, 'production')
        body = b'synthetic source bytes only'
        store.upload(identifier, 'photo.jpg', io.BytesIO(body), len(body))
        store.commit(identifier)
        claimed = self.claim('viewer-broker')
        linked = store.task(identifier)
        self.assertEqual(linked['job']['id'], claimed['id'])
        self.assertEqual(linked['job']['status'], 'running')
        self.assertIsNone(linked['node_id'])
        self.assertIsNone(linked['engine_uuid'])
        # This is the real Queue.claim -> ViewerTask assignment gap. The job
        # row already owns cluster, so maintenance cannot begin in that gap.
        self.submit('main-photo')
        main = self.claim('main-worker', 'main')
        queued = self.submit('waiting-photo')
        self.schedule_owned()
        with self.assertRaises(Conflict):
            self.begin()
        self.assertEqual(self.queue.get(claimed['id'])['status'], 'running')
        self.queue.complete(claimed['id'], {'claimToken': claimed['claimToken'],
                                            'status': 'succeeded', 'result': {}})
        self.assertEqual(self.begin()['node']['state'], 'updating')
        self.assertEqual(self.queue.get(main['id'])['status'], 'running')
        self.assertEqual(self.queue.get(queued['id'])['status'], 'queued')
        self.assertIsNone(self.claim('small-after-begin'))

    def test_claim_transaction_winning_before_owned_drain_is_visible_to_begin(self):
        self.submit('racing-photo')
        checked, release, schedule_started = threading.Event(), threading.Event(), threading.Event()
        original_node = self.queue._node
        claims, receipts, errors = [], [], []

        def observed_node(db, identifier):
            row = original_node(db, identifier)
            if threading.current_thread().name == 'retained-claim' and not checked.is_set():
                self.assertEqual(row['state'], 'active')
                checked.set()
                self.assertTrue(release.wait(3), 'test failed to release real claim transaction')
            return row

        def claim_first():
            try:
                claims.append(self.claim('racing-worker'))
            except Exception as error:
                errors.append(error)

        def drain_second():
            schedule_started.set()
            try:
                receipts.append(self.schedule_owned())
            except Exception as error:
                errors.append(error)

        claimant = threading.Thread(target=claim_first, name='retained-claim')
        scheduler = threading.Thread(target=drain_second, name='retained-drain')
        with patch.object(self.queue, '_node', side_effect=observed_node):
            claimant.start()
            try:
                self.assertTrue(checked.wait(3))
                scheduler.start()
                self.assertTrue(schedule_started.wait(3))
            finally:
                release.set()
                claimant.join(timeout=5)
                if scheduler.ident is not None:
                    scheduler.join(timeout=5)
        self.assertFalse(claimant.is_alive())
        self.assertFalse(scheduler.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(len(claims), 1)
        self.assertEqual(claims[0]['nodeId'], 'cluster')
        self.assertEqual(receipts[0]['state'], 'draining')
        self.assertEqual(self.queue.get(claims[0]['id'])['status'], 'running')
        with self.assertRaises(Conflict):
            self.begin()


if __name__ == '__main__':
    unittest.main()
