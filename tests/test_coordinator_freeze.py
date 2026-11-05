from contextlib import contextmanager
import tempfile
import threading
import unittest
import uuid
import http.client
import time
import os
from pathlib import Path
import subprocess
import sys
import select
import shutil
import json
import hashlib

from processing.coordinator_freeze import CoordinatorFrozen
from processing.maintenance import Conflict
from processing.prototype import Queue


class FreezeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.queue = Queue(self.temp.name)
        self.gate = self.queue._coordinator_freeze
        self.plan = str(uuid.uuid4())
        self.owner = 'synthetic-freeze-owner'

    def tearDown(self):
        if self.queue: self.queue.close()
        self.temp.cleanup()

    def request(self, action, **overrides):
        value = dict(requestId=str(uuid.uuid4()), planId=self.plan, action=action,
                     expectedRevision=self.gate.status()['revision'], startupId=self.queue.startup_id)
        value.update(overrides)
        return value

    def change(self, action, **kwargs):
        return self.gate.transition(self.request(action), owner_secret=self.owner, **kwargs)

    def test_intent_linearizes_admission_but_existing_nested_operation_finishes(self):
        with self.queue.coordinator_operation('http'):
            self.change('intent')
            outcome = []
            def denied():
                try:
                    with self.queue.coordinator_operation('http'): outcome.append('admitted')
                except CoordinatorFrozen: outcome.append('retry')
            thread = threading.Thread(target=denied)
            thread.start(); thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertEqual(outcome, ['retry'])
            with self.queue.coordinator_operation('http'):
                self.queue.submit(dict(requestId='existing', profile='treedar-synthetic-v1', environment='staging'))
            self.change('drain')
            with self.assertRaisesRegex(Conflict, 'still draining'): self.change('seal')
        self.assertEqual(self.change('seal')['phase'], 'frozen')
        self.assertEqual(len(self.queue.list()), 1)

    def test_frozen_blocks_direct_background_db_writer_but_keeps_reads(self):
        for action in ('intent', 'drain', 'seal'): self.change(action)
        self.assertEqual(self.gate.status()['phase'], 'frozen')
        self.assertEqual(self.queue.list(), [])
        with self.assertRaises(CoordinatorFrozen):
            self.queue.submit(dict(requestId='new', profile='treedar-synthetic-v1', environment='staging'))
        with self.assertRaises(CoordinatorFrozen), self.queue._db() as db:
            db.execute("UPDATE nodes SET revision=revision+1")
        self.assertEqual(self.queue.get_node('cluster')['revision'], 0)

    def test_exact_transition_replay_stale_cas_owner_and_health(self):
        request = self.request('intent')
        first = self.gate.transition(request, owner_secret=self.owner)
        self.assertEqual(self.gate.transition(request, owner_secret=self.owner), first)
        with self.assertRaises(Conflict):
            self.gate.transition(request, owner_secret='different-owner')
        self.change('drain'); self.change('seal')
        request = self.request('resume')
        with self.assertRaisesRegex(Conflict, 'health'): self.gate.transition(request, owner_secret=self.owner)
        resumed = self.gate.transition(request, owner_secret=self.owner, healthy=True)
        self.assertEqual(resumed['phase'], 'open')
        with self.queue.coordinator_operation('http'): pass

    def test_in_progress_node_update_prevents_freeze(self):
        with self.queue._db() as db: db.execute("UPDATE nodes SET state='updating'")
        with self.assertRaisesRegex(Conflict, 'maintenance'): self.change('intent')
        self.assertEqual(self.gate.status()['phase'], 'open')

    def test_frozen_state_survives_queue_ownership_restart(self):
        for action in ('intent', 'drain', 'seal'): self.change(action)
        authority = self.queue.authority_id
        old_epoch = self.queue.startup_id
        self.queue.close()
        self.queue = Queue(self.temp.name)
        self.gate = self.queue._coordinator_freeze
        self.assertEqual(self.queue.authority_id, authority)
        self.assertNotEqual(self.queue.startup_id, old_epoch)
        self.assertEqual(self.gate.status()['phase'], 'frozen')
        with self.assertRaises(CoordinatorFrozen), self.queue.coordinator_operation('http'): pass
        self.assertEqual(self.change('resume', healthy=True)['phase'], 'open')

    def test_control_reserve_preserves_replay_and_freeze_at_full_payload_quota(self):
        from processing.reference_store import ReferenceStore
        self.queue.allowed_profiles.add('reference-pipeline-v1')
        store = ReferenceStore(self.queue)
        store.max_metadata_bytes = 1
        with self.queue.coordinator_operation('http'):
            self.assertEqual(self.queue.list(), [])
            with self.assertRaisesRegex(Conflict, 'metadata quota'), self.queue._db() as db:
                store._quota(db, 1)
        request = self.request('intent')
        receipt = self.gate.transition(request, owner_secret=self.owner)
        self.assertEqual(self.gate.transition(request, owner_secret=self.owner), receipt)
        self.change('drain'); self.change('seal')
        self.assertEqual(self.gate.status()['phase'], 'frozen')

    def test_previous_disabled_barrier_marker_requires_reviewed_conversion(self):
        with self.queue._db() as db:
            db.execute('CREATE TABLE coordinator_admission(marker TEXT)')
        self.queue.close()
        with self.assertRaisesRegex(CoordinatorFrozen, 'legacy_barrier'):
            Queue(self.temp.name)

    def test_reference_freeze_rejects_before_large_body_read_with_retry_after(self):
        from processing.reference_http import create_reference_http_server
        from processing.reference_api import Response
        queue = self.queue
        class API:
            coordinator_queue = queue
            calls = 0
            def handle(self, *args, **kwargs):
                self.calls += 1
                return Response(200, {})
        api = API()
        server = create_reference_http_server(api)
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .01})
        thread.start()
        try:
            self.change('intent')
            connection = http.client.HTTPConnection(*server.server_address, timeout=2)
            try:
                connection.putrequest('POST', '/ltds/v2/jobs')
                connection.putheader('Content-Length', str(16 * 1024 * 1024))
                connection.endheaders()  # Deliberately send none of the body.
                response = connection.getresponse()
                self.assertEqual(response.status, 503)
                self.assertEqual(response.getheader('Retry-After'), '2')
                response.read()
            finally:
                connection.close()
            self.assertEqual(api.calls, 0)
            self.assertEqual(self.gate.status()['operations'], 0)
        finally:
            server.shutdown(); server.server_close(); thread.join(2)
            self.assertFalse(thread.is_alive())

    @unittest.skipUnless(os.name == 'posix', 'actual Linux original lease retirement')
    def test_http_process_death_recovers_only_original_registered_lease_not_clone(self):
        self.queue.close(); self.queue = None
        code = '''
import os,sys
from processing.prototype import Queue
from processing.reference_http import create_reference_http_server
q=Queue(sys.argv[1])
class API:
 coordinator_queue=q
 def handle(self,*args,**kwargs): os._exit(42)
s=create_reference_http_server(API())
print(s.server_port,flush=True)
s.serve_forever()
'''
        child = subprocess.Popen([sys.executable, '-c', code, self.temp.name],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            readable, _, _ = select.select([child.stdout], [], [], 5)
            self.assertTrue(readable, 'owned test child did not start')
            port = int(child.stdout.readline())
            connection = http.client.HTTPConnection('127.0.0.1', port, timeout=2)
            try:
                connection.request('POST', '/synthetic-crash', body=b'')
                with self.assertRaises((http.client.RemoteDisconnected, ConnectionResetError)):
                    connection.getresponse()
            finally:
                connection.close()
            self.assertEqual(child.wait(timeout=5), 42)
            original_hash = hashlib.sha256((Path(self.temp.name) / 'jobs.sqlite3').read_bytes()).hexdigest()
            with tempfile.TemporaryDirectory() as clone_root:
                clone = Path(clone_root) / 'copy'
                shutil.copytree(self.temp.name, clone)
                probe = '''
import json,os,sys
from processing.prototype import Queue
q=Queue(sys.argv[1])
with q._db() as db:
 pins=[row[0] for row in db.execute('SELECT state FROM coordinator_operation_pins')]
print(json.dumps(dict(phase=q._coordinator_freeze.status()['phase'],pins=pins)),flush=True)
os._exit(0)
'''
                result = subprocess.run([sys.executable, '-c', probe, str(clone)], capture_output=True,
                                        text=True, timeout=5, check=True)
                observed = json.loads(result.stdout)
                self.assertEqual(observed, {'phase': 'held', 'pins': ['active']})
            self.assertEqual(hashlib.sha256((Path(self.temp.name) / 'jobs.sqlite3').read_bytes()).hexdigest(), original_hash)
            self.queue = Queue(self.temp.name)
            self.gate = self.queue._coordinator_freeze
            self.assertEqual(self.gate.status()['phase'], 'open')
            self.assertEqual(self.gate.status()['operations'], 0)
            with self.queue._db() as db:
                self.assertEqual([row[0] for row in db.execute('SELECT state FROM coordinator_operation_pins')], ['retired'])
        finally:
            if child.poll() is None:
                child.kill()
            child.communicate(timeout=5)


if __name__ == '__main__': unittest.main()
