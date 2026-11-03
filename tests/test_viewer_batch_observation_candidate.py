import hashlib
import io
import tempfile
import unittest
from pathlib import Path
from processing.prototype import Queue
from processing.viewer_tasks import TaskStore
from processing.viewer_batch_observation_candidate import inspect_indexed_batch
from processing.maintenance import Conflict

class IndexedBatchTest(unittest.TestCase):
    def test_reserved_generation_and_digest(self):
        with tempfile.TemporaryDirectory() as tmp:
            queue = Queue(Path(tmp) / 'queue')
            try:
                store = TaskStore(queue, Path(tmp) / 'input')
                task = '11111111-2222-4333-8444-555555555555'
                capability = 'A' * 43
                store.reserve(task, 'production', capability)
                store.initialize(task, 'synthetic', [], None, 'production', capability)
                item = {'name': 'a.jpg', 'bytes': 3, 'sha256': hashlib.sha256(b'abc').hexdigest()}
                call = lambda: inspect_indexed_batch(store, task, 'production', capability, 1, [item])
                self.assertEqual(call()['files'], ['missing'])
                store.upload(task, 'a.jpg', io.BytesIO(b'abc'), 3, 'production', capability)
                self.assertEqual(call()['files'], ['matched'])
                item['sha256'] = hashlib.sha256(b'xyz').hexdigest()
                self.assertEqual(call()['files'], ['conflict'])
                with self.assertRaises(Conflict): inspect_indexed_batch(store, task, 'production', capability, 2, [item])
                with self.assertRaises((KeyError, Conflict)): inspect_indexed_batch(store, task, 'staging', capability, 1, [item])
            finally:
                queue.close()
