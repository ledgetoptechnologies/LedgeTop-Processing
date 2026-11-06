"""Existing admissions survive catalog churn; new requests still need discovery."""
import copy
import json
from pathlib import Path
import tempfile
import ssl
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener

from processing import reference_protocol as p
from processing.prototype import Queue
from processing.reference_api import ReferenceAPI
from processing.reference_control import ReferenceControlAPI
from processing.reference_http import create_reference_http_server
from processing.reference_runtime import _DynamicAPI
from processing.reference_source_intake import INTAKE_FIELDS
from processing.reference_store import ReferenceStore
from tests import test_reference_source_intake as source_fixture
from tests.test_reference_dynamic_source_intake_scale import _write_tls_material


class SourceReplayTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.queue = Queue(Path(self.temp.name) / 'state', reference_enabled=True)
        self.addCleanup(self.queue.close)
        fixture = source_fixture.ReferenceSourceIntakeTests(methodName='runTest')
        fixture.setUp()
        fixture.authority_id = self.queue.authority_id
        fixture.payload['pipeline']['odm']['options'] = {'numeric-probe': 1e-7}
        checked = fixture.validate(allowed_odm_options={'numeric-probe'})
        self.payload = {name: checked[name] for name in INTAKE_FIELDS}
        self.keys = fixture.trusted_keys
        self.spool = SimpleNamespace(check_capacity=Mock())
        self.store = ReferenceStore(self.queue)
        self.bearer = 'synthetic-source-replay-token'
        self.headers = {'Authorization': 'Bearer ' + self.bearer}
        self.old_options = self.payload['pipeline']['odm']['optionsFingerprint']
        self.api = self.make_api()
        response = self.post(self.api)
        self.assertEqual(response.status, 201, response.body)
        self.job = response.body

    def make_api(self, **changes):
        values = dict(bearer_tokens={self.bearer: 'staging'},
            capability_fingerprint='a' * 64, options_fingerprint=self.old_options,
            allowed_odm_options={'numeric-probe'}, source_spool=self.spool,
            source_attestation_keys=self.keys, authority_id=self.queue.authority_id,
            source_bearer_principals={self.bearer:'viewer-staging'})
        values.update(changes)
        return ReferenceAPI(self.store, **values)

    def post(self, api, payload=None, *, headers=None, protected=True):
        return api.handle('POST', '/ltds/v2/jobs', headers or self.headers,
            p.canonical_json(self.payload if payload is None else payload),
            protected_transport=protected)

    def dynamic(self):
        control = ReferenceControlAPI(self.store, viewer_bearers={self.bearer: 'staging'},
            worker_bearers={'synthetic-worker-replay-token': dict(node_id='cluster',
                worker_id='worker', worker_instance_id='instance')},
            capability_fingerprint='a' * 64, options_fingerprint=self.old_options,
            allowed_odm_options={'numeric-probe'}, source_spool=self.spool,
            source_attestation_keys=self.keys, authority_id=self.queue.authority_id,
            source_bearer_principals={self.bearer:'viewer-staging'})
        discover = Mock(side_effect=RuntimeError('synthetic all-node outage'))
        settings = SimpleNamespace(viewer_bearers={self.bearer: 'staging'},
            viewer_receipt_keys={}, source_spool=self.spool, source_attestation_keys=self.keys,
            source_bearer_principals={self.bearer:'viewer-staging'})
        api = _DynamicAPI(control, SimpleNamespace(), settings, discover, Mock())
        return api, discover

    def test_exact_existing_admission_ignores_changed_catalog_without_re_admission(self):
        changed = self.make_api(capability_fingerprint='f' * 64,
            options_fingerprint='e' * 64, allowed_odm_options=())
        response = self.post(changed)
        self.assertEqual(response.status, 200, response.body)
        self.assertEqual(response.body['jobId'], self.job['jobId'])
        self.assertEqual(len(self.queue.list()), 1)
        self.assertEqual(self.spool.check_capacity.call_count, 1)

    def test_existing_dynamic_replay_does_not_call_unavailable_discovery(self):
        api, discover = self.dynamic()
        response = self.post(api)
        self.assertEqual(response.status, 200, response.body)
        self.assertEqual(response.body['jobId'], self.job['jobId'])
        discover.assert_not_called()

    def test_actual_tls_replay_survives_catalog_outage_without_a_second_admission(self):
        api, discover = self.dynamic()
        key, cert = _write_tls_material(Path(self.temp.name))
        server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_tls.load_cert_chain(str(cert), str(key))
        server = create_reference_http_server(api, '127.0.0.1', 0,
            ssl_context=server_tls, request_timeout=3)
        thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .02})
        thread.start()
        try:
            client = build_opener(ProxyHandler({}), HTTPSHandler(
                context=ssl.create_default_context(cafile=str(cert))))
            request = Request('https://127.0.0.1:' + str(server.server_address[1])
                + '/ltds/v2/jobs', data=p.canonical_json(self.payload), method='POST',
                headers={**self.headers, 'Content-Type': 'application/json'})
            with client.open(request, timeout=3) as response:
                body = json.loads(response.read())
                self.assertEqual((response.status, body['jobId']), (200, self.job['jobId']))
            discover.assert_not_called()
            self.assertEqual(len(self.queue.list()), 1)
            self.assertEqual(self.spool.check_capacity.call_count, 1)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(3)
            self.assertFalse(thread.is_alive(), 'owned replay TLS server leaked')

    def test_unknown_submission_still_requires_current_catalog(self):
        api, discover = self.dynamic()
        payload = copy.deepcopy(self.payload)
        payload['submissionId'] = '00000000-0000-4000-8000-000000000099'
        self.assertEqual(self.post(api, payload).status, 503)
        discover.assert_called_once()
        self.assertEqual(len(self.queue.list()), 1)

    def test_changed_existing_request_never_falls_through_to_new_admission(self):
        api, discover = self.dynamic()
        payload = copy.deepcopy(self.payload)
        payload['pipeline']['odm']['options']['numeric-probe'] = 1
        self.assertEqual(self.post(api, payload).status, 409)
        discover.assert_not_called()
        self.assertEqual(self.spool.check_capacity.call_count, 1)

    def test_current_source_key_revocation_still_rejects_exact_replay(self):
        revoked = copy.deepcopy(self.keys)
        revoked[0]['keyId'] = 'different-configured-key'
        self.assertEqual(self.post(self.make_api(source_attestation_keys=revoked)).status, 400)

    def test_transport_environment_and_authority_are_not_bypassed(self):
        self.assertEqual(self.post(self.api, protected=False).status, 503)
        self.assertEqual(self.post(self.api, headers={'Authorization': 'Bearer unknown-replay-token'}).status, 401)
        self.assertEqual(self.post(self.make_api(bearer_tokens={self.bearer: 'production'})).status, 409)
        wrong = '00000000-0000-4000-8000-000000000098'
        self.assertEqual(self.post(self.make_api(authority_id=wrong)).status, 400)

    def test_persisted_manifest_corruption_does_not_mint_replay_ack(self):
        with self.queue._db() as db:
            db.execute('UPDATE reference_source_intakes SET source_manifest_sha=?', ('0' * 64,))
        self.assertEqual(self.post(self.api).status, 409)

    def test_binding_is_rechecked_after_signature_validation(self):
        import processing.reference_source_replay as replay
        validate = replay.validate_source_intake
        def concurrent_change(*args, **kwargs):
            result = validate(*args, **kwargs)
            with self.queue._db() as db:
                db.execute('UPDATE reference_source_intakes SET source_attestation_sha=?', ('0' * 64,))
            return result
        with patch.object(replay, 'validate_source_intake', side_effect=concurrent_change):
            self.assertEqual(self.post(self.api).status, 409)

    def test_read_only_replay_does_not_advance_or_replace_source_receipts(self):
        with self.queue._db() as db:
            before = [tuple(row) for row in db.execute('SELECT * FROM reference_source_intakes')]
        self.assertEqual(self.post(self.api).status, 200)
        with self.queue._db() as db:
            self.assertEqual([tuple(row) for row in db.execute('SELECT * FROM reference_source_intakes')], before)

