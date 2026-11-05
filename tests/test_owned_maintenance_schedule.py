"""Owned scheduling through real HTTP and temporary SQLite; no host actions."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from datetime import datetime, timedelta, timezone
import hashlib
from http.client import RemoteDisconnected
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import uuid

from processing.app import make_handler
from processing.config import Config
from processing.maintenance import canonical
from tests.test_service import TOKEN, start_server

OPERATOR='owned-maintenance-operator-test-token'
OBSERVER='owned-maintenance-observer-test-token'
IMAGE='sha256:'+'b'*64


class OwnedScheduleTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.root=Path(self.temp.name)
        self.start()

    def start(self):
        self.handler=make_handler(Config(TOKEN,'','',1000,20000,
            prototype_state_dir=str(self.root/'queue'),prototype_node_ids=('cluster','main'),
            prototype_operator_token=OPERATOR,prototype_observer_token=OBSERVER))
        self.queue=self.handler.prototype_queue
        self.server=start_server(self.handler)
        self.origin='http://127.0.0.1:'+str(self.server.server_port)

    def stop(self):
        self.server.shutdown();self.server.server_close();self.queue.close()

    def tearDown(self):
        self.stop();self.temp.cleanup()

    def payload(self):
        stamp=(datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()
        return dict(schemaVersion=1,requestId=str(uuid.uuid4()),authorityId=self.queue.authority_id,
            expectedRevision=0,drainAt=stamp,notBefore=stamp,afterJobId=None,targetImage=IMAGE)

    def request(self,path,payload=None,*,token=OPERATOR,raw=None):
        body=raw if raw is not None else json.dumps(payload).encode() if payload is not None else None
        headers={'Content-Type':'application/json'}
        if token is not None:headers['Authorization']='Bearer '+token
        with urlopen(Request(self.origin+path,data=body,headers=headers),timeout=5) as response:
            return response.status,json.load(response)

    def post(self,payload,node='cluster',**kwargs):
        return self.request('/v1/prototype/nodes/'+node+'/schedule-owned',payload,**kwargs)

    def lookup(self,payload,node='cluster',**kwargs):
        return self.request('/v1/prototype/nodes/'+node+'/schedule-owned/'+payload['requestId'],**kwargs)

    def test_http_receipt_exact_hash_and_operator_only_post_and_lookup(self):
        payload=self.payload()
        for token in (None,TOKEN,OBSERVER,'invalid'):
            with self.subTest(token=token):
                with self.assertRaises(HTTPError) as caught:self.post(payload,token=token)
                self.assertEqual(caught.exception.code,401)
        _,body=self.post(payload);receipt=body['receipt']
        self.assertEqual(receipt['requestSha256'],hashlib.sha256(canonical(dict(nodeId='cluster',request=payload)).encode()).hexdigest())
        self.assertEqual(receipt['receiptSha256'],hashlib.sha256(canonical({k:v for k,v in receipt.items() if k!='receiptSha256'}).encode()).hexdigest())
        self.assertEqual(receipt['revision'],1)
        self.assertEqual(receipt['targetImage'],IMAGE)
        self.assertNotIn('updateToken',receipt)
        self.assertEqual(self.lookup(payload)[1],body)
        for token in (None,TOKEN,OBSERVER):
            with self.assertRaises(HTTPError) as caught:self.lookup(payload,token=token)
            self.assertEqual(caught.exception.code,401)

    def test_owned_version_binding_is_immutable_and_replays_exactly(self):
        payload = dict(self.payload(),targetEngineVersion='3.8.5')
        first = self.post(payload)[1]
        self.assertEqual(first['receipt']['targetEngineVersion'],'3.8.5')
        self.assertEqual(self.post(payload)[1], first)
        with self.assertRaises(HTTPError) as caught:
            self.post(dict(payload,targetEngineVersion='3.8.6'))
        self.assertEqual(caught.exception.code,409)
        self.assertEqual(self.lookup(payload)[1],first)

    def test_same_request_concurrent_http_has_one_plan_and_one_revision(self):
        payload=self.payload()
        with ThreadPoolExecutor(max_workers=6) as pool:
            receipts=list(pool.map(lambda _:self.post(payload)[1]['receipt'],range(6)))
        self.assertTrue(all(value==receipts[0] for value in receipts))
        self.assertEqual(self.queue.get_node('cluster')['revision'],1)
        with closing(sqlite3.connect(self.queue.path)) as db:
            self.assertEqual(db.execute('SELECT count(*) FROM owned_maintenance_schedules').fetchone()[0],1)

    def test_lost_http_response_then_restart_uses_original_receipt(self):
        payload=self.payload();reply=self.handler._reply
        def drop(handler,status,value):
            if handler.path.endswith('/schedule-owned') and status==200:
                handler.close_connection=True
            else:reply(handler,status,value)
        with patch.object(self.handler,'_reply',drop):
            with self.assertRaises((RemoteDisconnected,URLError)):
                self.post(payload)
        original=self.lookup(payload)[1]
        self.stop();self.start()
        self.assertEqual(self.lookup(payload)[1],original)
        self.assertEqual(self.post(payload)[1],original)
        self.assertEqual(self.queue.get_node('cluster')['revision'],1)

    def test_historical_duplicate_does_not_refresh_or_adopt_new_peer_plan(self):
        payload=self.payload();original=self.post(payload)[1]
        node=self.queue.get_node('cluster')
        self.queue.cancel_node('cluster',dict(expectedRevision=node['revision'],planId=node['planId']))
        peer=self.queue.schedule_node('cluster',{key:payload[key] for key in ('drainAt','notBefore','afterJobId','targetImage')}|{'expectedRevision':2})
        with patch.object(self.queue,'_refresh_nodes',side_effect=AssertionError('historical lookup refreshed nodes')):
            self.assertEqual(self.post(payload)[1],original)
            self.assertEqual(self.lookup(payload)[1],original)
        self.assertEqual(self.queue.get_node('cluster'),peer)

    def test_same_id_changed_payload_or_node_conflicts_without_mutation(self):
        payload=self.payload();self.post(payload)
        before=self.queue.list_nodes()
        for changed,node in ((dict(payload,targetImage='sha256:'+'c'*64),'cluster'),(payload,'main')):
            with self.assertRaises(HTTPError) as caught:self.post(changed,node=node)
            self.assertEqual(caught.exception.code,409)
        self.assertEqual(self.queue.list_nodes(),before)

    def test_missing_wrong_node_and_invalid_lookup_have_no_side_effects(self):
        payload=self.payload()
        for node,expected in (('cluster',404),('main',404),('missing',400)):
            with self.assertRaises(HTTPError) as caught:self.lookup(payload,node=node)
            self.assertEqual(caught.exception.code,expected)
        self.post(payload)
        with self.assertRaises(HTTPError) as caught:self.lookup(payload,node='main')
        self.assertEqual(caught.exception.code,404)

    def test_closed_payload_strict_values_and_duplicate_json_rejected(self):
        payload=self.payload()
        bad=[dict(payload,schemaVersion=True),dict(payload,expectedRevision=True),
             dict(payload,expectedRevision=2**53),dict(payload,requestId='not-uuid'),
             dict(payload,authorityId='not-uuid'),dict(payload,afterJobId='not-uuid'),
             dict(payload,unexpected=True),{k:v for k,v in payload.items() if k!='targetImage'}]
        for value in bad:
            with self.subTest(value=value):
                with self.assertRaises(HTTPError) as caught:self.post(value)
                self.assertEqual(caught.exception.code,400)
        raw=json.dumps(payload)[:-1]+',"targetImage":"'+IMAGE+'"}'
        with self.assertRaises(HTTPError) as caught:self.post(payload,raw=raw.encode())
        self.assertEqual(caught.exception.code,400)
        self.assertEqual(self.queue.get_node('cluster')['revision'],0)

    def test_wrong_authority_refused_before_scheduling(self):
        with self.assertRaises(HTTPError) as caught:self.post(dict(self.payload(),authorityId=str(uuid.uuid4())))
        self.assertEqual(caught.exception.code,409)
        self.assertEqual(self.queue.get_node('cluster')['state'],'active')

    def test_receipt_insert_failure_rolls_back_node_schedule(self):
        payload=self.payload()
        with closing(sqlite3.connect(self.queue.path)) as db:
            with db:
                db.execute("CREATE TRIGGER fail_receipt BEFORE INSERT ON owned_maintenance_schedules BEGIN SELECT RAISE(ABORT,'synthetic receipt failure'); END")
        before=self.queue.get_node('cluster')
        with self.assertRaises(sqlite3.IntegrityError):self.queue.schedule_node_owned('cluster',payload)
        self.assertEqual(self.queue.get_node('cluster'),before)
        self.assertIsNone(self.queue.owned_schedule_receipt('cluster',payload['requestId']))

    def test_legacy_schedule_payload_and_response_remain_compatible(self):
        payload=self.payload();legacy={key:payload[key] for key in ('expectedRevision','drainAt','notBefore','afterJobId','targetImage')}
        status,result=self.request('/v1/prototype/nodes/cluster/schedule',legacy)
        self.assertEqual(status,200);self.assertEqual(set(result),{'node'})
        self.assertEqual(result['node']['state'],'draining')
        self.assertEqual(self.request('/v1/prototype/nodes/cluster',token=TOKEN)[1]['node'],result['node'])
