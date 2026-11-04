import json
import unittest

from processing.node_capacity_api import NodeCapacityAPI
from processing.node_capacity_client import CapacityReservationHeld, NodeCapacityClient


class NodeCapacityClientTests(unittest.TestCase):
    def setUp(self):
        self.key=b'k'*32
        self.request={'nodeId':'cluster','physicalHostId':'host-1','jobId':'job-1',
            'generation':1,'reservationId':'res-1','environment':'production',
            'manifestSha256':'a'*64,'inputBytes':100,'outputBudgetBytes':500,'ttlSeconds':300}
        self.receipt=NodeCapacityAPI._receipt if False else None
        payload={k:v for k,v in self.request.items() if k not in ('nodeId','physicalHostId','ttlSeconds')}
        self.receipt=NodeCapacityAPI.__new__(NodeCapacityAPI)
        self.receipt.node_id='cluster';self.receipt.physical_host_id='host-1';self.receipt._receipt_key=self.key
        self.receipt=self.receipt._receipt(dict(payload,state='reserved'))

    def client(self, response):
        return NodeCapacityClient(origin='https://node.example',bearer_token='t'*40,
            receipt_key=self.key,transport=lambda *_:response)

    def call_reserve(self, client):
        return client.reserve(**{'node_id':self.request['nodeId'],'physical_host_id':self.request['physicalHostId'],
            'job_id':self.request['jobId'],'generation':self.request['generation'],
            'reservation_id':self.request['reservationId'],'environment':self.request['environment'],
            'manifest_sha256':self.request['manifestSha256'],'input_bytes':self.request['inputBytes'],
            'output_budget_bytes':self.request['outputBudgetBytes'],'ttl_seconds':self.request['ttlSeconds']})

    def test_exact_signed_receipt_is_accepted(self):
        client=self.client((200,json.dumps(self.receipt).encode()))
        self.assertEqual(self.call_reserve(client),self.receipt)

    def test_wrong_node_host_or_job_is_held(self):
        for key,value in [('nodeId','main'),('physicalHostId','other-host'),('jobId','other-job')]:
            with self.subTest(key=key):
                receipt=dict(self.receipt);receipt[key]=value
                client=self.client((200,json.dumps(receipt).encode()))
                with self.assertRaises(CapacityReservationHeld):self.call_reserve(client)

    def test_tampered_budget_signature_state_and_duplicate_keys_are_held(self):
        for mutate in (
            lambda r:r.update(outputBudgetBytes=999),
            lambda r:r.update(signature='0'*64),
            lambda r:r.update(state='uncertain'),
        ):
            receipt=dict(self.receipt);mutate(receipt)
            with self.assertRaises(CapacityReservationHeld):self.call_reserve(self.client((200,json.dumps(receipt).encode())))
        raw=json.dumps(self.receipt,separators=(',',':')).replace('"state":"reserved"','"state":"reserved","state":"reserved"').encode()
        with self.assertRaises(CapacityReservationHeld):self.call_reserve(self.client((200,raw)))

    def test_transport_error_non_200_oversize_and_redirect_hold(self):
        for response in ((503,b'{}'),(302,b'{}'),(200,b' '*20000)):
            with self.subTest(status=response[0]):
                with self.assertRaises(CapacityReservationHeld):self.call_reserve(self.client(response))
        def fail(*_):raise TimeoutError()
        client=NodeCapacityClient(origin='https://node.example',bearer_token='t'*40,receipt_key=self.key,transport=fail)
        with self.assertRaises(CapacityReservationHeld):self.call_reserve(client)

    def test_retries_keep_the_exact_request_and_reservation_id(self):
        bodies=[];responses=iter([(503,b'{}'),(200,json.dumps(self.receipt).encode())])
        def transport(_url,_headers,body,_timeout):
            bodies.append(body);return next(responses)
        client=NodeCapacityClient(origin='https://node.example',bearer_token='t'*40,receipt_key=self.key,transport=transport)
        with self.assertRaises(CapacityReservationHeld):self.call_reserve(client)
        self.call_reserve(client)
        self.assertEqual(bodies[0],bodies[1])
        self.assertIn(b'"reservationId":"res-1"',bodies[0])

    def test_only_fixed_https_origin_is_accepted(self):
        for origin in ('http://node.example','https://user@node.example','https://node.example/path','https://node.example/?token=x'):
            with self.subTest(origin=origin),self.assertRaises(ValueError):
                NodeCapacityClient(origin=origin,bearer_token='t'*40,receipt_key=self.key)


if __name__=='__main__':unittest.main()
