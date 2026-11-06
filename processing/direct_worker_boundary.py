"""Unwired private worker boundary; not an HTTP listener or Viewer API.

The trusted future listener must derive protected_transport from its actual
socket, not a request header. This prototype cannot establish TLS or route
ownership on its own and must not be deployed as a service.
"""
import copy
import hmac

from .direct_worker_closure import verify_closure
from .direct_worker_receiver import _PreparedManifest, receive
from .direct_worker_receipt import WorkerClosureSigner
from .direct_worker_scratch import scratch_identity


class DirectBoundaryDenied(ValueError):pass


class DirectWorkerBoundary:
    def __init__(self,*,ledger,spool,manifest,item,derived_ids,expected_worker,
                 engine_id,coordinator_origin,gate_generation,bearer,signer):
        if (type(bearer) is not str or not 32<=len(bearer)<=4096
                or not all(0x21<=ord(c)<=0x7e for c in bearer)
                or not isinstance(signer,WorkerClosureSigner)):
            raise DirectBoundaryDenied('invalid private worker configuration')
        if signer.identity!=expected_worker:
            raise DirectBoundaryDenied('worker signer identity changed')
        scratch_identity(item,expected_worker=expected_worker)
        self.ledger,self.spool=ledger,spool
        self.item=copy.deepcopy(item)
        self.worker=copy.deepcopy(expected_worker)
        self._prepared=_PreparedManifest(copy.deepcopy(manifest),self.item,copy.deepcopy(derived_ids))
        self.engine_id,self.origin,self.generation=engine_id,coordinator_origin,gate_generation
        self._bearer,self.signer=bearer,signer

    @staticmethod
    def _header(headers,name):
        if not hasattr(headers,'items'):return None
        matches=[value for key,value in headers.items() if type(key) is str and key.lower()==name.lower()]
        return matches[0] if len(matches)==1 and type(matches[0]) is str else None

    def _authorize(self,headers,protected_transport):
        if protected_transport is not True:raise DirectBoundaryDenied('protected transport required')
        supplied=self._header(headers,'Authorization')
        authority=self._header(headers,'X-Coordinator-Authority')
        if (type(supplied) is not str or not supplied.startswith('Bearer ')
                or not supplied[7:].isascii() or not hmac.compare_digest(supplied[7:],self._bearer)
                or authority!=self.item['authorityId']):
            raise DirectBoundaryDenied('private coordinator authorization required')

    def put(self,headers,*,protected_transport,object_id,content_length,content_sha256,reader):
        self._authorize(headers,protected_transport)
        obj=self._prepared.object(self.item,object_id)
        if (type(content_length) is not int or content_length!=obj['byteSize']
                or content_sha256!=obj['sha256']):
            raise DirectBoundaryDenied('direct object metadata mismatch')
        return receive(self.ledger,self.spool,None,self.item,expected_worker=self.worker,
            engine_id=self.engine_id,coordinator_origin=self.origin,gate_generation=self.generation,
            object_id=object_id,reader=reader,_prepared=self._prepared)

    def complete(self,headers,*,protected_transport):
        self._authorize(headers,protected_transport)
        manifest,derived_ids=self._prepared.snapshot(self.item)
        return verify_closure(self.ledger,self.spool,manifest,self.item,derived_ids=derived_ids,
            expected_worker=self.worker,engine_id=self.engine_id,coordinator_origin=self.origin,
            gate_generation=self.generation,worker_signer=self.signer)
