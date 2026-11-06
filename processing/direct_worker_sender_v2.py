"""Private V2 coordinator→worker TLS sender; observations are not custody."""
import hashlib
import json
import time

from . import reference_protocol as p
from .direct_v2_inventory import VerifiedWorkerInventory
from .direct_worker_scratch_v2 import identity
from .direct_worker_sender import DirectWorkerSender, DirectDeliveryRejected, DirectDeliveryUncertain


class DirectWorkerSenderV2(DirectWorkerSender):
    def _response(self,connection,deadline):
        # A V2 worker may reject after durably consuming readiness and swapping
        # its pin. No HTTP error proves that this operation had no effect.
        try:
            return super()._response(connection,deadline)
        except DirectDeliveryRejected:
            raise DirectDeliveryUncertain('V2 worker denial outcome uncertain') from None

    def __init__(self, *, origin, connect_ip, ca_file, bearer, inventory, item, reservation_binding,
                 expected_worker, issuer_id, key_id, worker_public_key,
                 max_object_bytes=20*1024**3, response_limit=65536,
                 idle_timeout=10, total_timeout=120, client_cert_file=None, client_key_file=None,
                 enrolled_transport=None):
        self._configure_transport(origin=origin,connect_ip=connect_ip,ca_file=ca_file,bearer=bearer,
            max_object_bytes=max_object_bytes,response_limit=response_limit,
            idle_timeout=idle_timeout,total_timeout=total_timeout,
            client_cert_file=client_cert_file,client_key_file=client_key_file,enrolled_transport=enrolled_transport)
        if not isinstance(inventory,VerifiedWorkerInventory):raise ValueError('verified V2 inventory required')
        self._item,self._sha,_=identity(item,expected_worker=expected_worker)
        saved=json.loads(inventory._wire)
        if (p.canonical_json(reservation_binding)!=inventory._reservation_wire
                or any(self._item[key]!=reservation_binding[key] for key in reservation_binding)
                or any(self._item[key]!=saved[key] for key in ('sourceManifestSha256',
                    'derivedManifestSha256','pipelineManifestSha256','inventorySha256'))
                or self._item['objectCount']!=sum(len(saved[key]['objects']) for key in ('sourceManifest','derivedManifest'))
                or self._item['inputBytes']!=sum(obj['byteSize'] for key in ('sourceManifest','derivedManifest')
                                                for obj in saved[key]['objects'])):
            raise ValueError('V2 inventory and sender reservation differ')
        self._inventory,self._reservation=inventory,dict(reservation_binding)
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
        p._slug(issuer_id,'issuer');p._slug(key_id,'key')
        if not isinstance(worker_public_key,Ed25519PublicKey):raise ValueError('pinned worker public key required')
        self._issuer,self._key,self._public=issuer_id,key_id,worker_public_key
        self._source_count=len(saved['sourceManifest']['objects'])
        self._derived_count=len(saved['derivedManifest']['objects'])

    def put(self,object_id,reader):
        obj=self._inventory.object(object_id,reservation_binding=self._reservation)
        size=obj['byteSize']
        if size>self._max_object:raise DirectDeliveryRejected('worker object limit exceeded')
        if not callable(getattr(reader,'read',None)):raise ValueError('source reader required')
        deadline=time.monotonic()+self._total
        connection=guard=None
        try:
            connection,guard=self._connect(deadline)
            connection.putrequest('PUT','/direct/v2/objects/'+object_id)
            for name,value in {**self._headers(),'Content-Length':str(size),'Content-SHA256':obj['sha256']}.items():
                connection.putheader(name,value)
            connection.endheaders();self._timeout(connection,deadline)
            remaining=size;digest=hashlib.sha256();last=b''
            while remaining:
                chunk=reader.read(min(remaining,1024*1024))
                if type(chunk) is not bytes or not chunk or len(chunk)>remaining:
                    raise DirectDeliveryUncertain('source stream incomplete')
                remaining-=len(chunk);digest.update(chunk)
                if remaining:self._timeout(connection,deadline);connection.send(chunk)
                else:last=chunk
            if reader.read(1)!=b'' or digest.hexdigest()!=obj['sha256']:
                raise DirectDeliveryUncertain('source bytes changed')
            self._timeout(connection,deadline);connection.send(last)
            result=self._response(connection,deadline)
            expected=dict(protocol='ltds-direct-local-object-v2',reservationId=self._item['reservationId'],
                scratchBindingSha256=self._sha,objectId=object_id,byteSize=size,sha256=obj['sha256'],
                localObjectDurable=True,sourceCustodyAccepted=False,transferComplete=False)
            if (type(result) is not dict or set(result)!=set(expected)|{'replayed'}
                    or type(result['replayed']) is not bool
                    or p.canonical_json({k:v for k,v in result.items() if k!='replayed'})!=p.canonical_json(expected)):
                raise DirectDeliveryUncertain('V2 worker observation changed')
            if not guard.finish():raise DirectDeliveryUncertain('worker delivery outcome uncertain')
            return result
        except DirectDeliveryRejected:
            if guard is None or not guard.finish():raise DirectDeliveryUncertain('worker delivery outcome uncertain') from None
            raise
        except DirectDeliveryUncertain:raise
        except Exception:raise DirectDeliveryUncertain('worker delivery outcome uncertain') from None
        finally:
            if guard is not None:guard.finish()
            if connection is not None:connection.close()
    def authorize_reservation(self,operation_id):
        """Replay the exact reservation grant; this never acknowledges custody."""
        p._uuid(operation_id,'operation')
        if not self._client_identity_configured:
            raise ValueError('coordinator client certificate required')
        body=p.canonical_json(dict(operationId=operation_id,item=self._item))
        if len(body)>8192:raise ValueError('reservation grant exceeds body limit')
        deadline=time.monotonic()+self._total;connection=guard=None
        try:
            connection,guard=self._connect(deadline)
            connection.request('POST','/direct/v2/reservation',body=body,headers=self._headers())
            result=self._response(connection,deadline)
            expected=dict(reservationId=self._item['reservationId'],operationId=operation_id,
                sourceCustodyAccepted=False,transferComplete=False,processingAuthorized=False)
            if p.canonical_json(result)!=p.canonical_json(expected):
                raise DirectDeliveryUncertain('worker grant acknowledgement changed')
            if not guard.finish():raise DirectDeliveryUncertain('worker grant outcome uncertain')
            return result
        except DirectDeliveryUncertain:raise
        except Exception:raise DirectDeliveryUncertain('worker grant outcome uncertain') from None
        finally:
            if guard is not None:guard.finish()
            if connection is not None:connection.close()

    def _expected_closure(self):
        item=self._item
        return dict(protocol='ltds-direct-local-closure-v2',reservationId=item['reservationId'],
            scratchBindingSha256=self._sha,jobId=item['jobId'],attemptId=item['attemptId'],
            generation=item['generation'],authorityId=item['authorityId'],nodeId=item['nodeId'],
            workerId=item['workerId'],workerInstanceId=item['workerInstanceId'],
            sourceManifestSha256=item['sourceManifestSha256'],derivedManifestSha256=item['derivedManifestSha256'],
            pipelineManifestSha256=item['pipelineManifestSha256'],inventorySha256=item['inventorySha256'],
            sourceObjectCount=self._source_count,derivedObjectCount=self._derived_count,
            localClosureComplete=True,sourceCustodyAccepted=False,transferComplete=False,processingAuthorized=False)

    def complete(self):
        from .direct_worker_receipt_v2 import verify
        expected=self._expected_closure()
        deadline=time.monotonic()+self._total;connection=guard=None
        try:
            connection,guard=self._connect(deadline)
            connection.request('POST','/direct/v2/complete',body=b'',headers=self._headers())
            result=self._response(connection,deadline)
            checked=verify(result,expected_closure=expected,issuer_id=self._issuer,key_id=self._key,public_key=self._public)
            if not guard.finish():raise DirectDeliveryUncertain('V2 closure outcome uncertain')
            return checked
        except DirectDeliveryRejected:
            if guard is None or not guard.finish():raise DirectDeliveryUncertain('V2 closure outcome uncertain') from None
            raise
        except DirectDeliveryUncertain:raise
        except Exception:raise DirectDeliveryUncertain('V2 closure outcome uncertain') from None
        finally:
            if guard is not None:guard.finish()
            if connection is not None:connection.close()

    def observe_retention(self, signed_closure):
        """Verify the worker's local ledger fence, not Viewer custody."""
        from .direct_worker_receipt_v2 import verify_retention
        deadline=time.monotonic()+self._total;connection=guard=None
        try:
            connection,guard=self._connect(deadline)
            connection.request('GET','/direct/v2/retention',headers={**self._headers(),'Content-Length':'0'})
            result=self._response(connection,deadline)
            checked=verify_retention(result,signed_closure=signed_closure,expected_closure=self._expected_closure(),
                issuer_id=self._issuer,key_id=self._key,public_key=self._public)
            if not guard.finish():raise DirectDeliveryUncertain('V2 retention outcome uncertain')
            return checked
        except DirectDeliveryUncertain:raise
        except Exception:raise DirectDeliveryUncertain('V2 retention outcome uncertain') from None
        finally:
            if guard is not None:guard.finish()
            if connection is not None:connection.close()

    def observe(self,object_id):
        """Retrieve exact durable worker observation; absence is not a negative ACK."""
        obj=self._inventory.object(object_id,reservation_binding=self._reservation)
        deadline=time.monotonic()+self._total;connection=guard=None
        try:
            connection,guard=self._connect(deadline)
            connection.request('GET','/direct/v2/objects/'+object_id,headers={**self._headers(),'Content-Length':'0'})
            result=self._response(connection,deadline)
            expected=dict(protocol='ltds-direct-local-object-v2',reservationId=self._item['reservationId'],
                scratchBindingSha256=self._sha,objectId=object_id,byteSize=obj['byteSize'],sha256=obj['sha256'],
                localObjectDurable=True,sourceCustodyAccepted=False,transferComplete=False,replayed=True)
            if p.canonical_json(result)!=p.canonical_json(expected):
                raise DirectDeliveryUncertain('V2 worker observation changed')
            if not guard.finish():raise DirectDeliveryUncertain('V2 observation outcome uncertain')
            return result
        except DirectDeliveryUncertain:raise
        except Exception:raise DirectDeliveryUncertain('V2 observation outcome uncertain') from None
        finally:
            if guard is not None:guard.finish()
            if connection is not None:connection.close()

class DisabledDirectWorkerSender(DirectWorkerSenderV2):
    """Offline preparation cannot create any outgoing connection."""
    def _connect(self,deadline):
        raise RuntimeError('disabled sender cannot connect')

