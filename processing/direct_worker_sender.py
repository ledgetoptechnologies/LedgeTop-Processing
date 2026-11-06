"""Unwired coordinator-to-worker TLS sender; never a Viewer custody ACK."""
import hashlib
import http.client
import json
import math
from pathlib import Path
import re
import ssl
import time
from urllib.parse import urlsplit

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from . import reference_protocol as p
from .direct_worker_receiver import _PreparedManifest
from .direct_worker_http import _HeaderDeadline
from .direct_worker_receipt import verify as verify_worker_closure
from .direct_worker_scratch import scratch_identity


class DirectDeliveryUncertain(RuntimeError):
    """No custody is acknowledged; reconcile the exact request before retry."""


class DirectDeliveryRejected(ValueError):
    pass


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result: raise ValueError('duplicate JSON key')
        result[key] = value
    return result


class DirectWorkerSender:
    def __init__(self, *, origin, ca_file, bearer, manifest, item, derived_ids,
                 expected_worker, issuer_id, key_id, worker_public_key,
                 max_object_bytes=20*1024**3, response_limit=65536,
                 idle_timeout=10, total_timeout=120):
        if (type(origin) is not str or not origin.isascii()
                or any(ord(char)<33 or ord(char)==127 for char in origin)):
            raise ValueError('exact HTTPS worker origin required')
        try:
            parsed=urlsplit(origin); port=443 if parsed.port is None else parsed.port
        except ValueError:
            raise ValueError('exact HTTPS worker origin required') from None
        if (parsed.scheme!='https' or not parsed.hostname or parsed.username or parsed.password
                or parsed.path not in ('','/') or parsed.query or parsed.fragment
                or not 1 <= port <= 65535 or origin.rstrip('/') != 'https://'+parsed.netloc):
            raise ValueError('exact HTTPS worker origin required')
        if (type(bearer) is not str or not 32<=len(bearer)<=4096
                or not all(0x21<=ord(c)<=0x7e for c in bearer)):
            raise ValueError('scoped coordinator bearer required')
        p._slug(issuer_id,'issuer');p._slug(key_id,'key')
        if not isinstance(worker_public_key,Ed25519PublicKey):raise ValueError('pinned worker public key required')
        if (type(max_object_bytes) is not int or not 1<=max_object_bytes<=2**53-1
                or type(response_limit) is not int or not 1<=response_limit<=1024*1024
                or any(type(v) not in (int,float) or not math.isfinite(v) or v<=0 for v in (idle_timeout,total_timeout))):
            raise ValueError('bounded sender limits required')
        ca=Path(ca_file)
        if not ca.is_absolute() or not ca.is_file():raise ValueError('explicit CA file required')
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version=ssl.TLSVersion.TLSv1_2
        context.load_verify_locations(cafile=str(ca))
        if context.verify_mode!=ssl.CERT_REQUIRED or not context.check_hostname:raise ValueError('verified TLS required')
        self._context,self._host,self._port=context,parsed.hostname,port
        self._bearer=bearer
        self._item,self._sha,_=scratch_identity(item,expected_worker=expected_worker)
        self._prepared=_PreparedManifest(manifest,self._item,derived_ids)
        self._issuer,self._key,self._public=issuer_id,key_id,worker_public_key
        self._max_object,self._response_limit=max_object_bytes,response_limit
        self._idle,self._total=idle_timeout,total_timeout

    def _headers(self):
        return {'Authorization':'Bearer '+self._bearer,'X-Coordinator-Authority':self._item['authorityId']}

    def _timeout(self,connection,deadline):
        remaining=deadline-time.monotonic()
        if remaining<=0:raise TimeoutError('direct transfer deadline exceeded')
        if connection.sock is not None:connection.sock.settimeout(min(self._idle,remaining))

    def _connect(self, deadline):
        connection=http.client.HTTPSConnection(self._host,self._port,context=self._context,
                                              timeout=min(self._idle,self._total))
        try:
            connection.connect()
            remaining=deadline-time.monotonic()
            if remaining<=0:raise TimeoutError('direct transfer deadline exceeded')
            return connection,_HeaderDeadline(connection.sock,remaining)
        except BaseException:
            connection.close()
            raise

    def _response(self,connection,deadline):
        self._timeout(connection,deadline)
        response=connection.getresponse()
        pairs=response.getheaders()
        lengths=[v for k,v in pairs if k.lower()=='content-length']
        if (len(lengths)!=1 or not re.fullmatch(r'0|[1-9][0-9]{0,8}',lengths[0])
                or int(lengths[0])>self._response_limit
                or any(k.lower() in ('transfer-encoding','content-encoding') for k,_ in pairs)):
            raise DirectDeliveryUncertain('worker response framing uncertain')
        length=int(lengths[0]); chunks=[]; remaining=length
        while remaining:
            self._timeout(connection,deadline)
            chunk=response.read1(min(remaining,65536))
            if not chunk:raise DirectDeliveryUncertain('worker response truncated')
            chunks.append(chunk);remaining-=len(chunk)
        if response.status!=200:
            raise DirectDeliveryRejected('worker refused operation')
        try:
            return json.loads(b''.join(chunks).decode('utf-8'),object_pairs_hook=_unique_pairs,
                              parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        except (ValueError,UnicodeError,RecursionError):
            raise DirectDeliveryUncertain('worker response invalid') from None

    def put(self,object_id,reader):
        obj=self._prepared.object(self._item,object_id)
        size=obj['byteSize']
        if size>self._max_object:raise DirectDeliveryRejected('worker object limit exceeded')
        if not callable(getattr(reader,'read',None)):raise ValueError('source reader required')
        deadline=time.monotonic()+self._total
        connection=guard=None
        try:
            connection,guard=self._connect(deadline)
            connection.putrequest('PUT','/direct/v1/objects/'+object_id)
            for name,value in {**self._headers(),'Content-Length':str(size),'Content-SHA256':obj['sha256']}.items():
                connection.putheader(name,value)
            connection.endheaders()
            self._timeout(connection,deadline)
            remaining=size; digest=hashlib.sha256(); last=b''
            while remaining:
                chunk=reader.read(min(remaining,1024*1024))
                if type(chunk) is not bytes or not chunk or len(chunk)>remaining:
                    raise DirectDeliveryUncertain('source stream incomplete')
                remaining-=len(chunk);digest.update(chunk)
                # Hold the final chunk until source length and digest are known.
                if remaining: self._timeout(connection,deadline);connection.send(chunk)
                else: last=chunk
            if reader.read(1)!=b'' or digest.hexdigest()!=obj['sha256']:
                raise DirectDeliveryUncertain('source bytes changed')
            self._timeout(connection,deadline);connection.send(last)
            result=self._response(connection,deadline)
            expected=dict(protocol='ltds-direct-local-object-v1',reservationId=self._item['reservationId'],
                scratchBindingSha256=self._sha,jobId=self._item['jobId'],attemptId=self._item['attemptId'],
                generation=self._item['generation'],nodeId=self._item['nodeId'],objectId=object_id,
                byteSize=size,sha256=obj['sha256'],localObjectDurable=True,
                sourceCustodyAccepted=False,transferComplete=False)
            if (type(result) is not dict or set(result)!=set(expected)|{'replayed'}
                    or type(result['replayed']) is not bool
                    or p.canonical_json({k:v for k,v in result.items() if k!='replayed'})!=p.canonical_json(expected)):
                raise DirectDeliveryUncertain('worker object observation changed')
            if not guard.finish():raise DirectDeliveryUncertain('worker delivery outcome uncertain')
            return result
        except DirectDeliveryRejected:raise
        except DirectDeliveryUncertain:raise
        except Exception:raise DirectDeliveryUncertain('worker delivery outcome uncertain') from None
        finally:
            if guard is not None:guard.finish()
            if connection is not None:connection.close()

    def complete(self):
        manifest, derived=self._prepared.snapshot(self._item)
        expected=dict(protocol='ltds-direct-local-closure-v1',reservationId=self._item['reservationId'],
            scratchBindingSha256=self._sha,authorityId=self._item['authorityId'],nodeId=self._item['nodeId'],
            workerId=self._item['workerId'],workerInstanceId=self._item['workerInstanceId'],jobId=self._item['jobId'],
            attemptId=self._item['attemptId'],generation=self._item['generation'],environment=self._item['environment'],
            manifestSha256=self._item['manifestSha256'],derivedManifestSha256=self._item['derivedManifestSha256'],
            sourceObjectCount=len(manifest['objects'])-len(derived),derivedObjectCount=len(derived),
            localClosureComplete=True,sourceCustodyAccepted=False,processingAuthorized=False)
        if 'limitProfile' in self._item:expected['limitProfile']=self._item['limitProfile']
        deadline=time.monotonic()+self._total
        connection=guard=None
        try:
            connection,guard=self._connect(deadline)
            connection.request('POST','/direct/v1/complete',body=b'',headers=self._headers())
            result=self._response(connection,deadline)
            verified=verify_worker_closure(result,expected_closure=expected,issuer_id=self._issuer,
                                           key_id=self._key,public_key=self._public)
            if not guard.finish():raise DirectDeliveryUncertain('worker closure outcome uncertain')
            return verified
        except DirectDeliveryRejected:raise
        except DirectDeliveryUncertain:raise
        except Exception:raise DirectDeliveryUncertain('worker closure outcome uncertain') from None
        finally:
            if guard is not None:guard.finish()
            if connection is not None:connection.close()
