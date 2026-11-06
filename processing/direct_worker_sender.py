"""Unwired coordinator-to-worker TLS sender; never a Viewer custody ACK."""
import hashlib
import http.client
import ipaddress
import errno
import json
import math
from pathlib import Path
import re
import select
import socket
import ssl
import threading
import time
from urllib.parse import urlsplit

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from . import reference_protocol as p
from .direct_worker_receiver import _PreparedManifest
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


class _SocketDeadline:
    """Watch a duplicated socket through TCP connect, TLS and response."""
    def __init__(self, source, seconds):
        self._socket=source.dup();self._lock=threading.Lock()
        self._active=True;self._accepted=False;self._expires=time.monotonic()+seconds
        self._timer=threading.Timer(seconds,self._expire);self._timer.daemon=True;self._timer.start()

    def _close_locked(self, *, shutdown):
        self._active=False
        if shutdown:
            try:self._socket.shutdown(socket.SHUT_RDWR)
            except OSError:pass
        self._socket.close()

    def _expire(self):
        with self._lock:
            if self._active:
                remaining=self._expires-time.monotonic()
                if remaining>0:
                    self._timer=threading.Timer(remaining,self._expire);self._timer.daemon=True;self._timer.start()
                else:self._close_locked(shutdown=True)

    def finish(self):
        with self._lock:
            if not self._active:return self._accepted
            self._timer.cancel()
            if time.monotonic()>=self._expires:
                self._close_locked(shutdown=True);return False
            self._accepted=True;self._close_locked(shutdown=False);return True


class DirectWorkerSender:
    def __init__(self, *, origin, connect_ip, ca_file, bearer, manifest, item, derived_ids,
                 expected_worker, issuer_id, key_id, worker_public_key,
                 max_object_bytes=20*1024**3, response_limit=65536,
                 idle_timeout=10, total_timeout=120, client_cert_file=None, client_key_file=None):
        self._configure_transport(origin=origin, connect_ip=connect_ip, ca_file=ca_file, bearer=bearer,
            max_object_bytes=max_object_bytes, response_limit=response_limit,
            idle_timeout=idle_timeout, total_timeout=total_timeout,
            client_cert_file=client_cert_file, client_key_file=client_key_file)
        p._slug(issuer_id,'issuer');p._slug(key_id,'key')
        if not isinstance(worker_public_key,Ed25519PublicKey):raise ValueError('pinned worker public key required')
        self._item,self._sha,_=scratch_identity(item,expected_worker=expected_worker)
        self._prepared=_PreparedManifest(manifest,self._item,derived_ids)
        self._issuer,self._key,self._public=issuer_id,key_id,worker_public_key

    def _configure_transport(self, *, origin, connect_ip, ca_file, bearer,
                             max_object_bytes, response_limit, idle_timeout, total_timeout,
                             client_cert_file=None, client_key_file=None, enrolled_transport=None):
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
        try:
            if type(connect_ip) is not str or '%' in connect_ip:raise ValueError()
            address=ipaddress.ip_address(connect_ip)
        except ValueError:
            raise ValueError('pinned numeric worker address required') from None
        if (type(bearer) is not str or not 32<=len(bearer)<=4096
                or not all(0x21<=ord(c)<=0x7e for c in bearer)):
            raise ValueError('scoped coordinator bearer required')
        if (type(max_object_bytes) is not int or not 1<=max_object_bytes<=2**53-1
                or type(response_limit) is not int or not 1<=response_limit<=1024*1024
                or any(type(v) not in (int,float) or not math.isfinite(v) or v<=0 for v in (idle_timeout,total_timeout))):
            raise ValueError('bounded sender limits required')
        if enrolled_transport is not None:
            from .direct_protected_transport import DisabledProtectedTransport
            if (type(enrolled_transport) is not DisabledProtectedTransport
                    or (origin,str(address),bearer)!=(enrolled_transport.origin,enrolled_transport.connect_ip,enrolled_transport._bearer)):
                raise ValueError('protected transport binding changed')
            context=enrolled_transport._context
            self._client_identity_configured=True
        else:
            ca=Path(ca_file)
            if not ca.is_absolute() or not ca.is_file():raise ValueError('explicit CA file required')
            context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            context.minimum_version=ssl.TLSVersion.TLSv1_2
            context.load_verify_locations(cafile=str(ca))
            if (client_cert_file is None) != (client_key_file is None):
                raise ValueError('client certificate and key both required')
            if client_cert_file is not None:
                from .reference_runtime import _file
                cert, _ = _file(client_cert_file, private=False)
                key, _ = _file(client_key_file, private=True)
                context.load_cert_chain(str(cert), str(key))
            self._client_identity_configured = client_cert_file is not None
        if context.verify_mode!=ssl.CERT_REQUIRED or not context.check_hostname:raise ValueError('verified TLS required')
        self._context,self._host,self._port=context,parsed.hostname,port
        self._address=address
        self._bearer=bearer
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
        raw=wrapped=guard=None
        try:
            remaining=deadline-time.monotonic()
            if remaining<=0:raise TimeoutError('direct transfer deadline exceeded')
            family=socket.AF_INET6 if self._address.version==6 else socket.AF_INET
            raw=socket.socket(family,socket.SOCK_STREAM)
            raw.setblocking(False)
            guard=_SocketDeadline(raw,remaining)
            result=raw.connect_ex((str(self._address),self._port))
            pending={errno.EINPROGRESS,errno.EWOULDBLOCK,errno.EALREADY,
                     getattr(socket,'WSAEWOULDBLOCK',errno.EWOULDBLOCK),
                     getattr(socket,'WSAEINPROGRESS',errno.EINPROGRESS)}
            if result not in ({0}|pending):raise OSError(result,'worker connection failed')
            if result:
                _,writable,failed=select.select([], [raw], [raw], remaining)
                if not writable or failed:raise TimeoutError('worker connection deadline exceeded')
                error=raw.getsockopt(socket.SOL_SOCKET,socket.SO_ERROR)
                if error:raise OSError(error,'worker connection failed')
            remaining=deadline-time.monotonic()
            if remaining<=0:raise TimeoutError('direct transfer deadline exceeded')
            raw.settimeout(min(self._idle,remaining))
            wrapped=self._context.wrap_socket(raw,server_hostname=self._host,do_handshake_on_connect=False)
            wrapped.do_handshake()
            connection.sock=wrapped
            return connection,guard
        except BaseException:
            if guard is not None:guard.finish()
            if wrapped is not None:wrapped.close()
            if raw is not None:raw.close()
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
        try:
            result=json.loads(b''.join(chunks).decode('utf-8'),object_pairs_hook=_unique_pairs,
                              parse_constant=lambda _: (_ for _ in ()).throw(ValueError()))
        except (ValueError,UnicodeError,RecursionError):
            raise DirectDeliveryUncertain('worker response invalid') from None
        # Only this specific authenticated response is guaranteed by the
        # current worker boundary to happen before any object effect.
        if response.status==403 and result=={'error':'access_denied'}:
            raise DirectDeliveryRejected('worker refused operation')
        if response.status!=200:
            raise DirectDeliveryUncertain('worker response outcome uncertain')
        return result

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
        except DirectDeliveryRejected:
            if guard is None or not guard.finish():
                raise DirectDeliveryUncertain('worker delivery outcome uncertain') from None
            raise
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
        except DirectDeliveryRejected:
            if guard is None or not guard.finish():
                raise DirectDeliveryUncertain('worker closure outcome uncertain') from None
            raise
        except DirectDeliveryUncertain:raise
        except Exception:raise DirectDeliveryUncertain('worker closure outcome uncertain') from None
        finally:
            if guard is not None:guard.finish()
            if connection is not None:connection.close()
