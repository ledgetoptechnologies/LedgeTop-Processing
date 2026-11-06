"""Linux-only disabled direct transport preparation from protected files.

Not wired to a listener or scheduler. A protected config and every ancestor
must be owned by root or this service UID, and non-writable by group/others.
TLS is built from sealed in-memory copies of the exact opened bytes; no TLS
library path reopen reaches the original credential files.
"""
import fcntl
import hashlib
import ipaddress
import json
import os
from pathlib import PurePosixPath
import stat
import ssl
from dataclasses import dataclass, field

from . import reference_protocol as p
from .reference_result_proxy import _origin

LIMIT=1024*1024

def _read(path, *, private, maximum=LIMIT, trusted_uids=None):
    if os.name!='posix' or not hasattr(os,'memfd_create'):
        raise ValueError('Linux protected transport required')
    if type(path) is not str or not path.startswith('/') or '\0' in path:
        raise ValueError('absolute protected path required')
    parts=PurePosixPath(path).parts
    if str(PurePosixPath(path))!=path or any(x in ('.','..') for x in parts):
        raise ValueError('literal protected path required')
    owners={0,os.geteuid()} if trusted_uids is None else set(trusted_uids)
    if not owners or any(type(uid) is not int or uid<0 for uid in owners):
        raise ValueError('protected owners required')
    fd=os.open('/',os.O_RDONLY|os.O_DIRECTORY|os.O_CLOEXEC)
    try:
        for component in parts[1:-1]:
            info=os.fstat(fd)
            if info.st_uid not in owners or info.st_mode & 0o022:
                raise ValueError('unprotected ancestor')
            child=os.open(component,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=fd)
            os.close(fd);fd=child
        info=os.fstat(fd)
        if info.st_uid not in owners or info.st_mode & 0o022:raise ValueError('unprotected ancestor')
        f=os.open(parts[-1],os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC|os.O_NONBLOCK,dir_fd=fd)
        try:
            before=os.fstat(f)
            if (not stat.S_ISREG(before.st_mode) or before.st_nlink!=1 or before.st_uid not in owners
                    or before.st_mode & (0o077 if private else 0o022) or not 0<before.st_size<=maximum):
                raise ValueError('unprotected file')
            chunks=[];remaining=maximum+1
            while remaining:
                data=os.read(f,min(65536,remaining))
                if not data:break
                chunks.append(data);remaining-=len(data)
            raw=b''.join(chunks);after=os.fstat(f)
            if (len(raw)!=before.st_size or len(raw)>maximum
                    or (before.st_dev,before.st_ino,before.st_size,before.st_mtime_ns,before.st_ctime_ns)
                    !=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns)):
                raise ValueError('protected file changed')
            return raw
        finally:os.close(f)
    finally:os.close(fd)

def _sealed(raw):
    fd=os.memfd_create('direct-credential',os.MFD_CLOEXEC|os.MFD_ALLOW_SEALING)
    try:
        view=memoryview(raw)
        while view:
            count=os.write(fd,view)
            if count<=0:raise OSError('short sealed write')
            view=view[count:]
        fcntl.fcntl(fd,fcntl.F_ADD_SEALS,fcntl.F_SEAL_WRITE|fcntl.F_SEAL_GROW|fcntl.F_SEAL_SHRINK|fcntl.F_SEAL_SEAL)
        return fd
    except BaseException:os.close(fd);raise

@dataclass(frozen=True,repr=False)
class DisabledProtectedTransport:
    """Locally verified preparation; never a runtime activation grant."""
    node: str
    origin: str
    connect_ip: str
    issuer_id: str
    key_id: str
    worker_public_key: object
    _context: ssl.SSLContext = field(repr=False)
    _bearer: str = field(repr=False)
    enabled: bool = field(default=False,init=False)
    def __bool__(self):return False

def load_disabled_transport(config_path,node,*,expected_config_sha256):
    """Load one exact node from a private, disabled schema-6 config."""
    return _load_transport(config_path,node,expected_config_sha256=expected_config_sha256,enabled=False)

@dataclass(frozen=True,repr=False)
class EnrolledProtectedTransport:
    """Enabled candidate; caller must pin config digest from protected enrollment."""
    node: str
    origin: str
    connect_ip: str
    issuer_id: str
    key_id: str
    worker_public_key: object
    _context: ssl.SSLContext = field(repr=False)
    _bearer: str = field(repr=False)
    enabled: bool = field(default=True,init=False)

def load_enrolled_transport(config_path,node,*,expected_config_sha256,expected_credential_sha256):
    """Load an explicitly enabled config; not wired to automatic startup."""
    return _load_transport(config_path,node,expected_config_sha256=expected_config_sha256,enabled=True,
                           expected_credential_sha256=expected_credential_sha256)

def _load_transport(config_path,node,*,expected_config_sha256,enabled,expected_credential_sha256=None):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    p._slug(node,'node')
    p._hash(expected_config_sha256,'protected config digest')
    config_bytes=_read(config_path,private=True)
    if hashlib.sha256(config_bytes).hexdigest()!=expected_config_sha256:
        raise ValueError('protected config digest changed')
    value=p.decode_submission(config_bytes)
    if type(value) is not dict or value.get('schemaVersion')!=6:raise ValueError('schema 6 required')
    direct=value.get('directWorkers');p._closed(direct,('enabled','nodes'),'direct worker preparation')
    if direct['enabled'] is not enabled or type(direct['nodes']) is not dict or node not in direct['nodes']:
        raise ValueError('matching enrolled node configuration required')
    item=direct['nodes'][node]
    p._closed(item,('origin','connectIp','caFile','clientCertFile','clientKeyFile','bearerFile',
                    'issuerId','keyId','workerPublicKeyPem'),'direct worker node')
    origin=_origin(item['origin']);address=str(ipaddress.ip_address(item['connectIp']))
    p._slug(item['issuerId'],'issuer');p._slug(item['keyId'],'key')
    key=serialization.load_pem_public_key(item['workerPublicKeyPem'].encode('ascii'))
    if not isinstance(key,Ed25519PublicKey):raise ValueError('pinned worker key required')
    ca=_read(item['caFile'],private=False);cert=_read(item['clientCertFile'],private=False)
    secret_key=_read(item['clientKeyFile'],private=True)
    bearer_bytes=_read(item['bearerFile'],private=True,maximum=4096)
    if enabled:
        expected=expected_credential_sha256
        if type(expected) is not dict or set(expected)!={'ca','certificate','privateKey','bearer'}:
            raise ValueError('protected credential pins required')
        for name,raw in (('ca',ca),('certificate',cert),('privateKey',secret_key),('bearer',bearer_bytes)):
            p._hash(expected[name],'protected credential digest')
            if hashlib.sha256(raw).hexdigest()!=expected[name]:raise ValueError('protected credential changed')
    bearer=bearer_bytes.decode('ascii').rstrip('\r\n')
    if not 32<=len(bearer)<=4096 or any(ord(c)<33 or ord(c)>126 for c in bearer):raise ValueError('invalid bearer')
    context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT);context.minimum_version=ssl.TLSVersion.TLSv1_2
    context.load_verify_locations(cadata=ca.decode('ascii'))
    certfd=_sealed(cert);keyfd=None
    try:
        keyfd=_sealed(secret_key)
        context.load_cert_chain('/proc/self/fd/'+str(certfd),'/proc/self/fd/'+str(keyfd))
    finally:
        os.close(certfd)
        if keyfd is not None:os.close(keyfd)
    if context.verify_mode!=ssl.CERT_REQUIRED or not context.check_hostname:raise ValueError('verified TLS required')
    cls=EnrolledProtectedTransport if enabled else DisabledProtectedTransport
    return cls(node,origin,address,item['issuerId'],item['keyId'],key,context,bearer)

@dataclass(frozen=True,repr=False)
class DisabledSenderPreparation:
    """Prepared sender is retained for offline review, never a capability."""
    node: str
    _sender: object = field(repr=False)
    enabled: bool = field(default=False,init=False)
    def __bool__(self):return False

def prepare_disabled_sender(config_path,node,*,expected_config_sha256,reservations,inventory,item,reservation_binding):
    """Construct exact enrolled transport without accepting endpoint/token input.

    This does not expose or start a service. The config digest and reservation
    registry must be obtained from independently protected local enrollment;
    this function does not establish those authorities itself.
    """
    from .direct_v2_reservation import DirectV2Reservations
    from .direct_worker_sender_v2 import DisabledDirectWorkerSender
    from cryptography.hazmat.primitives import serialization
    if not isinstance(reservations,DirectV2Reservations):raise ValueError('enrolled worker registry required')
    settings=load_disabled_transport(config_path,node,expected_config_sha256=expected_config_sha256)
    pin=reservations._workers.get(node)
    if (pin is None or (settings.issuer_id,settings.key_id)!=(pin[1],pin[2])
            or settings.worker_public_key.public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw)
            !=pin[3].public_bytes(serialization.Encoding.Raw,serialization.PublicFormat.Raw)):
        raise ValueError('worker registry differs')
    sender=DisabledDirectWorkerSender(origin=settings.origin,connect_ip=settings.connect_ip,ca_file=None,
        bearer=settings._bearer,inventory=inventory,item=item,reservation_binding=reservation_binding,
        expected_worker=pin[0],issuer_id=settings.issuer_id,key_id=settings.key_id,
        worker_public_key=settings.worker_public_key,enrolled_transport=settings)
    return DisabledSenderPreparation(node,sender)
