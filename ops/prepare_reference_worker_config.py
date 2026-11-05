"""Prepare NEW owner-private worker inputs; no enrollment or activation effects.

All authority comes from explicitly hash-pinned private inputs and original Gate
metadata. The public return contains hashes only. Never call the worker factory,
open a Gate, create a lease, issue TLS keys, or mutate an existing artifact.
"""
import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import secrets
import stat
import re
from datetime import datetime, timedelta, timezone

from ops import enroll_reference_worker as install
from ops.enrolled_coordinator_transport import canonical, decode, require

PROTOCOL='ltds-first-worker-private-preparation-v1'
MAX_PRIVATE=65536


def descriptor(value):
    install.closed(value,('path','sha256','bytes','ownerUid'))
    install.absolute(value['path']);install.sha(value['sha256'])
    install.integer(value['bytes'],1);install.integer(value['ownerUid'])
    require(value['bytes']<=MAX_PRIVATE,'bounded_private_input_required')
    return value


def pinned(value):
    value=descriptor(value)
    raw=install.read_file(Path(value['path']),owner=value['ownerUid'],maximum=value['bytes'])
    require(len(raw)==value['bytes'] and hashlib.sha256(raw).hexdigest()==value['sha256'],
            'private_preparation_input_changed')
    return raw


def archive_pin(value):
    # Same bounded streaming FD/hash/identity policy as the installer, without
    # extraction, constructor work, or privileged installation.
    path=Path(value['path']);install.original(path);before=path.lstat()
    require(stat.S_ISREG(before.st_mode) and before.st_nlink==1 and before.st_uid==value['ownerUid']
            and stat.S_IMODE(before.st_mode)==0o600 and before.st_size==value['bytes'],
            'private_preparation_archive_required')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK|os.O_CLOEXEC)
    try:
        require(install.identity(before)==install.identity(os.fstat(fd)),'preparation_archive_changed')
        h=hashlib.sha256();total=0
        while True:
            block=os.read(fd,65536)
            if not block:break
            total+=len(block);require(total<=value['bytes'],'bounded_preparation_archive_required');h.update(block)
        require(total==value['bytes'] and h.hexdigest()==value['sha256'] and
                install.identity(before)==install.identity(os.fstat(fd))==install.identity(path.lstat()),
                'preparation_archive_changed')
    finally:os.close(fd)


def validate_spec(value):
    install.closed(value,('schemaVersion','protocol','manifest','runtime','nativeCredential','tls',
                          'listener','coordinator','policy','outputDirectory'))
    require(type(value['schemaVersion']) is int and value['schemaVersion']==1 and value['protocol']==PROTOCOL,
            'private_preparation_profile_required')
    m=install.validate_manifest(value['manifest'])
    require(os.name=='posix' and m['service']['uid']==os.geteuid(), 'intended_service_preparer_required')
    descriptor(value['runtime'])
    native=value['nativeCredential'];install.closed(native,('artifact','key'))
    descriptor(native['artifact'])
    require(type(native['key']) is str and re.fullmatch('[A-Z_][A-Z0-9_]{0,127}',native['key']),
            'literal_native_token_key_required')
    install.closed(value['tls'],('ca','certificate','privateKey'))
    for item in value['tls'].values():
        descriptor(item);require(item['ownerUid']==m['service']['uid'],'service_tls_owner_required')
    install.closed(value['coordinator'],('origin','certificate','listenerPort'))
    descriptor(value['coordinator']['certificate'])
    from ops.enrolled_coordinator_deployment import origin
    origin(value['coordinator']['origin']);install.integer(value['coordinator']['listenerPort'],1)
    require(value['coordinator']['listenerPort']<=65535,'coordinator_listener_required')
    install.closed(value['listener'],('host','port'))
    host=value['listener']['host'];require(type(host) is str,'literal_worker_ip_required')
    ip=ipaddress.ip_address(host)
    require(str(ip)==host and not ip.is_unspecified and not ip.is_multicast,'literal_worker_ip_required')
    install.integer(value['listener']['port'],1024)
    require(value['listener']['port']<=65535,'bounded_worker_listener_required')
    policy=value['policy']
    install.closed(policy,('safetyFloor','transferTtl','cleanupGrace','pollSeconds','allowedEnvironments',
                          'cleanupEnabled','engineCleanupEnabled','sourceRangeV2Enabled','minimumTlsValiditySeconds'))
    for key in ('safetyFloor','cleanupGrace','minimumTlsValiditySeconds'):install.integer(policy[key])
    install.integer(policy['transferTtl'],1)
    require(type(policy['pollSeconds']) in (int,float) and 0.1<=policy['pollSeconds']<=60 and
            policy['minimumTlsValiditySeconds']<=365*86400,'bounded_worker_policy_required')
    require(type(policy['allowedEnvironments']) is list and policy['allowedEnvironments'] and
            len(set(policy['allowedEnvironments']))==len(policy['allowedEnvironments']) and
            set(policy['allowedEnvironments'])<={'production','staging'},'worker_environments_required')
    for key in ('cleanupEnabled','engineCleanupEnabled','sourceRangeV2Enabled'):
        require(type(policy[key]) is bool,'explicit_worker_policy_required')
    out=install.absolute(value['outputDirectory']);install.original(out.parent)
    parent=out.parent.stat()
    require(parent.st_uid==os.geteuid() and stat.S_IMODE(parent.st_mode)==0o700 and
            not out.exists() and not out.is_symlink(),'new_private_preparation_directory_required')
    require(m['config']['path']==str(out/'worker-config.json') and m['config']['ownerUid']==os.geteuid(),
            'new_private_config_binding_required')
    # Placeholder descriptor is replaced only after exact private bytes exist.
    require(m['config']['sha256']=='0'*64 and m['config']['bytes']==1,
            'unprepared_config_placeholder_required')
    return m


def tls_material(spec,now):
    from cryptography import x509
    from cryptography.hazmat.primitives.serialization import load_pem_private_key, Encoding, PublicFormat
    from cryptography.x509.oid import ExtendedKeyUsageOID
    raw={key:pinned(item) for key,item in spec['tls'].items()}
    ca=x509.load_pem_x509_certificate(raw['ca']);cert=x509.load_pem_x509_certificate(raw['certificate'])
    key=load_pem_private_key(raw['privateKey'],password=None)
    cert.verify_directly_issued_by(ca)
    require(ca.extensions.get_extension_for_class(x509.BasicConstraints).value.ca and
            not cert.extensions.get_extension_for_class(x509.BasicConstraints).value.ca,
            'worker_tls_chain_required')
    require(ExtendedKeyUsageOID.SERVER_AUTH in cert.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value,
            'worker_server_tls_required')
    sans=set(cert.extensions.get_extension_for_class(x509.SubjectAlternativeName).value.get_values_for_type(x509.IPAddress))
    require({ipaddress.ip_address(spec['listener']['host']),ipaddress.ip_address('127.0.0.1')}<=sans,
            'worker_tls_ip_binding_required')
    public=lambda k:k.public_bytes(Encoding.DER,PublicFormat.SubjectPublicKeyInfo)
    require(public(key.public_key())==public(cert.public_key()),'worker_tls_private_key_differs')
    deadline=now+timedelta(seconds=spec['policy']['minimumTlsValiditySeconds'])
    require(all(c.not_valid_before_utc<=now and c.not_valid_after_utc>deadline for c in (ca,cert)),
            'worker_tls_validity_required')
    from urllib.parse import urlsplit
    remote=x509.load_pem_x509_certificate(pinned(spec['coordinator']['certificate']))
    remote.verify_directly_issued_by(ca)
    host=urlsplit(spec['coordinator']['origin']).hostname
    require(ipaddress.ip_address(host) in remote.extensions.get_extension_for_class(x509.SubjectAlternativeName)
            .value.get_values_for_type(x509.IPAddress) and
            ExtendedKeyUsageOID.SERVER_AUTH in remote.extensions.get_extension_for_class(x509.ExtendedKeyUsage).value and
            not remote.extensions.get_extension_for_class(x509.BasicConstraints).value.ca and
            remote.not_valid_before_utc<=now and remote.not_valid_after_utc>deadline,
            'coordinator_tls_origin_binding_required')
    return raw


def compose(spec,now):
    m=validate_spec(spec)
    require(now.tzinfo is not None and now.utcoffset()==timedelta(0),'aware_preparation_clock_required')
    runtime=decode(pinned(spec['runtime']))
    require(type(runtime) is dict and type(runtime.get('schemaVersion')) is int and
            runtime['schemaVersion'] in (2,3,4,5,6),'recovery_bound_runtime_required')
    workers=runtime.get('workers');bindings=runtime.get('recoveryBindings');node=m['gate']['nodeId']
    require(type(workers) is dict and type(bindings) is dict and node in workers and set(workers)==set(bindings),
            'original_runtime_worker_required')
    from processing.reference_protocol import _slug
    from processing.prototype import Queue
    forbidden=set()
    viewers=runtime.get('viewerBearers')
    require(type(viewers) is dict,'runtime_viewer_roles_required')
    forbidden.update(viewers.values())
    for worker in workers.values():
        install.closed(worker,('bearer','workerId','instanceId','resultOrigin','resultBearer'))
        Queue.slug(worker['workerId']);_slug(worker['instanceId'],'worker instance')
        for field in ('bearer','resultBearer'):
            token=worker[field]
            require(type(token) is str and 32<=len(token)<=4096 and all(0x21<=ord(c)<=0x7e for c in token),
                    'runtime_worker_roles_required')
            require(token not in forbidden,'distinct_runtime_worker_roles_required');forbidden.add(token)
    worker=workers[node];binding=bindings[node]
    install.closed(binding,('engineId','authorityId','coordinatorOrigin','gateGeneration','outputBudget'))
    require((binding['engineId'],binding['authorityId'],binding['gateGeneration'],binding['coordinatorOrigin'])==
            (m['host']['dockerEngineId'],m['gate']['authorityId'],m['gate']['generation'],m['gate']['coordinatorOrigin']),
            'original_runtime_gate_binding_differs')
    install.integer(binding['outputBudget'],1)
    host=spec['listener']['host'];literal='['+host+']' if ':' in host else host
    origin=worker['resultOrigin'];expected='https://'+literal+':'+str(spec['listener']['port'])
    require(origin==expected,'original_worker_listener_differs')
    # The coordinator TLS origin is pinned by its existing listener certificate
    # and explicit runtime metadata, never inferred from old Gate origin.
    listener=runtime.get('listener');install.closed(listener,('host','port','certificate','privateKey','clientCA'))
    # Public origin and container port mapping are reviewed spec inputs, matched
    # to pinned runtime/leaf bytes. This does not inspect a live Docker mapping.
    require(listener['port']==spec['coordinator']['listenerPort'] and type(listener['port']) is int,
            'reviewed_reference_listener_required')
    coordinator=spec['coordinator']['origin']
    raw_native=pinned(spec['nativeCredential']['artifact']).decode('utf-8')
    native={}
    for line in raw_native.splitlines():
        if not line or line.startswith('#'):continue
        require('=' in line,'literal_native_environment_required')
        name,value=line.split('=',1);require(name not in native,'duplicate_native_key');native[name]=value
    require(spec['nativeCredential']['key'] in native,'native_token_key_absent')
    token=native[spec['nativeCredential']['key']]
    require(type(token) is str and len(token)<=4096 and all(0x21<=ord(c)<=0x7e for c in token),
            'literal_native_token_required')
    require(token not in forbidden,'native_runtime_roles_overlap');forbidden.add(token)
    tls_material(spec,now)
    gate_raw=install.read_file(Path(m['gate']['path']),owner=m['service']['uid'],maximum=16384)
    require(hashlib.sha256(gate_raw).hexdigest()==m['gate']['sha256'],'original_gate_file_changed')
    gate=decode(gate_raw)
    for field,value in dict(schema=1,engineId=m['host']['dockerEngineId'],nodeId=node,
            authorityId=binding['authorityId'],generation=binding['gateGeneration'],
            coordinatorOrigin=binding['coordinatorOrigin'],reservation=None).items():
        require(gate.get(field)==value,'original_gate_state_differs')
    for artifact in ('source','dependencies'):archive_pin(m[artifact])
    readiness=secrets.token_urlsafe(48)
    require(readiness not in forbidden,'new_readiness_role_collision')
    root=Path(m['installationRoot']);home=Path(m['service']['home']);op=m['operationId']
    config=dict(schemaVersion=1,stateRoot=str(home/'.local/share/ledgetop-processing-reference/worker-state'/op),
        scratchRoot=str(home/'.local/share/ledgetop-processing-reference/worker-scratch'/op),
        nodeId=node,workerId=worker['workerId'],workerInstanceId=worker['instanceId'],
        authorityId=binding['authorityId'],engineId=binding['engineId'],gateGeneration=binding['gateGeneration'],
        gateCoordinatorOrigin=binding['coordinatorOrigin'],coordinatorOrigin=coordinator,brokerOrigin=coordinator,
        engineOrigin='http://127.0.0.1:'+str(m['engine']['loopbackPort']),engineToken=token,
        engineVersion=m['engine']['version'],workerBearer=worker['bearer'],resultReadBearer=worker['resultBearer'],
        caFile=spec['tls']['ca']['path'],serverCertFile=spec['tls']['certificate']['path'],
        serverKeyFile=spec['tls']['privateKey']['path'],listenHost=spec['listener']['host'],listenPort=spec['listener']['port'],
        outputBudget=binding['outputBudget'],shareResults=True,allowedOdmOptions=[],
        executionRouting=dict(schemaVersion=1,mode='per-job-v1',treedar=None),
        maintenanceReadiness=dict(schemaVersion=1,bearer=readiness,sourceRoot=str(root/'source'),
                                  sourceSha256=m['source']['footprintSha256'],activation=None))
    for key,value in spec['policy'].items():
        if key!='minimumTlsValiditySeconds':config[key]=value
    config['viewerReceiptKeys']=runtime.get('viewerReceiptKeys',{})
    for path in (config['stateRoot'],config['scratchRoot']):
        require(not Path(path).exists() and not Path(path).is_symlink(),'new_worker_state_required')
    raw=canonical(config);require(len(raw)<=MAX_PRIVATE,'bounded_prepared_config_required')
    m['config'].update(sha256=hashlib.sha256(raw).hexdigest(),bytes=len(raw))
    m['unitSha256']=hashlib.sha256(install.render_unit(m).encode()).hexdigest();install.validate_manifest(m)
    return m,raw


def prepare(spec,*,now=None):
    """Explicit new private artifacts only; partial outputs retained and refused."""
    m,raw=compose(spec,now or datetime.now(timezone.utc));out=Path(spec['outputDirectory'])
    parent=install.directory_identity(out.parent)
    fd=os.open(out.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
    target=None
    try:
        opened=os.fstat(fd)
        require([opened.st_dev,opened.st_ino,opened.st_mode,opened.st_uid,opened.st_gid]==parent==
                install.directory_identity(out.parent),'preparation_parent_changed')
        os.mkdir(out.name,0o700,dir_fd=fd)
        target=os.open(out.name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=fd)
        created=install.identity(os.fstat(target))
        for name,data in (('worker-config.json',raw),('enrollment-manifest.json',canonical(m))):
            f=os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=target)
            try:
                with os.fdopen(f,'wb',closefd=False) as stream:stream.write(data);stream.flush();os.fchmod(f,0o600);os.fsync(f)
            finally:os.close(f)
        # Validate real daemon schema/private TLS access without factory startup.
        from ops.reference_worker_daemon import load_config
        load_config(str(out/'worker-config.json'))
        os.fsync(target);os.fsync(fd)
        require(install.directory_identity(out.parent)==parent and
                created[:6]==install.identity(out.lstat())[:6],'preparation_parent_changed')
        return dict(schemaVersion=1,protocol=PROTOCOL,workerConfigSha256=m['config']['sha256'],
                    manifestSha256=hashlib.sha256(canonical(m)).hexdigest(),unitSha256=m['unitSha256'],
                    preparationOnly=True)
    finally:
        if target is not None:os.close(target)
        os.close(fd)


def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--spec',required=True);args=p.parse_args()
    try:
        raw=install.read_file(install.absolute(args.spec),owner=os.geteuid(),maximum=MAX_PRIVATE)
        print(json.dumps(prepare(decode(raw))))
    except Exception:raise SystemExit('Private worker preparation held; retained artifacts require inspection') from None


if __name__=='__main__':main()
