"""Prepare new private bootstrap/final/Viewer files; never stage or activate them.

Pinned artifacts are input evidence, not fresh physical custody. Runtime TLS,
NAS mount, provider and live worker preflight remain separate requirements.
"""
import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import stat
import uuid

from ops import enroll_reference_worker as install
from ops.prepare_reference_worker_config import pinned
from ops.enrolled_coordinator_transport import canonical, decode, require

PROTOCOL = 'ltds-reference-runtime-preparation-v1'
BINDING_PROTOCOL = 'ltds-reference-node-preparation-binding-v1'


def node_binding(value):
    """Validate captured host/Gate preparation evidence, never installed proof."""
    from processing.node_gate import ENGINE, _binding
    install.closed(value, ('schemaVersion', 'protocol', 'host', 'service', 'gate'))
    require(type(value['schemaVersion']) is int and value['schemaVersion'] == 1 and
            value['protocol'] == BINDING_PROTOCOL, 'node_preparation_binding_profile_required')
    host = value['host']; install.closed(host, ('hostname', 'bootId', 'dockerEngineId'))
    require(type(host['hostname']) is str and re.fullmatch('[A-Za-z0-9][A-Za-z0-9._-]{0,252}', host['hostname']) and
            type(host['dockerEngineId']) is str and ENGINE.fullmatch(host['dockerEngineId']) and
            type(host['bootId']) is str and str(uuid.UUID(host['bootId'])) == host['bootId'],
            'original_worker_host_required')
    service = value['service']; install.closed(service, ('name', 'uid', 'gid', 'home'))
    require(type(service['name']) is str and re.fullmatch('[a-z_][a-z0-9_-]{0,31}', service['name']),
            'fixed_service_account_required')
    install.integer(service['uid'], 1); install.integer(service['gid'], 1)
    home = install.absolute(service['home'])
    gate = value['gate']; install.closed(gate, ('path', 'sha256', 'nodeId', 'authorityId', 'generation', 'coordinatorOrigin'))
    install.sha(gate['sha256']); install.integer(gate['generation'], 1)
    require(_binding(gate['nodeId'], gate['coordinatorOrigin'], gate['authorityId'])['coordinatorOrigin'] ==
            gate['coordinatorOrigin'], 'original_gate_origin_required')
    expected = home/'.local/state/LedgeTop-Processing/node-gates'/hashlib.sha256(host['dockerEngineId'].encode()).hexdigest()/'gate.json'
    require(install.absolute(gate['path']) == expected, 'original_canonical_gate_required')
    return value


def token(artifact):
    value = pinned(artifact).decode('ascii')
    require(32 <= len(value) <= 4096 and all(33 <= ord(c) <= 126 for c in value),
            'literal_private_token_required')
    return value


def signer(value):
    install.closed(value, ('issuerId', 'keyId', 'privateKey'))
    from processing.reference_protocol import _slug
    for field in ('issuerId', 'keyId'):
        _slug(value[field], field)
    from cryptography.hazmat.primitives import serialization as s
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    raw = pinned(value['privateKey'])
    key = s.load_pem_private_key(raw, password=None)
    require(isinstance(key, Ed25519PrivateKey), 'ed25519_private_signer_required')
    return raw.decode('ascii'), key.public_key().public_bytes(s.Encoding.PEM, s.PublicFormat.SubjectPublicKeyInfo).decode('ascii')


def compose(spec):
    install.closed(spec, ('schemaVersion', 'protocol', 'preparationId', 'authorityId', 'listener',
        'coordinatorOrigin', 'nodes', 'environments', 'forbiddenCredentials', 'streamBudget',
        'sourceSpool', 'outputDirectory'))
    require(type(spec['schemaVersion']) is int and spec['schemaVersion'] == 1 and spec['protocol'] == PROTOCOL,
            'runtime_preparation_profile_required')
    for field in ('preparationId', 'authorityId'):
        require(str(uuid.UUID(spec[field])) == spec[field], 'canonical_preparation_identity_required')
    from ops.enrolled_coordinator_deployment import origin
    origin(spec['coordinatorOrigin'])
    listener = spec['listener']
    install.closed(listener, ('host', 'port', 'certificate', 'privateKey', 'clientCA'))
    require(str(ipaddress.ip_address(listener['host'])) == listener['host'], 'literal_listener_required')
    install.integer(listener['port'], 1)
    require(listener['port'] <= 65535, 'listener_port_required')
    for field in ('certificate', 'privateKey', 'clientCA'):
        path = install.absolute(listener[field])
        require(path.parent == Path('/run/processing-tls'), 'separate_tls_mount_required')
    budget = spec['streamBudget']
    install.closed(budget, ('maxConcurrent', 'maxObjectBytes'))
    install.integer(budget['maxConcurrent'], 1); install.integer(budget['maxObjectBytes'], 1)
    require(budget['maxConcurrent'] <= 16, 'bounded_stream_budget_required')
    spool = spec['sourceSpool']
    install.closed(spool, ('root', 'mountPath', 'mountId', 'markerName', 'safetyFloorBytes', 'maxObjectBytes', 'chunkBytes'))
    root = install.absolute(spool['root']); mount = install.absolute(spool['mountPath'])
    require(root == mount or mount in root.parents, 'spool_mount_namespace_required')
    require(type(spool['mountId']) is str and re.fullmatch('[a-f0-9]{32,64}', spool['mountId']) and
            type(spool['markerName']) is str and re.fullmatch(r'\.[a-z0-9][a-z0-9.-]{0,63}', spool['markerName']),
            'spool_identity_required')
    install.integer(spool['safetyFloorBytes'])
    for field in ('maxObjectBytes', 'chunkBytes'): install.integer(spool[field], 1)
    require(spool['chunkBytes'] <= 16*1024**2, 'bounded_spool_chunk_required')
    require(type(spec['forbiddenCredentials']) is list and len(spec['forbiddenCredentials']) <= 64,
            'bounded_legacy_credential_inventory_required')
    forbidden = {token(a) for a in spec['forbiddenCredentials']}
    credentials = []
    workers = {}; recovery = {}; endpoints = []; evidence = []
    nodes = spec['nodes']
    require(type(nodes) is list and 1 <= len(nodes) <= 32, 'configured_node_inventory_required')
    from processing.reference_protocol import _slug
    from processing.prototype import Queue
    from processing.reference_result_proxy import _origin
    from processing.reference_recovery import RecoveryNodeBinding
    for node in nodes:
        require(type(node) is dict and ('manifest' in node) != ('binding' in node), 'one_node_binding_source_required')
        source = 'manifest' if 'manifest' in node else 'binding'
        install.closed(node, (source, 'gate', 'endpoint', 'workerId', 'instanceId', 'resultOrigin',
                              'bearer', 'resultBearer', 'outputBudget'))
        evidence_value = decode(pinned(node[source]))
        manifest = install.validate_manifest(evidence_value) if source == 'manifest' else node_binding(evidence_value)
        gate_raw = pinned(node['gate']); gate = decode(gate_raw); binding = manifest['gate']; ident = binding['nodeId']
        require(ident not in workers and hashlib.sha256(gate_raw).hexdigest() == binding['sha256'],
                'original_gate_artifact_required')
        require(type(gate) is dict and gate.get('schema') == 1 and
                gate.get('engineId') == manifest['host']['dockerEngineId'] and
                all(gate.get(k) == binding[k] for k in ('nodeId', 'authorityId', 'generation', 'coordinatorOrigin')) and
                binding['authorityId'] == spec['authorityId'], 'original_physical_binding_differs')
        Queue.slug(node['workerId']); _slug(node['instanceId'], 'worker instance'); _origin(node['resultOrigin'])
        # Engine endpoint is configuration metadata, not a live discovery result.
        from urllib.parse import urlsplit
        parsed = urlsplit(node['endpoint'])
        require(parsed.scheme in ('http', 'https') and parsed.hostname and not parsed.username and not parsed.password and
                parsed.path in ('', '/') and not parsed.query and not parsed.fragment, 'engine_endpoint_required')
        credentials.extend((token(node['bearer']), token(node['resultBearer'])))
        workers[ident] = dict(bearer=credentials[-2], workerId=node['workerId'], instanceId=node['instanceId'],
                             resultOrigin=node['resultOrigin'], resultBearer=credentials[-1])
        physical = dict(engineId=manifest['host']['dockerEngineId'], authorityId=binding['authorityId'],
                        coordinatorOrigin=binding['coordinatorOrigin'], gateGeneration=binding['generation'],
                        outputBudget=node['outputBudget'])
        RecoveryNodeBinding(ident, **{dict(engineId='engine_id', authorityId='authority_id', coordinatorOrigin='coordinator_origin',
            gateGeneration='gate_generation', outputBudget='output_budget')[k]: v for k,v in physical.items()})
        recovery[ident] = physical; endpoints.append(dict(id=ident, url=node['endpoint']))
        # Preserve the preexisting manifest-mode output bytes/receipt semantics.
        evidence.append({source+'Sha256': node[source]['sha256'], 'gateSha256': node['gate']['sha256']})
    require(len({p['engineId'] for p in recovery.values()}) == len(recovery), 'distinct_physical_daemons_required')
    envs = spec['environments']
    require(type(envs) is dict and envs and set(envs) <= {'production', 'staging'}, 'explicit_environment_inventory_required')
    viewers = {}; packages = {}; source_keys = []; copy_keys = {}; public_keys = set()
    for env, record in envs.items():
        install.closed(record, ('providerId', 'principalId', 'bearer', 'source', 'snapshot', 'copy', 'viewerCaPath'))
        require(str(uuid.UUID(record['providerId'])) == record['providerId'], 'viewer_provider_identity_required')
        _slug(record['principalId'], 'source principal'); install.absolute(record['viewerCaPath'])
        viewers[env] = token(record['bearer']); credentials.append(viewers[env])
        package = dict(PROCESSING_PLATFORM_ENABLED='true', VIEWER_DEPLOYMENT_ENVIRONMENT=env,
            VIEWER_SOURCE_RELEASE_ENABLED='false', VIEWER_REFERENCE_SOURCE_INTAKE_ENABLED='true', VIEWER_REFERENCE_SOURCE_INTAKE_ENVIRONMENT=env,
            VIEWER_REFERENCE_COORDINATOR_ORIGIN=spec['coordinatorOrigin'], VIEWER_REFERENCE_COORDINATOR_BEARER_TOKEN=viewers[env],
            VIEWER_REFERENCE_COORDINATOR_PROVIDER_ID=record['providerId'], VIEWER_REFERENCE_COORDINATOR_AUTHORITY_ID=spec['authorityId'],
            VIEWER_REFERENCE_SOURCE_PRINCIPAL_ID=record['principalId'], NODE_EXTRA_CA_CERTS=record['viewerCaPath'])
        for role, prefix in (('source', 'VIEWER_REFERENCE_SOURCE_ATTESTATION'), ('snapshot', 'VIEWER_SOURCE_SNAPSHOT'),
                             ('copy', 'VIEWER_REFERENCE_COPY')):
            private, public = signer(record[role])
            require(public not in public_keys, 'distinct_environment_signing_roles_required'); public_keys.add(public)
            for suffix, val in (('ISSUER_ID', record[role]['issuerId']), ('KEY_ID', record[role]['keyId']), ('PRIVATE_KEY_PEM', private)):
                package[prefix+'_'+suffix] = val
            if role != 'source': package[prefix+'_PUBLIC_KEY_PEM'] = public
            if role == 'source': source_keys.append(dict(environment=env, issuerId=record[role]['issuerId'],
                keyId=record[role]['keyId'], principalId=record['principalId'], publicKeyPem=public))
            if role == 'copy':
                key_id = record[role]['keyId']; require(key_id not in copy_keys, 'distinct_copy_key_ids_required')
                copy_keys[key_id] = dict(issuerId=record[role]['issuerId'], publicKeyPem=public)
        packages['viewer-'+env+'.json'] = canonical(package)
    require(len(set(credentials)) == len(credentials) and not set(credentials) & forbidden,
            'disjoint_private_roles_required')
    from processing.viewer_source_attestation import validate_trusted_keys
    from processing.viewer_acceptance_receipt import validate_trusted_viewer_receipt_keys
    validate_trusted_keys(source_keys); validate_trusted_viewer_receipt_keys(copy_keys)
    bootstrap = dict(schemaVersion=2, listener=listener, viewerBearers=viewers, workers=workers,
                     streamBudget=budget, recoveryBindings=recovery)
    final = dict(bootstrap, schemaVersion=6, viewerReceiptKeys=copy_keys, viewerAcceptance={},
                 sourceSpool=spool, sourceAttestationKeys=source_keys)
    trust = dict(schemaVersion=1, authorityId=spec['authorityId'], nodeEndpoints=endpoints,
                 recoveryBindings=recovery, sourceAttestationKeys=source_keys, viewerReceiptKeys=copy_keys,
                 inputBindings=evidence)
    return dict(packages, **{'bootstrap.json': canonical(bootstrap), 'final.json': canonical(final),
                            'public-trust.json': canonical(trust)})


def prepare(spec):
    require(os.name == 'posix', 'posix_private_preparation_required')
    files = compose(spec)  # ALL input and crypto validation before mkdir/write.
    out = install.absolute(spec['outputDirectory']); parent = install.directory_identity(out.parent)
    require(parent[3] == os.geteuid() and stat.S_IMODE(parent[2]) == 0o700 and
            not out.exists() and not out.is_symlink(), 'new_private_output_required')
    receipt = dict(schemaVersion=1, protocol=PROTOCOL, preparationId=spec['preparationId'], preparationOnly=True,
        runtimePreflightProven=False, files={name: dict(sha256=hashlib.sha256(raw).hexdigest(), bytes=len(raw))
                                           for name,raw in files.items()})
    files['preparation-receipt.json'] = canonical(receipt)
    fd = os.open(out.parent, os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC); target = None
    try:
        require(install.directory_identity(out.parent) == parent and
                [os.fstat(fd).st_dev,os.fstat(fd).st_ino,os.fstat(fd).st_mode,os.fstat(fd).st_uid,os.fstat(fd).st_gid] == parent,
                'preparation_parent_changed')
        os.mkdir(out.name, 0o700, dir_fd=fd)
        target = os.open(out.name, os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC, dir_fd=fd)
        directory = os.fstat(target)
        for name, raw in files.items():
            f = os.open(name, os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC, 0o600, dir_fd=target)
            try:
                with os.fdopen(f,'wb',closefd=False) as stream: stream.write(raw); stream.flush()
                os.fsync(f); os.lseek(f,0,os.SEEK_SET)
                require(os.read(f,len(raw)+1) == raw and os.fstat(f).st_nlink == 1 and
                        stat.S_IMODE(os.fstat(f).st_mode) == 0o600, 'prepared_file_readback_failed')
            finally: os.close(f)
        os.fsync(target); os.fsync(fd)
        require(install.directory_identity(out.parent) == parent and
                (directory.st_dev,directory.st_ino) == (out.lstat().st_dev,out.lstat().st_ino), 'preparation_parent_changed')
        for name, raw in files.items():
            require(install.read_file(out/name, owner=os.geteuid(), maximum=len(raw)) == raw,
                    'prepared_file_readback_failed')
        require(install.directory_identity(out.parent) == parent and
                (directory.st_dev,directory.st_ino) == (out.lstat().st_dev,out.lstat().st_ino), 'preparation_parent_changed')
        return receipt
    finally:
        if target is not None: os.close(target)
        os.close(fd)


def main():
    parser = argparse.ArgumentParser(description=__doc__); parser.add_argument('--spec', required=True)
    args = parser.parse_args()
    try:
        raw = install.read_file(install.absolute(args.spec), owner=os.geteuid(), maximum=1024**2)
        print(json.dumps(prepare(decode(raw))))
    except Exception:
        raise SystemExit('Runtime preparation held; inspect retained private artifacts') from None


if __name__ == '__main__': main()
