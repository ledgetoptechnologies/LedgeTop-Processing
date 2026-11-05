"""Opt-in actual-Docker first-cutover qualification, never host enrollment.

The fixture profile below changes only process-local enrolled identities. The
production journal, driver, startup policy and custody validators are retained;
there is no alternate allowlist learned from candidate SQLite. Nothing executes
on import, and no image is pulled or built by this module.
"""
from contextlib import contextmanager
from pathlib import Path
import base64
import hashlib
import json
import secrets
import threading
import os
import socket
import sys
import re
import uuid

from . import enrolled_coordinator_cutover as journal
from . import enrolled_coordinator_deployment as deployment
from .enrolled_coordinator_transport import require
from .qualify_enrolled_docker_transport import TASK_LABEL


def private_write(path, raw):
    path=Path(path)
    with path.open('xb') as output:
        path.chmod(0o600);output.write(raw);output.flush();os.fsync(output.fileno())
    return dict(path=str(path),sha256=hashlib.sha256(raw).hexdigest())


def environment_bytes(value):
    require(all(type(k) is str and re.fullmatch('[A-Z_][A-Z0-9_]*',k) and
                type(v) is str and '\n' not in v and '\r' not in v for k,v in value.items()),
            'closed_fixture_environment_required')
    return ''.join(k+'='+v+'\n' for k,v in sorted(value.items())).encode()


def fixture_bundle(root, *, operation_id, authority_id, old, candidate_image,
                   candidate_source, state_volume, config_source, lease_source,
                   spool_volume, physical, material, source_root, cli_sha, daemon_id):
    """Bind files/config/source independently, before candidate constructors.

    All qualification receipts explicitly describe this synthetic fixture; they
    are not NAS, pilot-journal producer or installed-worker acceptance evidence.
    """
    from . import coordinator_startup_policy as policy
    from . import coordinator_predecessor_census as census
    from . import enrolled_coordinator_transport as t
    from .enrolled_coordinator_custody_ops import MODULE_NAMES
    root=Path(root);source_root=Path(source_root)
    raw_schema=(source_root/'contracts/coordinator-693cb7e-schema.json').read_bytes()
    key=secrets.token_bytes(32)
    files=dict(predecessorSchema=private_write(root/'schema.json',raw_schema),
        censusKey=private_write(root/'census-key',key),
        reviewedCounts=private_write(root/'counts.json',t.canonical(dict(jobs=22,nodes=2,viewer_tasks=7,viewer_reservations=0,
            viewer_files=155,viewer_option_evidence=7,owned_maintenance_schedules=16))))
    endpoints=[dict(id=node,url='http://127.0.0.1:'+str(19001+index))
               for index,node in enumerate(('cluster','main'))]
    hashes={name:hashlib.sha256((source_root/name).read_bytes()).hexdigest() for name in policy.SOURCES}
    configs={role:t.canonical(material[role]) for role in ('bootstrap','final')}
    policy_bytes=policy.build_startup_policy(source_root,expected_source_hashes=hashes,
        candidate_image_id=candidate_image,candidate_source_commit=candidate_source,
        bootstrap_config_bytes=configs['bootstrap'],final_config_bytes=configs['final'],
        node_endpoints=endpoints,authority_id=authority_id,reviewed_schema_bytes=raw_schema)
    files['policy']=private_write(root/'startup-policy.json',policy_bytes)
    recipes={};directory='first-cutover-'+operation_id
    native=dict(material['credentials'],PROCESSING_BIND_HOST='0.0.0.0',PROCESSING_PORT='8080',
        PROCESSING_PROTOTYPE_NODE_IDS='cluster,main',PROCESSING_TREEDAR_NODE_IDS='main',
        PROCESSING_PROTOTYPE_STATE_DIR='/prototype-state',PROCESSING_AUTHORITY_LEASE_DIR='/run/qualification-original-lease',
        PROCESSING_VIEWER_ENABLED='0',PYTHONNOUSERSITE='1',PYTHONDONTWRITEBYTECODE='1',
        LOCAL_NODE_URL=endpoints[0]['url'],MAIN_NODE_URL=endpoints[1]['url'])
    for role in ('bootstrap','final','holder'):
        env={} if role=='holder' else dict(native,
            PROCESSING_LEGACY_VIEWER_READ_ONLY='1' if role=='final' else '0',
            PROCESSING_REFERENCE_CONFIG='/run/processing-reference/'+directory+'/'+role+'.json')
        files[role+'Environment']=private_write(root/(role+'.env'),environment_bytes(env))
        mounts=[dict(type='volume',source=state_volume,target='/prototype-state',readOnly=role=='holder'),
            dict(type='bind',source=lease_source,target='/run/qualification-original-lease',readOnly=False),
            dict(type='bind',source=config_source,target='/run/processing-reference',readOnly=True)]
        if role=='final':mounts.append(dict(type='volume',source=spool_volume,target='/qualification-spool',readOnly=False))
        recipes[role]=dict(schemaVersion=1,name=journal.NAME if role=='final' else
            'ltds-phase-'+role+'-'+uuid.UUID(operation_id).hex[:16],imageId=candidate_image,
            user='10001:10001',workingDir='/app',environmentFile=files[role+'Environment']['path'],
            environmentSha256=files[role+'Environment']['sha256'],mounts=mounts,ports=[],
            network='bridge' if role=='final' else 'none',tmpfs={'/tmp':'rw,nosuid,nodev,noexec,size=64m,mode=1777'},
            labels={TASK_LABEL:operation_id},command=['python','-m','processing.app'],entrypoint=None,
            readOnly=True,init=role!='holder',pidsLimit=96,memoryBytes=384*1024**2,nanoCpus=250000000,
            stopSignal='SIGTERM',stopTimeout=-1,restartPolicy={'Name':'no','MaximumRetryCount':0})
        if role!='holder':files[role+'Config']=dict(path=config_source+'/'+directory+'/'+role+'.json',
            containerPath='/run/processing-reference/'+directory+'/'+role+'.json',sha256=hashlib.sha256(configs[role]).hexdigest())
    gates={};qualifications={}
    for name in ('nasQualificationSha256','pilotClosureSha256','legacyDeliverySha256','workerQualificationSha256'):
        qualifications[name]=private_write(root/(name+'.json'),t.canonical(dict(schemaVersion=1,
            fixtureOnly=True,installedQualified=False,operationId=operation_id,kind=name)))
        gates[name]=qualifications[name]['sha256']
    target=dict(imageId=candidate_image,sourceCommit=candidate_source,
        bootstrapRecipeSha256=t.digest(recipes['bootstrap']),finalRecipeSha256=t.digest(recipes['final']),
        bootstrapConfigSha256=files['bootstrapConfig']['sha256'],finalConfigSha256=files['finalConfig']['sha256'],
        declaredTransformSha256=files['policy']['sha256'])
    plan=dict(schemaVersion=1,mode=journal.MODE,operationId=operation_id,authorityId=authority_id,
        predecessor=dict(containerId=old['Id'],imageId=old['Image'],sourceCommit=journal.PREDECESSOR_SOURCE,
                         restartPolicy=old['HostConfig']['RestartPolicy']),candidate=target,
        bindings=dict(stateSha256=t.digest(physical['state']),authorityLeaseSha256=t.digest(physical['authorityLease']),
            predecessorRecipeSha256=t.digest(t.immutable_container_recipe(old)),
            predecessorEnvironmentSha256=t.digest(t.container_environment(old)),censusKeySha256=files['censusKey']['sha256'],
            reviewedCountsSha256=files['reviewedCounts']['sha256'],deploymentSha256='0'*64),gates=gates)
    # The SSH manifests are type-checked but never used by this fixture. Actual
    # factory/TLS readiness above supplies the explicit local transport seam.
    workers=[]
    for node in ('cluster','main'):
        workers.append(dict(nodeId=node,sshHost='192.0.2.1',sshUid=1000,
            settings=dict(schemaVersion=1,unitName='fixture-'+node+'.service',unitFile='/private/fixture.service',
                unitSha256='1'*64,pythonExecutable='/usr/bin/python3',configPath='/private/fixture.json',
                sourceRoot=str(source_root),sourceSha256='2'*64,readinessOrigin='https://127.0.0.1:19999',
                readinessCAFile='/private/fixture-ca.pem'),
            expected=dict(nodeId=node,workerId='worker-'+node,workerInstanceId='instance-'+node,
                authorityId=authority_id,engineId='fixture-engine-'+node,gateGeneration=1,engineVersion='3.8.5',
                configSha256='3'*64,sourceSha256='2'*64,activation=None)))
    value=dict(schemaVersion=1,profile=deployment.PROFILE,operationId=operation_id,authorityId=authority_id,
        candidateSourceCommit=candidate_source,reviewedSourceRoot=str(source_root),
        host=dict(dockerEngineId=daemon_id,bootId=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            dockerBinary='/usr/bin/docker',dockerBinarySha256=cli_sha,dockerSocket='unix:///run/docker.sock'),
        predecessor=dict(containerId=old['Id'],imageId=old['Image'],sourceCommit=journal.PREDECESSOR_SOURCE,
            name=journal.NAME,stateVolume=state_volume,leaseDirectory='/run/qualification-original-lease'),
        recipes=recipes,holderSettings=dict(schemaVersion=1,operationId=operation_id,authorityId=authority_id,
            stateRoot='/prototype-state',leaseDirectory='/run/qualification-original-lease'),
        artifacts=files,sourceHashes=hashes,custodySources={name:hashlib.sha256((source_root/'ops'/(name+'.py')).read_bytes()).hexdigest()
            for name in MODULE_NAMES},budgets=dict(maxArtifactBytes=16*1024**2,maxBackupBytes=64*1024**2,
            maxMetadataBytes=4*1024**2,maxSeconds=120,safetyFloorBytes=64*1024**2),nodeEndpoints=endpoints,
        workers=workers,qualifications=qualifications)
    plan['bindings']['deploymentSha256']=t.digest(value)
    deployment.validate_bundle(value,plan)
    return plan,value


def qualify(candidate_image, candidate_source, daemon_id, artifact_root, operation_id, cli_sha, *, rollback=False):
    """Run the production phase journal on NEW labeled resources only."""
    import importlib.util
    from . import enrolled_coordinator_transport as t
    from .enrolled_coordinator_driver import ConcreteCutoverDriver
    from .enrolled_coordinator_boundary import BoundaryProbe
    from .qualify_enrolled_docker_transport import QualificationDocker,cleanup_owned,initialize_fixture
    from .qualify_enrolled_holder_transport import owned_volume,verify_desktop_cli
    docker=QualificationDocker();volumes=[];events=[];result=None
    verify_desktop_cli(docker.binary,cli_sha)
    require(docker.command('info','--format','{{.ID}}',output_limit=256).decode().strip()==daemon_id,
            'explicit_fixture_daemon_required')
    for image,source in ((candidate_image,candidate_source),(journal.PREDECESSOR_IMAGE,journal.PREDECESSOR_SOURCE)):
        require(docker.image(image)['Config']['Labels']['org.opencontainers.image.revision']==source,
                'exact_packaged_fixture_runtime_required')
    require(str(uuid.UUID(operation_id))==operation_id,'canonical_fixture_operation_required')
    suffix=uuid.UUID(operation_id).hex[:16];name='ltds-phase-original-'+suffix
    authority=str(uuid.uuid5(uuid.UUID(operation_id),'synthetic-original-authority'))
    root=Path(artifact_root)/operation_id;root.mkdir(mode=0o700)
    operation=root/'operation';operation.mkdir(mode=0o700)
    source_root=Path(__file__).resolve().parents[1]
    names=dict(state=name+'-state',lease='ltds-phase-lease-'+suffix,
               config='ltds-phase-config-'+suffix,spool='ltds-phase-spool-'+suffix)
    material=configuration_material(operation_id,authority)
    try:
        owned={}
        for role,value in names.items():
            volumes.append(value);owned[role]=owned_volume(docker,value,operation_id)
        for role,source in (('lease','import pathlib,os,json\np=pathlib.Path("/prototype-state")\n'
             'assert p.stat().st_uid==p.stat().st_gid==10001 and next(p.iterdir(),None) is None\n'
             'p.chmod(0o711)\nprint(json.dumps({"uid":os.getuid(),"gid":os.getgid()}))\n'),
             ('config',material['configSeed']),('spool',material['spoolSeed'])):
            initialize_fixture(docker,candidate_image,operation_id,'ltds-phase-seed-'+role+'-'+suffix,names[role],source)
        external=owned['lease']['Mountpoint'];config_source=owned['config']['Mountpoint']
        fixture_path=source_root/'tests/fixtures/enrolled_coordinator_daemon_fixture.py'
        spec=importlib.util.spec_from_file_location('phase_original_fixture',fixture_path)
        fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
        seed=fixture.serializable_source(lease_directory='/run/qualification-original-lease',authority_id=authority,
            schema_bytes=(source_root/'contracts/coordinator-693cb7e-schema.json').read_bytes())
        seed_id=docker.command('container','create','--name','ltds-phase-state-seed-'+suffix,
            '--label',TASK_LABEL+'='+operation_id,'--network','none','--read-only','--user','10001:10001',
            '--cpus','0.125','--memory','128m','--memory-swap','128m','--pids-limit','32',
            '--mount','type=volume,src='+names['state']+',dst=/prototype-state',
            '--mount','type=bind,src='+external+',dst=/run/qualification-original-lease',
            '--entrypoint','python',candidate_image,'-I','-c',seed.decode(),mutation=True,output_limit=4096).decode().strip()
        docker.start(seed_id)
        require(docker.command('container','wait',seed_id,timeout=20,output_limit=256).decode().strip()=='0',
                'actual_693_fixture_seed_exit_required')
        seed_result=t.decode(docker.command('container','logs',seed_id,output_limit=16384))
        require(seed_result['expectedFileCount']==202,'exact_202_original_fixture_required')
        old_environment=dict(material['credentials'],PROCESSING_BIND_HOST='0.0.0.0',PROCESSING_PORT='8080',
            PROCESSING_PROTOTYPE_STATE_DIR='/prototype-state',PROCESSING_AUTHORITY_LEASE_DIR='/run/qualification-original-lease',
            PROCESSING_PROTOTYPE_NODE_IDS='cluster,main',PROCESSING_VIEWER_ENABLED='0',PYTHONDONTWRITEBYTECODE='1',
            LOCAL_NODE_URL='http://127.0.0.1:19001',MAIN_NODE_URL='http://127.0.0.1:19002')
        env=private_write(root/'predecessor.env',environment_bytes(old_environment))
        old_id=docker.command('container','create','--name',name,'--label',TASK_LABEL+'='+operation_id,
            '--network','none','--read-only','--user','10001:10001','--workdir','/app','--init',
            '--restart','no','--stop-signal','SIGTERM','--stop-timeout','-1',
            '--cpus','0.25','--memory','384m','--memory-swap','384m','--pids-limit','96',
            '--cap-drop','ALL','--security-opt','no-new-privileges','--tmpfs','/tmp:rw,nosuid,nodev,noexec,size=64m,mode=1777',
            '--mount','type=volume,src='+names['state']+',dst=/prototype-state',
            '--mount','type=bind,src='+external+',dst=/run/qualification-original-lease',
            '--mount','type=bind,src='+config_source+',dst=/run/processing-reference,readonly',
            '--env-file',env['path'],journal.PREDECESSOR_IMAGE,mutation=True,output_limit=4096).decode().strip()
        docker.start(old_id)
        physical_source=t.physical_binding_script(authority,'/run/qualification-original-lease')
        physical=docker.exec_python(old_id,physical_source,timeout=10,output_limit=32768)
        with fixture_enrollment(docker,operation_id=operation_id,predecessor_id=old_id,authority_id=authority,
                original_name=name,state_volume=names['state'],original_image_id=journal.PREDECESSOR_IMAGE,
                original_source=journal.PREDECESSOR_SOURCE,operation_directory=operation) as profile:
            old=docker.inspect(old_id)
            plan,bundle=fixture_bundle(operation,operation_id=operation_id,authority_id=authority,old=old,
                candidate_image=candidate_image,candidate_source=candidate_source,state_volume=names['state'],
                config_source=config_source,lease_source=external,spool_volume=names['spool'],physical=physical,
                material=material,source_root=source_root,cli_sha=cli_sha,daemon_id=daemon_id)
            class DesktopBoundary(BoundaryProbe):
                def host(self):
                    verify_desktop_cli(self.bundle['host']['dockerBinary'],cli_sha)
                    require(self.docker.command('info','--format','{{.ID}}',output_limit=256).decode().strip()==daemon_id,
                            'fixture_daemon_changed')
                    require(Path('/proc/sys/kernel/random/boot_id').read_text().strip()==self.bundle['host']['bootId'],
                            'fixture_boot_changed')
            with LocalWorkerReadinessFixtures(root/'workers',material,authority) as workers:
                class PhaseDriver(ConcreteCutoverDriver):
                    def _workers(self):return workers.ready()
                driver=PhaseDriver(plan,bundle,operation,docker=docker)
                driver.boundary=DesktopBoundary(plan,bundle,docker,driver.tracked)
                controller=journal.FirstEnrolledCutover(operation,plan,driver);controller.prepare()
                for _ in range(40):
                    state=controller.journal.read()
                    if rollback and state['phase']=='final-ready':controller.begin_rollback()
                    if state['phase'] in ('complete','rolled-back-rebaseline-required'):break
                    state=controller.advance();events.append(state['phase'])
                    print(json.dumps({'checkpoint':state['phase']}),flush=True)
                expected='rolled-back-rebaseline-required' if rollback else 'complete'
                require(state['phase']==expected,'actual_phase_sequence_incomplete')
                require(state['admissionOpened'] is (not rollback),'actual_admission_projection_changed')
                result=dict(schemaVersion=1,profile=profile,operationId=operation_id,
                    originalImageId=journal.PREDECESSOR_IMAGE,candidateImageId=candidate_image,candidateSourceCommit=candidate_source,
                    phases=events,terminalPhase=state['phase'],admissionOpened=state['admissionOpened'],
                    actualDockerPhases=True,actualApplicationGate=True,actualFullCustodyTar=True,
                    actualFactoryWorkerTLS=True,connectedWorkerSSHQualified=False,
                    fixtureSystemdEngineCoordinatorTransport=True,productionHostEnrollmentQualified=False,
                    desktopReadonly0777Exemption=True,installedQualified=False)
    finally:
        cleanup_owned(docker,operation_id,volumes)
    print(json.dumps(result,sort_keys=True),flush=True)
    return result


def main(argv=None):
    import argparse
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--candidate-image',required=True);parser.add_argument('--candidate-source',required=True)
    parser.add_argument('--daemon',required=True);parser.add_argument('--artifact-root',required=True)
    parser.add_argument('--operation',required=True);parser.add_argument('--cli-sha256',required=True)
    parser.add_argument('--rollback',action='store_true')
    args=parser.parse_args(argv)
    qualify(args.candidate_image,args.candidate_source,args.daemon,args.artifact_root,args.operation,args.cli_sha256,
            rollback=args.rollback)


def configuration_material(operation_id, authority_id):
    """Make ephemeral synthetic TLS/signing inputs for NEW owned volumes only.

    No original or user key is read. The returned seeder runs as UID10001 and
    writes only into the caller's newly created, empty config/spool volumes.
    Source release remains disabled and bootstrap has no source reconciliation.
    """
    from datetime import datetime, timedelta, timezone
    import ipaddress
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa, ed25519
    from cryptography.x509.oid import NameOID
    for value in (operation_id, authority_id):
        require(str(uuid.UUID(value)) == value, 'canonical_fixture_identity_required')
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, 'localhost')])
    cert = (x509.CertificateBuilder().subject_name(name).issuer_name(name).public_key(key.public_key())
        .serial_number(x509.random_serial_number()).not_valid_before(datetime.now(timezone.utc)-timedelta(minutes=1))
        .not_valid_after(datetime.now(timezone.utc)+timedelta(days=1))
        .add_extension(x509.SubjectAlternativeName([x509.DNSName('localhost'),
            x509.IPAddress(ipaddress.ip_address('127.0.0.1'))]), critical=False)
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256()))
    pem_key = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                               serialization.NoEncryption())
    pem_cert = cert.public_bytes(serialization.Encoding.PEM)
    public = ed25519.Ed25519PrivateKey.generate().public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo).decode()
    credentials = {name:'synthetic-phase-'+name.lower()+'-'+secrets.token_hex(24) for name in
                   ('PROCESSING_API_TOKEN','PROCESSING_PROTOTYPE_OPERATOR_TOKEN',
                    'PROCESSING_VIEWER_TOKEN','PROCESSING_VIEWER_STAGING_TOKEN')}
    workers = {node:dict(bearer='synthetic-worker-'+secrets.token_hex(24), workerId='worker-'+node,
        instanceId='instance-'+node, resultOrigin='https://127.0.0.1:'+str(19991+index),
        resultBearer='synthetic-result-'+secrets.token_hex(24))
        for index,node in enumerate(('cluster','main'))}
    common = dict(schemaVersion=2,listener=dict(host='0.0.0.0',port=8443,
        certificate='/run/processing-reference/fixture-cert.pem',
        privateKey='/run/processing-reference/fixture-key.pem',clientCA='/run/processing-reference/fixture-cert.pem'),
        viewerBearers=dict(production=credentials['PROCESSING_VIEWER_TOKEN'],
                          staging=credentials['PROCESSING_VIEWER_STAGING_TOKEN']),
        workers=workers,streamBudget=dict(maxConcurrent=1,maxObjectBytes=1048576),
        recoveryBindings={node:dict(engineId='fixture-engine-'+node,authorityId=authority_id,
            coordinatorOrigin='http://127.0.0.1:8080',gateGeneration=1,outputBudget=1048576)
            for node in workers})
    marker = secrets.token_hex(32)
    final = dict(common, schemaVersion=6, viewerAcceptance={},
        viewerReceiptKeys={'fixture-result':dict(issuerId='synthetic-phase-viewer',publicKeyPem=public)},
        sourceSpool=dict(root='/qualification-spool/source',mountPath='/qualification-spool',
            mountId=marker,markerName='.ltds-phase-marker',safetyFloorBytes=0,maxObjectBytes=1048576,chunkBytes=65536),
        sourceAttestationKeys=[dict(environment=environment,issuerId='synthetic-phase-viewer',
            keyId='fixture-source-'+environment,principalId='viewer-'+environment,publicKeyPem=public)
            for environment in ('production','staging')])
    encode = lambda value: json.dumps(value,sort_keys=True,separators=(',',':')).encode()
    directory = 'first-cutover-'+operation_id
    files = {directory+'/bootstrap.json':encode(common),directory+'/final.json':encode(final),
             'fixture-key.pem':pem_key,'fixture-cert.pem':pem_cert}
    packet = {name:base64.b64encode(raw).decode() for name,raw in files.items()}
    config_seed = ('import base64,json,os,pathlib\nroot=pathlib.Path("/prototype-state")\n'
        'assert os.getuid()==os.getgid()==10001 and next(root.iterdir(),None) is None\n'
        'root.chmod(0o700)\nfiles='+repr(packet)+'\n'
        'for name,raw in files.items():\n'
        ' p=root/name;p.parent.mkdir(mode=0o700,parents=True,exist_ok=True)\n'
        ' with p.open("xb") as out:out.write(base64.b64decode(raw))\n'
        ' p.chmod(0o600)\n'
        'print(json.dumps({"uid":os.getuid(),"gid":os.getgid()}))\n')
    spool_seed = ('import json,os,pathlib\nroot=pathlib.Path("/prototype-state")\n'
        'assert os.getuid()==os.getgid()==10001 and next(root.iterdir(),None) is None\n'
        'root.chmod(0o700)\n(root/"source").mkdir(mode=0o700)\n'
        'p=root/".ltds-phase-marker";p.write_bytes('+repr(marker.encode())+');p.chmod(0o600)\n'
        'print(json.dumps({"uid":os.getuid(),"gid":os.getgid()}))\n')
    return dict(bootstrap=common,final=final,configurationFiles=files,credentials=credentials,
                configSeed=config_seed,spoolSeed=spool_seed,
                configurationSha256={name:hashlib.sha256(raw).hexdigest() for name,raw in files.items()})


class LocalWorkerReadinessFixtures:
    """Actual factory/TLS/pins/readers; synthetic engine and coordinator I/O.

    This replaces only the driver-to-systemd/SSH transport in the Desktop
    qualification. It never claims connected SSH, physical host ownership,
    NodeODM processing, or the installed worker configuration.
    """
    def __init__(self, root, material, authority_id):
        self.root = Path(root); self.material = material; self.authority = authority_id
        self.instances = []; self.previous_state_home = os.environ.get('XDG_STATE_HOME')

    def __enter__(self):
        from . import reference_worker_daemon as daemon
        from .native_node_fixture import NativeNodeFixture
        from .node_ingress import save
        from processing.node_gate import NodeGate
        from processing.reference_worker_readiness import source_digest
        self.root.mkdir(mode=0o700)
        os.environ['XDG_STATE_HOME'] = str(self.root/'node-gates')
        try:
            for node in ('cluster','main'):
                root = self.root/node; root.mkdir(mode=0o700)
                for name in ('fixture-key.pem','fixture-cert.pem'):
                    target=root/name;target.write_bytes(self.material['configurationFiles'][name]);target.chmod(0o600)
                backend=NativeNodeFixture(root/'native.sqlite',engine_version='3.8.5',synthetic_enabled=True).start()
                instance=dict(node=node,root=root,backend=backend,thread=None,stop=None,server=None)
                self.instances.append(instance)
                with socket.socket() as probe:
                    probe.bind(('127.0.0.1',0));port=probe.getsockname()[1]
                source=Path(__file__).resolve().parents[1]
                engine='fixture-engine-'+node
                with NodeGate(engine) as gate:
                    gate.provision(node,'http://127.0.0.1:8080',self.authority)
                worker=self.material['bootstrap']['workers'][node]
                config=dict(schemaVersion=1,stateRoot=str(root/'state'),scratchRoot=str(root/'scratch'),
                    nodeId=node,workerId=worker['workerId'],workerInstanceId=worker['instanceId'],
                    authorityId=self.authority,engineId=engine,gateGeneration=1,
                    gateCoordinatorOrigin='http://127.0.0.1:8080',
                    coordinatorOrigin='https://coordinator.invalid',brokerOrigin='https://broker.invalid',
                    engineOrigin=backend.origin,engineToken='',workerBearer=worker['bearer'],
                    resultReadBearer=worker['resultBearer'],caFile=str(root/'fixture-cert.pem'),
                    serverCertFile=str(root/'fixture-cert.pem'),serverKeyFile=str(root/'fixture-key.pem'),
                    listenHost='127.0.0.1',listenPort=port,outputBudget=1048576,safetyFloor=0,
                    transferTtl=600,cleanupGrace=3600,pollSeconds=0.1,engineVersion='3.8.5',shareResults=True,
                    maintenanceReadiness=dict(schemaVersion=1,bearer='synthetic-readiness-'+secrets.token_hex(24),
                        sourceRoot=str(source),sourceSha256=source_digest(source),activation=None))
                path=root/'worker.json';save(path,config)
                driver,server=daemon.build_worker(daemon.load_config(str(path)))
                instance['server']=server
                driver.worker.engine_probe=lambda deadline, engine=engine:engine
                authority=self.authority
                def request(method,route,payload=None,**kwargs):
                    if route=='/authority':return 200,dict(authorityId=authority)
                    if route=='/next':return 200,dict(job=None)
                    raise RuntimeError('unexpected_fixture_worker_route')
                driver._request=request
                stop=threading.Event();service=daemon.WorkerService(driver,server,poll_seconds=0.1)
                thread=threading.Thread(target=service.run,args=(stop,),name='phase-worker-'+node)
                instance.update(stop=stop,thread=thread,service=service,path=path,config=config,port=port)
                thread.start()
            return self
        except BaseException:
            self.__exit__(None,None,None)
            raise

    def ready(self):
        import http.client
        import ssl
        import time
        from processing.reference_worker_readiness import PATH,PROTOCOL
        from .enrolled_coordinator_transport import digest
        result=[]
        for instance in self.instances:
            config=instance['config'];nonce=str(uuid.uuid4());raw=instance['path'].read_bytes()
            request=json.dumps(dict(schemaVersion=1,nonce=nonce,requireIdle=True)).encode()
            context=ssl.create_default_context(cafile=config['caFile'])
            deadline=time.monotonic()+10
            while True:
                connection=http.client.HTTPSConnection('127.0.0.1',instance['port'],context=context,timeout=3)
                try:
                    connection.request('POST',PATH,request,headers={'Content-Type':'application/json',
                        'Authorization':'Bearer '+config['maintenanceReadiness']['bearer']})
                    response=connection.getresponse();body=response.read(65537)
                    if response.status==503 and time.monotonic()<deadline:
                        time.sleep(0.02);continue
                    require(response.status==200 and len(body)<=65536,'actual_fixture_worker_unready')
                    value=json.loads(body);break
                finally:connection.close()
            expected=dict(protocol=PROTOCOL,nonce=nonce,nodeId=instance['node'],workerId=config['workerId'],
                workerInstanceId=config['workerInstanceId'],authorityId=self.authority,engineId=config['engineId'],
                gateGeneration=1,engineVersion='3.8.5',configSha256=hashlib.sha256(raw).hexdigest(),
                sourceSha256=config['maintenanceReadiness']['sourceSha256'],ready=True,idle=True,pid=os.getpid())
            require(all(value.get(name)==item for name,item in expected.items()) and
                    instance['thread'].is_alive() and instance['service'].health()['state']=='ready',
                    'actual_fixture_worker_binding_changed')
            stamp=Path('/proc/self/stat').read_text().rsplit(')',1)[1].split()[19]
            result.append(dict(nodeId=instance['node'],configSha256=value['configSha256'],
                sourceSha256=value['sourceSha256'],engineVersion=value['engineVersion'],
                instanceId=value['workerInstanceId'],bootId=value['bootId'],pid=value['pid'],startTime=stamp,
                driverBindingSha256=value['driverBindingSha256'],loadedSourceSha256=value['loadedSourceSha256'],
                optionsSha256=value['optionsSha256'],safeOptionsSha256=value['safeOptionsSha256']))
        require(len(result)==2,'complete_fixture_worker_inventory_required')
        return result

    def __exit__(self, *_):
        for instance in reversed(self.instances):
            if instance.get('stop') is not None:instance['stop'].set()
            if instance.get('thread') is not None:
                instance['thread'].join(10)
                require(not instance['thread'].is_alive(),'task_worker_thread_drain_uncertain')
            elif instance.get('server') is not None:
                instance['server'].server_close()
            instance['backend'].close()
        if self.previous_state_home is None:os.environ.pop('XDG_STATE_HOME',None)
        else:os.environ['XDG_STATE_HOME']=self.previous_state_home


@contextmanager
def fixture_enrollment(docker, *, operation_id, predecessor_id, authority_id,
                       original_name, state_volume, original_image_id,
                       original_source, operation_directory):
    """Explicit test-only fixed-ID substitution, after task ownership checks.

    The image/source remain the real immutable693 predecessor. This context
    never alters a file, container, authority row or production configuration.
    It is deliberately separate from validate_plan()/validate_bundle(), and
    callers must report productionHostEnrollmentQualified=false.
    """
    from . import install_reference_coordinator as installer
    require(str(uuid.UUID(operation_id)) == operation_id and
            str(uuid.UUID(authority_id)) == authority_id,
            'exact_fixture_operation_authority_required')
    require(re.fullmatch('[a-f0-9]{64}', predecessor_id) is not None and
            re.fullmatch('ltds-phase-original-[a-f0-9]{16}', original_name) is not None and
            state_volume == original_name + '-state', 'closed_fixture_identity_required')
    require(original_image_id == journal.PREDECESSOR_IMAGE and
            original_source == journal.PREDECESSOR_SOURCE,
            'real_frozen693_predecessor_required')
    old = docker.inspect(predecessor_id)
    require(old['Id'] == predecessor_id and old['Image'] == original_image_id and
            old['Name'] == '/' + original_name and
            old['Config']['Labels'].get(TASK_LABEL) == operation_id,
            'task_owned_original_fixture_required')
    mounts = [m for m in old['Mounts'] if m['Destination'] == '/prototype-state']
    require(len(mounts) == 1 and mounts[0]['Type'] == 'volume' and
            mounts[0]['Name'] == state_volume and mounts[0]['RW'] is True,
            'owned_original_state_volume_required')
    volume = docker.command('volume', 'inspect', state_volume, output_limit=65536)
    from .enrolled_coordinator_transport import decode
    values = decode(volume)
    require(type(values) is list and len(values) == 1 and
            values[0]['Name'] == state_volume and
            values[0].get('Labels', {}).get(TASK_LABEL) == operation_id,
            'owned_original_volume_label_required')
    root = Path(operation_directory).absolute()
    require(root.resolve() == root and root.is_dir(), 'existing_fixture_operation_directory_required')
    changes = [(journal, 'PREDECESSOR_ID', predecessor_id),
               (journal, 'AUTHORITY', authority_id), (journal, 'NAME', original_name),
               (deployment, 'PREDECESSOR_ID', predecessor_id),
               (deployment, 'NAME', original_name),
               (installer, 'LOCK_PATH', root / 'fixture-install.lock')]
    before = [(module, name, getattr(module, name)) for module, name, _ in changes]
    try:
        for module, name, value in changes:
            setattr(module, name, value)
        yield dict(profile='task-owned-actual-693-first-cutover-fixture-v1',
                   productionHostEnrollmentQualified=False,
                   fixtureEnrollmentIdentitySubstitution=True,
                   originalImageId=original_image_id, originalSourceCommit=original_source)
    finally:
        for module, name, value in reversed(before):
            setattr(module, name, value)


if __name__=='__main__':main()

