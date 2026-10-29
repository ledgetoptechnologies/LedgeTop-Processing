"""Inert resource planner and real-controller entrypoint for an EMPTY private lab.

This helper never creates containers, stops services, pulls/builds images or
cleans resources. Provisioning commands require separate review and execution.
The run-owned action invokes the unchanged worker-aware managed updater, with
no FakeCLI, subclassed Docker methods, or production-module monkeypatch.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid

ENGINE = '45bc889d-d645-442b-8fb9-d78b77b14fe9'
WORKER = 'sha256:b31c1fb4414144a3d8b9804666f567df1a009c54f8cab7c36755ffba280545a8'
BACKENDS = ('sha256:841177349ebcae8c467787b5a64507497560d346b964b5b157670a76f4251776',
            'sha256:b9d9a1a4603db9f092b790a6e58586fcf944758b7ca1149daefb2bd55dc6bdde')
NGINX = 'sha256:84ed8c0dd296c188bfc24ed506f46589e9285fb0b11cf3a04d91d524d3067e11'
OPERATION = '93823dba-2c51-4d60-9758-f670b34e1dda'
PREFIX = 'ltds-mwq-' + OPERATION.replace('-', '')[:12]
ROOT = '/home/bkoltz/.local/share/ledgetop-processing-reference/runtime-maintenance-' + OPERATION
PORTS = dict(control=24780, privateCoordinator=24781, worker=24782, backend=24783, ingress=24784)
BASELINE = frozenset((
    'a817f90f25de309569b07fad98a4ee36907c1e374c702bf2cf2c96d3e6086e69',
    '263fc96b765a5da6f706ce384c7190d63e181c12b69aeae353cd4729d3c7bf4b',
    '7147630118a3b25c6e84317314d02ff957ff8e5b7cc523036b5fe67e16a60ac0',
    '0feb2bbdb1b62e21b2ea3d5988a2bebef6b7ba9a39fe24370cddc5f607f14b12',
    '091d1defa0c804211e4f7f83ece1868c76e8c5c106757708b331ebf0ecdd9752'))


def require(value, message):
    if not value:
        raise ValueError(message)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def make_plan(preflight, cache):
    require(preflight['engineId'] == ENGINE and preflight['uid'] == 1000, 'unexpected main identity')
    require({row['id'] for row in preflight['containers']} == BASELINE, 'existing-container baseline changed')
    observed_ports = {row['port']:row['loopbackBindAvailable'] for row in preflight['ports']}
    require(set(PORTS.values()) <= set(observed_ports)
            and all(observed_ports[port] is True for port in PORTS.values()), 'planned port unavailable')
    ids = {row['id'] for row in preflight['cachedImages']} | {row['id'] for row in cache['backendCandidates']}
    require({WORKER, NGINX, *BACKENDS} <= ids, 'required immutable cached image missing')
    label = ['--label', 'ledgetop.qa.operation=' + OPERATION]
    probes = []
    for index, image in enumerate(BACKENDS):
        name = PREFIX + '-probe-' + str(index)
        probes.append(dict(name=name, image=image, create=[
            'docker', '--host', 'unix:///var/run/docker.sock', 'create', '--pull=never',
            '--name', name, *label, '--network=none', '--runtime=runc',
            '--restart=no', '--no-healthcheck', '--log-driver=local',
            '--log-opt', 'max-size=1m', '--log-opt', 'max-file=2',
            '-e', 'NVIDIA_VISIBLE_DEVICES=void', '--cap-drop=ALL',
            '--security-opt', 'no-new-privileges', '--tmpfs', '/tmp:rw,nosuid,nodev,size=64m',
            '--tmpfs', '/var/www/tmp:rw,nosuid,nodev,size=16m',
            '--tmpfs', '/var/www/data:rw,nosuid,nodev,size=64m',
            '--tmpfs', '/var/www/logs:rw,nosuid,nodev,size=16m', image, '--config', '/var/www/qa-probe.json'],
            configuration=dict(port=3000, logger=dict(level='info', logDirectory='/var/www/logs',
                maxFileSize=1048576, maxFiles=2)),
            copyConfiguration=['docker', '--host', 'unix:///var/run/docker.sock', 'cp', '-', '<exact-created-CID>:/var/www/'],
            start=['docker', '--host', 'unix:///var/run/docker.sock', 'start', '<exact-created-CID>'],
            lifetimeSecondsMaximum=90,
            stop=['docker', '--host', 'unix:///var/run/docker.sock', 'stop', '--time', '10', '<exact-created-CID>'],
            observe=['docker', '--host', 'unix:///var/run/docker.sock', 'exec', name, '/usr/bin/node', '-e',
                "setTimeout(()=>process.exit(7),5000).unref();const h=require('http');function g(p){return new Promise((ok,no)=>{const q=h.get('http://127.0.0.1:3000'+p,r=>{let s='';r.on('data',b=>{s+=b;if(s.length>16384)process.exit(3)});r.on('end',()=>{try{r.statusCode===200?ok(JSON.parse(s)):no(Error('status'))}catch(e){no(Error('json'))}})});q.setTimeout(3000,()=>q.destroy(Error('timeout')));q.on('error',no)})}Promise.all([g('/info'),g('/task/list')]).then(([x,t])=>{if(!Array.isArray(t)||t.length||x.taskQueueCount!==0||!['odm','odx'].includes(x.engine)||typeof x.engineVersion!=='string'||!/^[A-Za-z0-9][A-Za-z0-9_.+-]{0,127}$/.test(x.engineVersion))process.exit(6);console.log(JSON.stringify({engine:x.engine,engineVersion:x.engineVersion,version:x.version,taskQueueCount:x.taskQueueCount,taskListEmpty:true}))}).catch(()=>process.exit(5));"]))
    gate = ROOT+'/xdg/LedgeTop-Processing/node-gates/'+hashlib.sha256(ENGINE.encode()).hexdigest()
    mounts = [('private','/run/ledgetop-worker',True),('worker-state','/var/lib/ledgetop-worker/state',False),
              ('worker-scratch','/var/lib/ledgetop-worker/scratch',False)]
    mount_args = []
    for source,target,readonly in mounts:
        mount_args += ['--mount','type=bind,source='+ROOT+'/'+source+',target='+target+(',readonly' if readonly else '')]
    mount_args += ['--mount','type=bind,source='+gate+',target=/var/lib/ledgetop-gates/LedgeTop-Processing/node-gates/'+hashlib.sha256(ENGINE.encode()).hexdigest()]
    common = ['--pull=never','--runtime=runc','--read-only','--user','1000:1000','--cap-drop=ALL',
              '--security-opt','no-new-privileges',*label]
    setup = dict(
        bootstrap=['docker','--host','unix:///var/run/docker.sock','run','--name',PREFIX+'-bootstrap',
            '--network=none',*common,*mount_args,'--entrypoint','python',WORKER,
            '-m','ops.bootstrap_worker_input_accounting','--config','/run/ledgetop-worker/worker.json'],
        workerCreate=['docker','--host','unix:///var/run/docker.sock','create','--name',PREFIX+'-worker',
            '--network=host','--stop-timeout','-1',*common,*mount_args,WORKER,'--config','/run/ledgetop-worker/worker.json'],
        backendCreate=['docker','--host','unix:///var/run/docker.sock','compose','-p',PREFIX,'-f',ROOT+'/managed/compose.json',
            'up','-d','--no-deps','--no-build','--pull','never','node'],
        ingressCreate=['docker','--host','unix:///var/run/docker.sock','run','-d','--name',PREFIX+'-ingress',
            '--network=host',*common,'--tmpfs','/tmp:rw,nosuid,nodev,size=32m,uid=1000,gid=1000,mode=700',
            '--mount','type=bind,source='+ROOT+'/managed/ingress/public-config,target=/config,readonly',
            '--entrypoint','nginx',NGINX,'-c','/config/nginx.conf','-g','daemon off;'],
        workerStart=['docker','--host','unix:///var/run/docker.sock','start',PREFIX+'-worker'])
    compose = dict(name=PREFIX, services=dict(node=dict(image=BACKENDS[0],pull_policy='never',
        container_name=PREFIX+'-node',network_mode='bridge',runtime='runc',read_only=False,
        command=['--config','/run/ltds-backend.json'],
        volumes=[ROOT+'/managed/backend-config.json:/run/ltds-backend.json:ro'],
        logging=dict(driver='local',options={'max-size':'1m','max-file':'2'}),
        cap_drop=['ALL'],security_opt=['no-new-privileges:true'],
        environment=dict(NVIDIA_VISIBLE_DEVICES='void'),ports=['127.0.0.1:'+str(PORTS['backend'])+':3000'],
        tmpfs=['/tmp:rw,nosuid,nodev,size=64m','/var/www/tmp:rw,nosuid,nodev,size=16m',
               '/var/www/data:rw,nosuid,nodev,size=64m','/var/www/logs:rw,nosuid,nodev,size=16m'],
        labels={'ledgetop.qa.operation':OPERATION})))
    return dict(schemaVersion=1, purpose='empty-real-docker-worker-maintenance-acceptance', operationId=OPERATION,
        host='bkoltz@192.168.50.88', uid=1000, engineId=ENGINE, root=ROOT, project=PREFIX,
        names=dict(worker=PREFIX+'-worker', backend=PREFIX+'-node', ingress=PREFIX+'-ingress'),
        images=dict(worker=WORKER, initialBackend=BACKENDS[0], replacementBackend=BACKENDS[1], ingress=NGINX),
        ports=PORTS, listenerAddress='127.0.0.1', existingContainerIds=sorted(BASELINE),
        probeCommands=probes, proposedSetupCommands=setup, proposedCompose=compose,
        proposedBackendConfiguration=dict(port=3000,logger=dict(level='info',logDirectory='/var/www/logs',maxFileSize=1048576,maxFiles=2)),
        sourceDirectory=ROOT+'-source',
        sourceCommit=None, sourceInventorySha256=None, prerequisitesSatisfied=False,
        stages=['review/qualify new source registry and private bind option',
            'review exact probe commands; create ONLY two network-none empty probe instances',
            'observe actual info/version/empty task list; preserve observations; do not invent a version difference',
            'bind reviewed source commit/inventory, namespace, actual versions, recipes and resource CIDs',
            'review full loopback-only lab preparation and source bootstrap plan',
            'create empty real NodeODX/real nginx/real canonical v2 worker plus fresh actual coordinator',
            'run same owned worker-aware update, verify publication/finish and five-container baseline',
            'run second owned image transition with registered prior artifact; never repeat an uncertain mutation'],
        workerMounts=[dict(source=ROOT+'/private', destination='/run/ledgetop-worker', writable=False),
            dict(source=ROOT+'/worker-state', destination='/var/lib/ledgetop-worker/state', writable=True),
            dict(source=ROOT+'/worker-scratch', destination='/var/lib/ledgetop-worker/scratch', writable=True),
            dict(source=ROOT+'/xdg/LedgeTop-Processing/node-gates/'+hashlib.sha256(ENGINE.encode()).hexdigest(),
                 destination='/var/lib/ledgetop-gates/LedgeTop-Processing/node-gates/'+hashlib.sha256(ENGINE.encode()).hexdigest(), writable=True)],
        constraints=dict(realTaskSubmission=False, realData=False, existingCredentials=False, existingMounts=False,
            existingContainerMutation=False, pulls=False, builds=False, fakeDockerCalls=False,
            coldMappingAdoption=False, automaticRollbackAfterUnknown=False, automaticCleanup=False),
        status='DRAFT: resource identities fixed; source acceptance, probes, recipes and second review still required')


def docker_read(args):
    result = subprocess.run(['docker', '--host', 'unix:///var/run/docker.sock', *args],
                            capture_output=True, text=True, timeout=20)
    require(result.returncode == 0 and len(result.stdout) <= 128*1024, 'bounded Docker observation failed')
    return result.stdout


def baseline_observation():
    """Read only; no command lines, raw HostConfig, environment or client mounts returned."""
    require(docker_read(['info', '--format', '{{.ID}}']).strip() == ENGINE, 'wrong Engine')
    values = json.loads(docker_read(['inspect', *sorted(BASELINE)]))
    return [dict(id=v['Id'], image=v['Image'], startedAt=v['State']['StartedAt'],
                 restartCount=v['RestartCount'], hostConfigSha256=digest(v['HostConfig'])) for v in values]


def run_owned(plan_path, receipt):
    """Actual managed protocol, only after separately reviewed lab provisioning.

    No resource creation or override/fallback is provided here. New-process cold
    mapping, an unknown start, or any protocol hold remains a hold.
    """
    plan = json.loads(Path(plan_path).read_bytes())
    require(plan['operationId'] == OPERATION and plan['root'] == ROOT and plan['engineId'] == ENGINE,
            'reviewed resource binding changed')
    require(plan['prerequisitesSatisfied'] is True and type(plan['sourceCommit']) is str
            and len(plan['sourceCommit']) == 40 and type(plan['sourceInventorySha256']) is str,
            'draft resource plan is not execution authority')
    root = Path(ROOT)
    require(os.name == 'posix' and os.geteuid() == 1000 and root.resolve() == root, 'actual reviewed main ownership required')
    require(os.environ.get('XDG_STATE_HOME') == str(root/'xdg'), 'private canonical gate namespace required')
    require(os.environ.get('DOCKER_HOST') == 'unix:///var/run/docker.sock' and not os.environ.get('DOCKER_CONTEXT'),
            'exact task-local Docker endpoint required')
    source = Path(__file__).resolve().parents[1]
    require(str(source) == plan['sourceDirectory'] == ROOT+'-source', 'unreviewed source directory')
    observed_commit = subprocess.run(['git', '-C', str(source), 'rev-parse', 'HEAD'],
        capture_output=True, text=True, check=True, timeout=10).stdout.strip()
    require(observed_commit == plan['sourceCommit'], 'actual source commit differs')
    status = subprocess.run(['git', '-C', str(source), 'status', '--porcelain'],
        capture_output=True, text=True, check=True, timeout=10).stdout
    require(not status, 'qualification source checkout is not clean')
    tracked = subprocess.run(['git', '-C', str(source), 'ls-files', '-z', 'processing', 'ops'],
        capture_output=True, check=True, timeout=10).stdout.decode().split('\0')
    inventory = {p:hashlib.sha256((source/p).read_bytes()).hexdigest() for p in tracked if p.endswith('.py')}
    require('ops/qualify_worker_maintenance_runtime.py' in inventory and digest(inventory) == plan['sourceInventorySha256'],
            'actual tracked source inventory differs')
    owned_receipt = Path(receipt).absolute()
    require(owned_receipt.resolve() == owned_receipt and owned_receipt.parent == root/'managed'/'receipts'
            and re.fullmatch(r'op-[a-f0-9-]{36}', owned_receipt.name), 'unowned receipt path')
    from ops.reference_worker_maintenance import load_profile
    settings, policy = load_profile(root/'managed')
    require(settings['coordinator'] == 'http://127.0.0.1:'+str(PORTS['control'])
            and settings['nodeId'] == 'cluster' and settings['project'] == PREFIX and settings['service'] == 'node'
            and policy.engine_id == ENGINE and policy.image_id == WORKER and policy.container_name == PREFIX+'-worker'
            and policy.state_root == str(root/'worker-state') and policy.scratch_root == str(root/'worker-scratch')
            and policy.config_directory == str(root/'private')
            and policy.result_origin == 'https://127.0.0.1:'+str(PORTS['worker']), 'worker sandbox identity differs')
    worker_config = json.loads((root/'private'/'worker.json').read_bytes())
    expected = dict(nodeId='cluster',engineId=ENGINE,listenHost='127.0.0.1',listenPort=PORTS['worker'],
        coordinatorOrigin='https://127.0.0.1:'+str(PORTS['privateCoordinator']),
        brokerOrigin='https://127.0.0.1:'+str(PORTS['privateCoordinator']),
        gateCoordinatorOrigin=settings['coordinator'],engineOrigin='http://127.0.0.1:'+str(PORTS['backend']),
        stateRoot='/var/lib/ledgetop-worker/state',scratchRoot='/var/lib/ledgetop-worker/scratch',
        allowedEnvironments=['staging'],cleanupEnabled=False,engineCleanupEnabled=False,
        serviceProfile=dict(schemaVersion=1,mode='odm_only'),
        inputCopyAccounting=dict(schemaVersion=2,fetchJournal=dict(schemaVersion=1,policyId='indexed-worker-fetch-v1')))
    require(all(canonical(worker_config.get(k)) == canonical(v) for k,v in expected.items()),
            'worker configuration is not the empty isolated staging lab')
    gateway = json.loads((root/'managed'/'ingress'/'gateway.json').read_bytes())
    require(gateway['proxyName'] == PREFIX+'-ingress' and gateway['backendName'] == PREFIX+'-node'
            and gateway['proxyImage'] == NGINX and gateway['engineId'] == ENGINE
            and gateway['listenPort'] == PORTS['ingress'] and gateway['backendPort'] == PORTS['backend']
            and gateway['listenAddress'] == '127.0.0.1', 'sandbox ingress identity differs')
    fresh = json.loads(docker_read(['inspect', PREFIX+'-worker', PREFIX+'-node', PREFIX+'-ingress']))
    for item in fresh:
        require(item['Id'] not in BASELINE and item['Config']['Labels'].get('ledgetop.qa.operation') == OPERATION,
                'container is not owned by this fresh lab')
        for mount in item['Mounts']:
            if mount['Type'] == 'bind':
                require(Path(mount['Source']).is_relative_to(root), 'foreign existing bind mount forbidden')
    before = baseline_observation()
    require(before == json.loads((root/'baseline.json').read_bytes()), 'five-container baseline changed')
    # These are NEW generated sandbox credentials, never an existing account.
    private = json.loads((root/'private'/'synthetic-control.json').read_bytes())
    from processing.odx_smoke_worker import Client
    from ops import node_maintenance as managed
    api = Client('http://127.0.0.1:'+str(PORTS['control']), private['apiToken'])
    operator = Client(api.origin, private['operatorToken'])
    operation = json.loads(Path(receipt, 'lab-owned-schedule.json').read_bytes())
    require(operation['targetImage'] in BACKENDS, 'unreviewed target image')
    result = managed.run(root/'managed', api, operator, Path(receipt), 30, 30, worker_aware=True,
        owned_schedule_request_id=operation['requestId'], expected_plan_id=operation['planId'],
        expected_target=operation['targetImage'])
    require(baseline_observation() == before, 'existing-container baseline changed during acceptance')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest='action', required=True)
    planner = actions.add_parser('plan'); planner.add_argument('--preflight', required=True); planner.add_argument('--cache', required=True)
    runner = actions.add_parser('run-owned'); runner.add_argument('--plan', required=True); runner.add_argument('--receipt', required=True)
    args = parser.parse_args()
    if args.action == 'plan':
        result = make_plan(json.loads(Path(args.preflight).read_bytes()), json.loads(Path(args.cache).read_bytes()))
    else:
        result = run_owned(args.plan, args.receipt)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
