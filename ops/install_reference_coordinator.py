"""Reviewed, split sole-coordinator cutover. Never run implicitly on import.

prepare is read-only except a new private receipt/helper containers. activate
requires that exact prepared checkpoint. No images are pulled/built, no engine
updater is run, no DB is restored, and no unrelated service or NAS is touched.
"""
import argparse
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import pwd
import re
import socket
import sqlite3
import ssl
import stat
import subprocess
import sys
import time
from urllib.error import HTTPError
from urllib.request import HTTPSHandler, HTTPRedirectHandler, ProxyHandler, Request, build_opener

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from processing.odx_smoke_worker import Client

NAME = 'ledgetop-processing-prototype-c3fcb3a'
VOLUME = NAME + '-state'
AUTHORITY = '7131882e-38ce-4d3e-99cc-4ba5a94d2ee8'
ROOT = Path('/home/bkoltz/.local/share/ledgetop-processing-reference/reference-10a3848')
NATIVE_ENV = Path('/home/bkoltz/.local/share/ledgetop-processing-prototype/viewer-ba01f37/runtime.env')
TLS_MOUNT = ROOT / 'coordinator-tls'
CONFIG_PATH = '/run/processing-reference/runtime.json'
MAIN_IMAGE = 'sha256:841177349ebcae8c467787b5a64507497560d346b964b5b157670a76f4251776'
NATIVE_PORTS = {'8080/tcp': [{'HostIp':'127.0.0.1','HostPort':'18080'}, {'HostIp':'192.168.50.89','HostPort':'3005'}]}
REFERENCE_PORTS = dict(NATIVE_PORTS, **{'8443/tcp':[{'HostIp':'192.168.50.89','HostPort':'18443'}]})
LOCK_PATH = ROOT.parent / 'coordinator-install.lock'
ACTIVATION_REVIEWED = True  # Independent source review and 11 Linux fault tests passed.


class CutoverStopped(ValueError):
    pass


@contextmanager
def operation_lock():
    """One fixed host operation, across all receipt directories and rollback."""
    import fcntl
    parent=LOCK_PATH.parent
    if parent.resolve()!=parent or any(part.is_symlink() for part in (parent,*parent.parents)):
        raise CutoverStopped('private_install_lock_parent_required')
    info=parent.stat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode & 0o077:
        raise CutoverStopped('private_install_lock_parent_required')
    descriptor=os.open(LOCK_PATH,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
    try:
        observed,current=os.fstat(descriptor),LOCK_PATH.lstat()
        if (not stat.S_ISREG(observed.st_mode) or observed.st_nlink!=1 or observed.st_uid!=os.geteuid()
                or stat.S_IMODE(observed.st_mode)!=0o600
                or (observed.st_dev,observed.st_ino)!=(current.st_dev,current.st_ino)):
            raise CutoverStopped('owned_private_install_lock_required')
        try:fcntl.flock(descriptor,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError:raise CutoverStopped('another_install_operation_owns_lock') from None
        yield
    finally:
        os.close(descriptor)  # Never unlink a lock another invocation may open.


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def command(args, *, timeout=60):
    try:
        result = subprocess.run(args, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, timeout=timeout, check=False)
        if result.returncode:
            raise CutoverStopped('command_failed')
        return result.stdout.decode('utf-8')
    except Exception:
        raise CutoverStopped('command_failed') from None


def docker(*args, timeout=60):
    return command(['docker', '--host', 'unix:///run/docker.sock', *map(str, args)], timeout=timeout)


def inspected(identifier, *, timeout=60):
    value = json.loads(docker('inspect', identifier, timeout=timeout))
    if type(value) is not list or len(value) != 1:
        raise CutoverStopped('container_identity_unavailable')
    return value[0]


def image(identifier):
    return json.loads(docker('image', 'inspect', identifier))[0]


def private_file(path):
    path = Path(path).absolute()
    if path.resolve() != path or any(part.is_symlink() for part in (path, *path.parents)):
        raise CutoverStopped('private_regular_file_required')
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.geteuid()
            or info.st_mode & 0o077):
        raise CutoverStopped('private_regular_file_required')
    return path


def environment():
    result = {}
    for line in private_file(NATIVE_ENV).read_text().splitlines():
        if not line or line.startswith('#'):
            continue
        if '=' not in line:
            raise CutoverStopped('native_environment_invalid')
        key, value = line.split('=', 1)
        if key in result:
            raise CutoverStopped('duplicate_native_environment_key')
        result[key] = value
    return result


def save(root, state):
    temporary = root / ('checkpoint-' + os.urandom(8).hex() + '.tmp')
    descriptor = os.open(temporary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(descriptor, 'wb') as output:
        output.write(canonical(state) + b'\n')
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, root / 'checkpoint.json')
    descriptor = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def checkpoint(root, state, phase, **facts):
    state.update(phase=phase, **facts)
    save(root, state)


def local_clients():
    env = environment()
    api = Client('http://127.0.0.1:18080', env['PROCESSING_API_TOKEN'])
    operator = Client(api.origin, env['PROCESSING_PROTOTYPE_OPERATOR_TOKEN'])
    if not operator.token or operator.token == api.token:
        raise CutoverStopped('distinct_operator_credential_required')
    return env, api, operator


def activity(api):
    if api.json('GET', '/v1/prototype/authority')['authorityId'] != AUTHORITY:
        raise CutoverStopped('queue_authority_changed')
    jobs = api.json('GET', '/v1/prototype/jobs')['jobs']
    if any(job['status'] in ('queued','running','held') for job in jobs):
        raise CutoverStopped('queue_not_idle')
    fields = ('id','requestId','environment','profile','status','workerId','nodeId','heldReason','result','createdAt','updatedAt')
    rows = [{key: job.get(key) for key in fields} for job in sorted(jobs, key=lambda job: job['id'])]
    env = environment()
    for key in ('PROCESSING_VIEWER_TOKEN','PROCESSING_VIEWER_STAGING_TOKEN'):
        if env.get(key):
            info = Client(api.origin, env[key], node=True).json('GET', '/info')
            if type(info.get('taskQueueCount')) is not int or info['taskQueueCount'] != 0:
                raise CutoverStopped('native_viewer_not_idle')
    # This facade intentionally has no /task/list endpoint. Read only aggregate
    # state through the existing owner container; never mistake a 404 for empty.
    query = """import json,sqlite3
db=sqlite3.connect('file:/prototype-state/jobs.sqlite3?mode=ro',uri=True,timeout=3)
try: print(json.dumps({'nativeViewerTasks':db.execute('SELECT COUNT(*) FROM viewer_tasks WHERE removed=0').fetchone()[0]}))
finally: db.close()
"""
    native = json.loads(docker('exec','--user','10001:10001',NAME,'python','-c',query))
    if native != {'nativeViewerTasks':0}:
        raise CutoverStopped('native_viewer_tasks_present')
    return dict(count=len(jobs), statusCounts=dict(Counter(job['status'] for job in jobs)), sha256=digest(rows))


def ports(value):
    return {key: sorted(items, key=lambda item: (item['HostIp'],item['HostPort'])) for key, items in value.items()}


def facts(item, *, reference=False):
    host, config = item['HostConfig'], item['Config']
    expected = dict(Init=True, NetworkMode='bridge', ReadonlyRootfs=True, Privileged=False,
        PidsLimit=64, NanoCpus=500000000, Memory=268435456, MemorySwap=536870912,
        IpcMode='private', ShmSize=67108864, AutoRemove=False)
    if any(host.get(key) != value for key, value in expected.items()):
        raise CutoverStopped('container_security_or_resources_changed')
    if (config.get('User') != '10001:10001' or config.get('WorkingDir') != '/app'
            or config.get('Cmd') != ['python','-m','processing.app'] or config.get('Entrypoint')
            or host.get('CapDrop') != ['ALL'] or host.get('CapAdd')
            or host.get('SecurityOpt') not in (['no-new-privileges'],['no-new-privileges:true'])
            or host['RestartPolicy']['Name'] != 'unless-stopped' or host['LogConfig']['Type'] != 'json-file'
            or host['LogConfig'].get('Config') or any(host.get(key) for key in ('ExtraHosts','Dns','DnsSearch','DnsOptions','Devices','DeviceRequests','Links'))):
        raise CutoverStopped('container_configuration_changed')
    if ports(host['PortBindings']) != ports(REFERENCE_PORTS if reference else NATIVE_PORTS):
        raise CutoverStopped('container_port_bindings_changed')
    mounts = item['Mounts']
    volumes = [mount for mount in mounts if mount['Type'] == 'volume']
    if (len(volumes) != 1 or volumes[0]['Name'] != VOLUME or volumes[0]['Destination'] != '/prototype-state' or not volumes[0]['RW']):
        raise CutoverStopped('original_state_volume_required')
    if reference:
        binds = [mount for mount in mounts if mount['Type'] == 'bind']
        if (len(mounts) != 2 or len(binds) != 1 or binds[0]['Source'] != str(TLS_MOUNT)
                or binds[0]['Destination'] != '/run/processing-reference' or binds[0]['RW']):
            raise CutoverStopped('reference_tls_mount_changed')
    elif len(mounts) != 1:
        raise CutoverStopped('unexpected_original_mount')
    return dict(image=item['Image'], containerId=item['Id'], security=expected, user=config['User'],
        workdir=config['WorkingDir'], ports=REFERENCE_PORTS if reference else NATIVE_PORTS,
        stateVolume=VOLUME, restart='unless-stopped', logging='json-file')


def sole_owner(expected_id):
    identifiers = docker('ps', '--filter', 'volume=' + VOLUME, '--format', '{{.ID}}').split()
    if len(identifiers) != 1 or not expected_id.startswith(identifiers[0]):
        raise CutoverStopped('state_volume_has_another_running_owner')


def check_native_environment(old, env):
    observed = dict(item.split('=', 1) for item in old['Config']['Env'])
    defaults = dict(item.split('=', 1) for item in image(old['Image'])['Config']['Env'])
    if 'PROCESSING_REFERENCE_CONFIG' in observed or observed != dict(defaults, **env):
        raise CutoverStopped('native_environment_parity_failed')
    return observed


LOADER = """def _require(condition):
    if not condition:
        raise ValueError("coordinator_safety_condition_failed")

import json,ssl
from processing.config import from_environment
from processing.reference_runtime import load_reference_settings
config=from_environment(); settings=load_reference_settings('/run/processing-reference/runtime.json',config)
ctx=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);ctx.load_cert_chain(settings.listener['certificate'],settings.listener['privateKey'])
ssl.create_default_context(cafile=settings.listener['clientCA'])
_require(set(settings.viewer_bearers.values())=={'staging'})
print(json.dumps({'settingsValid':True,'uidValidated':10001,'stagingOnly':True}))
"""


def candidate_loader(target, operation):
    result = docker('run','--rm','--name','reference-loader-' + operation,'--pull=never','--network','none',
        '--read-only','--user','10001:10001','--cap-drop','ALL','--security-opt','no-new-privileges',
        '--env-file',NATIVE_ENV,'-e','PROCESSING_REFERENCE_CONFIG=' + CONFIG_PATH,
        '--mount','type=bind,src=' + str(TLS_MOUNT) + ',dst=/run/processing-reference,readonly',
        '--entrypoint','python',target,'-c',LOADER)
    if json.loads(result) != dict(settingsValid=True,uidValidated=10001,stagingOnly=True):
        raise CutoverStopped('candidate_uid_or_settings_validation_failed')


def prepare(root, target, revision, main_image):
    if main_image != MAIN_IMAGE:
        raise CutoverStopped('verified_main_image_differs_from_reviewed_pin')
    old = inspected(NAME)
    baseline = facts(old)
    if not old['State']['Running'] or old['State'].get('Health',{}).get('Status') != 'healthy':
        raise CutoverStopped('old_coordinator_not_running_healthy')
    sole_owner(old['Id'])
    env, api, _operator = local_clients()
    check_native_environment(old, env)
    candidate = image(target)
    if (candidate['Id'] != target or candidate['Config'].get('Labels',{}).get('org.opencontainers.image.revision') != revision
            or candidate['Config'].get('User') != '10001:10001'
            or candidate['Config'].get('WorkingDir') != '/app' or candidate['Config'].get('Cmd') != ['python','-m','processing.app']
            or candidate['Config'].get('Healthcheck') != old['Config'].get('Healthcheck')):
        raise CutoverStopped('candidate_image_or_revision_not_verified')
    history = activity(api)
    nodes = {node['id']: node for node in api.json('GET','/v1/prototype/nodes')['nodes']}
    if set(nodes) != {'cluster','main'} or any(node['state'] != 'active' for node in nodes.values()):
        raise CutoverStopped('nodes_have_other_maintenance')
    cluster = inspected('node-odx-gpu')
    if not cluster['State']['Running']:
        raise CutoverStopped('cluster_engine_not_running')
    operation = os.urandom(6).hex()
    state = dict(schemaVersion=1,operation=operation,phase='preparing',image=target,sourceCommit=revision,
        old=baseline,history=history,authorityId=AUTHORITY,plans={},
        engineImages=dict(cluster=cluster['Image'],main=main_image),
        rollbackName=NAME + '-pre-reference-' + revision[:7], failedName=NAME + '-failed-reference-' + operation)
    save(root, state)
    for reserved in (state['rollbackName'],state['failedName']):
        if docker('ps','--all','--filter','name=^/' + reserved + '$','--format','{{.ID}}').strip():
            raise CutoverStopped('retention_name_already_exists')
    candidate_loader(target, operation)
    checkpoint(root,state,'prepared',nativeEnvironmentParity=True,privateCandidateSettingsValidated=True)
    return state


BACKUP = r"""def _require(condition):
    if not condition:
        raise ValueError("coordinator_safety_condition_failed")

import hashlib,json,os,sqlite3,stat
from pathlib import Path
source,target=Path('/source'),Path('/backup')
_require(target.stat().st_uid==10001 and target.stat().st_gid==10001 and stat.S_IMODE(target.stat().st_mode)==0o700)
_require(not list(target.iterdir()))
_require(not any((source/('jobs.sqlite3'+suffix)).exists() for suffix in ('-wal','-shm','-journal')))
manifest=[]; directories=[target]; counts={'regularFiles':0,'directories':1,'bytes':0}
def copy(src,dst,relative=''):
 for entry in sorted(src.iterdir(),key=lambda p:p.name):
  info=entry.lstat(); rel=relative+'/'+entry.name
  _require(info.st_uid==10001 and info.st_gid==10001 and not stat.S_ISLNK(info.st_mode))
  output=dst/entry.name
  if stat.S_ISDIR(info.st_mode):
   output.mkdir(mode=stat.S_IMODE(info.st_mode));directories.append(output);counts['directories']+=1
   manifest.append([rel,'directory',stat.S_IMODE(info.st_mode)]);copy(entry,output,rel)
  else:
   _require(stat.S_ISREG(info.st_mode) and info.st_nlink==1)
   read=os.open(entry,os.O_RDONLY|os.O_NOFOLLOW);write=os.open(output,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,stat.S_IMODE(info.st_mode))
   sha=hashlib.sha256();total=0
   try:
    observed=os.fstat(read);_require((observed.st_dev,observed.st_ino)==(info.st_dev,info.st_ino))
    with os.fdopen(read,'rb',closefd=False) as inp,os.fdopen(write,'wb',closefd=False) as out:
     while True:
      chunk=inp.read(1048576)
      if not chunk:break
      total+=len(chunk);_require(total<=info.st_size);sha.update(chunk);out.write(chunk)
     out.flush();os.fsync(write)
    current=entry.lstat();_require(total==info.st_size and (current.st_dev,current.st_ino,current.st_size,current.st_mtime_ns)==(info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns))
   finally:os.close(read);os.close(write)
   counts['regularFiles']+=1;counts['bytes']+=total;manifest.append([rel,'file',stat.S_IMODE(info.st_mode),total,sha.hexdigest()])
copy(source,target)
for directory in reversed(directories):
 fd=os.open(directory,os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)
def database(path):
 db=sqlite3.connect('file:'+str(path)+'?mode=ro&immutable=1',uri=True);db.row_factory=sqlite3.Row
 try:
  _require(db.execute('PRAGMA quick_check').fetchall()[0][0]=='ok')
  jobs=[dict(row) for row in db.execute('SELECT * FROM jobs ORDER BY seq')]
  _require(not any(row['status'] in ('queued','running','held') for row in jobs))
  _require(db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()[0]=='7131882e-38ce-4d3e-99cc-4ba5a94d2ee8')
  tables={row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
  for table in ('viewer_tasks','viewer_files','viewer_incarnations'):
   if table in tables:_require(db.execute('SELECT COUNT(*) FROM '+table).fetchone()[0]==0)
  return {'count':len(jobs),'sha256':hashlib.sha256(json.dumps(jobs,sort_keys=True,separators=(',',':')).encode()).hexdigest()}
 finally:db.close()
original=database(source/'jobs.sqlite3');_require(database(target/'jobs.sqlite3')==original)
counts.update(sqliteQuickCheck='ok',databaseHistory=original,treeSha256=hashlib.sha256(json.dumps(manifest,sort_keys=True,separators=(',',':')).encode()).hexdigest())
print(json.dumps(counts))
"""


def backup(root, state):
    destination=root/'original-state-backup'
    if destination.exists() or destination.is_symlink():
        raise CutoverStopped('backup_destination_already_exists')
    # Numeric UID 10001 has no host passwd entry. install -o 10001 performs a
    # name lookup here; use the already-qualified numeric chown mechanism.
    make_backup = """def _require(condition):
    if not condition:
        raise ValueError("coordinator_safety_condition_failed")

import os,stat,sys
from pathlib import Path
p=Path(sys.argv[1]);_require(p.is_absolute() and p.name=='original-state-backup')
_require(p.parent.resolve()==p.parent and not any(q.is_symlink() for q in p.parents))
s=p.parent.stat();_require(s.st_uid==1000 and not stat.S_IMODE(s.st_mode)&0o077)
os.mkdir(p,0o700);os.chown(p,10001,10001)
fd=os.open(p.parent,os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)
"""
    command(['sudo','-n','python3','-c',make_backup,str(destination)])
    info=destination.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid!=10001 or info.st_gid!=10001 or stat.S_IMODE(info.st_mode)!=0o700:
        raise CutoverStopped('private_uid_backup_directory_required')
    result=docker('run','--rm','--name','reference-backup-'+state['operation'],'--pull=never','--network','none',
        '--read-only','--user','10001:10001','--cap-drop','ALL','--security-opt','no-new-privileges',
        '--mount','type=volume,src='+VOLUME+',dst=/source,readonly',
        '--mount','type=bind,src='+str(destination)+',dst=/backup',
        '--entrypoint','python',state['image'],'-c',BACKUP,timeout=120)
    result=json.loads(result)
    if result['sqliteQuickCheck']!='ok' or result['databaseHistory']['count']!=state['history']['count']:
        raise CutoverStopped('offline_backup_validation_failed')
    return result


def own_plans(api, state):
    for node, plan in state['plans'].items():
        current=api.json('GET','/v1/prototype/nodes/'+node)['node']
        if (current['state']!='draining' or current['planId']!=plan['planId']
                or current['targetImage']!=state['engineImages'][node]):
            raise CutoverStopped('owned_drain_plan_changed')


def cancel_plans(root,state,api,operator):
    if activity(api)!=state['history']:
        raise CutoverStopped('history_or_activity_changed_leave_nodes_draining')
    for node,plan in state['plans'].items():
        current=api.json('GET','/v1/prototype/nodes/'+node)['node']
        if current['planId']!=plan['planId'] or current['targetImage']!=state['engineImages'][node]:
            raise CutoverStopped('cannot_cancel_unowned_or_changed_plan')
        if current['state']=='active' and plan.get('cancelled'):continue
        intent=plan.get('cancelIntent')
        if (current['state']=='active' and intent and current['revision']==intent['expectedRevision']+1):
            plan['cancelled']=True;save(root,state);continue
        if current['state']!='draining' or current['planId']!=plan['planId'] or current['targetImage']!=state['engineImages'][node]:
            raise CutoverStopped('cannot_cancel_unowned_or_changed_plan')
        payload=dict(expectedRevision=current['revision'],planId=plan['planId'])
        plan['cancelIntent']=payload;save(root,state)
        try:
            operator.json('POST','/v1/prototype/nodes/'+node+'/cancel',payload)
        except Exception:
            after=api.json('GET','/v1/prototype/nodes/'+node)['node']
            if (after['state']!='active' or after['revision']!=payload['expectedRevision']+1
                    or after['planId']!=plan['planId'] or after['targetImage']!=state['engineImages'][node]):
                raise CutoverStopped('cancel_outcome_requires_inspection') from None
        plan['cancelled']=True
        save(root,state)


def health(api, *, seconds=60, container_id=None):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        try:
            if api.json('GET','/healthz',timeout=min(3,max(0.1,deadline-time.monotonic())))['status']=='ok' and (
                    container_id is None or inspected(container_id,timeout=min(3,max(0.1,deadline-time.monotonic())))['State'].get('Health',{}).get('Status')=='healthy'):
                return
        except Exception:
            pass
        time.sleep(min(1,max(0,deadline-time.monotonic())))
    raise CutoverStopped('coordinator_health_deadline')


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):
        return None


def verify_reference():
    credentials=json.loads(private_file(ROOT/'config/credentials.json').read_text())
    context=ssl.create_default_context(cafile=str(private_file(ROOT/'tls/ca.crt')))
    opener=build_opener(ProxyHandler({}),NoRedirect(),HTTPSHandler(context=context))
    def get(path,token,expected=200):
        url='https://192.168.50.89:18443'+path
        request=Request(url,headers={'Authorization':'Bearer '+token,'Accept':'application/json'})
        try:
            with opener.open(request,timeout=10) as response:
                if response.status!=expected or response.geturl()!=url:
                    raise CutoverStopped('reference_response_invalid')
                raw=response.read(4*1024*1024+1)
                if len(raw)>4*1024*1024:raise CutoverStopped('reference_response_invalid')
                return json.loads(raw)
        except HTTPError as error:
            status=error.code;error.close()
            if status!=expected:raise CutoverStopped('reference_role_verification_failed') from None
            return {}
    caps=get('/ltds/v1/capabilities',credentials['viewerStaging'])
    options=get('/ltds/v1/options',credentials['viewerStaging'])
    if (caps.get('environment')!='staging' or caps['engines']['treedar']['available'] is not False
            or caps['engines']['gaussian_splat']['enabled'] is not False
            or caps['engines']['gaussian_splat']['available'] is not False
            or type(options.get('options')) is not list or not options['options']):
        raise CutoverStopped('reference_capabilities_invalid')
    for key in ('clusterWorker','mainWorker'):
        if get('/ltds/worker/v1/authority',credentials[key]).get('authorityId')!=AUTHORITY:
            raise CutoverStopped('worker_authority_verification_failed')
        get('/ltds/v1/capabilities',credentials[key],401)
    get('/ltds/worker/v1/authority',credentials['viewerStaging'],401)
    native=environment()
    get('/ltds/v1/capabilities',native['PROCESSING_VIEWER_TOKEN'],401)
    get('/ltds/v1/capabilities',native['PROCESSING_API_TOKEN'],401)
    return dict(tlsVerified=True,referenceEnvironment='staging',workerAuthoritiesVerified=True,
                nativeProductionCredentialsRejected=True,optionCount=len(options['options']))


def recover_candidate(root,state):
    """Recover a lost create response only from our exact unique label/image."""
    identifier=state.get('newContainerId')
    if identifier is None:
        matches=docker('ps','--all','--filter','label=org.ledgetop.reference.cutover='+state['operation'],'--format','{{.ID}}').split()
        if len(matches)>1:raise CutoverStopped('multiple_owned_replacements_require_inspection')
        if not matches:return None
        identifier=matches[0]
    item=inspected(identifier)
    labels=item['Config'].get('Labels',{}) or {}
    if (item['Image']!=state['image'] or labels.get('org.ledgetop.reference.cutover')!=state['operation']
            or item['Name'] not in ('/'+NAME,'/'+state['failedName'])):
        raise CutoverStopped('replacement_ownership_unavailable')
    state['newContainerId']=item['Id'];save(root,state)
    return item


def completed_rollback_precheck(state):
    """Refuse post-install downgrade unless the installed owner is still unused.

    This deliberately performs no checkpoint writes or Docker mutations. The
    original binary cannot serve reference jobs, including terminal jobs whose
    queue slot has already been released. The prepared baseline predates the
    reference schema, so *any* reference row blocks this downgrade.
    """
    try:
        identifier=state['newContainerId']
        if not re.fullmatch(r'[0-9a-f]{64}',identifier):
            raise CutoverStopped('replacement_identity_unavailable')
        current=inspected(NAME)
        def exact_owner(item):
            return (item['Id']==identifier and item['Name']=='/'+NAME
                and item['Image']==state['image']
                and (item['Config'].get('Labels') or {}).get('org.ledgetop.reference.cutover')==state['operation']
                and item['State']['Running'] is True
                and item['State']['Health']['Status']=='healthy')
        if not exact_owner(current):
            raise CutoverStopped('installed_primary_changed')
        facts(current,reference=True)
        sole_owner(identifier)
        _env,api,_operator=local_clients()
        if activity(api)!=state['history']:
            raise CutoverStopped('installed_history_changed')
        query="""import json,sqlite3
db=sqlite3.connect('file:/prototype-state/jobs.sqlite3?mode=ro',uri=True)
try:
 print(json.dumps({'referenceJobs':db.execute('SELECT COUNT(*) FROM reference_jobs').fetchone()[0]}))
finally: db.close()
"""
        counts=json.loads(docker('exec','--user','10001:10001',identifier,'python','-c',query))
        if (set(counts)!={'referenceJobs'} or type(counts['referenceJobs']) is not int
                or counts['referenceJobs']!=0):
            raise CutoverStopped('reference_history_prevents_downgrade')
        # Recheck identity after the network/database reads. Never recover a
        # candidate by label or modify the checkpoint during this preflight.
        if not exact_owner(inspected(NAME)):
            raise CutoverStopped('installed_primary_changed')
    except Exception:
        raise CutoverStopped('completed_rollback_precheck_failed') from None


OFFLINE_ROLLBACK_CENSUS = """def _require(condition):
    if not condition:
        raise ValueError("coordinator_safety_condition_failed")

import json,sqlite3,hashlib,stat
from pathlib import Path
from collections import Counter
p=Path('/source/jobs.sqlite3');s=p.lstat()
_require(stat.S_ISREG(s.st_mode) and s.st_nlink==1 and s.st_uid==10001)
_require(not any(Path(str(p)+suffix).exists() for suffix in ('-wal','-shm','-journal')))
db=sqlite3.connect('file:/source/jobs.sqlite3?mode=ro&immutable=1',uri=True);db.row_factory=sqlite3.Row
try:
 _require(db.execute('PRAGMA quick_check').fetchone()[0]=='ok')
 authority=db.execute('SELECT authority_id FROM coordinator_identity WHERE singleton=1').fetchone()[0]
 rows=[];statuses=[]
 for r in db.execute('SELECT * FROM jobs ORDER BY id'):
  statuses.append(r['status'])
  rows.append(dict(id=r['id'],requestId=r['request_id'],environment=r['environment'],profile=r['profile'],
   status=r['status'],workerId=r['worker_id'],nodeId=r['node_id'],heldReason=r['held_reason'],
   result=json.loads(r['result']) if r['result'] else None,createdAt=r['created_at'],updatedAt=r['updated_at']))
 _require(not any(s in ('queued','running','held') for s in statuses))
 native=db.execute('SELECT COUNT(*) FROM viewer_tasks WHERE removed=0').fetchone()[0]
 reference=db.execute('SELECT COUNT(*) FROM reference_jobs').fetchone()[0]
 history=dict(count=len(rows),statusCounts=dict(Counter(statuses)),sha256=hashlib.sha256(json.dumps(rows,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest())
 print(json.dumps(dict(authorityId=authority,history=history,nativeViewerTasks=native,referenceJobs=reference)))
finally: db.close()
"""


def offline_rollback_precheck(state):
    """Census the stopped owner's volume without constructing a Queue."""
    if docker('ps','--filter','volume='+VOLUME,'--format','{{.ID}}').strip():
        raise CutoverStopped('offline_volume_still_owned')
    result=json.loads(docker('run','--rm','--pull=never','--network','none','--read-only',
        '--user','10001:10001','--cap-drop','ALL','--security-opt','no-new-privileges',
        '--mount','type=volume,src='+VOLUME+',dst=/source,readonly','--entrypoint','python',
        state['image'],'-c',OFFLINE_ROLLBACK_CENSUS))
    if result!=dict(authorityId=AUTHORITY,history=state['history'],nativeViewerTasks=0,referenceJobs=0):
        raise CutoverStopped('offline_downgrade_baseline_changed')


def retain_replacement(root,state,candidate):
    """On unsafe downgrade, resume only our exact original-name replacement."""
    healthy=False
    try:
        item=inspected(candidate)
        if (item['Id']!=state['newContainerId'] or item['Name']!='/'+NAME or item['Image']!=state['image']
                or (item['Config'].get('Labels') or {}).get('org.ledgetop.reference.cutover')!=state['operation']):
            raise CutoverStopped('replacement_identity_changed')
        facts(item,reference=True)
        if not item['State']['Running']:
            docker('start',candidate)
        _env,api,_operator=local_clients()
        health(api,container_id=candidate)
        sole_owner(candidate)
        healthy=True
    except Exception:
        pass
    checkpoint(root,state,'rollback_needs_inspection',sameVolumeRetained=True,
        downgradeRefused=True,replacementHealthVerified=healthy)
    return state


def rollback(root,state):
    """Stop only our replacement, restore only the recorded old identity/name."""
    completed_origin=state['phase']=='completed' or state.get('rollbackFromCompleted') is True
    if completed_origin:
        completed_rollback_precheck(state)
    # Retain the origin across interrupted retries; changing the phase must
    # never turn a post-install rollback into initial activation recovery.
    checkpoint(root,state,'rollback_pending',**({'rollbackFromCompleted':True} if completed_origin else {}))
    item=recover_candidate(root,state)
    if item is not None:
        candidate=item['Id']
        started=(item['State']['Running'] or state.get('replacementStarted') is True
            or bool(item['State'].get('StartedAt') and not item['State']['StartedAt'].startswith('0001-')))
        if started:
            checkpoint(root,state,'rollback_pending',replacementStarted=True)
        if item['State']['Running']:
            docker('stop','--time','-1',candidate,timeout=None)
        if inspected(candidate)['State']['Running']:
            raise CutoverStopped('replacement_not_confirmed_stopped')
        if started:
            try:
                offline_rollback_precheck(state)
            except Exception:
                return retain_replacement(root,state,candidate)
        if item['Name']=='/'+NAME:
            docker('rename',candidate,state['failedName'])
    old=inspected(state['old']['containerId'])
    if old['Image']!=state['old']['image'] or old['Name'] not in ('/'+NAME,'/'+state['rollbackName']):
        raise CutoverStopped('old_container_identity_changed')
    if old['Name']!='/'+NAME:
        docker('rename',old['Id'],NAME)
    if not old['State']['Running']:
        docker('start',old['Id'])
    env,api,operator=local_clients()
    health(api,container_id=old['Id'])
    restored=inspected(old['Id'])
    facts(restored)
    sole_owner(old['Id'])
    check_native_environment(restored,env)
    try: unchanged=activity(api)==state['history']
    except Exception: unchanged=False
    if not unchanged:
        checkpoint(root,state,'rollback_needs_inspection',sameVolumeRetained=True)
        return state
    cancel_plans(root,state,api,operator)
    if state.get('pendingPlan'):
        checkpoint(root,state,'rollback_needs_inspection',sameVolumeRetained=True,unknownScheduleOutcome=True)
        return state
    checkpoint(root,state,'rolled_back',sameVolumeRetained=True,databaseRestored=False)
    return state


def activate(root,state,main_image):
    if state['phase']!='prepared' or main_image!=state['engineImages']['main']:
        raise CutoverStopped('exact_prepared_checkpoint_required')
    env,api,operator=local_clients()
    old=inspected(NAME)
    if facts(old)!=state['old'] or not old['State']['Running'] or old['State']['Health']['Status']!='healthy':
        raise CutoverStopped('prepared_primary_changed')
    sole_owner(old['Id']);check_native_environment(old,env)
    if activity(api)!=state['history'] or inspected('node-odx-gpu')['Image']!=state['engineImages']['cluster']:
        raise CutoverStopped('prepared_history_or_cluster_engine_changed')
    try:
        for node in ('cluster','main'):
            current=api.json('GET','/v1/prototype/nodes/'+node)['node']
            if current['state']!='active':raise CutoverStopped('node_no_longer_active')
            stamp=datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
            payload=dict(expectedRevision=current['revision'],targetImage=state['engineImages'][node],
                drainAt=stamp,notBefore=stamp,afterJobId=None)
            checkpoint(root,state,'plan_pending',pendingPlan=dict(nodeId=node,payload=payload))
            plan=operator.json('POST','/v1/prototype/nodes/'+node+'/schedule',payload)['node']
            if plan['state']!='draining' or plan['targetImage']!=state['engineImages'][node]:
                raise CutoverStopped('draining_plan_not_confirmed')
            state['plans'][node]=dict(planId=plan['planId'],targetImage=plan['targetImage'])
            state.pop('pendingPlan',None);save(root,state)
        own_plans(api,state)
        if activity(api)!=state['history']:raise CutoverStopped('activity_appeared_after_draining')
        checkpoint(root,state,'stop_old_pending')
        docker('stop','--time','-1',old['Id'],timeout=None)
        if inspected(old['Id'])['State']['Running']:raise CutoverStopped('old_primary_did_not_stop')
        checkpoint(root,state,'rename_old_pending')
        docker('rename',old['Id'],state['rollbackName'])
        checkpoint(root,state,'backup_pending')
        backed=backup(root,state)
        checkpoint(root,state,'backup_verified',backup=backed)
        checkpoint(root,state,'create_primary_pending')
        new_id=docker('create','--name',NAME,'--pull=never','--init','--restart','unless-stopped','--network','bridge',
            '--read-only','--user','10001:10001','--cap-drop','ALL','--security-opt','no-new-privileges',
            '--pids-limit','64','--cpus','0.5','--memory','256m','--memory-swap','512m','--ipc','private','--shm-size','64m',
            '--log-driver','json-file','--workdir','/app','--env-file',NATIVE_ENV,'-e','PROCESSING_REFERENCE_CONFIG='+CONFIG_PATH,
            '--mount','type=volume,src='+VOLUME+',dst=/prototype-state',
            '--mount','type=bind,src='+str(TLS_MOUNT)+',dst=/run/processing-reference,readonly',
            '-p','127.0.0.1:18080:8080','-p','192.168.50.89:3005:8080','-p','192.168.50.89:18443:8443',
            '--label','org.ledgetop.reference.cutover='+state['operation'],state['image']).strip()
        if not re.fullmatch(r'[0-9a-f]{64}',new_id):raise CutoverStopped('new_container_id_unavailable')
        checkpoint(root,state,'new_primary_created',newContainerId=new_id)
        candidate=inspected(new_id);facts(candidate,reference=True)
        old_env=dict(item.split('=',1) for item in old['Config']['Env'])
        new_env=dict(item.split('=',1) for item in candidate['Config']['Env'])
        if new_env!=dict(old_env,PROCESSING_REFERENCE_CONFIG=CONFIG_PATH):
            raise CutoverStopped('replacement_native_environment_changed')
        checkpoint(root,state,'start_primary_pending')
        docker('start',new_id)
        health(api,container_id=new_id)
        if activity(api)!=state['history']:raise CutoverStopped('replacement_history_or_activity_changed')
        sole_owner(new_id);own_plans(api,state)
        verified=verify_reference()
        checkpoint(root,state,'verification_passed',verification=verified)
        cancel_plans(root,state,api,operator)
        checkpoint(root,state,'completed',sameAuthority=True,sameStateVolume=True,realJobsSubmitted=False)
        return state
    except Exception:
        checkpoint(root,state,'activation_failed')
        return rollback(root,state)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=('prepare','activate','rollback'))
    parser.add_argument('--image',required=True)
    parser.add_argument('--source-commit',required=True)
    parser.add_argument('--receipt-dir',required=True)
    parser.add_argument('--verified-main-image',required=True,help='fresh root-verified main backend image; never guessed')
    args=parser.parse_args()
    try:
        if (os.name != 'posix' or os.geteuid()!=1000 or pwd.getpwuid(os.geteuid()).pw_name != 'bkoltz'
                or socket.gethostname().split('.')[0].lower() != 'cluster'):
            raise CutoverStopped('fixed_cluster_host_account_required')
        if not re.fullmatch(r'sha256:[0-9a-f]{64}',args.image) or not re.fullmatch(r'[0-9a-f]{40}',args.source_commit):
            raise CutoverStopped('exact_image_and_full_source_commit_required')
        root=Path(args.receipt_dir)
        if not root.is_absolute() or root.resolve()!=root or any(part.is_symlink() for part in (root,*root.parents)):
            raise CutoverStopped('absolute_private_receipt_required')
        with operation_lock():
            if args.action=='prepare':
                parent=root.parent.stat()
                if parent.st_uid!=os.geteuid() or parent.st_mode & 0o077:
                    raise CutoverStopped('private_owned_receipt_parent_required')
                root.mkdir(mode=0o700)
                state=prepare(root,args.image,args.source_commit,args.verified_main_image)
            else:
                if not ACTIVATION_REVIEWED:
                    raise CutoverStopped('activation_requires_final_source_review')
                info=root.stat()
                if not stat.S_ISDIR(info.st_mode) or info.st_uid!=os.geteuid() or info.st_mode & 0o077:
                    raise CutoverStopped('private_owned_receipt_required')
                state=json.loads(private_file(root/'checkpoint.json').read_text())
                if (state['image']!=args.image or state['sourceCommit']!=args.source_commit
                        or state['engineImages']['main']!=args.verified_main_image):
                    raise CutoverStopped('prepared_checkpoint_binding_changed')
                state=activate(root,state,args.verified_main_image) if args.action=='activate' else rollback(root,state)
        print(json.dumps({'phase':state['phase'],'operation':state['operation'],'authorityId':AUTHORITY}))
        return 0 if state['phase']=={'prepare':'prepared','activate':'completed','rollback':'rolled_back'}[args.action] else 2
    except Exception:
        print('Coordinator cutover stopped; retain private checkpoint and inspect locally.',file=sys.stderr)
        return 2


if __name__=='__main__':
    raise SystemExit(main())
