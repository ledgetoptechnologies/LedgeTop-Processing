"""Opt-in actual daemon qualification of only task-owned holder/custody resources.

No image pull, install, host dataset, service cutover or job processing. This
component fixture intentionally does not validate the real693 predecessor bundle
or qualify final projection/admission. Production BoundaryProbe observation and
closed lease-holder recipe are exercised unchanged against owned fixture IDs.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import secrets
import time
import uuid

from ops import enrolled_coordinator_transport as t
from ops import enrolled_coordinator_lease_helper as holder
from ops.enrolled_coordinator_boundary import BoundaryProbe
from ops.enrolled_coordinator_custody_ops import CustodyOps,MODULE_NAMES
from ops.qualify_enrolled_docker_transport import QualificationDocker,TASK_LABEL,cleanup_owned,initialize_fixture


class FixtureBoundaryProbe(BoundaryProbe):
    """Only bypass the real-ID deployment constructor in this opt-in fixture.

    All inherited host/recipe/writer/actual socket/kernel/fresh-retirement checks
    run unchanged. This class is never an activation or production bundle gate.
    """
    def __init__(self,plan,bundle,settings,docker,tracked):
        self.plan,self.bundle,self.settings=plan,bundle,settings
        self.docker,self.tracked=docker,tracked

    def host(self):
        # Desktop's RO bind reports synthetic0777 bits. This explicit fixture
        # exemption is NOT production Linux host enrollment or a source change.
        verify_desktop_cli(self.bundle['host']['dockerBinary'],self.bundle['host']['dockerBinarySha256'])
        t.require(self.docker.command('info','--format','{{.ID}}',output_limit=256,timeout=10).decode().strip()==
                  self.bundle['host']['dockerEngineId'],'qualification_daemon_changed')
        t.require(Path('/proc/sys/kernel/random/boot_id').read_text().strip()==self.bundle['host']['bootId'],
                  'qualification_boot_changed')


class DropConfirmedReply:
    """Model loss AFTER one known-completed actual dispatch RPC, not ambiguity."""
    def __init__(self,docker):self.docker=docker;self.dispatched=0;self.drop=True
    def exec_python(self,identifier,source,**kwargs):
        value=self.docker.exec_python(identifier,source,**kwargs)
        if kwargs.get('mutation'):
            self.dispatched+=1
            if self.drop:
                self.drop=False
                raise t.TransportHeld('qualification_dropped_confirmed_dispatch_reply')
        return value


def verify_desktop_cli(binary,expected_sha):
    import stat
    path=Path(binary);before=path.lstat()
    t.require(path.resolve()==path and stat.S_ISREG(before.st_mode) and before.st_uid==0 and before.st_gid==0 and
              before.st_nlink==1 and before.st_size<=256*1048576 and stat.S_IMODE(before.st_mode)==0o777 and
              not os.access(path,os.W_OK),'explicit_Desktop_readonly0777_fixture_required')
    mounts=[line.split(' - ',1)[0].split() for line in Path('/proc/self/mountinfo').read_text().splitlines()]
    t.require(any(len(row)>5 and row[4]==str(path) and 'ro' in row[5].split(',') for row in mounts),
              'actual_readonly_cli_bind_required')
    t.require(type(expected_sha) is str and t.HASH.fullmatch(expected_sha),'external_cli_digest_required')
    descriptor=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
    same=lambda value:(value.st_dev,value.st_ino,value.st_mode,value.st_uid,value.st_gid,value.st_nlink,
                       value.st_size,value.st_mtime_ns,value.st_ctime_ns)
    try:
        t.require(same(os.fstat(descriptor))==same(before),'native_cli_identity_changed')
        sha=hashlib.sha256()
        while True:
            chunk=os.read(descriptor,65536)
            if not chunk:break
            sha.update(chunk)
        t.require(sha.hexdigest()==expected_sha and same(os.fstat(descriptor))==same(path.lstat())==same(before),
                  'external_native_cli_digest_changed')
    finally:os.close(descriptor)


def owned_volume(docker,name,operation):
    existing=docker.command('volume','ls','--filter','name='+name,'--format','{{.Name}}',output_limit=4096).decode().splitlines()
    t.require(name not in existing,'new_holder_task_volume_required')
    docker.command('volume','create','--label',TASK_LABEL+'='+operation,name,mutation=True,output_limit=4096)
    value=t.decode(docker.command('volume','inspect',name,output_limit=65536))[0]
    t.require(value['Name']==name and value.get('Labels',{}).get(TASK_LABEL)==operation and
              type(value.get('Mountpoint')) is str and value['Mountpoint'].startswith('/'),
              'new_owned_volume_mountpoint_required')
    t._absolute_path(value['Mountpoint'])
    return value


def setup_arguments(image,operation,name,state,lease,source):
    return ['container','create','--name',name,'--label',TASK_LABEL+'='+operation,
        '--network','none','--read-only','--user','10001:10001','--cpus','0.25','--memory','128m','--memory-swap','128m',
        '--pids-limit','32','--cap-drop','ALL','--security-opt','no-new-privileges',
        '--mount','type=volume,source='+state+',target=/prototype-state',
        '--mount','type=volume,source='+lease+',target=/run/qualification-original-lease',
        '--entrypoint','python',image,'-I','-c',source.decode('utf-8')]


def run_probe(docker,image,operation,suffix,state,external,authority):
    source=('import json,os,fcntl\nresult={}\n'
        'for key,path,flags in [("mainAvailable","/prototype-state/coordinator.lock",os.O_RDONLY),'
        '("externalAvailable",'+repr('/run/qualification-original-lease/'+authority+'.lease')+',os.O_RDWR)]:\n'
        ' fd=os.open(path,flags|os.O_NOFOLLOW|os.O_CLOEXEC)\n'
        ' try:\n'
        '  try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB);result[key]=True\n'
        '  except BlockingIOError:result[key]=False\n'
        ' finally:os.close(fd)\n'
        'print(json.dumps(result))\n')
    raw=docker.command('run','--rm','--name','ltds-holder-flock-probe-'+suffix,
        '--label',TASK_LABEL+'='+operation,'--network','none','--read-only','--user','10001:10001',
        '--cpus','0.125','--memory','128m','--memory-swap','128m','--pids-limit','32','--cap-drop','ALL',
        '--security-opt','no-new-privileges','--mount','type=volume,source='+state+',target=/prototype-state,readonly',
        '--mount','type=bind,source='+external+',target=/run/qualification-original-lease',
        '--entrypoint','python',image,'-I','-c',source,mutation=True,timeout=15,output_limit=4096)
    return t.decode(raw)


def checkpoint(completed,name):
    completed.append(name);print(json.dumps({"checkpoint":name}),flush=True)


def qualify(image_id,source_commit,daemon_id,artifact_root,operation,expected_cli_sha256):
    docker=QualificationDocker();completed=[];volumes=[];long_state=None;holder_id=None
    verify_desktop_cli(docker.binary,expected_cli_sha256)
    t.require(str(uuid.UUID(operation))==operation,'canonical_qualification_operation_required')
    t.require(docker.command('info','--format','{{.ID}}',output_limit=4096).decode().strip()==daemon_id,
              'explicit_test_daemon_required')
    image=docker.image(image_id)
    t.require(image['Config'].get('Labels',{}).get('org.opencontainers.image.revision')==source_commit,
              'exact_qualification_image_required')
    outer=Path(artifact_root).absolute();t.require(outer.resolve()==outer and outer.is_dir(),'persistent_task_artifact_root_required')
    root=outer/operation;root.mkdir(mode=0o700)
    for name in ('tracked','custody'):(root/name).mkdir(mode=0o700)
    suffix=uuid.UUID(operation).hex[:16];authority=str(uuid.uuid5(uuid.UUID(operation),'synthetic-original-authority'))
    names={'state':'ltds-holder-state-'+suffix,'lease':'ltds-holder-lease-'+suffix}
    holder_name='ltds-holder-owner-'+suffix;seed_name='ltds-holder-seed-'+suffix
    tracked=t.TrackedExec(docker,operation,root/'tracked');result=None
    try:
        for name in names.values():
            volumes.append(name);owned_volume(docker,name,operation)
        # Copy-up of the existing image's10001-owned /prototype-state supplies
        # the new external volume owner. Only this NEW owned empty root's mode
        # changes; no chown/sudo or source/admin path is used.
        initialize_fixture(docker,image_id,operation,'ltds-holder-lease-prepare-'+suffix,names['lease'],
            'import pathlib,os,json\np=pathlib.Path("/prototype-state")\n'
            'assert p.stat().st_uid==p.stat().st_gid==10001 and next(p.iterdir(),None) is None\n'
            'p.chmod(0o711)\nprint(json.dumps({"uid":os.getuid(),"gid":os.getgid()}))\n')
        repository=Path(__file__).resolve().parents[1]
        fixture_path=repository/'tests/fixtures/enrolled_coordinator_daemon_fixture.py'
        spec=importlib.util.spec_from_file_location('owned_daemon_fixture',fixture_path)
        fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture)
        schema=(repository/'contracts/coordinator-693cb7e-schema.json').read_bytes()
        fixture_source=fixture.serializable_source(lease_directory='/run/qualification-original-lease',authority_id=authority,
                                                   schema_bytes=schema)
        seed_id=docker.command(*setup_arguments(image_id,operation,seed_name,names['state'],names['lease'],fixture_source),
                               mutation=True,timeout=20,output_limit=4096).decode().strip()
        t.require(t.ID.fullmatch(seed_id),'exact_synthetic_writer_required');docker.start(seed_id)
        t.require(docker.command('container','wait',seed_id,timeout=15,output_limit=4096).decode().strip()=='0',
                  'fixture_seed_exit_required')
        seeded=t.decode(docker.command('container','logs',seed_id,output_limit=16384))
        seed=docker.inspect(seed_id)
        t.require(seeded['expectedFileCount']==202 and seeded['expectedDirectoryCount']==13 and
                  seed['State']['Running'] is False and seed['State']['Pid']==0 and seed['State']['ExitCode']==0,
                  'full_fixture_stopped_original_writer_required')
        checkpoint(completed,'standalone_frozen693_uid10001_fixture_and_stopped_writer')
        lease=owned_volume_readback(docker,names['lease'],operation)
        external=lease['Mountpoint']  # ONLY the returned new-owned volume path.
        physical_source=t.physical_binding_script(authority,'/run/qualification-original-lease')
        physical=t.decode(docker.command('run','--rm','--name','ltds-holder-physical-seed-'+suffix,
            '--label',TASK_LABEL+'='+operation,'--network','none','--read-only','--user','10001:10001',
            '--cpus','0.125','--memory','128m','--memory-swap','128m','--pids-limit','32','--cap-drop','ALL',
            '--security-opt','no-new-privileges','--mount','type=volume,source='+names['state']+',target=/prototype-state,readonly',
            '--mount','type=bind,source='+external+',target=/run/qualification-original-lease',
            '--entrypoint','python',image_id,'-I','-c',physical_source.decode(),mutation=True,timeout=15,output_limit=16384))
        # Explicit component fixture profile: production real693 ID/bundle
        # enrollment is NOT claimed. Observation/recipe/host methods unchanged.
        plan=dict(operationId=operation,authorityId=authority,predecessor=dict(containerId=seed_id),
                  candidate=dict(imageId=image_id,sourceCommit=source_commit),bindings=dict(
                    stateSha256=t.digest(physical['state']),authorityLeaseSha256=t.digest(physical['authorityLease'])))
        settings=dict(schemaVersion=1,operationId=operation,planSha256=t.digest(plan),authorityId=authority,
                      stateRoot='/prototype-state',leaseDirectory='/run/qualification-original-lease')
        environment=b'PYTHONNOUSERSITE=1\n'
        env=root/'holder.env';env.write_bytes(environment);env.chmod(0o600)
        recipe=dict(schemaVersion=1,name=holder_name,imageId=image_id,user='10001:10001',workingDir='/app',
            environmentFile=str(env),environmentSha256=hashlib.sha256(environment).hexdigest(),
            mounts=[dict(type='volume',source=names['state'],target='/prototype-state',readOnly=True),
                    dict(type='bind',source=external,target=settings['leaseDirectory'],readOnly=False)],
            ports=[],network='none',tmpfs={'/tmp':'rw,nosuid,nodev,noexec,size=64m,mode=1777'},labels={TASK_LABEL:operation},
            command=['python','-m','processing.app'],entrypoint=None,readOnly=True,init=False,pidsLimit=64,
            memoryBytes=256*1048576,nanoCpus=250000000,stopSignal='SIGTERM',stopTimeout=-1,
            restartPolicy={'Name':'no','MaximumRetryCount':0})
        creation=str(uuid.uuid4());args=t.create_arguments(recipe,operation_id=operation,role='lease-holder',
                                                          intent_id=creation,holder_settings=settings)
        holder_id=docker.command(*args,mutation=True,timeout=20,output_limit=4096).decode().strip()
        t.require(t.ID.fullmatch(holder_id),'exact_created_holder_required');docker.start(holder_id)
        bundle=dict(host=dict(dockerBinary=docker.binary,dockerBinarySha256=expected_cli_sha256,
             dockerEngineId=daemon_id,bootId=Path('/proc/sys/kernel/random/boot_id').read_text().strip()),
             predecessor=dict(stateVolume=names['state']),recipes=dict(holder=recipe))
        boundary=FixtureBoundaryProbe(plan,bundle,settings,docker,tracked)
        context=dict(containerId=holder_id,creationIntentId=creation,gate=None)
        # New readonly challenge after every known-retired unsuccessful readiness
        # probe. Never retry an unresolved dispatch or accept absence as ready.
        deadline=time.monotonic()+10;observed=None
        while observed is None:
            request=str(uuid.uuid4());source=holder.serializable_source(settings,observer=True)
            try:packet=tracked.dispatch(holder_id,request,source,timeout=5,maximum=4096)
            except t.TransportHeld:
                if docker.mutation_unknown:raise
                packet=tracked.inspect(holder_id,request,source,maximum=4096,allow_failed=True)
            if packet['bodySucceeded']:observed=packet['result']
            else:t.require(time.monotonic()<deadline,'actual_holder_readiness_deadline');time.sleep(.05)
        t.require(observed['pid']==1,'non_init_actual_pid1_required')
        boundary('holder',context)
        t.require(run_probe(docker,image_id,operation,suffix+'-held',names['state'],external,authority)==
                  {'mainAvailable':False,'externalAvailable':False},'both_original_kernel_flocks_required')
        checkpoint(completed,'production_closed_pid1_recipe_positive_socket_and_both_original_flocks')
        drop=DropConfirmedReply(docker);lost=t.TrackedExec(drop,operation,root/'tracked')
        request=str(uuid.uuid4());marker='/tmp/once-'+request
        once=('import json,os,pathlib\np=pathlib.Path('+repr(marker)+')\n'
              'fd=os.open(p,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)\nos.write(fd,b"once");os.fsync(fd);os.close(fd)\n'
              'print(json.dumps({"once":True,"pid":os.getpid()}))\nraise SystemExit(0)\n').encode()
        try:lost.dispatch(holder_id,request,once,timeout=10,maximum=4096)
        except t.TransportHeld as error:t.require(str(error)=='qualification_dropped_confirmed_dispatch_reply','expected_deliberate_lost_reply')
        else:raise t.TransportHeld('deliberate_lost_reply_not_exercised')
        recovered=lost(holder_id,once,request_id=request,timeout=10,output_limit=4096)
        again=lost(holder_id,once,request_id=request,timeout=10,output_limit=4096)
        t.require(recovered==again and recovered['bodySucceeded'] is True and recovered['process']['retired'] is True
                  and recovered['result']['once'] is True and drop.dispatched==1,'actual_retired_once_only_SystemExit0_required')
        checkpoint(completed,'tracked_SystemExit0_lost_reply_recovery_without_second_body')
        modules={name:(repository/'ops'/(name+'.py')).read_bytes() for name in MODULE_NAMES}
        key=secrets.token_bytes(32);key_path=root/'synthetic-census.key';key_path.write_bytes(key);key_path.chmod(0o600)
        # Fail-closed sentinel: this component DOES NOT authorize projection.
        policy=t.canonical(dict(profile='qualification-only-no-projection',projectionQualified=False))
        counts=t.canonical(dict(jobs=22,viewer_tasks=7,viewer_reservations=0,viewer_option_evidence=7,
            viewer_files=155,reference_jobs=0,owned_maintenance_schedules=16))
        ops=CustodyOps(docker,holder_id=holder_id,holder_settings=settings,operation_directory=root/'custody',
            key=key,key_sha256=hashlib.sha256(key).hexdigest(),module_sources=modules,
            module_sha256={name:hashlib.sha256(raw).hexdigest() for name,raw in modules.items()},
            reviewed_schema_bytes=schema,policy_bytes=policy,policy_sha256=hashlib.sha256(policy).hexdigest(),
            reviewed_counts_bytes=counts,reviewed_counts_sha256=hashlib.sha256(counts).hexdigest(),
            tracked_exec=tracked,boundary_check=boundary,max_artifact_bytes=16*1048576,max_backup_bytes=64*1048576,
            max_seconds=15,safety_floor_bytes=1048576)
        intent=lambda action:dict(schemaVersion=1,operationId=operation,planSha256=settings['planSha256'],
            intentId=str(uuid.uuid5(uuid.UUID(operation),action)),direction='forward',step=0,action=action)
        baseline=ops.capture(intent('capture-full-custody'),context=context)
        copy_intent=intent('copy-verified-backup')
        dispatch=dict(intentSha256=t.digest(copy_intent),attempt=1,dispatchId=str(uuid.uuid5(uuid.UUID(operation),'backup-dispatch')))
        backup=ops.backup(baseline,copy_intent,dispatch=dispatch,context=context)
        t.require(ops.inspect_backup(baseline,copy_intent,dispatch=dispatch,context=context)==backup,'actual_backup_readback_required')
        retained=ops.validate_backup_artifact(baseline,backup)
        t.require(retained['tarValidation']['fileCount']==202 and retained['tarValidation']['directoryCount']==13 and
                  retained['originalAuthorityProven'] is False and retained['restoreAuthorized'] is False,
                  'actual_full_stream_tar_custody_required')
        checkpoint(completed,'actual_tracked_full_census_stream_tar_readback_202files13dirs')
        long_state=start_long_observer(docker,tracked,holder_id,operation)
        docker.command('container','kill','--signal','TERM',holder_id,mutation=True,timeout=10,output_limit=4096)
        time.sleep(.2)
        state=docker.inspect(holder_id)['State']
        t.require(state['Running'] is True and state['Pid']>0,'holder_must_wait_for_live_namespace_observer')
        live=long_observer_status(tracked,holder_id,long_state)
        t.require(live['live'] is True,'exact_long_observer_must_still_be_live')
        t.require(run_probe(docker,image_id,operation,suffix+'-draining',names['state'],external,authority)==
                  {'mainAvailable':False,'externalAvailable':False},'namespace_drain_must_retain_both_original_flocks')
        checkpoint(completed,'SIGTERM_retains_original_flocks_while_exact_owned_observer_alive')
        release_long_observer(docker,holder_id,long_state);long_state['released']=True
        t.require(docker.command('container','wait',holder_id,timeout=15,output_limit=4096).decode().strip()=='0',
                  'holder_graceful_namespace_exit_required')
        state=docker.inspect(holder_id)['State']
        t.require(state['Running'] is False and state['Pid']==0 and state['ExitCode']==0,'actual_holder_exit0_required')
        t.require(run_probe(docker,image_id,operation,suffix+'-released',names['state'],external,authority)==
                  {'mainAvailable':True,'externalAvailable':True},'original_kernel_leases_available_after_exact_release')
        checkpoint(completed,'explicit_observer_release_holder_exit0_and_original_leases_available')
        result=dict(schemaVersion=1,operationId=operation,completed=completed,imageId=image_id,sourceCommit=source_commit,
            fixtureProfile='component-owned-synthetic693',projectionQualified=False,fullCutoverQualified=False,
            productionHostEnrollmentQualified=False,desktopReadonly0777Exemption=True,
            backupArchiveSha256=retained['tarValidation']['archiveSha256'],backupFileCount=202,backupDirectoryCount=13,backupControllerUid=os.geteuid(),
            privateArtifactDirectory=str(root))
    except BaseException:
        print(json.dumps(dict(completedBeforeFailure=completed,operationId=operation,privateArtifactDirectory=str(root))),flush=True)
        raise
    finally:
        if long_state and not long_state.get('released') and not docker.mutation_unknown:
            try:release_long_observer(docker,holder_id,long_state);long_state['released']=True
            except BaseException:docker.mutation_unknown=True
        cleanup_owned(docker,operation,volumes)
    return result


def owned_volume_readback(docker,name,operation):
    item=t.decode(docker.command('volume','inspect',name,output_limit=65536))[0]
    t.require(item['Name']==name and item.get('Labels',{}).get(TASK_LABEL)==operation,'exact_owned_external_volume_required')
    t._absolute_path(item['Mountpoint']);return item


def start_long_observer(docker,tracked,identifier,operation):
    request=str(uuid.uuid4());prefix='/tmp/owned-long-'+request
    source=('import os,json,time,pathlib\npid=os.getpid();start=pathlib.Path("/proc/self/stat").read_text().rsplit(")",1)[1].split()[19]\n'
        'value={"operationId":'+repr(operation)+',"requestId":'+repr(request)+',"pid":pid,"start":start}\n'
        'fd=os.open('+repr(prefix+'.start')+',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)\n'
        'os.write(fd,json.dumps(value,sort_keys=True,separators=(",",":")).encode());os.fsync(fd);os.close(fd)\n'
        'while not pathlib.Path('+repr(prefix+'.release')+').exists():time.sleep(.02)\n')
    docker.command('container','exec','--detach','--user','10001:10001',identifier,'python','-I','-c',source,
                   mutation=True,timeout=10,output_limit=4096)
    expected=dict(operationId=operation,requestId=request,prefix=prefix,released=False)
    try:
        expected.update(long_observer_status(tracked,identifier,expected,wait=True))
        t.require(expected['live'] is True,'started_exact_owned_observer_required')
        return expected
    except BaseException:
        # Accepted detached dispatch with no exact start observation is unknown.
        docker.mutation_unknown=True;raise


def long_observer_status(tracked,identifier,value,wait=False):
    source=('import pathlib,json,time\np=pathlib.Path('+repr(value['prefix']+'.start')+')\n'
        'deadline=time.monotonic()+5\n'
        'while not p.exists():\n'
        ' if time.monotonic()>deadline:raise RuntimeError("owned_observer_start_unavailable")\n'
        ' time.sleep(.02)\n'
        'v=json.loads(p.read_bytes());assert v["operationId"]=='+repr(value['operationId'])+' and v["requestId"]=='+repr(value['requestId'])+'\n'
        'stat=pathlib.Path("/proc/"+str(v["pid"])+"/stat")\n'
        'v["live"]=stat.exists() and stat.read_text().rsplit(")",1)[1].split()[19]==v["start"]\n'
        'print(json.dumps(v))\n').encode()
    packet=tracked(identifier,source,request_id=str(uuid.uuid4()),timeout=10,output_limit=4096)
    t.require(packet['bodySucceeded'] is True and packet['process']['retired'] is True,'retired_exact_observer_status_required')
    return packet['result']


def release_long_observer(docker,identifier,value):
    source=('import pathlib,json,os\nv=json.loads(pathlib.Path('+repr(value['prefix']+'.start')+').read_bytes())\n'
        'assert v["operationId"]=='+repr(value['operationId'])+' and v["requestId"]=='+repr(value['requestId'])+'\n'
        'assert v["pid"]=='+repr(value['pid'])+' and v["start"]=='+repr(value['start'])+'\n'
        'assert pathlib.Path("/proc/"+str(v["pid"])+"/stat").read_text().rsplit(")",1)[1].split()[19]==v["start"]\n'
        'fd=os.open('+repr(value['prefix']+'.release')+',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600);os.write(fd,b"release");os.fsync(fd);os.close(fd)\n'
        'print(json.dumps({"released":True}))\n').encode()
    t.require(docker.exec_python(identifier,source,mutation=True,timeout=10,output_limit=4096)=={'released':True},
              'explicit_exact_observer_release_required')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('image','source-commit','expected-daemon-id','artifact-root','operation-id','expected-cli-sha256'):parser.add_argument('--'+name,required=True)
    args=parser.parse_args()
    try:result=qualify(args.image,args.source_commit,args.expected_daemon_id,args.artifact_root,args.operation_id,args.expected_cli_sha256)
    except Exception as error:
        print(json.dumps(dict(ok=False,error=str(error) if isinstance(error,t.TransportHeld) else type(error).__name__)),flush=True)
        return 1
    print(json.dumps(dict(ok=True,taskResourcesRemoved=True,**result)),flush=True);return 0


if __name__=='__main__':raise SystemExit(main())
