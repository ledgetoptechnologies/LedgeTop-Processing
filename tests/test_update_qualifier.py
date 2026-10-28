import copy
from dataclasses import asdict,replace
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

from processing.update_registry import DockerRegistry,RegistryPolicy,RegistryChannel,_json,_sha
from processing.update_qualifier import StartupPolicy,StartupQualifier,QualifierError,QualifierHeld,PROBE,PROBE_SHA,startup_spec,WRITE_PATHS
from processing.update_supervisor import UpdateSupervisor,SupervisorPolicy
from tests import test_update_registry as registry_fixture
from tests import test_update_supervisor as supervisor_fixture


DEFAULTS=dict(User='',WorkingDir='/var/www',Entrypoint=['/usr/bin/node'],Cmd=['/var/www/index.js'],Volumes=None)


class FakeDocker:
    def __init__(self):
        self.registry=registry_fixture.FakeDocker();self.calls=[];self.defaults=copy.deepcopy(DEFAULTS)
        self.id='9'*64;self.running=False;self.removed=False;self.created=False;self.binding=None
        self.exit_code=0
        self.mismatch={};self.faults={};self.wire=None;self.engine='test-engine';self.after=None
        self.info=dict(version='2.3.1',engine='odx',engineVersion='3.8.3')
        self.options=[dict(name='numeric-limit',type='int',value=7,min=1,max=20),dict(name='bool-flag',type='bool',value=False)]

    def inspect(self):
        host=dict(NetworkMode='none',ReadonlyRootfs=False,Privileged=False,
            Tmpfs={path:'rw,nosuid,nodev,mode=1777' for path in WRITE_PATHS},PortBindings={},RestartPolicy={'Name':'no'},
            AutoRemove=False,CapDrop=['ALL'],SecurityOpt=['no-new-privileges'],Binds=None,Devices=[],DeviceRequests=None,
            PidMode='',IpcMode='private',NanoCpus=0,Memory=0,PidsLimit=None,LogConfig={'Type':'none'})
        value=dict(id=self.id,image=self.registry.image['Id'],running=self.running,status='running' if self.running else 'exited' if self.started else 'created',
            pid=4321 if self.running else 0,exitCode=self.exit_code,binding=self.binding,
            config=dict(User='0:0',WorkingDir='/var/www',Entrypoint=self.defaults['Entrypoint'],Cmd=self.defaults['Cmd']),host=host,mounts=[])
        for key,item in self.mismatch.items():
            if key in ('host','config'):value[key].update(item)
            else:value[key]=item
        return value

    def __call__(self,command,*,timeout,max_bytes):
        args=command[3:];self.calls.append(args)
        kind=args[0]
        if kind=='container':kind=args[1]
        fault=self.faults.get(kind)
        if fault=='before':raise TimeoutError('private error do-not-print')
        if args[0]=='info':result=self.engine
        elif args[:2]==['image','inspect'] and '.Config.User' in args[3]:result=_json(self.defaults)
        elif args[:2]==['container','inspect']:
            if not self.created or self.removed:raise OSError('container absent')
            result=_json(self.inspect())
        elif args[:2]==['container','ls']:result='' if self.removed or not self.created else self.id
        elif kind=='create':
            self.created=True;self.started=False;self.removed=False
            self.binding=args[args.index('--label')+1].split('=',1)[1];result=self.id
        elif kind=='start':self.running=True;self.started=True;result=self.id
        elif kind=='exec':result=self.wire if self.wire is not None else _json(dict(schemaVersion=1,info=_json(self.info),options=_json(self.options)))
        elif kind=='stop':self.running=False;result=self.id
        elif kind=='rm':self.removed=True;result=self.id
        else:return self.registry(command,timeout=timeout,max_bytes=max_bytes)
        if self.after:self.after(kind,args)
        if fault=='after':raise TimeoutError('private response do-not-print')
        return subprocess.CompletedProcess(command,0,result,b'')

    def mutations(self):return [args[0] for args in self.calls if args[0] in ('create','start','exec','stop','rm')]


@unittest.skipUnless(sys.platform.startswith('linux'),'Linux host qualifier, private state and flock')
class QualifierTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.docker=FakeDocker();self.authority=str(uuid.uuid4())
        registry_policy=RegistryPolicy(sys.executable,'unix:///var/run/docker.sock','test-engine',
            (RegistryChannel('test',registry_fixture.REPO,'approved-test'),),4096)
        self.registry=DockerRegistry(self.root/'registry.sqlite',registry_policy,capacity_admit=lambda *_:True,runner=self.docker)
        self.candidate=self.registry.discover('test');self.registry.stage(self.candidate)
        self.docker.calls.clear()
        self.policy=StartupPolicy('node-startup-only-v1','test-engine','main',self.authority,'odx',
            startup_spec(DEFAULTS),str(self.root),'0:0')
        self.qualifier=self.open()
        self.request=self.make_request()

    def open(self,policy=None):return StartupQualifier(self.root/'qualifier.sqlite',policy or self.policy,registry=self.registry,runner=self.docker)

    def make_request(self,operation='d'*64):
        path=self.root
        for component in ('receipts',operation,'qualification'):
            path=path/component;path.mkdir(mode=0o700,exist_ok=True)
        value=dict(schemaVersion=1,operationId=operation,nodeId='main',engineId='test-engine',authorityId=self.authority,
            candidateId=self.candidate['candidateId'],repository=self.candidate['repository'],manifestDigest=self.candidate['manifestDigest'],
            configDigest=self.candidate['configDigest'],localImageId=self.docker.registry.image['Id'],platform='linux/amd64',previousImageId=supervisor_fixture.OLD,hostGeneration=0,
            supervisorPolicySha256='f'*64,qualifierId=self.policy.qualifier_id,qualificationPolicySha256=self.qualifier.policy_sha256,
            receiptDirectory=str(path))
        return dict(value,requestSha256=_sha(value))

    def test_real_registry_proof_full_durable_lifecycle_and_exact_supervisor_receipt(self):
        receipt=self.qualifier.qualify(self.request)
        self.assertEqual(self.docker.mutations(),['create','start','exec','stop','rm'])
        self.assertEqual(UpdateSupervisor._receipt(receipt,self.request,'qualification'),receipt)
        self.assertTrue(self.docker.removed);self.assertFalse(self.docker.running)
        with self.qualifier._exclusive():
            row=self.qualifier._row(self.request['requestSha256']);evidence=json.loads(row['evidence'])
            with self.qualifier._db() as db:self.assertEqual(db.execute('PRAGMA synchronous').fetchone()[0],2)
        self.assertEqual(evidence['scope'],'startup-api-options-only-v1')
        self.assertEqual(evidence['filesystemMode'],'ephemeral-node-defaults-v1')
        self.assertTrue(evidence['containerWritableLayerDisposed'])
        self.assertFalse(evidence['reconstructionQualified']);self.assertFalse(evidence['gpuKernelsQualified'])
        self.assertEqual(evidence['optionsSha256'],_sha(sorted(self.docker.options,key=lambda item:item['name'])))
        self.assertNotIn('Cmd',row['defaults']);self.assertNotIn('/var/www/index.js',row['defaults'])
        before=(self.root/'qualifier.sqlite').read_bytes()
        self.assertEqual(self.open().lookup(self.request),receipt)
        self.assertEqual((self.root/'qualifier.sqlite').read_bytes(),before)
        self.assertEqual(self.open().qualify(self.request),receipt)
        self.assertEqual(self.docker.mutations(),['create','start','exec','stop','rm'])

    def manifest_store(self):
        self.docker.registry.image.update(Id=self.candidate['manifestDigest'],Descriptor={
            key:self.docker.registry.selected['Descriptor'][key] for key in ('mediaType','digest','size')})
        metadata=self.registry.reconcile(self.candidate)
        self.assertEqual(metadata['imageIdKind'],'manifest')
        self.request=self.make_request()
        return metadata

    def test_manifest_store_uses_actual_local_manifest_id_and_retains_config_descriptor(self):
        metadata=self.manifest_store()
        receipt=self.qualifier.qualify(self.request)
        self.assertEqual(receipt['localImageId'],self.candidate['manifestDigest'])
        self.assertEqual(receipt['configDigest'],registry_fixture.CONFIG)
        self.assertNotEqual(receipt['localImageId'],receipt['configDigest'])
        self.assertEqual(UpdateSupervisor._receipt(receipt,self.request,'qualification'),receipt)
        create=next(args for args in self.docker.calls if args[0]=='create')
        self.assertEqual(create[-1],metadata['imageId'])
        defaults=[args for args in self.docker.calls if args[:2]==['image','inspect'] and '.Config.User' in args[3]]
        self.assertTrue(defaults);self.assertTrue(all(args[-1]==metadata['imageId'] for args in defaults))
        with self.qualifier._exclusive():
            row=self.qualifier._row(self.request['requestSha256'])
        self.assertEqual(json.loads(row['stopped'])['image'],metadata['imageId'])
        evidence=json.loads(row['evidence'])
        self.assertEqual(evidence['imageIdKind'],'manifest')
        self.assertEqual(evidence['configDigest'],registry_fixture.CONFIG)
        self.assertEqual(evidence['localImageId'],metadata['imageId'])
        self.assertEqual(self.open().lookup(self.request),receipt)

    def test_manifest_store_does_not_silently_alias_config_as_runtime_image(self):
        self.manifest_store()
        request=dict(self.request,localImageId=self.candidate['configDigest'])
        request['requestSha256']=_sha({key:value for key,value in request.items() if key!='requestSha256'})
        with self.assertRaises(QualifierHeld):self.qualifier.qualify(request)
        self.assertEqual(self.docker.mutations(),[])
        self.docker.mismatch={'image':self.candidate['configDigest']}
        with self.assertRaises(QualifierHeld):self.qualifier.qualify(self.request)
        self.assertEqual(self.docker.mutations(),['create'])

    def test_manifest_id_without_matching_descriptor_is_not_runtime_proof(self):
        actual={key:self.docker.registry.selected['Descriptor'][key] for key in ('mediaType','digest','size')}
        for descriptor in (None,dict(actual,digest='sha256:'+'e'*64),dict(actual,size=actual['size']+1),
                dict(actual,mediaType='application/vnd.oci.image.index.v1+json')):
            with self.subTest(descriptor=descriptor):
                self.docker.registry.image.update(Id=self.candidate['manifestDigest'],Descriptor=descriptor)
                self.request=self.make_request()
                with self.assertRaises(QualifierHeld):self.qualifier.qualify(self.request)
        self.assertEqual(self.docker.mutations(),[])

    def test_create_has_no_mount_port_gpu_or_processing_resource_caps(self):
        self.qualifier.qualify(self.request)
        create=next(args for args in self.docker.calls if args[0]=='create')
        for denied in ('--cpus','--memory','--pids-limit','--gpus','--mount','--volume','-p','--publish','--privileged'):
            self.assertNotIn(denied,create)
        self.assertEqual(create[create.index('--network')+1],'none')
        self.assertNotIn('--read-only',create)
        self.assertFalse(any('THREAD' in value or 'concurrency' in value for value in create))
        executed=next(args for args in self.docker.calls if args[0]=='exec')
        self.assertEqual(executed,['exec',self.docker.id,'/usr/bin/node','-e',PROBE])
        self.assertNotIn('POST',PROBE);self.assertNotIn('/task',PROBE)
        self.assertFalse(any(args[0]=='pull' for args in self.docker.calls))

    def test_unknown_create_start_exec_or_stop_never_replayed_or_cleaned_by_lookup(self):
        for kind in ('create','start','exec','stop'):
            with self.subTest(kind=kind):
                root=self.root/kind;root.mkdir(mode=0o700)
                policy=replace(self.policy,qualifier_id='startup-'+kind,supervisor_state_root=str(self.root))
                qualifier=StartupQualifier(root/'journal.sqlite',policy,registry=self.registry,runner=self.docker)
                request=dict(self.request,qualifierId=policy.qualifier_id,qualificationPolicySha256=qualifier.policy_sha256)
                request['requestSha256']=_sha({key:value for key,value in request.items() if key!='requestSha256'})
                self.docker.faults={kind:'after'};before=len(self.docker.mutations())
                with self.assertRaises(QualifierHeld):qualifier.qualify(request)
                attempted=self.docker.mutations()[before:]
                restarted=StartupQualifier(root/'journal.sqlite',policy,registry=self.registry,runner=self.docker)
                with self.assertRaises(QualifierHeld):restarted.qualify(request)
                with self.assertRaises(QualifierHeld):restarted.lookup(request)
                self.assertEqual(self.docker.mutations()[before:],attempted)
                self.assertNotIn('rm',attempted)

    def test_lost_remove_response_recovers_only_prior_verified_stop_and_exact_id_absence(self):
        self.docker.faults['rm']='after'
        with self.assertRaises(QualifierHeld):self.qualifier.qualify(self.request)
        before=list(self.docker.mutations());receipt=self.open().lookup(self.request)
        self.assertTrue(receipt['passed']);self.assertEqual(self.docker.mutations(),before)
        listings=[args for args in self.docker.calls if args[:2]==['container','ls']]
        self.assertTrue(all('id='+self.docker.id in args and '--no-trunc' in args for args in listings))

    def test_missing_or_renamed_container_before_recorded_stop_does_not_manufacture_success(self):
        self.docker.faults['exec']='after'
        with self.assertRaises(QualifierHeld):self.qualifier.qualify(self.request)
        self.docker.removed=True
        with self.assertRaises(QualifierHeld):self.open().lookup(self.request)
        self.assertNotIn('rm',self.docker.mutations())

    def test_unknown_removal_with_container_still_present_or_list_error_is_held(self):
        self.docker.faults['rm']='before'
        with self.assertRaises(QualifierHeld):self.qualifier.qualify(self.request)
        before=list(self.docker.mutations())
        with self.assertRaises(QualifierHeld):self.open().lookup(self.request)
        self.docker.faults['ls']='before';self.docker.removed=True
        with self.assertRaises(QualifierHeld):self.open().lookup(self.request)
        self.assertEqual(self.docker.mutations(),before)

    def test_unstaged_or_wrong_local_image_refused_before_create(self):
        self.docker.registry.image['Id']=self.candidate['manifestDigest']
        with self.assertRaises(QualifierHeld):self.qualifier.qualify(self.request)
        self.assertEqual(self.docker.mutations(),[])
        self.docker.registry.image['Id']=self.candidate['configDigest']
        with self.registry._db() as db:db.execute("UPDATE registry_staging SET state='held'")
        with self.assertRaises(QualifierHeld):self.qualifier.qualify(self.request)
        self.assertEqual(self.docker.mutations(),[])

    def test_wrong_reviewed_defaults_or_credential_args_refused_before_create(self):
        self.docker.defaults['Cmd']=['/var/www/changed.js']
        with self.assertRaises(QualifierHeld):self.qualifier.qualify(self.request)
        self.docker.defaults['Cmd']=['/var/www/index.js','--token','do-not-print']
        with self.assertRaises(QualifierHeld) as error:self.qualifier.qualify(self.request)
        self.assertNotIn('do-not-print',str(error.exception));self.assertEqual(self.docker.mutations(),[])
        self.assertNotIn(b'do-not-print',(self.root/'qualifier.sqlite').read_bytes())

    def test_foreign_container_label_image_or_mount_never_started_or_removed(self):
        for change in (dict(binding='e'*64),dict(image='sha256:'+'e'*64),dict(mounts=[dict(Type='bind',Source='/nas',Destination='/var/www/data',RW=True)])):
            with self.subTest(change=change):
                path=self.root/('case'+str(len(self.docker.calls)));path.mkdir(mode=0o700)
                qualifier=StartupQualifier(path/'journal.sqlite',self.policy,registry=self.registry,runner=self.docker)
                self.docker.mismatch=change;before=len(self.docker.mutations())
                with self.assertRaises(QualifierHeld):qualifier.qualify(self.request)
                self.assertEqual(self.docker.mutations()[before:],['create'])

    def test_unexpected_readonly_root_network_caps_or_default_cmd_spec_fails(self):
        changes=[{'host':{'ReadonlyRootfs':True}},{'host':{'NetworkMode':'bridge'}},{'host':{'NanoCpus':2*10**9}},
                 {'host':{'DeviceRequests':[{'Count':-1}]}},{'config':{'Cmd':['changed']}}]
        for index,change in enumerate(changes):
            path=self.root/str(index);path.mkdir(mode=0o700)
            qualifier=StartupQualifier(path/'journal.sqlite',self.policy,registry=self.registry,runner=self.docker)
            self.docker.mismatch=change;before=len(self.docker.mutations())
            with self.assertRaises(QualifierHeld):qualifier.qualify(self.request)
            self.assertEqual(self.docker.mutations()[before:],['create'])

    def test_filesystem_mode_is_single_frozen_reviewed_profile(self):
        for mode in ('read-only','writable','ephemeral-node-defaults-v2',''):
            with self.subTest(mode=mode),self.assertRaises(QualifierError):replace(self.policy,filesystem_mode=mode)

    def test_writable_layer_disposal_cannot_be_claimed_before_exact_id_absence(self):
        self.docker.faults['rm']='before'
        with self.assertRaises(QualifierHeld):self.qualifier.qualify(self.request)
        with self.qualifier._exclusive():row=self.qualifier._row(self.request['requestSha256'])
        evidence=json.loads(row['evidence'])
        self.assertEqual(evidence['filesystemMode'],'ephemeral-node-defaults-v1')
        self.assertNotIn('containerWritableLayerDisposed',evidence)
        self.assertIsNone(row['receipt'])
        with self.assertRaises(QualifierHeld):self.open().lookup(self.request)

    def test_old_filesystem_profile_binding_and_held_intent_are_not_converted(self):
        old_policy=asdict(self.policy);old_policy.pop('filesystem_mode')
        old_hash=_sha(dict(policy=old_policy,registryPolicy=asdict(self.registry.policy),probeSha256=PROBE_SHA,
            scope='startup-api-options-only-v1'))
        self.assertNotEqual(old_hash,self.qualifier.policy_sha256)
        self.docker.faults['exec']='after'
        with self.assertRaises(QualifierHeld):self.qualifier.qualify(self.request)
        with self.qualifier._exclusive():
            with self.qualifier._db() as db:
                binding=json.loads(db.execute('SELECT value FROM qualifier_binding').fetchone()[0])
                binding['policySha256']=old_hash
                db.execute('UPDATE qualifier_binding SET value=?',(_json(binding),))
                original=dict(db.execute('SELECT * FROM qualifications').fetchone())
        before=list(self.docker.mutations())
        with self.assertRaises(QualifierHeld):self.open()
        with self.qualifier._exclusive():self.assertEqual(self.qualifier._row(original['id']),original)
        self.assertEqual(original['state'],'exec_intent')
        self.assertEqual(self.docker.mutations(),before)

    def test_actual_dialect_malformed_duplicate_options_or_secret_response_never_passes(self):
        cases=[dict(schemaVersion=1,info=_json(dict(self.docker.info,engine='odm')),options=_json(self.docker.options)),
            dict(schemaVersion=1,info='{"engine":"odx","engine":"odm"}',options='[]'),
            dict(schemaVersion=1,info=_json(self.docker.info),options=_json(self.docker.options*2)),
            dict(schemaVersion=1,info=_json(self.docker.info),options='[{"name":"x","password":"do-not-print"}]')]
        for index,wire in enumerate(cases):
            path=self.root/('wire'+str(index));path.mkdir(mode=0o700)
            qualifier=StartupQualifier(path/'journal.sqlite',self.policy,registry=self.registry,runner=self.docker)
            self.docker.wire=_json(wire);before=len(self.docker.mutations())
            with self.assertRaises(QualifierHeld) as error:qualifier.qualify(self.request)
            self.assertNotIn('do-not-print',str(error.exception));self.assertNotIn('rm',self.docker.mutations()[before:])
            self.assertNotIn(b'do-not-print',(path/'journal.sqlite').read_bytes())

    def test_changed_policy_request_root_or_engine_cannot_be_rebound(self):
        with self.assertRaises(QualifierHeld):self.open(replace(self.policy,expected_engine='odm'))
        for key,value in (('authorityId',str(uuid.uuid4())),('receiptDirectory','/tmp/unowned'),('manifestDigest','sha256:'+'e'*64)):
            request=dict(self.request,**{key:value});request['requestSha256']=_sha({key:item for key,item in request.items() if key!='requestSha256'})
            with self.assertRaises(QualifierHeld):self.qualifier.qualify(request)
        self.docker.engine='another-engine'
        with self.assertRaises(QualifierHeld):self.qualifier.qualify(self.request)
        self.assertEqual(self.docker.mutations(),[])

    def test_intent_commit_precedes_every_mutation_and_unknown_commit_does_not_start(self):
        def observe(kind,args):
            if kind not in ('create','start','exec','stop','rm'):return
            with self.qualifier._db() as db:state=db.execute('SELECT state FROM qualifications').fetchone()[0]
            self.assertEqual(state,{'create':'create_intent','start':'start_intent','exec':'exec_intent','stop':'stop_intent','rm':'remove_intent'}[kind])
        self.docker.after=observe;self.qualifier.qualify(self.request)
        self.assertEqual(len(self.docker.mutations()),5)

    def test_lost_checkpoint_after_verified_removal_is_reconciled_without_mutations(self):
        original=self.qualifier._commit
        def lost(db):
            row=db.execute('SELECT state FROM qualifications').fetchone()
            original(db)
            if row and row[0]=='completed':raise OSError('lost commit ack')
        with patch.object(self.qualifier,'_commit',side_effect=lost):
            with self.assertRaises(QualifierHeld):self.qualifier.qualify(self.request)
        before=list(self.docker.mutations());receipt=self.open().lookup(self.request)
        self.assertTrue(receipt['passed']);self.assertEqual(self.docker.mutations(),before)

    def test_corrupt_terminal_or_clean_output_evidence_cannot_turn_absence_into_success(self):
        self.docker.faults['rm']='after'
        with self.assertRaises(QualifierHeld):self.qualifier.qualify(self.request)
        with self.qualifier._exclusive():
            row=self.qualifier._row(self.request['requestSha256']);stopped=json.loads(row['stopped'])
            stopped['status']='restarting'
            self.qualifier._phase(row['id'],'remove_intent',stopped=stopped)
        with self.assertRaises(QualifierHeld):self.open().lookup(self.request)

    def test_only_successful_same_engine_query_proves_container_absence(self):
        self.docker.faults['rm']='after'
        with self.assertRaises(QualifierHeld):self.qualifier.qualify(self.request)
        self.docker.engine='different-engine'
        with self.assertRaises(QualifierHeld):self.open().lookup(self.request)
        self.assertEqual(self.docker.mutations(),['create','start','exec','stop','rm'])

    def test_completed_evidence_is_immutable_not_repaired_by_lookup(self):
        self.qualifier.qualify(self.request)
        with self.qualifier._exclusive():
            row=self.qualifier._row(self.request['requestSha256']);evidence=json.loads(row['evidence'])
            evidence['containerId']='e'*64
            self.qualifier._phase(row['id'],'completed',evidence=evidence)
        before=list(self.docker.mutations())
        with self.assertRaises(QualifierHeld):self.open().lookup(self.request)
        self.assertEqual(self.docker.mutations(),before)

    def test_engine_crash_after_valid_probe_is_not_passing_startup_receipt(self):
        def crash(kind,args):
            if kind=='exec':self.docker.running=False;self.docker.exit_code=1
        self.docker.after=crash
        with self.assertRaises(QualifierHeld):self.qualifier.qualify(self.request)
        self.assertEqual(self.docker.mutations(),['create','start','exec'])
        with self.assertRaises(QualifierHeld):self.open().lookup(self.request)
        self.assertEqual(self.docker.mutations(),['create','start','exec'])

    def test_reentrant_or_competing_qualifier_cannot_mutate_same_journal(self):
        def attempt(kind,args):
            if kind!='exec':return
            with self.assertRaises(QualifierHeld):self.qualifier.lookup(self.request)
            with self.assertRaises(QualifierHeld):self.open()
        self.docker.after=attempt
        self.assertTrue(self.qualifier.qualify(self.request)['passed'])
        self.assertEqual(self.docker.mutations(),['create','start','exec','stop','rm'])

    def test_null_default_cmd_is_supported_with_explicit_node_entrypoint(self):
        defaults=dict(DEFAULTS,Entrypoint=['/usr/bin/node','/var/www/index.js'],Cmd=None)
        self.docker.defaults=defaults
        policy=replace(self.policy,startup_spec_sha256=startup_spec(defaults))
        qualifier=StartupQualifier(self.root/'nullcmd.sqlite',policy,registry=self.registry,runner=self.docker)
        request=dict(self.request,qualificationPolicySha256=qualifier.policy_sha256)
        request['requestSha256']=_sha({key:value for key,value in request.items() if key!='requestSha256'})
        self.assertTrue(qualifier.qualify(request)['passed'])

    def test_actual_supervisor_accepts_only_startup_scoped_policy_receipt(self):
        policy=SupervisorPolicy('main','test',supervisor_fixture.OLD,self.authority,self.policy.qualifier_id,
            self.qualifier.policy_sha256,'managed-update',4096)
        backend=supervisor_fixture.DurableAdapters(self.root/'adapter.sqlite',policy,'test-engine')
        supervisor=UpdateSupervisor(self.root/'supervisor.sqlite',policy,registry=self.registry,
            qualifier=self.qualifier,maintenance=supervisor_fixture.FixtureMaintenance(backend))
        with supervisor:
            identifier=supervisor.discover()['operationId']
            self.assertEqual(supervisor.advance(identifier)['state'],'staged')
            self.assertEqual(supervisor.advance(identifier)['state'],'qualified')
            self.assertEqual(backend.count('schedule'),0)
            self.assertEqual(self.docker.mutations(),['create','start','exec','stop','rm'])


@unittest.skipUnless(shutil.which('node'),'existing Node.js required; no package installation')
class ActualProbeTests(unittest.TestCase):
    def execute(self,trickle):
        # Actual fixed source, real loopback HTTP, dependency-injected port/timer
        # scaling only. No Docker, production origin, credential or image used.
        harness=r'''
const http=require('http'),vm=require('vm'),source=JSON.parse(process.argv[1]);
const server=http.createServer((req,res)=>{
 if(req.method!=='GET'||!['/info','/options'].includes(req.url)){process.exit(9);return;}
 res.writeHead(200,{'Content-Type':'application/json','Connection':'close'});
 if(TRICKLE){res.write(' ');setInterval(()=>res.write(' '),5);return;}
 res.end(req.url==='/info'?JSON.stringify({engine:'odx',version:'2.3.1',engineVersion:'3.8.3'}):JSON.stringify([{name:'n',type:'int',value:7}]));
});server.listen(0,'127.0.0.1',()=>{
 const wrapped={get:(options,callback)=>http.get({...options,port:server.address().port},callback)};
 vm.runInNewContext(source,{require:name=>{if(name!=='http')process.exit(10);return wrapped;},
 process,Buffer,Date,setTimeout:(fn,ms)=>setTimeout(fn,ms===65000?65:ms)});
 server.unref();
});'''.replace('TRICKLE','true' if trickle else 'false')
        return subprocess.run([shutil.which('node'),'-e',harness,json.dumps(PROBE)],stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,timeout=3,check=False)

    def test_actual_fixed_probe_successful_get_wire_without_task_post(self):
        result=self.execute(False)
        self.assertEqual(result.returncode,0,result.stderr.decode())
        response=json.loads(result.stdout);self.assertEqual(response['schemaVersion'],1)
        self.assertEqual(json.loads(response['info'])['engine'],'odx')
        self.assertEqual(json.loads(response['options'])[0]['value'],7)

    def test_actual_fixed_probe_hard_watchdog_terminates_infinite_trickling_get(self):
        start=time.monotonic();result=self.execute(True)
        self.assertEqual(result.returncode,2)
        self.assertLess(time.monotonic()-start,2)
        self.assertEqual(result.stdout,b'');self.assertEqual(result.stderr,b'')


if __name__=='__main__':unittest.main()
