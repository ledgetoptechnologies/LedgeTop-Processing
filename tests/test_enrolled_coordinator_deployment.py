"""Pure private-bundle binding; no Docker, host, NAS or service mutations."""
import copy
import unittest
import uuid
from unittest.mock import patch

from ops import enrolled_coordinator_deployment as d
from tests.test_enrolled_coordinator_cutover import plan as make_plan
from tests.test_enrolled_coordinator_transport import recipe


def bundle():
    plan=make_plan();recipes={}
    for role in ('bootstrap','final','holder'):
        r=recipe(bootstrap=role!='final');r['name']='synthetic-'+role
        r['environmentFile']='/private/'+role+'.env'
        r['mounts'][0]['source']=d.NAME+'-state'
        r['mounts'].append(dict(type='bind',source='/private/leases',target='/authority-leases',readOnly=False))
        if role=='holder':r['init']=False;r['mounts'][0]['readOnly']=True
        recipes[role]=r
    for role in ('bootstrap','final'):plan['candidate'][role+'RecipeSha256']=d.digest(recipes[role])
    files={name:dict(path='/private/'+name,sha256='5'*64) for name in d.FILES}
    for role in ('bootstrap','final'):
        files[role+'Config']['sha256']=plan['candidate'][role+'ConfigSha256']
        files[role+'Config']['path']='/private/tls/'+role+'.json'
        files[role+'Config']['containerPath']='/run/processing-reference/'+role+'.json'
    for role in ('bootstrap','final','holder'):
        files[role+'Environment']=dict(path=recipes[role]['environmentFile'],sha256=recipes[role]['environmentSha256'])
    files['policy']['sha256']=plan['candidate']['declaredTransformSha256']
    files['censusKey']['sha256']=plan['bindings']['censusKeySha256']
    files['reviewedCounts']['sha256']=plan['bindings']['reviewedCountsSha256']
    workers=[]
    for node in ('cluster','main'):
        workers.append(dict(nodeId=node,sshHost='192.0.2.1',sshUid=1000,settings=dict(schemaVersion=1,
            unitName='synthetic-'+node+'.service',unitFile='/private/worker.service',unitSha256='1'*64,
            pythonExecutable='/usr/bin/python3',configPath='/private/worker.json',sourceRoot='/private/source',
            sourceSha256='2'*64,readinessOrigin='https://127.0.0.1:1234',readinessCAFile='/private/ca.pem'),
            expected=dict(nodeId=node,workerId='worker-'+node,workerInstanceId='instance-'+node,
                authorityId=plan['authorityId'],engineId='synthetic-engine',gateGeneration=1,
                engineVersion='3.8.5',configSha256='3'*64,sourceSha256='2'*64,activation=None)))
    value=dict(schemaVersion=1,profile=d.PROFILE,operationId=plan['operationId'],authorityId=plan['authorityId'],
        candidateSourceCommit=plan['candidate']['sourceCommit'],reviewedSourceRoot='/private/reviewed-source',
        host=dict(dockerEngineId='synthetic-engine',bootId=str(uuid.uuid4()),dockerBinary='/usr/bin/docker',dockerBinarySha256='9'*64,
                  dockerSocket='unix:///run/docker.sock'),
        predecessor=dict(containerId=d.PREDECESSOR_ID,imageId=d.PREDECESSOR_IMAGE,sourceCommit=d.PREDECESSOR_SOURCE,
                         name=d.NAME,stateVolume=d.NAME+'-state',leaseDirectory='/authority-leases'),
        recipes=recipes,holderSettings=dict(schemaVersion=1,operationId=plan['operationId'],
            authorityId=plan['authorityId'],stateRoot='/prototype-state',leaseDirectory='/authority-leases'),
        artifacts=files,sourceHashes={name:'6'*64 for name in d.SOURCES},
        custodySources={name:'7'*64 for name in ('coordinator_predecessor_census','coordinator_candidate_projection','coordinator_startup_policy')},
        budgets=dict(maxArtifactBytes=2*1024**3,maxBackupBytes=4*1024**3,maxMetadataBytes=64*1024**2,maxSeconds=300,safetyFloorBytes=64*1024**2),
        nodeEndpoints=[dict(id=node,url='http://192.0.2.1:3000') for node in ('cluster','main')],workers=workers,
        qualifications={name:dict(path='/private/'+name+'.json',sha256=pin) for name,pin in plan['gates'].items()})
    plan['bindings']['deploymentSha256']=d.digest(value)
    return value,plan


class DeploymentBundleTests(unittest.TestCase):
    def test_configured_ordered_node_inventory_accepts_one_to_eight_and_rejects_mismatch(self):
        value,plan=bundle()
        third=copy.deepcopy(value['workers'][1]);third['nodeId']='gpu3';third['expected']['nodeId']='gpu3'
        value['nodeEndpoints'].insert(1,dict(id='gpu3',url='http://192.0.2.3:3000'))
        value['workers'].insert(1,third)
        plan['bindings']['deploymentSha256']=d.digest(value)
        self.assertEqual([n['id'] for n in d.validate_bundle(value,plan)['nodeEndpoints']],['cluster','gpu3','main'])
        for mutate in (lambda b:b['nodeEndpoints'].append(copy.deepcopy(b['nodeEndpoints'][0])),
                       lambda b:b['workers'].pop(),
                       lambda b:b['workers'][0].update(nodeId='extra'),
                       lambda b:b['workers'].reverse()):
            bad=copy.deepcopy(value);mutate(bad);changed=copy.deepcopy(plan);changed['bindings']['deploymentSha256']=d.digest(bad)
            with self.assertRaises((d.TransportHeld,ValueError,KeyError)):d.validate_bundle(bad,changed)
        one=copy.deepcopy(value);one['nodeEndpoints']=one['nodeEndpoints'][:1];one['workers']=one['workers'][:1]
        changed=copy.deepcopy(plan);changed['bindings']['deploymentSha256']=d.digest(one)
        self.assertEqual(len(d.validate_bundle(one,changed)['workers']),1)
        fleet=copy.deepcopy(one)
        for index in range(1,8):
            node='worker'+str(index);worker=copy.deepcopy(one['workers'][0]);worker['nodeId']=node;worker['expected']['nodeId']=node
            fleet['workers'].append(worker);fleet['nodeEndpoints'].append(dict(id=node,url='http://192.0.2.1:3000'))
        changed['bindings']['deploymentSha256']=d.digest(fleet)
        self.assertEqual(len(d.validate_bundle(fleet,changed)['workers']),8)
        too_many=copy.deepcopy(fleet);worker=copy.deepcopy(one['workers'][0]);worker['nodeId']='worker8';worker['expected']['nodeId']='worker8'
        too_many['workers'].append(worker);too_many['nodeEndpoints'].append(dict(id='worker8',url='http://192.0.2.1:3000'))
        changed['bindings']['deploymentSha256']=d.digest(too_many)
        with self.assertRaises(d.TransportHeld):d.validate_bundle(too_many,changed)

    def test_service_owned_config_readback_uses_exact_original_ro_mount_not_operator_file_access(self):
        import hashlib
        value,plan=bundle();raw=b'{}'
        value['artifacts']['bootstrapConfig']['sha256']=hashlib.sha256(raw).hexdigest()
        plan['candidate']['bootstrapConfigSha256']=value['artifacts']['bootstrapConfig']['sha256']
        original=dict(Id=d.PREDECESSOR_ID,Image=d.PREDECESSOR_IMAGE,Config={'Env':['SAFE=synthetic']},HostConfig={},
            Mounts=[dict(Type='bind',Source='/private/tls',Destination='/run/processing-reference',RW=False)])
        plan['bindings']['predecessorRecipeSha256']=d.digest(d.immutable_container_recipe(original))
        plan['bindings']['predecessorEnvironmentSha256']=d.digest(d.container_environment(original))
        plan['bindings']['deploymentSha256']=d.digest(value)
        class Docker:
            reads=[]
            def inspect(self,identity):return original
            def read_private_file(self,identity,path):self.reads.append((identity,path));return raw
        docker=Docker()
        with patch.object(d,'private_bytes',side_effect=AssertionError('service UID10001 file is not operator-readable')):
            self.assertEqual(d.read_config_from_predecessor(docker,value,plan,'bootstrap'),raw)
        self.assertEqual(docker.reads,[(d.PREDECESSOR_ID,'/run/processing-reference/bootstrap.json')])
        original['Mounts'][0]['RW']=True
        with self.assertRaises(d.TransportHeld):d.read_config_from_predecessor(docker,value,plan,'bootstrap')
        self.assertEqual(len(docker.reads),1)

    def test_operation_staged_configs_are_read_as_one_exact_private_directory(self):
        import hashlib
        value,plan=bundle();directory='first-cutover-'+plan['operationId'];raw=b'{}'
        for role in ('bootstrap','final'):
            value['artifacts'][role+'Config']['path']='/private/tls/'+directory+'/'+role+'.json'
            value['artifacts'][role+'Config']['sha256']=hashlib.sha256(raw).hexdigest()
            plan['candidate'][role+'ConfigSha256']=hashlib.sha256(raw).hexdigest()
            value['recipes'][role]['mounts'][1]['source']='/private/tls/'+directory
            plan['candidate'][role+'RecipeSha256']=d.digest(value['recipes'][role])
        original=dict(Id=d.PREDECESSOR_ID,Image=d.PREDECESSOR_IMAGE,Config={'Env':['SAFE=synthetic']},HostConfig={},
            Mounts=[dict(Type='bind',Source='/private/tls',Destination='/run/processing-reference',RW=False)])
        plan['bindings']['predecessorRecipeSha256']=d.digest(d.immutable_container_recipe(original))
        plan['bindings']['predecessorEnvironmentSha256']=d.digest(d.container_environment(original))
        plan['bindings']['deploymentSha256']=d.digest(value)
        class Docker:
            def inspect(self,identity):return original
            def read_private_config_set(self,identity,path,pins):
                self.called=(identity,path,pins);return {'bootstrap.json':raw,'final.json':raw}
        docker=Docker()
        self.assertEqual(d.read_config_from_predecessor(docker,value,plan,'final'),raw)
        self.assertEqual(docker.called[1],'/run/processing-reference/'+directory)
        self.assertEqual(set(docker.called[2]),{'bootstrap.json','final.json'})
    def test_private_environment_keeps_one_scheduler_and_exact_mounted_config(self):
        value,_=bundle()
        for role in ('bootstrap','final'):
            environment=mode_environment(value,role)
            d.validate_environment(value,role,environment)
            for changes in (dict(PROCESSING_VIEWER_ENABLED='1'),dict(PROCESSING_REFERENCE_CONFIG='/run/processing-reference/other.json'),
                            dict(PROCESSING_AUTHORITY_LEASE_DIR='/other'),dict(PYTHONPATH='/private/shadow')):
                bad=dict(environment,**changes)
                with self.assertRaises(d.TransportHeld):d.validate_environment(value,role,bad)
        with self.assertRaises(d.TransportHeld):d.validate_environment(value,'holder',{'LD_PRELOAD':'/private/shadow.so'})
        for change in (dict(LD_AUDIT='/private/shadow.so'),dict(PYTHONUSERBASE='/private'),dict(HOME='/private'),
                       dict(PATH='/private'),dict(PYTHONNOUSERSITE='0')):
            with self.assertRaises(d.TransportHeld):d.validate_environment(value,'final',dict(mode_environment(value,'final'),**change))
    def test_worker_unit_normal_service_is_accepted_and_backslash_is_rejected_directly(self):
        value,plan=bundle();worker=copy.deepcopy(value['workers'][0])
        worker['settings']['unitName']='normal.service'
        d.worker(worker,plan['authorityId'])
        for unit in ('normal\\xservice','normal\\.service','normalXservice'):
            worker['settings']['unitName']=unit
            with self.assertRaises(d.TransportHeld):d.worker(worker,plan['authorityId'])

    def test_exact_bundle_consumes_existing_plan_and_has_no_hash_fixed_point(self):
        value,plan=bundle();result=d.validate_bundle(value,plan)
        self.assertEqual(result,value);self.assertNotIn('planSha256',result['holderSettings'])
        self.assertEqual(d.digest(result),plan['bindings']['deploymentSha256'])
        self.assertEqual(d.derived_holder_settings(value,plan)['planSha256'],d.digest(plan))

    def test_extra_credentials_and_unknown_fields_cannot_enter_bundle(self):
        value,plan=bundle()
        for target in ('top','worker','artifact'):
            bad=copy.deepcopy(value)
            if target=='top':bad['token']='synthetic-secret'
            elif target=='worker':bad['workers'][0]['settings']['bearer']='synthetic-secret'
            else:bad['artifacts']['censusKey']['contents']='synthetic-secret'
            plan['bindings']['deploymentSha256']=d.digest(bad)
            with self.assertRaises((d.TransportHeld,RuntimeError,ValueError)):d.validate_bundle(bad,plan)

    def test_digest_and_existing_recipe_config_source_qualification_pins_cannot_change(self):
        value,plan=bundle()
        changes=(lambda b:b['artifacts']['finalConfig'].update(sha256='7'*64),
                 lambda b:b['recipes']['bootstrap'].update(imageId='sha256:'+'7'*64),
                 lambda b:b['qualifications']['nasQualificationSha256'].update(sha256='7'*64),
                 lambda b:b['workers'][0]['expected'].update(sourceSha256='7'*64),
                 lambda b:b['host'].update(dockerSocket='tcp://127.0.0.1:2375'))
        for change in changes:
            bad=copy.deepcopy(value);change(bad);updated=copy.deepcopy(plan)
            updated['bindings']['deploymentSha256']=d.digest(bad)
            with self.assertRaises((d.TransportHeld,RuntimeError,ValueError)):d.validate_bundle(bad,updated)
        bad=copy.deepcopy(value);bad['host']['bootId']=str(uuid.uuid4())
        with self.assertRaises(d.TransportHeld):d.validate_bundle(bad,plan)

    def test_worker_inventory_is_complete_and_uses_existing_closed_readiness_expectations(self):
        value,plan=bundle()
        for change in (lambda b:b['workers'].pop(),lambda b:b['workers'][0].update(nodeId='other'),
                       lambda b:b['workers'][0]['settings'].update(readinessOrigin='http://127.0.0.1:1234'),
                       lambda b:b['workers'][0].update(sshHost='-oProxyCommand=bad')):
            bad=copy.deepcopy(value);change(bad);updated=copy.deepcopy(plan)
            updated['bindings']['deploymentSha256']=d.digest(bad)
            with self.assertRaises((d.TransportHeld,RuntimeError,ValueError)):d.validate_bundle(bad,updated)

    def test_missing_existing_gate_remains_explicitly_unqualified(self):
        value,plan=bundle();plan['gates']['pilotClosureSha256']=None;value['qualifications']['pilotClosureSha256']=None
        plan['bindings']['deploymentSha256']=d.digest(value)
        self.assertIsNone(d.validate_bundle(value,plan)['qualifications']['pilotClosureSha256'])
        bad=copy.deepcopy(value);bad['qualifications']['pilotClosureSha256']=dict(path='/private/pilot.json',sha256='e'*64)
        plan['bindings']['deploymentSha256']=d.digest(bad)
        with self.assertRaises(d.TransportHeld):d.validate_bundle(bad,plan)

    def test_changed_bundle_or_missing_activation_gate_refuses_before_private_io(self):
        value,plan=bundle();plan['gates']['pilotClosureSha256']=None;value['qualifications']['pilotClosureSha256']=None
        plan['bindings']['deploymentSha256']=d.digest(value)
        with patch.object(d,'private_bytes',side_effect=AssertionError('must refuse before private read')):
            with self.assertRaises(d.TransportHeld):d.verify_private_artifacts(value,plan,config_reader=None,require_all_qualifications=True)
            value['host']['bootId']=str(uuid.uuid4())
            with self.assertRaises(d.TransportHeld):d.verify_private_artifacts(value,plan,config_reader=None)

    def test_private_artifact_verification_reuses_independent_policy_builder_not_candidate_state(self):
        import hashlib
        value,plan=bundle()
        raw={name:(b'k'*32 if name=='censusKey' else b'KEY=value\n' if name.endswith('Environment') else name.encode()) for name in d.FILES}
        for name,entry in value['artifacts'].items():entry['sha256']=hashlib.sha256(raw[name]).hexdigest()
        for role in ('bootstrap','final','holder'):value['recipes'][role]['environmentSha256']=value['artifacts'][role+'Environment']['sha256']
        for role in ('bootstrap','final'):
            plan['candidate'][role+'RecipeSha256']=d.digest(value['recipes'][role])
            plan['candidate'][role+'ConfigSha256']=value['artifacts'][role+'Config']['sha256']
        plan['candidate']['declaredTransformSha256']=value['artifacts']['policy']['sha256']
        plan['bindings']['censusKeySha256']=value['artifacts']['censusKey']['sha256']
        plan['bindings']['reviewedCountsSha256']=value['artifacts']['reviewedCounts']['sha256']
        for name in plan['gates']:plan['gates'][name]=None;value['qualifications'][name]=None
        plan['bindings']['deploymentSha256']=d.digest(value)
        by_path={entry['path']:raw[name] for name,entry in value['artifacts'].items()}
        by_environment={value['artifacts'][role+'Environment']['path']:mode_environment(value,role) for role in ('bootstrap','final','holder')}
        with patch.object(d,'private_bytes',side_effect=lambda path,**_:by_path[path]),patch.object(d,'environment_file',side_effect=lambda path:by_environment[path]):
            with patch.object(d,'build_startup_policy',return_value=raw['policy']) as builder:
                proof=d.verify_private_artifacts(value,plan,config_reader=lambda role,entry:raw[role+'Config'])
                self.assertEqual(proof['censusKey']['bytes'],32)
                self.assertEqual(builder.call_args.kwargs['reviewed_schema_bytes'],raw['predecessorSchema'])
                self.assertEqual(builder.call_args.kwargs['expected_source_hashes'],value['sourceHashes'])
                self.assertNotIn('root',builder.call_args.kwargs)
            with patch.object(d,'build_startup_policy',return_value=b'different derived policy'):
                with self.assertRaises(d.TransportHeld):d.verify_private_artifacts(value,plan,config_reader=lambda role,entry:raw[role+'Config'])


def mode_environment(value,role):
    if role=='holder':return {}
    return dict(PROCESSING_VIEWER_ENABLED='0',PROCESSING_LEGACY_VIEWER_READ_ONLY='1' if role=='final' else '0',
        PYTHONNOUSERSITE='1',
        PROCESSING_PROTOTYPE_STATE_DIR='/prototype-state',PROCESSING_AUTHORITY_LEASE_DIR='/authority-leases',
        PROCESSING_PROTOTYPE_NODE_IDS='cluster,main',PROCESSING_REFERENCE_CONFIG='/run/processing-reference/'+role+'.json')


if __name__=='__main__':unittest.main()
