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
    for role in ('bootstrap','final','holder'):
        files[role+'Environment']=dict(path=recipes[role]['environmentFile'],sha256=recipes[role]['environmentSha256'])
    files['policy']['sha256']=plan['candidate']['declaredTransformSha256']
    files['censusKey']['sha256']=plan['bindings']['censusKeySha256']
    files['reviewedCounts']['sha256']=plan['bindings']['reviewedCountsSha256']
    workers=[]
    for node in ('cluster','main'):
        workers.append(dict(nodeId=node,sshHost='192.0.2.1',settings=dict(schemaVersion=1,
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
        nodeEndpoints=[dict(id=node,url='http://192.0.2.1:3000') for node in ('cluster','main')],workers=workers,
        qualifications={name:dict(path='/private/'+name+'.json',sha256=pin) for name,pin in plan['gates'].items()})
    plan['bindings']['deploymentSha256']=d.digest(value)
    return value,plan


class DeploymentBundleTests(unittest.TestCase):
    def test_exact_bundle_consumes_existing_plan_and_has_no_hash_fixed_point(self):
        value,plan=bundle();result=d.validate_bundle(value,plan)
        self.assertEqual(result,value);self.assertNotIn('planSha256',result['holderSettings'])
        self.assertEqual(d.digest(result),plan['bindings']['deploymentSha256'])

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
            with self.assertRaises(d.TransportHeld):d.verify_private_artifacts(value,plan,require_all_qualifications=True)
            value['host']['bootId']=str(uuid.uuid4())
            with self.assertRaises(d.TransportHeld):d.verify_private_artifacts(value,plan)

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
        with patch.object(d,'private_bytes',side_effect=lambda path,**_:by_path[path]),patch.object(d,'environment_file',return_value={}):
            with patch.object(d,'build_startup_policy',return_value=raw['policy']) as builder:
                proof=d.verify_private_artifacts(value,plan)
                self.assertEqual(proof['censusKey']['bytes'],32)
                self.assertEqual(builder.call_args.kwargs['reviewed_schema_bytes'],raw['predecessorSchema'])
                self.assertEqual(builder.call_args.kwargs['expected_source_hashes'],value['sourceHashes'])
                self.assertNotIn('root',builder.call_args.kwargs)
            with patch.object(d,'build_startup_policy',return_value=b'different derived policy'):
                with self.assertRaises(d.TransportHeld):d.verify_private_artifacts(value,plan)


if __name__=='__main__':unittest.main()
