"""Host-only regressions for the real Docker mount-order observation.

These exercise the shared capture helper and actual inspection method using
synthetic Docker responses; they do not qualify a real worker or policy rebind.
"""
import copy
import hashlib
import itertools
import json
from pathlib import Path
from types import SimpleNamespace
import unittest

from ops import reference_worker_maintenance as m


class WorkerRecipeProjectionTests(unittest.TestCase):
    def fixture(self):
        engine='test-engine'
        gate='/var/lib/ledgetop-gates/LedgeTop-Processing/node-gates/'+hashlib.sha256(engine.encode()).hexdigest()
        value=dict(id='a'*64,name='/worker',image='sha256:'+'b'*64,
            config=dict(User='1000:1000',Entrypoint=['python','-m','ops.reference_worker_daemon'],
                Cmd=['--config',str(Path('/private')/'worker.json')],Env=['A=1','B=2'],StopTimeout=-1),
            host=dict(NetworkMode='host',ReadonlyRootfs=True,Privileged=False,UsernsMode='',AutoRemove=False,
                Mounts=[dict(Target='/state'),dict(Target='/scratch')]),
            mounts=[dict(Type='bind',Source=source,Destination=target,RW=rw,Propagation='rprivate')
                for source,target,rw in (('/host/private','/private',False),('/host/state','/state',True),
                    ('/host/scratch','/scratch',True),('/host/gate',gate,True))],
            running=True,pid=123,exitCode=0)
        policy=SimpleNamespace(container_id=value['id'],container_name='worker',image_id=value['image'],
            container_config_directory='/private',config_directory='/host/private',state_root='/host/state',
            scratch_root='/host/scratch',gate_directory='/host/gate',engine_id=engine)
        policy.recipe_sha256=self.capture(value)
        controller=SimpleNamespace(policy=policy,docker=lambda _args:json.dumps(value),
            config=lambda:({'stateRoot':'/state','scratchRoot':'/scratch'},None))
        return value,controller

    def capture(self,value):
        return m.worker_recipe_sha256(*(value[k] for k in ('image','config','host','mounts')))

    def test_all_twenty_four_effective_mount_orders_match_capture_and_inspection(self):
        value,controller=self.fixture();original=copy.deepcopy(value)
        for mounts in itertools.permutations(original['mounts']):
            value['mounts']=list(mounts)
            with self.subTest(destinations=[x['Destination'] for x in mounts]):
                self.assertEqual(self.capture(value),controller.policy.recipe_sha256)
                self.assertEqual(m.WorkerMaintenance.inspect(controller),value)
        self.assertEqual(original['config'],value['config'])

    def test_raw_policy_digest_is_not_silently_adopted_even_when_already_sorted(self):
        value,controller=self.fixture();value['mounts'].sort(key=lambda item:item['Destination'])
        controller.policy.recipe_sha256=m.digest({k:value[k] for k in ('image','config','host','mounts')})
        with self.assertRaisesRegex(m.WorkerMaintenanceHeld,'worker_recipe_changed'):
            m.WorkerMaintenance.inspect(controller)

    def test_every_authority_field_remains_bound(self):
        changes=(('image',lambda v:v.update(image='sha256:'+'c'*64)),
            ('source',lambda v:v['mounts'][0].update(Source='/foreign')),
            ('destination',lambda v:v['mounts'][0].update(Destination='/foreign')),
            ('writable',lambda v:v['mounts'][0].update(RW=True)),
            ('propagation',lambda v:v['mounts'][0].update(Propagation='shared')),
            ('extra-mount-field',lambda v:v['mounts'][0].update(Unexpected=True)),
            ('config',lambda v:v['config'].update(Unexpected='new')),
            ('host',lambda v:v['host'].update(Privileged=True)),
            ('env-order',lambda v:v['config']['Env'].reverse()),
            ('argv-order',lambda v:v['config']['Cmd'].reverse()),
            ('host-array-order',lambda v:v['host']['Mounts'].reverse()))
        for name,change in changes:
            value,controller=self.fixture();change(value)
            with self.subTest(field=name),self.assertRaises(m.WorkerMaintenanceHeld):
                m.WorkerMaintenance.inspect(controller)

    def test_duplicate_missing_extra_or_untyped_mounts_are_refused(self):
        for mode in ('duplicate','missing','extra','not-list','nonmapping','no-destination','relative'):
            value,controller=self.fixture()
            if mode=='duplicate':value['mounts'][1]['Destination']=value['mounts'][0]['Destination']
            elif mode=='missing':value['mounts'].pop()
            elif mode=='extra':value['mounts'].append(copy.deepcopy(value['mounts'][0]))
            elif mode=='not-list':value['mounts']=tuple(value['mounts'])
            elif mode=='nonmapping':value['mounts'][0]=None
            elif mode=='no-destination':del value['mounts'][0]['Destination']
            else:value['mounts'][0]['Destination']='relative'
            with self.subTest(mode=mode),self.assertRaises(m.WorkerMaintenanceHeld):self.capture(value)

    def test_projection_does_not_mutate_capture_input(self):
        value,_=self.fixture();before=copy.deepcopy(value)
        self.capture(value);self.assertEqual(value,before)

    def test_semantic_mount_guard_still_refuses_foreign_source_with_matching_digest(self):
        value,controller=self.fixture();value['mounts'][0]['Source']='/foreign'
        controller.policy.recipe_sha256=self.capture(value)
        with self.assertRaisesRegex(m.WorkerMaintenanceHeld,'canonical_worker_mount_changed'):
            m.WorkerMaintenance.inspect(controller)


if __name__=='__main__':unittest.main()
