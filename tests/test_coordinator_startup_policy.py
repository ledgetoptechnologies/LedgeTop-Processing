"""Policy generation uses pinned source/configuration, never candidate SQLite."""
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import unittest
import uuid
from unittest.mock import patch

from ops import coordinator_startup_policy as s
from ops import coordinator_predecessor_census as old

ROOT=Path(__file__).resolve().parents[1]


def settings(authority):
    workers={node:dict(bearer='synthetic-'+node+'-'+'a'*32,workerId='worker-'+node,
        instanceId='instance-'+node,resultOrigin='https://127.0.0.1:'+str(19000+i),
        resultBearer='synthetic-result-'+node+'-'+'b'*32) for i,node in enumerate(('cluster','main'))}
    physical={node:dict(engineId='physical-'+node,authorityId=authority,
        coordinatorOrigin='http://127.0.0.1:18080',gateGeneration=1,outputBudget=1048576)
        for node in workers}
    bootstrap=dict(schemaVersion=2,workers=workers,recoveryBindings=physical)
    final=dict(bootstrap,schemaVersion=6,viewerAcceptance={})
    return bootstrap,final


class StartupPolicyTests(unittest.TestCase):
    def setUp(self):
        self.authority=str(uuid.uuid4());self.bootstrap,self.final=settings(self.authority)
        self.hashes={name:hashlib.sha256((ROOT/name).read_bytes()).hexdigest() for name in s.SOURCES}

    def build(self,**changes):
        args=dict(expected_source_hashes=self.hashes,candidate_image_id='sha256:'+'a'*64,
            candidate_source_commit='b'*40,bootstrap_config_bytes=old._canonical(self.bootstrap),
            final_config_bytes=old._canonical(self.final),
            node_endpoints=[dict(id='cluster',url='http://127.0.0.1:19001'),dict(id='main',url='http://127.0.0.1:19002')],
            authority_id=self.authority)
        args.update(changes)
        return s.build_startup_policy(ROOT,**args)

    def test_generated_schemas_use_only_memory_and_known_phase_groups(self):
        connect=sqlite3.connect;opened=[]
        def memory_only(path,*args,**kwargs):
            self.assertEqual(path,':memory:');opened.append(path);return connect(path,*args,**kwargs)
        with patch.object(sqlite3,'connect',side_effect=memory_only):policy=json.loads(self.build())
        self.assertEqual(opened,[':memory:'])
        boot={row[1] for row in policy['phases']['bootstrap-frozen']['objects'] if row[0]=='table'}
        final={row[1] for row in policy['phases']['final-frozen']['objects'] if row[0]=='table'}
        self.assertEqual(final-boot,s.READERS)
        self.assertEqual(len(boot),31);self.assertEqual(len(final),35)
        self.assertEqual(policy['bindingNames'],list(s.BINDINGS))
        self.assertNotIn('bearer',self.build().decode())
        self.assertEqual(policy['startup']['bootstrapConfigSha256'],hashlib.sha256(old._canonical(self.bootstrap)).hexdigest())

    def test_source_hash_change_refused(self):
        bad=dict(self.hashes);bad[s.SOURCES[0]]='0'*64
        with self.assertRaisesRegex(old.CensusRefused,'source_changed'):self.build(expected_source_hashes=bad)

    def test_config_changes_and_source_release_are_not_silently_supported(self):
        for change in ({'sourceRelease':{}},{'viewerAcceptance':{'unexpected':{}}}):
            with self.subTest(change=change):
                final=dict(self.final,**change)
                with self.assertRaisesRegex(old.CensusRefused,'closed_first_startup'):
                    self.build(final_config_bytes=old._canonical(final))
        final=copy.deepcopy(self.final);final['workers']['main']['workerId']='foreign'
        with self.assertRaises(old.CensusRefused):self.build(final_config_bytes=old._canonical(final))

    def test_inventory_comes_from_configuration_not_candidate_database(self):
        policy=json.loads(self.build())
        self.assertEqual(policy['startup']['inventory']['nodeOrder'],['cluster','main'])
        self.assertEqual(policy['startup']['inventory']['nodes']['cluster']['engineOrigin'],['http','127.0.0.1',19001])
        self.assertEqual(policy['startup']['recovery']['main']['authority_id'],self.authority)

    def test_ordered_dynamic_inventory_and_exact_configuration_binding(self):
        bootstrap,final=settings(self.authority)
        worker=copy.deepcopy(bootstrap['workers']['main']);binding=copy.deepcopy(bootstrap['recoveryBindings']['main'])
        worker['workerId']='worker-gpu3';worker['instanceId']='instance-gpu3';binding['engineId']='physical-gpu3'
        for config in (bootstrap,final):
            config['workers']['gpu3']=copy.deepcopy(worker);config['recoveryBindings']['gpu3']=copy.deepcopy(binding)
        endpoints=[dict(id='cluster',url='http://127.0.0.1:19001'),dict(id='gpu3',url='http://127.0.0.1:19003'),dict(id='main',url='http://127.0.0.1:19002')]
        inventory,_=s._inventory(bootstrap,final,endpoints,self.authority)
        self.assertEqual(inventory['nodeOrder'],['cluster','gpu3','main'])
        for bad in (endpoints+[endpoints[0]],endpoints[:-1],endpoints+[dict(id='extra',url='http://127.0.0.1:19004')],[]):
            with self.assertRaises(old.CensusRefused):s._inventory(bootstrap,final,bad,self.authority)

    def test_bootstrap_does_not_invent_a_future_final_start_window(self):
        policy=json.loads(self.build());startup=str(uuid.uuid4());plan=str(uuid.uuid4())
        context=dict(bootstrapStartupId=startup,currentStartupId=startup,planId=plan,ownerSha256='a'*64,
            requestIds={action:str(uuid.uuid5(uuid.UUID(plan),action)) for action in ('intent','drain','seal')},
            operationBinding={'authorityId':self.authority},sourceReaderBinding=None,sourceCustody=None,
            bootstrapWindow={'earliest':'2026-11-05 20:00:00','latest':'2026-11-05 20:00:30'},finalWindow=None)
        result=s.materialize(policy,context,'bootstrap-frozen',b'x'*32)
        self.assertEqual(result['rows']['coordinator_freeze_state'][0]['phase'],'frozen')
        context['finalWindow']=dict(context['bootstrapWindow'])
        with self.assertRaisesRegex(old.CensusRefused,'future_final_window'):s.materialize(policy,context,'bootstrap-frozen',b'x'*32)

    def test_clock_window_is_bounded_and_canonical(self):
        self.assertEqual(s._window({'earliest':'2026-11-05 20:00:00','latest':'2026-11-05 20:00:30'})['latest'],'2026-11-05 20:00:30')
        for window in ({'earliest':'bad','latest':'bad'},
                       {'earliest':'2026-11-05 20:00:00','latest':'2026-11-05 20:03:00'}):
            with self.assertRaises(old.CensusRefused):s._window(window)
