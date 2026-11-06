"""Actual readonly SQLite/TLS readiness with synthetic engine/owner transports."""
import hashlib
from contextlib import closing
import http.client
import json
import os
from pathlib import Path
import sqlite3
import ssl
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import MagicMock,patch
import uuid

from processing import reference_protocol as p
from processing.reference_api import Response
from processing.reference_http import create_reference_http_server
from processing.reference_worker_readiness import WorkerReadinessAPI,PATH,source_digest,loaded_source_snapshot
from tests.test_reference_dynamic_source_intake_scale import _write_tls_material


class ReadinessTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.root.chmod(0o700)
        self.source=Path(__file__).resolve().parents[1]
        if os.name=='nt':
            guard=patch('processing.reference_worker_readiness.immutable_source_paths',return_value={})
            guard.start();self.addCleanup(guard.stop)
        self.path=self.root/'driver.sqlite'
        db=sqlite3.connect(self.path)
        db.executescript("CREATE TABLE driver_engine_pin(singleton INTEGER PRIMARY KEY,pin TEXT); CREATE TABLE driver_binding(singleton INTEGER PRIMARY KEY,binding TEXT);")
        db.execute('INSERT INTO driver_engine_pin VALUES(1,?)',('pinned-v2',));db.execute('INSERT INTO driver_binding VALUES(1,?)',('{}',))
        db.commit();db.close();self.path.chmod(0o600)
        self.engine=MagicMock()
        self.engine.token=''
        self.options=[dict(name='fast-orthophoto',type='bool',value=False)]
        self.engine.read.side_effect=lambda route,**kwargs: (dict(engine='odm',engineVersion='pinned-v2',taskQueueCount=0) if route=='/info'
            else self.options if route=='/options' else [])
        worker=SimpleNamespace(node_id='node-one',worker_id='worker-one',authority_id=str(uuid.uuid4()),engine_id='engine-one',
            gate_generation=1,version='pinned-v2',engine=self.engine,_verify_local_engine=MagicMock())
        self.driver=SimpleNamespace(worker=worker,path=self.path,_lock=threading.Lock(),worker_instance_id='stable-instance')
        self.owner=MagicMock();self.session=SimpleNamespace(check=MagicMock(),epoch='reader-epoch')
        self.settings=dict(schemaVersion=1,bearer='synthetic-separate-maintenance-'+ 'x'*32,sourceRoot=str(self.source),
            sourceSha256=source_digest(self.source),activation=None)
        self.inner=MagicMock();self.inner.handle.return_value=Response(404,{'error':'not_found'})
        self.api=WorkerReadinessAPI(self.inner,settings=self.settings,config_sha256='a'*64,
            driver=self.driver,owner=self.owner,reader_session=self.session)
        self.api.service=SimpleNamespace(health=lambda:dict(state='ready',driverAlive=True,listenerAlive=True))
        self.headers={'Authorization':'Bearer '+self.settings['bearer']}

    def request(self,*,body=None,headers=None,idle=True):
        nonce=str(uuid.uuid4())
        raw=p.canonical_json(dict(schemaVersion=1,nonce=nonce,requireIdle=idle)) if body is None else body
        return nonce,self.api.handle('POST',PATH,self.headers if headers is None else headers,raw,protected_transport=True)

    def test_fresh_nonce_loaded_pin_and_source_are_read_only_not_a_thread_health_flag(self):
        before=self.path.read_bytes();stamp=self.path.stat().st_mtime_ns
        nonce,response=self.request()
        self.assertEqual(response.status,200,response.body)
        self.assertEqual(response.body['nonce'],nonce)
        self.assertEqual(response.body['engineVersion'],'pinned-v2')
        self.assertTrue(response.body['idle'])
        self.assertEqual((self.path.read_bytes(),self.path.stat().st_mtime_ns),(before,stamp))
        self.owner.check.assert_called();self.session.check.assert_called()
        with closing(sqlite3.connect(self.path)) as db:
            db.execute("UPDATE driver_engine_pin SET pin='wrong'");db.commit()
        self.assertEqual(self.request()[1].status,503,'thread-ready without committed correct pin must fail')

    def test_authentication_precedes_payload_parsing_and_does_not_echo_values(self):
        for headers in ({},{'Authorization':'Bearer wrong'},{'Authorization':'Bearer ü'}):
            with self.subTest(headers=headers):
                response=self.request(body=b'not-json',headers=headers)[1]
                self.assertEqual(response.status,401)
        response=self.request(body=b'not-json')[1]
        self.assertEqual(response.status,400)
        self.assertNotIn(self.settings['bearer'],json.dumps(response.body))

    def test_busy_driver_changed_source_and_dead_thread_all_hold(self):
        with self.driver._lock:self.assertEqual(self.request()[1].status,503)
        with patch.object(p,'__file__',str(self.root/'foreign-protocol.py')):
            self.assertEqual(self.request()[1].status,503)
        original=self.api.loaded_source['modules']['processing.reference_protocol']['sha256']
        self.api.loaded_source['modules']['processing.reference_protocol']['sha256']='b'*64
        self.assertEqual(self.request()[1].status,503)
        self.api.loaded_source['modules']['processing.reference_protocol']['sha256']=original
        self.api.service.health=lambda:dict(state='ready',driverAlive=False,listenerAlive=True)
        self.assertEqual(self.request()[1].status,503)

    def test_direct_listener_failure_prevents_worker_readiness(self):
        for state in ('failed','draining','not_started','closed'):
            self.api.service.health=lambda:dict(state='ready',driverAlive=True,listenerAlive=True,
                                                directPreparationListener=state)
            with self.subTest(state=state):self.assertEqual(self.request()[1].status,503)

    def test_serving_mode_does_not_claim_idle_during_active_engine_work(self):
        identifier=str(uuid.uuid4())
        self.engine.read.side_effect=lambda route,**kwargs: (dict(engineVersion='pinned-v2',taskQueueCount=1) if route=='/info'
            else self.options if route=='/options' else [dict(uuid=identifier)] if route=='/task/list' else dict(uuid=identifier,status=dict(code=20)))
        self.assertEqual(self.request()[1].status,503)
        response=self.request(idle=False)[1]
        self.assertEqual(response.status,200);self.assertFalse(response.body['idle'])

    def test_invalid_actual_option_catalog_prevents_ready_without_mutating_driver(self):
        self.options[:]=[dict(name='broken',type='unknown-new-type',value='unsupported')]
        self.assertEqual(self.request()[1].status,503)

    def test_identical_shadow_tree_cannot_claim_actual_loaded_source(self):
        import shutil
        shadow=self.root/'shadow'
        for package in ('ops','processing'):
            shutil.copytree(self.source/package,shadow/package,ignore=shutil.ignore_patterns('__pycache__'))
        self.assertEqual(source_digest(shadow),source_digest(self.source))
        with self.assertRaisesRegex(ValueError,'reviewed installation'):loaded_source_snapshot(shadow)

    @unittest.skipUnless(os.name!='nt','Linux readonly source contract')
    def test_service_writable_source_is_not_automatically_maintenance_ready(self):
        from processing.reference_worker_readiness import immutable_source_paths
        writable=self.root/'writable';(writable/'ops').mkdir(parents=True);(writable/'processing').mkdir()
        (writable/'ops'/'source.py').write_text('# owned writable source')
        with self.assertRaisesRegex(ValueError,'readonly worker source'):immutable_source_paths(writable)

    def test_foreign_package_search_path_refuses_readiness_before_later_import(self):
        import processing
        with patch.object(processing,'__path__',[str(self.source/'processing'),str(self.root/'foreign')]):
            self.assertEqual(self.request()[1].status,503)

    def test_forged_module_spec_origin_refuses_readiness(self):
        with patch.object(p.__spec__,'origin',str(self.root/'foreign.py')):
            self.assertEqual(self.request()[1].status,503)

    def test_actual_tls_listener_enforces_separate_readiness_credential(self):
        key,cert=_write_tls_material(self.root)
        context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(str(cert),str(key))
        server=create_reference_http_server(self.api,ssl_context=context)
        thread=threading.Thread(target=server.serve_forever,kwargs={'poll_interval':0.02});thread.start()
        self.addCleanup(lambda:(server.shutdown(),server.server_close(),thread.join(2)))
        client=ssl.create_default_context(cafile=str(cert))
        nonce=str(uuid.uuid4());body=p.canonical_json(dict(schemaVersion=1,nonce=nonce,requireIdle=True))
        connection=http.client.HTTPSConnection('127.0.0.1',server.server_port,context=client,timeout=3)
        connection.request('POST',PATH,body=body,headers=self.headers)
        response=connection.getresponse();value=json.loads(response.read());connection.close()
        self.assertEqual(response.status,200);self.assertEqual(value['nonce'],nonce)
        self.assertEqual(value['pid'],os.getpid())


if __name__=='__main__':unittest.main()
