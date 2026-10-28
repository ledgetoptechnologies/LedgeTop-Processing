import base64
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

from processing.update_registry import (DockerRegistry,RegistryChannel,RegistryPolicy,
    RegistryError,RegistryHeld,_json,_run)


REPO='docker.io/example/engine'
CONFIG='sha256:'+'b'*64


def entry(*,config=CONFIG,platform=None,annotations=None):
    manifest=dict(schemaVersion=2,mediaType='application/vnd.oci.image.manifest.v1+json',
        config=dict(mediaType='application/vnd.oci.image.config.v1+json',digest=config,size=123),
        layers=[dict(mediaType='application/vnd.oci.image.layer.v1.tar+gzip',digest='sha256:'+'c'*64,size=1000)])
    if annotations is not None:manifest['annotations']=annotations
    raw=_json(manifest).encode();digest='sha256:'+hashlib.sha256(raw).hexdigest()
    return dict(Ref=REPO+':approved-test@'+digest,Descriptor=dict(mediaType=manifest['mediaType'],digest=digest,
        size=len(raw),platform=platform or dict(os='linux',architecture='amd64')),
        Raw=base64.b64encode(raw).decode(),OCIManifest=manifest)


class FakeDocker:
    def __init__(self):
        self.selected=entry();self.first=[self.selected,entry(config='sha256:'+'d'*64,platform=dict(os='unknown',architecture='unknown'))]
        self.last=None;self.immutable=None;self.tags=0;self.calls=[];self.engine='test-engine';self.error=None
        self.image=dict(Id=CONFIG,Os='linux',Architecture='amd64',RepoDigests=[REPO+'@'+self.selected['Descriptor']['digest']],Size=2000)

    def __call__(self,command,*,timeout,max_bytes):
        args=command[3:];self.calls.append(args)
        if args[0]=='manifest':
            if '@' in args[-1]:value=self.immutable if self.immutable is not None else self.selected
            else:
                self.tags+=1;value=self.last if self.tags>1 and self.last is not None else self.first
        elif args[0]=='info':return subprocess.CompletedProcess(command,0,self.engine,b'')
        elif args[0]=='pull':
            if self.error:raise self.error
            time.sleep(.01);return subprocess.CompletedProcess(command,0,b'pull completed',b'')
        elif args[:2]==['image','inspect']:value=self.image
        else:raise AssertionError(args)
        return subprocess.CompletedProcess(command,0,_json(value).encode(),b'')


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.policy=RegistryPolicy(sys.executable,'unix:///var/run/docker.sock','test-engine',
            (RegistryChannel('test',REPO,'approved-test'),),4096,max_output_bytes=128*1024)
        self.docker=FakeDocker();self.admissions=[]
        self.registry=self.open()

    def admit(self,candidate,amount):
        self.admissions.append((candidate['candidateId'],amount));return True

    def open(self,**kwargs):
        return DockerRegistry(self.root/'registry.sqlite',self.policy,capacity_admit=kwargs.pop('capacity_admit',self.admit),
            runner=kwargs.pop('runner',self.docker),**kwargs)

    def test_discovery_refetches_immutable_platform_digest_distinct_from_config(self):
        candidate=self.registry.discover('test')
        self.assertNotEqual(candidate['manifestDigest'],candidate['localImageId'])
        self.assertEqual(candidate['localImageId'],candidate['configDigest'])
        self.assertEqual(candidate['compressedBytes'],1123)
        self.assertEqual(self.docker.calls,[['manifest','inspect','--verbose',REPO+':approved-test'],
            ['manifest','inspect','--verbose',REPO+'@'+candidate['manifestDigest']],
            ['manifest','inspect','--verbose',REPO+':approved-test']])
        self.assertFalse(any(key in _json(candidate) for key in ('Raw','annotations','OCIManifest')))
        with self.registry._db() as db:self.assertEqual(db.execute('PRAGMA synchronous').fetchone()[0],2)

    def test_staging_only_pulls_immutable_digest_and_exact_config_platform_repository(self):
        candidate=self.registry.discover('test');metadata=self.registry.stage(candidate)
        self.assertEqual(metadata['imageId'],CONFIG)
        self.assertEqual(self.admissions,[(candidate['candidateId'],4096)])
        pulls=[args for args in self.docker.calls if args[0]=='pull']
        self.assertEqual(pulls,[['pull','--platform','linux/amd64',REPO+'@'+candidate['manifestDigest']]])
        self.assertEqual(self.open().stage(candidate),metadata)
        self.assertEqual(len([args for args in self.docker.calls if args[0]=='pull']),1)

    def test_readonly_staged_proof_preserves_journal_and_never_pulls(self):
        candidate=self.registry.discover('test');metadata=self.registry.stage(candidate)
        with self.registry._db() as db:before=[tuple(row) for row in db.execute('SELECT * FROM registry_staging')]
        self.docker.calls.clear()
        self.assertEqual(self.registry.inspect_staged(candidate),metadata)
        with self.registry._db() as db:after=[tuple(row) for row in db.execute('SELECT * FROM registry_staging')]
        self.assertEqual(after,before)
        self.assertFalse(any(args[0]=='pull' for args in self.docker.calls))
        self.docker.image['Id']='sha256:'+'e'*64
        with self.assertRaises(RegistryError):self.registry.inspect_staged(candidate)
        with self.registry._db() as db:self.assertEqual([tuple(row) for row in db.execute('SELECT * FROM registry_staging')],before)

    def test_readonly_proof_cannot_promote_an_unstaged_or_unknown_pull(self):
        candidate=self.registry.discover('test')
        with self.assertRaisesRegex(RegistryHeld,'definite_staged_image_required'):
            self.registry.inspect_staged(candidate)
        self.docker.error=TimeoutError('private pull response lost')
        with self.assertRaises(RegistryHeld):self.registry.stage(candidate)
        self.docker.calls.clear()
        with self.assertRaisesRegex(RegistryHeld,'definite_staged_image_required'):
            self.registry.inspect_staged(candidate)
        self.assertEqual(self.docker.calls,[])
        with self.registry._db() as db:self.assertEqual(db.execute('SELECT state FROM registry_staging').fetchone()[0],'held')

    def test_stage_rejects_forged_or_nonpersisted_candidate(self):
        candidate=self.registry.discover('test')
        for changed in (dict(candidate,configDigest='sha256:'+'e'*64),dict(candidate,candidateId='f'*64)):
            with self.assertRaisesRegex(RegistryError,'persisted_candidate_required'):self.registry.stage(changed)
        self.assertEqual(self.admissions,[])

    def test_corrupt_persisted_candidate_cannot_escape_fixed_repository_or_digest_binding(self):
        candidate=self.registry.discover('test')
        changed=dict(candidate,repository='docker.io/unapproved/engine')
        changed['candidateId']=hashlib.sha256(_json({key:value for key,value in changed.items() if key!='candidateId'}).encode()).hexdigest()
        with self.registry._db() as db:db.execute('INSERT INTO registry_candidates VALUES(?,?)',(changed['candidateId'],_json(changed)))
        with self.assertRaisesRegex(RegistryError,'persisted_candidate_required'):self.registry.stage(changed)
        changed=dict(candidate,compressedBytes=1)
        with self.registry._db() as db:db.execute('UPDATE registry_candidates SET value=? WHERE id=?',(_json(changed),candidate['candidateId']))
        with self.assertRaisesRegex(RegistryError,'persisted_candidate_required'):self.registry.stage(changed)
        self.assertFalse(any(args[0]=='pull' for args in self.docker.calls))

    def test_unknown_channels_and_repository_credential_or_tag_injection_denied(self):
        with self.assertRaisesRegex(RegistryError,'unconfigured_channel'):self.registry.discover('production')
        for repo in ('https://user:password@example.com/repo','example/repo','docker.io/example/repo@sha256:'+'a'*64):
            with self.assertRaises(RegistryError):RegistryChannel('test',repo,'test')
        for tag in ('latest --all-tags','x;whoami','--help'):
            with self.assertRaises(RegistryError):RegistryChannel('test',REPO,tag)

    def test_tag_mapping_change_and_immutable_refetch_change_reject(self):
        self.docker.last=[entry(config='sha256:'+'e'*64)]
        with self.assertRaisesRegex(RegistryError,'tag_changed_during_discovery'):self.registry.discover('test')
        self.docker.tags=0;self.docker.last=None;self.docker.immutable=entry(config='sha256:'+'f'*64)
        with self.assertRaisesRegex(RegistryError,'immutable_manifest_changed'):self.registry.discover('test')
        with self.registry._db() as db:self.assertEqual(db.execute('SELECT COUNT(*) FROM registry_candidates').fetchone()[0],0)

    def test_ambiguous_or_non_exact_platform_rejects(self):
        for platform in (dict(os='linux',architecture='arm64'),dict(os='linux',architecture='amd64',variant='v3')):
            self.docker.first=[entry(platform=platform)]
            with self.assertRaisesRegex(RegistryError,'unambiguous_linux_amd64'):self.registry.discover('test')
        self.docker.first=[entry(),entry()]
        with self.assertRaisesRegex(RegistryError,'unambiguous_linux_amd64'):self.registry.discover('test')

    def test_known_oci_attestation_with_empty_embedded_config_is_not_execution_candidate(self):
        artifact=entry(platform=dict(os='unknown',architecture='unknown'))
        manifest=artifact['OCIManifest']
        manifest.update(artifactType='application/vnd.docker.attestation.manifest.v1+json',
            subject={key:self.docker.selected['Descriptor'][key] for key in ('mediaType','digest','size')},
            config=dict(mediaType='application/vnd.oci.empty.v1+json',digest='sha256:'+hashlib.sha256(b'{}').hexdigest(),size=2,data='e30='),
            layers=[dict(mediaType='application/vnd.in-toto+json',digest='sha256:'+'e'*64,size=100)])
        raw=_json(manifest).encode();digest='sha256:'+hashlib.sha256(raw).hexdigest()
        artifact.update(Raw=base64.b64encode(raw).decode(),Ref=REPO+'@'+digest)
        artifact['Descriptor'].update(digest=digest,size=len(raw))
        self.docker.first=[self.docker.selected,artifact]
        candidate=self.registry.discover('test')
        self.assertEqual(candidate['configDigest'],CONFIG)
        self.assertNotEqual(candidate['manifestDigest'],digest)

    def test_unknown_schema_raw_digest_and_duplicate_json_keys_reject(self):
        for mutate in (
            lambda item:item['Descriptor'].update(mediaType='application/unknown'),
            lambda item:item.update(Raw=base64.b64encode(b'{}').decode()),
            lambda item:item['OCIManifest'].update(schemaVersion=1),
            lambda item:item['Descriptor'].update(unknown='field')):
            item=entry();mutate(item);self.docker.first=[item]
            with self.assertRaises(RegistryError):self.registry.discover('test')
        self.registry.runner=lambda *args,**kw:subprocess.CompletedProcess([],0,b'{"Ref":"x","Ref":"y"}',b'')
        with self.assertRaisesRegex(RegistryError,'ambiguous_metadata'):self.registry.discover('test')

    def test_secret_metadata_and_raw_stderr_never_persist_or_escape(self):
        self.docker.first=[entry(annotations={'private.token':'do-not-print'})]
        with self.assertRaisesRegex(RegistryError,'secret_bearing_metadata') as error:self.registry.discover('test')
        self.assertNotIn('do-not-print',str(error.exception))
        self.registry.runner=lambda *args,**kw:subprocess.CompletedProcess([],1,b'',b'Bearer do-not-print')
        with self.assertRaisesRegex(RegistryError,'docker_command_failed') as error:self.registry.discover('test')
        self.assertNotIn('do-not-print',str(error.exception))
        self.assertNotIn(b'do-not-print',(self.root/'registry.sqlite').read_bytes())

    def test_signed_or_credential_urls_in_annotations_are_denied(self):
        for url in ('https://example.com/file?token=do-not-print','https://user:do-not-print@example.com/file'):
            self.docker.first=[entry(annotations={'org.opencontainers.image.source':url})]
            with self.assertRaisesRegex(RegistryError,'secret_bearing_metadata') as error:self.registry.discover('test')
            self.assertNotIn('do-not-print',str(error.exception))

    def test_capacity_denial_and_compressed_bound_happen_before_pull(self):
        candidate=self.registry.discover('test')
        denied=self.open(capacity_admit=lambda *args:False)
        with self.assertRaisesRegex(RegistryError,'capacity_admission_required'):denied.stage(candidate)
        self.assertFalse(any(args[0]=='pull' for args in self.docker.calls))
        small=replace(self.policy,staging_reservation_bytes=1024)
        other=DockerRegistry(self.root/'small.sqlite',small,capacity_admit=self.admit,runner=self.docker)
        candidate=other.discover('test')
        with self.assertRaisesRegex(RegistryError,'compressed_image_exceeds_reservation'):other.stage(candidate)
        self.assertEqual(self.admissions,[])

    def test_timeout_after_pull_intent_survives_restart_and_only_inspection_reconciles(self):
        candidate=self.registry.discover('test');self.docker.error=subprocess.TimeoutExpired('redacted',1)
        with self.assertRaises(RegistryHeld):self.registry.stage(candidate)
        restarted=self.open();self.docker.error=None
        with self.assertRaises(RegistryHeld):restarted.stage(candidate)
        self.assertEqual(len([args for args in self.docker.calls if args[0]=='pull']),1)
        metadata=restarted.reconcile(candidate)
        self.assertEqual(metadata['imageId'],CONFIG)
        self.assertEqual(restarted.stage(candidate),metadata)
        self.assertEqual(len([args for args in self.docker.calls if args[0]=='pull']),1)

    def test_wrong_local_id_cannot_be_substituted_with_repo_digest(self):
        candidate=self.registry.discover('test')
        self.docker.image['Id']=candidate['manifestDigest']
        with self.assertRaises(RegistryHeld):self.registry.stage(candidate)
        with self.assertRaises(RegistryHeld):self.registry.reconcile(candidate)
        self.assertEqual(len([args for args in self.docker.calls if args[0]=='pull']),1)

    def test_wrong_platform_missing_digest_oversized_local_result_hold_without_pruning(self):
        for field,value in (('Os','windows'),('Architecture','arm64'),('RepoDigests',[REPO+'@'+'sha256:'+'e'*64]),('Size',4097)):
            with self.subTest(field=field):
                registry=DockerRegistry(self.root/(field+'.sqlite'),self.policy,capacity_admit=self.admit,runner=self.docker)
                candidate=registry.discover('test');old=self.docker.image[field];self.docker.image[field]=value
                with self.assertRaises(RegistryHeld):registry.stage(candidate)
                self.docker.image[field]=old
        self.assertFalse(any(args[0] in ('rm','prune','system','tag') for args in self.docker.calls))

    def test_engine_identity_change_blocks_before_pull(self):
        candidate=self.registry.discover('test');self.docker.engine='different-engine'
        with self.assertRaisesRegex(RegistryError,'docker_engine_changed'):self.registry.stage(candidate)
        self.assertFalse(any(args[0]=='pull' for args in self.docker.calls))

    def test_policy_binding_change_and_remote_docker_rejected(self):
        with self.assertRaisesRegex(RegistryError,'registry_policy_changed'):
            DockerRegistry(self.root/'registry.sqlite',replace(self.policy,engine_id='new-engine'),capacity_admit=self.admit,runner=self.docker)
        with self.assertRaisesRegex(RegistryError,'fixed_local_docker_endpoint'):
            replace(self.policy,docker_endpoint='tcp://example.com:2375')

    def test_concurrent_staging_issues_one_pull(self):
        candidate=self.registry.discover('test');barrier=threading.Barrier(2)
        def capacity(*args):barrier.wait(timeout=3);return True
        workers=[self.open(capacity_admit=capacity) for _ in range(2)]
        def stage(worker):
            try:return worker.stage(candidate)
            except RegistryHeld:return 'held'
        with ThreadPoolExecutor(max_workers=2) as pool:results=list(pool.map(stage,workers))
        self.assertEqual(results.count('held'),1)
        self.assertEqual(len([args for args in self.docker.calls if args[0]=='pull']),1)

    def test_crash_after_intent_before_pull_does_not_replay(self):
        candidate=self.registry.discover('test')
        with self.registry._db() as db:db.execute("INSERT INTO registry_staging VALUES(?,'pull_pending',NULL)",(self.registry._pull_id(candidate),))
        with self.assertRaises(RegistryHeld):self.open().stage(candidate)
        self.assertFalse(any(args[0]=='pull' for args in self.docker.calls))

    def test_new_descriptor_observation_cannot_bypass_uncertain_immutable_pull_fence(self):
        candidate=self.registry.discover('test');self.docker.error=TimeoutError()
        with self.assertRaises(RegistryHeld):self.registry.stage(candidate)
        self.docker.error=None
        self.docker.first=self.docker.first[:1]  # Attestation set changed, image did not.
        self.docker.tags=0
        later=self.registry.discover('test')
        self.assertNotEqual(later['candidateId'],candidate['candidateId'])
        self.assertEqual(later['manifestDigest'],candidate['manifestDigest'])
        with self.assertRaises(RegistryHeld):self.registry.stage(later)
        self.registry.reconcile(later)
        self.assertEqual(self.registry.stage(candidate)['imageId'],CONFIG)
        self.assertEqual(len([args for args in self.docker.calls if args[0]=='pull']),1)

    def test_other_candidate_cannot_start_pull_while_any_prior_pull_is_unknown(self):
        first=self.registry.discover('test');self.docker.error=TimeoutError()
        with self.assertRaises(RegistryHeld):self.registry.stage(first)
        self.docker.selected=entry(config='sha256:'+'e'*64);self.docker.first=[self.docker.selected];self.docker.tags=0
        later=self.registry.discover('test')
        self.assertNotEqual(first['manifestDigest'],later['manifestDigest'])
        with self.assertRaises(RegistryHeld):self.registry.stage(later)
        self.assertEqual(len([args for args in self.docker.calls if args[0]=='pull']),1)

    def test_journal_close_fault_after_pull_does_not_leak_or_replay(self):
        candidate=self.registry.discover('test')
        with patch.object(self.registry,'_finish',side_effect=OSError('private detail')):
            with self.assertRaisesRegex(RegistryHeld,'pull_outcome_requires_reconciliation') as error:self.registry.stage(candidate)
        self.assertNotIn('private detail',str(error.exception))
        with self.assertRaises(RegistryHeld):self.open().stage(candidate)
        self.assertEqual(len([args for args in self.docker.calls if args[0]=='pull']),1)

    def test_actual_subprocess_stdout_stderr_bounds_timeout_and_no_shell(self):
        with self.assertRaisesRegex(RegistryError,'docker_output_limit'):
            _run([sys.executable,'-c','import sys;sys.stderr.write("x"*200000)'],timeout=3,max_bytes=1024)
        with self.assertRaisesRegex(RegistryError,'docker_command_timeout'):
            _run([sys.executable,'-c','import time;time.sleep(3)'],timeout=.05,max_bytes=1024)
        result=_run([sys.executable,'-c','print("bounded")'],timeout=3,max_bytes=1024)
        self.assertEqual(result.stdout.strip(),b'bounded')

    @unittest.skipUnless(os.name=='posix','POSIX process-group termination regression')
    def test_actual_exited_parent_with_live_inherited_pipes_obeys_deadline_and_reaps_group(self):
        pidfile=self.root/'helper.pid'
        child='import os,time;open('+repr(str(pidfile))+',"w").write(str(os.getpid()));time.sleep(5)'
        parent='import subprocess,sys;subprocess.Popen([sys.executable,"-c",'+repr(child)+']);print("exited")'
        before={thread.ident for thread in threading.enumerate()}
        started=time.monotonic()
        with self.assertRaisesRegex(RegistryError,'docker_command_timeout'):
            _run([sys.executable,'-c',parent],timeout=.2,max_bytes=1024)
        self.assertLess(time.monotonic()-started,1.5)
        self.assertEqual([thread for thread in threading.enumerate()
            if thread.ident not in before and thread.name=='registry-command-output'],[])
        pid=int(pidfile.read_text())
        status=Path('/proc')/str(pid)/'stat'
        # The container's PID1 may retain an orphan zombie; it must not execute
        # or retain either pipe after our owned process-group termination.
        if status.exists():self.assertEqual(status.read_text().split()[2],'Z')


if __name__=='__main__':unittest.main()
