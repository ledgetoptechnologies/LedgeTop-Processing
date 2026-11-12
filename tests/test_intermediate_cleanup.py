"""Synthetic Linux filesystem tests; never imports or deletes live client data."""
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sqlite3
import struct
import sys
import subprocess
import tempfile
import unittest
from unittest.mock import patch
import uuid
import zipfile

from processing.intermediate_cleanup import CacheCleanup, CleanupHeld, ALLOWLIST, digest
from processing.intermediate_dependencies import DependencyHeld, resource_references
from processing.intermediate_mounts import MountTopologyHeld, parse_mountinfo, mount_overlaps, sealed_metrics_observer, mounts_fingerprint, constrained_peer_host
from ops.early_intermediate_cleanup import assert_no_external_readers, verify_execution_fence, HostObserver, IMAGE, CONTAINER

BINDING = ('authorityId','nodeId','engineId','containerId','imageId','viewerTaskId',
           'generation','jobId','engineTaskId','maintenancePlanId')


class NamespaceTopologyTests(unittest.TestCase):
    def test_capability_aliases_and_malformed_authority_fail_closed(self):
        bad=[['SYS_ADMIN'],['sys_admin'],['CAP_SYS_ADMIN'],['cap_sys_admin'],['ALL'],['all'],
             ['CAP_ALL'],['cap_all'],'SYS_ADMIN',{},[None],[1],[''],['CAP_'],['NOT_A_CAP'],['CHOWN,ALL']]
        for caps in bad:
            with self.subTest(caps=caps):
                self.assertFalse(constrained_peer_host({'Privileged':False,'CapAdd':caps}))
        for caps in (None,[],['CHOWN'],['cap_net_bind_service']):
            self.assertTrue(constrained_peer_host({'Privileged':False,'CapAdd':caps}))
        for privileged in (True,1,'false',[]):
            self.assertFalse(constrained_peer_host({'Privileged':privileged,'CapAdd':None}))

    def test_cleanup_observer_uses_same_full_seal_before_ro_proc_overlap(self):
        runtime=alias_observer(Path('/synthetic-data'),Path('/proc'))
        runtime.gate=Path('/synthetic-gate')
        other=runtime.inspect('b'*64)
        other['Config']={}
        other['Mounts']=[{'Type':'bind','RW':False,'Source':'/proc'},
                         {'Type':'bind','RW':False,'Source':'/sys'}]
        seal={'metricsOnlyReviewed':True,'imageId':other['Image'],
              'applicationConfigSha256':digest(other['Config']),
              'hostConfigSha256':digest(other['HostConfig']),
              'mountsSha256':mounts_fingerprint(other['Mounts'])}
        runtime.auth['readOnlyObservers']={other['Id']:seal}
        with patch.object(Path,'read_text',return_value='1 0 8:1 / / rw - ext4 /dev/a rw'),patch(
                'ops.early_intermediate_cleanup.mount_overlaps',side_effect=MountTopologyHeld('unsupported_mount_source_topology')) as overlap,patch.object(
                Path,'lstat',side_effect=RuntimeError('gate-boundary')):
            with self.assertRaisesRegex(RuntimeError,'gate-boundary'):runtime(runtime.auth)
            overlap.assert_not_called()
            runtime.auth['readOnlyObservers']={}
            with self.assertRaises(MountTopologyHeld):runtime(runtime.auth)
            runtime.auth['readOnlyObservers']={other['Id']:dict(seal,hostConfigSha256='changed')}
            with self.assertRaisesRegex(CleanupHeld,'reviewed_observer_role_changed'):runtime(runtime.auth)
            other['Mounts'][0]['RW']=True
            runtime.auth['readOnlyObservers']={other['Id']:dict(seal,mountsSha256=mounts_fingerprint(other['Mounts']))}
            with self.assertRaises(MountTopologyHeld):runtime(runtime.auth)
            other['Mounts'][0]['RW']=False
            for caps in (['cap_sys_admin'],['all'],['CAP_ALL'],['NOT_A_CAP'],[None],'CHOWN'):
                other['HostConfig']['CapAdd']=caps
                runtime.auth['readOnlyObservers']={other['Id']:dict(seal,
                    hostConfigSha256=digest(other['HostConfig']),mountsSha256=mounts_fingerprint(other['Mounts']))}
                with self.subTest(caps=caps):
                    with self.assertRaisesRegex(CleanupHeld,'unconstrained_container_authority'):runtime(runtime.auth)

    def test_unrelated_opaque_namespace_roots_preserved(self):
        rows=parse_mountinfo('1 0 8:1 / / rw - ext4 /dev/a rw\n'
            '2 1 0:4 net:[4026532000] /run/netns/test rw - nsfs nsfs rw\n'
            '3 1 0:4 mnt:[4026532001] /run/mntns/test rw - nsfs nsfs rw')
        self.assertEqual(rows[1]['root'],'net:[4026532000]')
        self.assertEqual(rows[2]['root'],'mnt:[4026532001]')
        with self.assertRaisesRegex(MountTopologyHeld,'unsupported_mount_source_topology'):
            mount_overlaps('/native','/run/netns/test',rows,
                resolve=lambda p:Path(p),stat_path=lambda p:type('S',(),{'st_dev':os.makedev(8,1)})())

    def test_non_namespace_relative_or_malformed_root_still_refused(self):
        for kind,root in [('ext4','relative'),('nsfs','net:bad'),('nsfs','opaque')]:
            with self.subTest(kind=kind,root=root):
                with self.assertRaisesRegex(MountTopologyHeld,'relative_mount_identity'):
                    parse_mountinfo(f'1 0 8:1 {root} /point rw - {kind} source rw')

    def test_metrics_seal_binds_full_role_and_rejects_privilege(self):
        other={'Image':'image','Config':{},'HostConfig':{'Privileged':False,'CapAdd':[]},
               'Mounts':[{'Type':'bind','RW':False,'Source':'/proc'}]}
        auth={'metricsOnlyReviewed':True,'imageId':'image','applicationConfigSha256':digest(other['Config']),
              'hostConfigSha256':digest(other['HostConfig']),'mountsSha256':mounts_fingerprint(other['Mounts'])}
        self.assertTrue(sealed_metrics_observer(other,auth))
        for key in ('imageId','applicationConfigSha256','hostConfigSha256','mountsSha256'):
            changed=dict(auth);changed[key]='changed'
            self.assertFalse(sealed_metrics_observer(other,changed))
        for field,value in [('Privileged',True),('CapAdd',['SYS_ADMIN'])]:
            changed=copy.deepcopy(other);changed['HostConfig'][field]=value
            rebound=dict(auth,hostConfigSha256=digest(changed['HostConfig']))
            self.assertFalse(sealed_metrics_observer(changed,rebound))


def sha(data):
    return hashlib.sha256(data).hexdigest()


def glb(uri,rtc=None):
    value={'asset':{'version':'2.0'},'images':[{'uri':uri}]}
    if rtc is not None:
        value.update({'extensionsUsed':['CESIUM_RTC','KHR_draco_mesh_compression','KHR_materials_unlit'],
                      'extensionsRequired':['KHR_draco_mesh_compression'],'extensions':{'CESIUM_RTC':rtc}})
    data=json.dumps(value).encode()
    data += b' ' * ((-len(data)) % 4)
    return struct.pack('<4sII',b'glTF',2,20+len(data))+struct.pack('<I4s',len(data),b'JSON')+data


def b3dm(uri):
    value=b'{"BATCH_LENGTH":0}   '
    value += b' ' * ((-len(value)) % 4)
    body=glb(uri)
    return struct.pack('<4sIIIIII',b'b3dm',1,28+len(value)+len(body),len(value),0,0,0)+value+body


def pdf(*bodies):
    data=bytearray(b'%PDF-1.4\n%\xe2\xe3\xcf\xd3\n');offsets=[]
    for number,body in enumerate(bodies,1):
        offsets.append(len(data));data.extend(str(number).encode()+b' 0 obj\n'+body+b'\nendobj\n')
    xref=len(data);data.extend(b'xref\n0 '+str(len(bodies)+1).encode()+b'\n0000000000 65535 f \n')
    for offset in offsets:data.extend(('%010d 00000 n \n'%offset).encode())
    data.extend(b'trailer\n<< /Size '+str(len(bodies)+1).encode()+b' >>\nstartxref\n'+str(xref).encode()+b'\n%%EOF\n')
    return bytes(data)


class Fixture:
    def __init__(self):
        self.tmp=tempfile.TemporaryDirectory(prefix='lt-intermediate-test-')
        self.base=Path(self.tmp.name)
        self.root=self.base/str(uuid.uuid4())
        self.root.mkdir(mode=0o700)
        self.receipt=self.base/'receipt'
        self.receipt.mkdir(mode=0o700)
        self.finals={'odm_texturing/model.obj':b'mtllib model.mtl\nv 0 0 0\n',
                     'odm_texturing/model.mtl':b'newmtl t\nmap_Kd texture.png\n',
                     'odm_texturing/texture.png':b'\x89PNG\r\n\x1a\nfinal-texture',
                     'odm_orthophoto/orthophoto.tif':b'II*\0final-ortho',
                     'cameras.json':b'{}','images.json':b'[]'}
        self.preserved={'images/original.jpg':b'ORIGINAL',
                        'opensfm/reconstruction.json':b'POSES',
                        'opensfm/tracks.csv':b'TRACKS',
                        'opensfm/camera_models.json':b'CAMERAS',
                        'opensfm/undistorted/reconstruction.json':b'UNDISTORTED POSES',
                        'opensfm/undistorted/tracks.csv':b'UNDISTORTED TRACKS',
                        'opensfm/undistorted/openmvs/scene.ply':b'SPARSE OR MVS',
                        'opensfm/undistorted/depthmaps/depth.npz':b'RETAINED DEPTH'}
        for name,data in {**self.finals,**self.preserved}.items():
            self.write(name,data)
        for name in ALLOWLIST:
            self.write(name+'/cache.bin',b'rebuildable '+name.encode())
        self.auth={'schemaVersion':1,'operationId':str(uuid.uuid4()),'authorityId':str(uuid.uuid4()),
                   'nodeId':'main','engineId':'fixture-engine','containerId':'fixture-container',
                   'imageId':'sha256:'+'a'*64,'viewerTaskId':str(uuid.uuid4()),'generation':1,
                   'jobId':str(uuid.uuid4()),'engineTaskId':self.root.name,'maintenancePlanId':str(uuid.uuid4()),
                   'nativeStatus':40,'coordinatorArchiveDurable':True,'finalInventoryComplete':True,
                   'futureCacheConsumers':[],'selectedCaches':list(ALLOWLIST),'archiveRelativePath':'all.zip'}
        self.archive()
    def write(self,name,data):
        p=self.root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(data)
    def archive(self):
        with zipfile.ZipFile(self.root/'all.zip','w',compression=zipfile.ZIP_STORED) as z:
            for name,data in self.finals.items():
                self.write(name,data);z.writestr(name,data)
        self.auth['archiveSha256']=sha((self.root/'all.zip').read_bytes())
        self.auth['finalInventory']=[{'relativePath':p,'byteSize':len(b),'sha256':sha(b)}
                                     for p,b in self.finals.items()]
    def observer(self,auth):
        return {k:auth[k] for k in BINDING}
    def cleaner(self,**kw):
        return CacheCleanup(self.root,self.auth,self.observer,**kw)
    def assert_preserved(self,t):
        for name,data in {**self.finals,**self.preserved}.items():
            t.assertEqual((self.root/name).read_bytes(),data,name)
        t.assertEqual(sha((self.root/'all.zip').read_bytes()),self.auth['archiveSha256'])


@unittest.skipUnless(os.name=='posix','Linux dirfd fixture required')
class FilesystemTests(unittest.TestCase):
    def setUp(self):
        self.f=Fixture();self.addCleanup(self.f.tmp.cleanup)
    def test_plan_is_read_only(self):
        before={str(p.relative_to(self.f.root)) for p in self.f.root.rglob('*')}
        plan=self.f.cleaner().plan()
        self.assertGreater(plan['reclaimBytes'],0)
        self.assertEqual(before,{str(p.relative_to(self.f.root)) for p in self.f.root.rglob('*')})
        self.assertFalse((self.f.receipt/'cleanup.sqlite').exists())
    def test_apply_only_allowlist_and_repeat_idempotent(self):
        c=self.f.cleaner();plan=c.plan()
        self.assertEqual(c.apply(plan,self.f.receipt)['status'],'complete')
        self.assertEqual(c.apply(plan,self.f.receipt)['status'],'complete')
        self.assertTrue(all(not (self.f.root/p).exists() for p in ALLOWLIST))
        self.f.assert_preserved(self)
    def assert_journal_connections_closed(self, connections):
        self.assertEqual(len(connections),1)
        for db in connections:
            with self.assertRaises(sqlite3.ProgrammingError):
                db.execute('SELECT 1')
        journal=str(self.f.receipt/'cleanup.sqlite')
        for fd in Path('/proc/self/fd').iterdir():
            try:target=os.readlink(fd)
            except FileNotFoundError:continue
            self.assertFalse(target==journal or target.startswith(journal+'-'),target)
    def tracked_connect(self, connections):
        real=sqlite3.connect
        def connect(*args,**kwargs):
            db=real(*args,**kwargs);connections.append(db);return db
        return connect
    def test_successful_apply_closes_journal_connection_and_fd(self):
        c=self.f.cleaner();plan=c.plan();connections=[]
        with patch('processing.intermediate_cleanup.sqlite3.connect',self.tracked_connect(connections)):
            self.assertEqual(c.apply(plan,self.f.receipt)['status'],'complete')
        self.assert_journal_connections_closed(connections)
        self.f.assert_preserved(self)
    def test_exception_apply_closes_journal_and_preserves_replay(self):
        def checkpoint(stage,_):
            if stage=='after_rename':raise RuntimeError('synthetic interrupted apply')
        c=self.f.cleaner(checkpoint=checkpoint);plan=c.plan();connections=[]
        with patch('processing.intermediate_cleanup.sqlite3.connect',self.tracked_connect(connections)):
            with self.assertRaisesRegex(RuntimeError,'synthetic interrupted apply'):
                c.apply(plan,self.f.receipt)
        self.assert_journal_connections_closed(connections)
        self.assertEqual(self.f.cleaner().apply(plan,self.f.receipt)['status'],'complete')
        self.f.assert_preserved(self)
    def test_unknown_cache_selection_refused(self):
        self.f.auth['selectedCaches']=['opensfm']
        with self.assertRaises(CleanupHeld):self.f.cleaner()
    def test_active_consumer_refused(self):
        self.f.auth['futureCacheConsumers']=['opensplat-pending']
        with self.assertRaises(CleanupHeld):self.f.cleaner()
    def test_failed_or_unsettled_task_refused(self):
        self.f.auth['nativeStatus']=30
        with self.assertRaises(CleanupHeld):self.f.cleaner()
    def test_symlink_cache_refused(self):
        target=self.f.root/ALLOWLIST[0];shutil.rmtree(target)
        outside=self.f.base/'outside';outside.mkdir();(outside/'safe').write_bytes(b'keep')
        target.symlink_to(outside,target_is_directory=True)
        with self.assertRaises((CleanupHeld,OSError)):self.f.cleaner().plan()
        self.assertEqual((outside/'safe').read_bytes(),b'keep')
    def test_symlink_ancestor_refused(self):
        alias=self.f.base/'alias';alias.symlink_to(self.f.base,target_is_directory=True)
        with self.assertRaises(CleanupHeld):CacheCleanup(alias/self.f.root.name,self.f.auth,self.f.observer)
    def test_hardlinked_cache_refused_original_survives(self):
        cache=self.f.root/ALLOWLIST[0]/'cache.bin';cache.unlink()
        os.link(self.f.root/'images/original.jpg',cache)
        with self.assertRaises(CleanupHeld):self.f.cleaner().plan()
        self.f.assert_preserved(self)
    def test_partial_rename_crash_reconciles_same_inode(self):
        fired=[]
        def checkpoint(stage,_):
            if stage=='after_rename' and not fired:
                fired.append(True);raise RuntimeError('synthetic crash')
        c=self.f.cleaner(checkpoint=checkpoint);plan=c.plan()
        with self.assertRaises(RuntimeError):c.apply(plan,self.f.receipt)
        self.assertEqual(self.f.cleaner().apply(plan,self.f.receipt)['status'],'complete')
        self.f.assert_preserved(self)
    def test_actual_owned_process_loss_after_quarantine_reconciles(self):
        plan=self.f.cleaner().plan()
        payload=self.f.base/'child-input.json'
        payload.write_text(json.dumps({'root':str(self.f.root),'auth':self.f.auth,
                                       'plan':plan,'receipt':str(self.f.receipt)}))
        code="""import json,os,signal,sys
from processing.intermediate_cleanup import CacheCleanup
v=json.load(open(sys.argv[1]))
def observe(a):
 return {k:a[k] for k in ('authorityId','nodeId','engineId','containerId','imageId','viewerTaskId','generation','jobId','engineTaskId','maintenancePlanId')}
def checkpoint(stage,name):
 if stage=='after_rename':os.kill(os.getpid(),signal.SIGKILL)
CacheCleanup(v['root'],v['auth'],observe,checkpoint=checkpoint).apply(v['plan'],v['receipt'])
"""
        child=subprocess.run([sys.executable,'-B','-c',code,str(payload)],
                             cwd=Path(__file__).resolve().parents[1],timeout=15)
        self.assertEqual(child.returncode,-9)
        self.assertEqual(self.f.cleaner().apply(plan,self.f.receipt)['status'],'complete')
        self.f.assert_preserved(self)
    def test_partial_unlink_crash_reconciles(self):
        fired=[]
        def checkpoint(stage,_):
            if stage=='after_unlink' and not fired:
                fired.append(True);raise RuntimeError('synthetic crash')
        c=self.f.cleaner(checkpoint=checkpoint);plan=c.plan()
        with self.assertRaises(RuntimeError):c.apply(plan,self.f.receipt)
        self.assertEqual(self.f.cleaner().apply(plan,self.f.receipt)['status'],'complete')
        self.f.assert_preserved(self)
    def test_new_source_cache_after_quarantine_is_held(self):
        def checkpoint(stage,name):
            if stage=='after_rename':
                self.f.write(name+'/new-writer',b'new');raise RuntimeError('synthetic crash')
        c=self.f.cleaner(checkpoint=checkpoint);plan=c.plan()
        with self.assertRaises(RuntimeError):c.apply(plan,self.f.receipt)
        with self.assertRaises(CleanupHeld):self.f.cleaner().apply(plan,self.f.receipt)
        self.assertEqual((self.f.root/ALLOWLIST[0]/'new-writer').read_bytes(),b'new')
    def test_changed_file_after_plan_is_held(self):
        c=self.f.cleaner();plan=c.plan()
        self.f.write(ALLOWLIST[0]+'/cache.bin',b'changed')
        with self.assertRaises(CleanupHeld):c.apply(plan,self.f.receipt)
        self.assertEqual((self.f.root/ALLOWLIST[0]/'cache.bin').read_bytes(),b'changed')
    def test_replaced_leaf_before_unlink_is_retained(self):
        fired=[]
        def checkpoint(stage,name):
            if stage=='before_unlink' and not fired and name.endswith('cache.bin'):
                fired.append(True)
                q=self.f.root/'.lt-intermediate-cleanup'/self.f.auth['operationId']/name
                q.unlink();q.symlink_to(self.f.root/'images/original.jpg')
        c=self.f.cleaner(checkpoint=checkpoint);plan=c.plan()
        with self.assertRaises(CleanupHeld):c.apply(plan,self.f.receipt)
        self.f.assert_preserved(self)
    def test_generation_change_rejects_old_plan(self):
        plan=self.f.cleaner().plan();self.f.auth['generation']=2
        with self.assertRaises(CleanupHeld):self.f.cleaner().apply(plan,self.f.receipt)
    def test_full_crc_corruption_rejected_even_matching_outer_sha(self):
        path=self.f.root/'all.zip'
        with zipfile.ZipFile(path) as z:first=z.infolist()[0]
        data=bytearray(path.read_bytes());offset=first.header_offset+30+len(first.filename.encode())+len(first.extra)
        data[offset]^=1;path.write_bytes(data);self.f.auth['archiveSha256']=sha(data)
        with self.assertRaises((CleanupHeld,zipfile.BadZipFile)):self.f.cleaner().plan()
        self.assertTrue((self.f.root/ALLOWLIST[0]).exists())
    def test_missing_archive_entry_refused(self):
        with zipfile.ZipFile(self.f.root/'all.zip','w') as z:
            for p,b in list(self.f.finals.items())[1:]:z.writestr(p,b)
        self.f.auth['archiveSha256']=sha((self.f.root/'all.zip').read_bytes())
        with self.assertRaises(CleanupHeld):self.f.cleaner().plan()
    def test_obj_texture_depends_on_cache_refused(self):
        self.f.finals['odm_texturing/model.mtl']=b'newmtl t\nmap_Kd ../opensfm/undistorted/images/cache.bin\n';self.f.archive()
        with self.assertRaises(CleanupHeld):self.f.cleaner().plan()
    def test_glb_cache_uri_refused_before_mutation(self):
        self.f.finals['model.glb']=glb('opensfm/undistorted/images/cache.bin');self.f.archive()
        with self.assertRaises(CleanupHeld):self.f.cleaner().plan()
        self.assertFalse((self.f.root/'.lt-intermediate-cleanup').exists())
    def test_tileset_cache_uri_refused_before_mutation(self):
        self.f.finals['tileset.json']=json.dumps({'asset':{'version':'1.0'},'root':{'content':{'uri':'opensfm/undistorted/images/cache.bin'}}}).encode();self.f.archive()
        with self.assertRaises(CleanupHeld):self.f.cleaner().plan()
        self.assertFalse((self.f.root/'.lt-intermediate-cleanup').exists())
    def test_b3dm_embedded_glb_cache_uri_refused(self):
        self.f.finals['tile.b3dm']=b3dm('opensfm/undistorted/images/cache.bin');self.f.archive()
        with self.assertRaises(CleanupHeld):self.f.cleaner().plan()
    def test_normal_explicit_tiles_glb_texture_admitted(self):
        self.f.finals['model.glb']=glb('odm_texturing/texture.png')
        self.f.finals['tile.b3dm']=b3dm('odm_texturing/texture.png')
        self.f.finals['tileset.json']=json.dumps({'asset':{'version':'1.0'},'root':{'children':[{'content':{'uri':'tile.b3dm'}}]}}).encode();self.f.archive()
        c=self.f.cleaner();self.assertEqual(c.apply(c.plan(),self.f.receipt)['status'],'complete')
        self.f.assert_preserved(self)
    def test_cesium_rtc_real_shape_and_annotation_pdf_admitted(self):
        self.f.finals['model.glb']=glb('odm_texturing/texture.png',{'center':[123.5,-456.75,1000]})
        self.f.finals['odm_report/report.pdf']=pdf(b'<</Type /Annot /Subtype /Link /F % ordinary flags\n4 /Rect [0 0 1 1]>>')
        self.f.archive();self.assertGreater(self.f.cleaner().plan()['reclaimBytes'],0)
    def test_cesium_rtc_nonfinite_boolean_or_additional_property_refused(self):
        for rtc in ({'center':[0,0,float('inf')]},{'center':[0,True,0]},
                    {'center':[0,0]},{'center':[0,0,0],'url':'opensfm/undistorted/images/cache.bin'}):
            with self.subTest(rtc=rtc):
                self.f.finals['model.glb']=glb('odm_texturing/texture.png',rtc);self.f.archive()
                with self.assertRaises(DependencyHeld):self.f.cleaner().plan()
    def test_pdf_file_values_escaped_comments_and_indirect_references_refused(self):
        for field in (b'/F (../opensfm/undistorted/images/cache.bin)',
                      b'/#46 % hidden filename\n(../opensfm/undistorted/images/cache.bin)',
                      b'/F % indirect\n4 % generation\n0 R',b'/F <6361636865>',
                      b'/F << /Unix (cache) >>',b'/F 4294967296',b'/F 4.5'):
            with self.subTest(field=field):
                self.f.finals['report.pdf']=pdf(b'<<'+field+b'>>')
                self.f.archive()
                with self.assertRaises(DependencyHeld):self.f.cleaner().plan()
    def test_pdf_binary_stream_strings_comments_skipped_and_indirect_length_resolved(self):
        payload=b'\x00/F (cache) /Type /Filespec /ObjStm /URI /F 4 0 R endstream\xff'
        self.f.finals['report.pdf']=pdf(
            b'<< /Length 2 0 R /Filter /FlateDecode >>\nstream\n'+payload+b'\nendstream',
            str(len(payload)).encode(),
            b'<< /Type /Annot /F 4 /Contents (literal /F \\(cache\\) /Filespec) >> % /URI /F (comment)')
        self.f.archive();self.assertGreater(self.f.cleaner().plan()['reclaimBytes'],0)
    def test_pdf_indirect_filespec_and_escaped_dictionary_names_refused(self):
        for body in (b'<< /Type /Annot /#46 % reference\n2 0 R >>',
                     b'<< /Type /Catalog /A 2 0 R >>'):
            self.f.finals['report.pdf']=pdf(body,b'<< /Type /File#73pec /F (../opensfm/undistorted/images/cache.bin) >>')
            self.f.archive()
            with self.assertRaises(DependencyHeld):self.f.cleaner().plan()
    def test_pdf_file_attachment_fs_literal_and_indirect_string_refused_before_mutation(self):
        for raw in (pdf(b'<< /Type /Annot /Subtype /FileAttachment /FS (../opensfm/undistorted/images/cache.bin) >>'),
                    pdf(b'<< /Type /Annot /Subtype /FileAttachment /#46S 2 0 R >>',
                        b'(../opensfm/undistorted/images/cache.bin)')):
            self.f.finals['report.pdf']=raw;self.f.archive()
            with self.assertRaisesRegex(DependencyHeld,'unsupported_pdf_external_resource'):
                self.f.cleaner().plan()
            self.assertFalse((self.f.root/'.lt-intermediate-cleanup').exists())
    def test_pdf_catalog_local_page_open_action_fith_null_admitted(self):
        for destination in (b'[2 0 R /FitH null]',b'[2 0 R /XYZ null 123.5 1]',
                            b'[2 0 R /FitR 0 0 100 200]',b'[2 0 R /Fit]'):
            self.f.finals['report.pdf']=pdf(b'<< /Type /Catalog /OpenAction '+destination+b' >>',
                                            b'<< /Type /Page >>')
            self.f.archive();self.assertGreater(self.f.cleaner().plan()['reclaimBytes'],0)
    def test_pdf_catalog_open_action_external_or_malformed_destination_refused(self):
        for action in (b'<< /S /URI /URI (https://invalid.example/) >>',
                       b'<< /S /GoTo /D [2 0 R /FitH null] >>',
                       b'(../opensfm/undistorted/images/cache.bin)',
                       b'[1 /FitH null]',b'[2 0 R /Unknown null]',b'[2 0 R /FitH]',
                       b'[2 0 R /FitH null 0]',b'[2 0 R /FitR 0 0 null 100]',
                       b'[2 0 R /FitH true]',b'[2 0 R /FitH (cache)]',
                       b'[2 0 R /FitH '+b'9'*400+b'.0]'):
            self.f.finals['report.pdf']=pdf(b'<< /Type /Catalog /OpenAction '+action+b' >>',
                                            b'<< /Type /Page >>')
            self.f.archive()
            with self.assertRaises(DependencyHeld):self.f.cleaner().plan()
            self.assertFalse((self.f.root/'.lt-intermediate-cleanup').exists())
        self.f.finals['report.pdf']=pdf(b'<< /Type /Catalog /OpenAction [2 0 R /FitH null] >>',
                                        b'<< /Type /Filespec /F (cache) >>')
        self.f.archive()
        with self.assertRaises(DependencyHeld):self.f.cleaner().plan()
    def test_tileset_renamed_glb_binary_cache_reference_refused_before_mutation(self):
        self.f.finals['model.bin']=glb('opensfm/undistorted/images/cache.bin')
        self.f.finals['tileset.json']=json.dumps({'asset':{'version':'1.0'},'root':{'content':{'uri':'model.bin'}}}).encode()
        self.f.archive()
        with self.assertRaisesRegex(CleanupHeld,'output_depends_on_cache_or_missing_asset'):
            self.f.cleaner().plan()
        self.assertFalse((self.f.root/'.lt-intermediate-cleanup').exists())
    def test_raw_gltf_buffer_admitted_but_unknown_tile_or_untyped_binary_refused(self):
        self.f.finals['model.bin']=b'\x00\x01raw geometry buffer'
        self.f.finals['model.gltf']=json.dumps({'asset':{'version':'2.0'},
            'buffers':[{'uri':'model.bin','byteLength':len(self.f.finals['model.bin'])}]}).encode()
        self.f.archive();self.assertGreater(self.f.cleaner().plan()['reclaimBytes'],0)
        self.f.finals['tileset.json']=json.dumps({'asset':{'version':'1.0'},'root':{'content':{'uri':'model.bin'}}}).encode()
        self.f.archive()
        with self.assertRaisesRegex(DependencyHeld,'untyped_binary_resource'):self.f.cleaner().plan()
        self.f.finals.pop('tileset.json');(self.f.root/'tileset.json').unlink()
        self.f.finals.pop('model.gltf');(self.f.root/'model.gltf').unlink();self.f.archive()
        with self.assertRaisesRegex(DependencyHeld,'untyped_binary_resource'):self.f.cleaner().plan()
    def test_pdf_stream_boundary_unresolved_length_and_object_stream_refused(self):
        for raw in (pdf(b'<< /Length 200 >>\nstream\n/F (cache)\nendstream'),
                    pdf(b'<< /Length 2 0 R >>\nstream\nx\nendstream'),
                    pdf(b'<< /Type /ObjStm /Length 1 >>\nstream\nx\nendstream')):
            self.f.finals['report.pdf']=raw;self.f.archive()
            with self.assertRaises(DependencyHeld):self.f.cleaner().plan()
    def test_vrt_cache_dependency_refused(self):
        self.f.finals['map.vrt']=b'<VRTDataset><VRTRasterBand><SimpleSource><SourceFilename relativeToVRT="1">opensfm/undistorted/images/cache.bin</SourceFilename></SimpleSource></VRTRasterBand></VRTDataset>'
        self.f.archive()
        with self.assertRaises(CleanupHeld):self.f.cleaner().plan()
    def test_ply_external_texture_in_cache_refused(self):
        self.f.finals['mesh.ply']=b'ply\nformat ascii 1.0\ncomment TextureFile opensfm/undistorted/images/cache.bin\nelement vertex 0\nend_header\n'
        self.f.archive()
        with self.assertRaises(CleanupHeld):self.f.cleaner().plan()
    def test_implicit_tiles_hold_cleanup(self):
        self.f.finals['tileset.json']=json.dumps({'asset':{'version':'1.1'},'root':{'implicitTiling':{'subtrees':{'uri':'s/{level}.subtree'}}}}).encode();self.f.archive()
        with self.assertRaises(DependencyHeld):self.f.cleaner().plan()
    def test_wrl_and_unknown_final_container_refuse_before_mutation(self):
        for name in ('model.wrl','model.unreviewed'):
            with self.subTest(name=name):
                self.f.finals[name]=b'#VRML V2.0 utf8\nImageTexture { url "../opensfm/undistorted/images/cache.bin" }'
                self.f.archive()
                with self.assertRaisesRegex(DependencyHeld,'unsupported_final_resource_format'):
                    self.f.cleaner().plan()
                self.assertFalse((self.f.root/'.lt-intermediate-cleanup').exists())
                self.f.finals.pop(name)
                (self.f.root/name).unlink()
    def test_normal_nodeodm_leaf_formats_admitted(self):
        self.f.finals.update({'odm_georeferencing/model.laz':b'LASFpointcloud',
            'odm_georeferencing/model.las':b'LASFpointcloud',
            'odm_texturing/texture.jpg':b'\xff\xd8\xffjpeg',
            'odm_georeferencing/model.bounds':b'0 1 2 3\n',
            'odm_georeferencing/model.prj':b'PROJCS["WGS 84"]',
            'odm_report/report.pdf':pdf(b'<</Type /Catalog>>'),
            'odm_report/stats.txt':b'photos: 20\n','log.log':b'finished\n',
            'odm_georeferencing/model.csv':b'x,y,z\n0,0,0\n',
            'buffers/model.bin':b'pointdata','model.gltf':json.dumps({'asset':{'version':'2.0'},
                'buffers':[{'uri':'buffers/model.bin','byteLength':9}]}).encode()})
        self.f.archive();self.assertGreater(self.f.cleaner().plan()['reclaimBytes'],0)
    def test_renamed_container_and_external_pdf_hold(self):
        for name,data in (('not-image.png',b'#VRML V2.0 utf8'),
                          ('report.txt',b'<ImageTexture url="cache"/>'),
                          ('report.pdf',pdf(b'<< /Type /Filespec /F (../opensfm/undistorted/images/cache.bin) >>')),
                          ('report.pdf',pdf(b'<< /Type /ObjStm >>'))):
            with self.subTest(name=name,data=data):
                self.f.finals[name]=data;self.f.archive()
                with self.assertRaises(DependencyHeld):self.f.cleaner().plan()
                self.f.finals.pop(name);(self.f.root/name).unlink()
    def test_archive_count_above_ten_thousand_has_no_photo_cap(self):
        data=b'\x89PNG\r\n\x1a\nx';self.f.write('shared.png',data)
        items=[]
        with zipfile.ZipFile(self.f.root/'all.zip','w') as z:
            for n in range(10005):
                name='files/'+str(n)+'.bin';z.writestr(name,data)
                items.append({'archivePath':name,'relativePath':'shared.png','byteSize':len(data),'sha256':sha(data)})
        self.f.auth['finalInventory']=items;self.f.auth['archiveSha256']=sha((self.f.root/'all.zip').read_bytes())
        self.assertGreater(self.f.cleaner().plan()['reclaimBytes'],0)
    def test_hardlink_journal_refused(self):
        c=self.f.cleaner();plan=c.plan();os.link(self.f.root/'images/original.jpg',self.f.receipt/'cleanup.sqlite')
        with self.assertRaises(CleanupHeld):c.apply(plan,self.f.receipt)
        self.f.assert_preserved(self)


class ReaderAndMountTests(unittest.TestCase):
    def test_external_fd_and_mapping_refused(self):
        with tempfile.TemporaryDirectory(prefix='lt-proc-test-') as t:
            root=Path(t);task=root/'task';task.mkdir();(task/'cache').write_bytes(b'x')
            proc=root/'proc';entry=proc/'999999';(entry/'fd').mkdir(parents=True);(entry/'maps').write_text('')
            (entry/'fd/3').symlink_to(task/'cache')
            with self.assertRaises(CleanupHeld):assert_no_external_readers(task,proc=proc)
            (entry/'fd/3').unlink();(entry/'maps').write_text('1-2 r--p 0 00:00 0 '+str(task/'cache')+'\n')
            with self.assertRaises(CleanupHeld):assert_no_external_readers(task,proc=proc)
    def test_mountinfo_bind_alias_subtree_not_lexically_under_data(self):
        data='/native/data';alias='/alias'
        text='1 0 8:1 / / rw - ext4 /dev/x rw\n2 1 8:1 /native/data/old-task/opensfm /alias rw - ext4 /dev/x rw\n'
        class Info:st_dev=os.makedev(8,1)
        self.assertTrue(mount_overlaps(data,alias,parse_mountinfo(text),
                        resolve=lambda p:Path(p),stat_path=lambda _:Info()))
    def test_disjoint_same_filesystem_not_false_overlap(self):
        class Info:st_dev=os.makedev(8,1)
        self.assertFalse(mount_overlaps('/native/data','/different',parse_mountinfo(
            '1 0 8:1 / / rw - ext4 /dev/x rw\n'),resolve=lambda p:Path(p),stat_path=lambda _:Info()))
    def test_overlay_topology_refused(self):
        class Info:st_dev=os.makedev(0,42)
        with self.assertRaises(MountTopologyHeld):
            mount_overlaps('/native/data','/overlay',parse_mountinfo(
                '1 0 0:42 / / rw - overlay overlay rw\n'),resolve=lambda p:Path(p),stat_path=lambda _:Info())
    @unittest.skipUnless(os.environ.get('LT_CLEANUP_TEST_BIND')=='1',
                         'explicit isolated private-mount-namespace qualification required')
    def test_actual_preexisting_bind_alias_refused(self):
        # Enable ONLY in an approved private mount namespace, never host namespace.
        with tempfile.TemporaryDirectory(prefix='lt-bind-test-') as t:
            data=Path(t)/'data';alias=Path(t)/'alias';(data/'task/cache').mkdir(parents=True);alias.mkdir()
            subprocess.run(['mount','--bind',str(data/'task'),str(alias)],check=True)
            try:
                self.assertTrue(mount_overlaps(data,alias,parse_mountinfo(Path('/proc/self/mountinfo').read_text())))
                runtime=alias_observer(data,alias)
                with patch('ops.early_intermediate_cleanup.DATA',data):
                    with self.assertRaisesRegex(CleanupHeld,'another_container_can_write_native_data'):
                        runtime(runtime.auth)
            finally:
                subprocess.run(['umount',str(alias)],check=True)


class ActualObserverPredicateTests(unittest.TestCase):
    def values(self):
        a={'authorityId':str(uuid.uuid4()),'engineId':'fixture','maintenancePlanId':str(uuid.uuid4()),
           'maintenanceRevision':3,'maintenanceTargetImage':IMAGE,
           'operationId':str(uuid.uuid4()),'viewerTaskId':str(uuid.uuid4()),
           'generation':1,'engineTaskId':str(uuid.uuid4())}
        values={'engine_id':'fixture','authority':{'authorityId':a['authorityId']},
                'node':{'state':'updating','planId':a['maintenancePlanId'],'revision':3,'targetImage':IMAGE},
                'owned':{'requestId':a['operationId'],'planId':a['maintenancePlanId'],
                         'authorityId':a['authorityId'],'nodeId':'main','targetImage':IMAGE},
                'jobs':[],'job':{'authorityId':a['authorityId'],'status':'succeeded',
                         'requestId':a['viewerTaskId'],'nodeId':'main',
                         'result':{'engineTaskId':a['engineTaskId'],'nodeId':'main','cancelled':False}},
                'container':{'Id':CONTAINER,'Image':IMAGE,'State':{'Running':False,'Pid':0},
                             'HostConfig':{'RestartPolicy':{'Name':'no'}}}}
        return a,values
    def test_exact_authenticated_fence_accepts(self):
        a,v=self.values();verify_execution_fence(a,**v)
    def test_sealed_swap_target_accepts_only_old_stopped_container(self):
        a,v=self.values();target='sha256:'+'d'*64
        a['maintenanceTargetImage']=target
        v['node']['targetImage']=v['owned']['targetImage']=target
        verify_execution_fence(a,**v)
        v['container']['Image']=target
        with self.assertRaisesRegex(CleanupHeld,'native_engine_not_stopped'):
            verify_execution_fence(a,**v)
    def test_missing_or_changed_sealed_swap_target_refused(self):
        a,v=self.values();a.pop('maintenanceTargetImage')
        with self.assertRaisesRegex(CleanupHeld,'sealed_maintenance_target_required'):
            verify_execution_fence(a,**v)
        a,v=self.values();v['owned']['targetImage']='sha256:'+'d'*64
        with self.assertRaisesRegex(CleanupHeld,'owned_schedule_receipt_changed'):
            verify_execution_fence(a,**v)
        a,v=self.values();v['node']['targetImage']='sha256:'+'d'*64
        with self.assertRaisesRegex(CleanupHeld,'owned_maintenance_fence_changed'):
            verify_execution_fence(a,**v)
    def test_todd_or_other_unsettled_claim_refuses(self):
        a,v=self.values();v['jobs']=[{'nodeId':'main','status':'running'}]
        with self.assertRaises(CleanupHeld):verify_execution_fence(a,**v)
    def test_running_engine_refuses(self):
        a,v=self.values();v['container']['State']={'Running':True,'Pid':123}
        with self.assertRaises(CleanupHeld):verify_execution_fence(a,**v)
    def test_generation_change_refuses(self):
        a,v=self.values();a['generation']=2
        with self.assertRaises(CleanupHeld):verify_execution_fence(a,**v)
    def test_foreign_authority_or_plan_refuses(self):
        a,v=self.values();v['owned']['requestId']=str(uuid.uuid4())
        with self.assertRaises(CleanupHeld):verify_execution_fence(a,**v)
    def test_cancelled_or_failed_task_refuses(self):
        a,v=self.values();v['job']['status']='failed'
        with self.assertRaises(CleanupHeld):verify_execution_fence(a,**v)
    def test_preexisting_alias_writer_observer_refuses_before_filesystem_work(self):
        with tempfile.TemporaryDirectory(prefix='lt-observer-alias-') as t:
            data=Path(t)/'data';alias=Path(t)/'alias';data.mkdir();alias.mkdir()
            runtime=alias_observer(data,alias)
            with patch('ops.early_intermediate_cleanup.DATA',data), patch(
                    'ops.early_intermediate_cleanup.mount_overlaps',return_value=True):
                with self.assertRaisesRegex(CleanupHeld,'another_container_can_write_native_data'):
                    runtime(runtime.auth)
    def test_new_image_requires_explicit_profile_requalification(self):
        a,v=self.values();v['container']['Image']='sha256:'+'f'*64
        with self.assertRaises(CleanupHeld):verify_execution_fence(a,**v)


def alias_observer(data,alias):
    a,v=ActualObserverPredicateTests().values()
    a.update({'nodeId':'main','containerId':CONTAINER,'imageId':IMAGE})
    runtime=HostObserver.__new__(HostObserver)
    runtime.auth=a
    runtime.gate_fd=123  # No actual lock used: guard MUST refuse before accessing it.
    routes={'/v1/prototype/authority':v['authority'],'/v1/prototype/nodes/main':{'node':v['node']},
            '/v1/prototype/nodes/main/schedule-owned/'+a['operationId']:{'receipt':v['owned']},
            '/v1/prototype/jobs':{'jobs':v['jobs']},'/v1/prototype/jobs/'+a.get('jobId','missing'):{'job':v['job']}}
    a['jobId']=a.get('jobId','missing')
    class Api:
        def json(self,method,path):return routes[path]
    runtime.api=runtime.operator=Api()
    other='b'*64
    native=copy.deepcopy(v['container'])
    native['Mounts']=[{'Type':'bind','RW':True,'Source':str(data),'Destination':'/var/www/data'}]
    foreign={'Id':other,'Image':'fixture-other','State':{'Running':True},
             'HostConfig':{'Privileged':False,'CapAdd':[]},
             'Mounts':[{'Type':'bind','RW':True,'Source':str(alias)}]}
    runtime.inspect=lambda cid:native if cid==CONTAINER else foreign
    runtime.command=lambda args: a['engineId'] if args[-1]=='{{.ID}}' else CONTAINER+'\n'+other
    return runtime


if __name__=='__main__':
    unittest.main()
