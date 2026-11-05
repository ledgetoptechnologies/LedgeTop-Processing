"""10k tiny owned files: namespace/identity scale, not 50 GB or auth proof."""
import hashlib
import json
import os
from pathlib import Path
import sys
import tempfile
import tracemalloc
import unittest
from unittest.mock import patch
import uuid

from processing import reference_protocol as p
from processing.source_spool import SourceSpool
from processing.source_release_filesystem import SourceReleaseFilesystem,ReleaseFilesystemError,encode_physical


@unittest.skipUnless(sys.platform.startswith('linux'),'real Linux private namespace mechanics')
class SourceReleaseNamespaceTests(unittest.TestCase):
    def test_ten_thousand_tiny_files_have_one_linear_inventory_and_bounded_target_work(self):
        count=10000;digest=hashlib.sha256(b'x').hexdigest()
        with tempfile.TemporaryDirectory() as directory,patch('processing.source_spool.os.path.ismount',return_value=True):
            root=Path(directory)/'spool';root.mkdir(mode=0o700)
            marker=root/'.ltds-source-spool-id';marker.write_text('a'*64);marker.chmod(0o600)
            spool=SourceSpool(root,mount_id='a'*64,safety_floor_bytes=0,max_object_bytes=1024,chunk_bytes=3)
            job=str(uuid.uuid4());release=str(uuid.uuid4());job_dir=root/job;job_dir.mkdir(mode=0o700)
            generation=job_dir/'1';generation.mkdir(mode=0o700)
            objects=[dict(id=f'00000000-0000-4000-8000-{i:012d}',objectKind='source',byteSize=1,sha256=digest) for i in range(1,count+1)]
            for obj in objects:
                target=root/spool.storage_key(job,1,obj['id']);target.write_bytes(b'x');target.chmod(0o600)
            qualification=dict(mountId=spool.mount_id,renameNoReplace=True,directoryFsync=True)
            for prefix,identity in (('root',spool._root_identity),('mount',spool._mount_identity),('marker',spool._marker_identity)):
                qualification[prefix+'Device']=str(identity[0]);qualification[prefix+'Inode']=str(identity[1])
            fs=SourceReleaseFilesystem(spool,qualification)
            auth=dict(jobId=job,generation=1,releaseId=release,eligibleObjects=objects,eligibleClosureSha256=p.digest(objects))
            original_stat=os.stat;calls=[0]
            def counted(*args,**kwargs):
                calls[0]+=1
                return original_stat(*args,**kwargs)
            with patch('processing.source_release_filesystem.os.stat',side_effect=counted):
                tracemalloc.start()
                try:
                    namespace,entries=fs.capture_namespace(auth,[],'e'*64)
                    _,peak=tracemalloc.get_traced_memory()
                finally:tracemalloc.stop()
                self.assertEqual(len(entries),count)
                self.assertLess(peak,64*1024*1024)
                self.assertLess(calls[0],count+256)
                records={(entry['objectKind'],entry['id']):dict(release_id=release,object_kind=entry['objectKind'],
                    object_id=entry['id'],phase='listed',physical_json=entry['physical']) for entry in entries}
                for obj in objects[:4]:
                    key=(obj['objectKind'],obj['id']);row=records[key]
                    def persist(phase,physical):
                        snapshot=dict(physical);snapshot.pop('effectIntent',None)
                        row.update(phase=phase,physical_json=encode_physical(phase,snapshot,obj))
                    def effect(kind,operation):
                        physical=json.loads(row['physical_json'])
                        physical['effectIntent']=dict(id=str(uuid.uuid4()),kind=kind,outcome='prepared')
                        row['physical_json']=encode_physical(row['phase'],physical,obj)
                        result=operation()
                        physical['effectIntent']['outcome']='completed'
                        row['physical_json']=encode_physical(row['phase'],physical,obj)
                        return result
                    fs.remove_one(auth,obj,row,persist,effect,namespace=namespace)
                    self.assertEqual(row['phase'],'removed')
                # Four deletions do not repeat a 10k-file inventory/stat scan.
                self.assertLess(calls[0],count+1024)
            unknown=generation/'unrecognized-owned-test-file';unknown.write_bytes(b'keep')
            with self.assertRaises(ReleaseFilesystemError):fs.audit(auth,list(records.values()),[],namespace=namespace)
            self.assertEqual(unknown.read_bytes(),b'keep')
            self.assertTrue((root/spool.storage_key(job,1,objects[-1]['id'])).is_file())
