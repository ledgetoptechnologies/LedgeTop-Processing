import os
import platform
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from ops import xfs_archive_observation as x

@unittest.skipUnless(os.name=='posix','Linux directory descriptors required')
class ObservationTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.mount=Path(self.tmp.name);self.project=self.mount/'project';self.project.mkdir()
        dev=self.mount.stat().st_dev
        self.record=('123','%d:%d'%(os.major(dev),os.minor(dev)),'/',str(self.mount),'rw','xfs','/dev/loop6','rw')
    def check(self,records=None,ids=None,project=(42001,0x200),quota=(64*1024*1024,0)):
        with patch.object(x.os,'geteuid',return_value=0),patch.object(x,'_mount_record',side_effect=records or [self.record,self.record]),\
             patch.object(x,'_mount_id',side_effect=ids or [123,123,123,123]),patch.object(x,'_project',return_value=project),\
             patch.object(x,'_kernel_quota',return_value=quota):
            return x._observe_unqualified(str(self.mount),str(self.project),42001,64*1024*1024)
    def test_draft_happy_path_and_public_disabled(self):
        self.assertEqual(self.check()['hardBytes'],64*1024*1024)
        with self.assertRaises(x.ObservationUnavailable):x.observe(str(self.mount),str(self.project),42001,64*1024*1024)
    def test_enforcement_and_substitution_fail_closed(self):
        for kwargs in ({'project':(42001,0)},{'project':(42002,0x200)},{'quota':(0,0)},
                       {'ids':[123,124]},{'records':[self.record,(*self.record[:-1],'changed')]}):
            with self.subTest(kwargs=kwargs),self.assertRaises(x.ObservationUnavailable):self.check(**kwargs)
        self.project.rename(self.mount/'elsewhere');self.project.symlink_to('elsewhere',target_is_directory=True)
        with self.assertRaises(x.ObservationUnavailable):self.check()
    def test_no_follow_ancestor(self):
        link=self.mount/'alias';link.symlink_to(self.project,target_is_directory=True)
        with self.assertRaises(OSError):x._open_directory(link)
    def test_non_xfs_or_foreign_mount_refused(self):
        for record in (self.record[:5]+('ext4',)+self.record[6:],self.record[:3]+('/different',)+self.record[4:]):
            with self.subTest(record=record),self.assertRaises(x.ObservationUnavailable):self.check(records=[record])
    @unittest.skipUnless(platform.machine()=='x86_64','specific syscall ABI')
    def test_actual_kernel_refuses_unqualified_non_xfs_directory(self):
        fd=os.open(self.mount,os.O_RDONLY|os.O_DIRECTORY)
        try:
            with self.assertRaises((OSError,x.ObservationUnavailable)):x._kernel_quota(fd,42001)
        finally:os.close(fd)
