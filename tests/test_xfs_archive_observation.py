"""No mounts, quotas, or host settings are modified by these parser tests."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ops import xfs_archive_observation as x

REPORT='#0 0 0 0 00 [--------]\n#42001 1024 0 65536 00 [--------]\n'


@unittest.skipUnless(os.name=='posix','Linux filesystem descriptors required')
class ObservationTest(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.mount=Path(self.tmp.name);self.project=self.mount/'project';self.project.mkdir()
        device=self.mount.stat().st_dev
        self.state='Project quota state on '+str(self.mount)+' (/dev/loop6)\n  Accounting: ON\n  Enforcement: ON\n'
        self.mapping={'id':123,'target':str(self.mount),'source':'/dev/loop6','fstype':'xfs','options':'rw,prjquota',
                      'maj:min':'%d:%d'%(os.major(device),os.minor(device))}

    def check(self,state=None,report=REPORT,mounts=None,project=(42001,0x200)):
        if state is None:state=self.state
        with patch.object(x.os,'geteuid',return_value=0),patch.object(x,'_mount',side_effect=mounts or [self.mapping,self.mapping]),\
             patch.object(x,'_project',return_value=project),patch.object(x,'_run',side_effect=[state,report]):
            return x.observe(str(self.mount),str(self.project),42001,64*1024*1024)

    def test_observed_kernel_shape(self):
        self.assertEqual(self.check()['hardBytes'],64*1024*1024)

    def test_empty_ambiguous_and_no_enforcement_hold(self):
        for state in ('',self.state.replace('Enforcement: ON','Enforcement: OFF'),self.state+'  Accounting: ON\n'):
            with self.subTest(state=state),self.assertRaises(x.ObservationUnavailable):self.check(state=state)
        for report in ('',REPORT+REPORT,REPORT.replace('65536','0')):
            with self.subTest(report=report),self.assertRaises(x.ObservationUnavailable):self.check(report=report)

    def test_project_and_mount_substitution_hold(self):
        with self.assertRaises(x.ObservationUnavailable):self.check(project=(42001,0))
        with self.assertRaises(x.ObservationUnavailable):self.check(project=(42002,0x200))
        with self.assertRaises(x.ObservationUnavailable):self.check(mounts=[self.mapping,dict(self.mapping,source='/dev/other')])
        self.project.rename(self.mount/'elsewhere');self.project.symlink_to('elsewhere',target_is_directory=True)
        with self.assertRaises(x.ObservationUnavailable):self.check()

    def test_findmnt_rejects_ext4_empty_and_multiple(self):
        import json
        for filesystems in ([],[dict(self.mapping,fstype='ext4')],[self.mapping,self.mapping]):
            with self.subTest(filesystems=filesystems),patch.object(x,'_run',return_value=json.dumps({'filesystems':filesystems})):
                with self.assertRaises(x.ObservationUnavailable):x._mount(str(self.mount))
