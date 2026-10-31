import io,os,stat,tarfile,tempfile,unittest
from pathlib import Path
from ops.verify_viewer_export import check,InvalidExport

class ViewerExportTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name);self.export=self.root/'export';self.export.mkdir(mode=0o700)
  self.app=self.root/'app.tar';self.shared=self.root/'shared.tar'
  self.archive(self.app,'apps/viewer');self.archive(self.shared,'packages/shared')
  self.archives=[(self.app,'apps/viewer'),(self.shared,'packages/shared')]
 def archive(self,path,prefix,extra=None):
  with tarfile.open(path,'w') as tar:
   directory=tarfile.TarInfo(prefix+'/');directory.type=tarfile.DIRTYPE;directory.mode=0o755;tar.addfile(directory)
   entry=tarfile.TarInfo(prefix+'/file');entry.mode=0o644;entry.size=4;tar.addfile(entry,io.BytesIO(b'data'))
   if extra:tar.addfile(extra)
 def test_extract_verify_and_current_owner(self):
  check(self.export,self.archives,extract=True);check(self.export,self.archives)
  self.assertEqual((self.export/'apps/viewer/file').stat().st_uid,os.geteuid())
 def test_modified_bytes_mode_unexpected_missing_and_symlink_fail(self):
  check(self.export,self.archives,extract=True);path=self.export/'apps/viewer/file'
  path.write_bytes(b'evil')
  with self.assertRaises(InvalidExport):check(self.export,self.archives)
  path.write_bytes(b'data');path.chmod(0o666)
  with self.assertRaises(InvalidExport):check(self.export,self.archives)
  path.chmod(0o644);extra=self.export/'unexpected';extra.write_text('x')
  with self.assertRaises(InvalidExport):check(self.export,self.archives)
  extra.unlink();path.unlink()
  with self.assertRaises(InvalidExport):check(self.export,self.archives)
  path.symlink_to('/etc/passwd')
  with self.assertRaises(InvalidExport):check(self.export,self.archives)
 def test_archive_traversal_link_and_corruption_fail_before_extract(self):
  for name,kind in (('apps/viewer/../escape','file'),('apps/viewer/link','symlink'),('/absolute','file')):
   with self.subTest(name=name):
    item=tarfile.TarInfo(name);item.mode=0o644
    if kind=='symlink':item.type=tarfile.SYMTYPE;item.linkname='/etc/passwd'
    self.archive(self.app,'apps/viewer',item)
    with self.assertRaises(InvalidExport):check(self.export,self.archives,extract=True)
    self.assertEqual(list(self.export.iterdir()),[])
  self.app.write_bytes(b'not a tar')
  with self.assertRaises(tarfile.TarError):check(self.export,self.archives,extract=True)
