"""Launcher safety checks use no Docker daemon or network."""
import contextlib,io,json,sys,tempfile,unittest
from pathlib import Path
from unittest.mock import patch
from ops import run_native_cross_rehearsal as wrapper

class WrapperSafety(unittest.TestCase):
    def test_unpinned_refuses_before_git_or_docker(self):
        with patch.object(sys,'argv',['fixture','--viewer-repo','.','--processing-repo','.']), \
             patch.object(wrapper,'capture',side_effect=AssertionError('must not inspect')), \
             self.assertRaisesRegex(SystemExit,'not been pinned'):
            wrapper.main()

    def test_archive_cap_refuses_before_writing_oversize_block(self):
        with tempfile.TemporaryDirectory() as temporary,patch.object(wrapper,'MAX_ARCHIVE_BYTES',1024):
            target=Path(temporary)/'bounded.tar'
            with self.assertRaisesRegex(RuntimeError,'archive too large'):
                wrapper.archive(Path.cwd(),wrapper.FIXTURE_COMMIT,['processing'],target)
            self.assertLessEqual(target.stat().st_size,1024)

    def test_uncertain_create_preserves_evidence_and_never_cleans_by_name(self):
        import subprocess
        calls=[];viewer='a'*40
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            def capture(args,timeout=15):
                calls.append(args)
                if args[0]=='git':return args[-1].split('^')[0]
                if args[1:3]==['info','--format']:return 'synthetic-daemon'
                if args[1:3]==['image','inspect']:return wrapper.IMAGE
                if args[1]=='create':raise subprocess.TimeoutExpired(args,timeout)
                raise AssertionError('unexpected Docker command')
            def archive(_repo,_revision,_paths,destination):destination.write_bytes(b'synthetic');return 'b'*64
            with patch.object(sys,'argv',['fixture','--viewer-repo',str(root),'--processing-repo',str(root)]), \
                 patch.object(wrapper,'VIEWER_COMMIT_PIN',viewer),patch.object(wrapper,'capture',side_effect=capture), \
                 patch.object(wrapper,'call'),patch.object(wrapper,'archive',side_effect=archive), \
                 patch.object(wrapper.tempfile,'mkdtemp',return_value=str(root)), \
                 contextlib.redirect_stdout(io.StringIO()),contextlib.redirect_stderr(io.StringIO()), \
                 self.assertRaises(subprocess.TimeoutExpired):
                wrapper.main()
            self.assertTrue((root/'run.json').is_file())
            self.assertFalse((root/'container.id').exists())
            self.assertFalse(any(args[0]=='docker' and args[1] in ('stop','rm') for args in calls))

    def test_cleanup_uncertainty_fails_successful_run_without_mutation(self):
        calls=[];daemon_checks=[0];viewer='a'*40;cid='c'*64
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            def capture(args,timeout=15):
                calls.append(args)
                if args[0]=='git':return args[-1].split('^')[0]
                if args[1]=='info':
                    daemon_checks[0]+=1
                    return 'synthetic-daemon' if daemon_checks[0]<3 else 'other-daemon'
                if args[1:3]==['image','inspect']:return wrapper.IMAGE
                if args[1]=='create':return cid
                if args[1]=='inspect':return json.dumps([{'Id':cid,'Image':wrapper.IMAGE,'Config':{'Labels':{wrapper.LABEL:'d'*32}},'HostConfig':{'NetworkMode':'none'}}])
                if args[1]=='wait':return '0'
                raise AssertionError('unexpected command')
            def archive(_repo,_revision,_paths,destination):destination.write_bytes(b'synthetic');return 'b'*64
            with patch.object(sys,'argv',['fixture','--viewer-repo',str(root),'--processing-repo',str(root)]), \
                 patch.object(wrapper,'VIEWER_COMMIT_PIN',viewer),patch.object(wrapper,'capture',side_effect=capture), \
                 patch.object(wrapper.secrets,'token_hex',return_value='d'*32),patch.object(wrapper,'call'), \
                 patch.object(wrapper,'archive',side_effect=archive),patch.object(wrapper.tempfile,'mkdtemp',return_value=str(root)), \
                 contextlib.redirect_stdout(io.StringIO()),contextlib.redirect_stderr(io.StringIO()), \
                 self.assertRaisesRegex(RuntimeError,'cleanup uncertain'):
                wrapper.main()
            self.assertEqual((root/'container.id').read_text().strip(),cid)
            self.assertFalse(any(args[0]=='docker' and args[1] in ('stop','rm') for args in calls))

if __name__=='__main__':unittest.main()
