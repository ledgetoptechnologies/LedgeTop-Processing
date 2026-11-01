"""Local, mocked safety checks; does not call Docker or start the probe."""
import ast
import base64
import hashlib
import io
import os
from pathlib import Path
import selectors
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
import unittest

SOURCE=Path(__file__).with_name('real_engine_bound_host.py')


def pinned_archive():
    marker=SOURCE.parent.parent/'.git'
    executable='git.exe' if marker.is_file() and 'gitdir: C:/' in marker.read_text(encoding='utf-8') else 'git'
    if not shutil.which(executable):raise unittest.SkipTest('matching Git executable unavailable')
    return subprocess.check_output([executable,'-c','core.autocrlf=false','-c','core.eol=lf','archive','--format=tar',
        'f0646ab2e1b92e079234f1ff960ca82dd80ca0f7','processing'],timeout=10)


def function(name, namespace):
    tree=ast.parse(SOURCE.read_text(encoding='utf-8'))
    node=next(item for item in ast.walk(tree) if isinstance(item,ast.FunctionDef) and item.name==name)
    exec(compile(ast.Module(body=[node],type_ignores=[]),str(SOURCE),'exec'),namespace)
    return namespace[name]


@unittest.skipUnless(os.name=='posix','pipe selector semantics require POSIX')
class HostSafety(unittest.TestCase):
    def test_live_output_and_timeout_are_bounded(self):
        namespace={'subprocess':subprocess,'selectors':selectors,'os':os,'time':time,'env':dict(os.environ)}
        run=function('run',namespace)
        start=time.monotonic()
        with self.assertRaisesRegex(RuntimeError,'bounded command output exceeded'):
            run([sys.executable,'-c','import sys;sys.stdout.write("x"*1000000);sys.stdout.flush()'],timeout=3)
        with self.assertRaisesRegex(RuntimeError,'owned command timeout'):
            run([sys.executable,'-c','import time;time.sleep(5)'],timeout=.15)
        self.assertLess(time.monotonic()-start,3)

    def test_command_failure_does_not_echo_raw_output(self):
        namespace={'subprocess':subprocess,'selectors':selectors,'os':os,'time':time,'env':dict(os.environ)}
        run=function('run',namespace)
        with self.assertRaises(RuntimeError) as caught:
            run([sys.executable,'-c','import sys;sys.stderr.write("PermissionError synthetic-secret");sys.exit(1)'])
        self.assertIn('permissionerror',str(caught.exception));self.assertNotIn('synthetic-secret',str(caught.exception))

    def test_invalid_json_reports_phase_and_length_only(self):
        decode=function('decode_json',{'json':__import__('json')})
        with self.assertRaises(RuntimeError) as caught:decode('probe result',b'not-json-secret')
        self.assertIn('probe result invalid JSON; byte_count=15',str(caught.exception))
        self.assertNotIn('secret',str(caught.exception))

    def test_fixed_reason_is_allowlisted_and_does_not_echo_unknown_error(self):
        safe=function('safe_reason',{'re':__import__('re')})
        self.assertEqual(safe(RuntimeError('owned command failed: exec; categories=enoent,typeerror')),
                         {'code':'exec_failed','categories':['enoent','typeerror']})
        self.assertEqual(safe(RuntimeError('owned command failed: cp; categories=enoent')),
                         {'code':'cp_failed','categories':['enoent']})
        self.assertEqual(safe(RuntimeError('stored_hash_mismatch')),
                         {'code':'stored_hash_mismatch'})
        self.assertEqual(safe(RuntimeError('stored object invalid JSON; byte_count=0; lines=0; expected_objects=0')),
                         {'code':'stored_object_invalid_json','byteCount':0,'lineCount':0,'expectedJsonObjects':0})
        self.assertIsNone(safe(RuntimeError('owned command failed: exec; categories=syntheticsecret')))
        self.assertIsNone(safe(RuntimeError('unknown synthetic-secret')))

    def test_stored_output_shape_does_not_echo_non_json(self):
        decode=function('decode_json',{'json':__import__('json')})
        safe=function('safe_reason',{'re':__import__('re')})
        with self.assertRaises(RuntimeError) as caught:
            decode('stored object',b'synthetic-secret\n{"name":"x","bytes":1,"sha256":"x","matchingCount":1}\n')
        result=safe(caught.exception)
        self.assertEqual(result['lineCount'],2);self.assertEqual(result['expectedJsonObjects'],1)
        self.assertNotIn('secret',str(result))

    def test_stored_json_rejects_duplicate_keys_and_nonfinite_numbers(self):
        decode=function('decode_json',{'json':__import__('json')})
        for raw in (b'{"name":"a","name":"b","bytes":1,"sha256":"x","matchingCount":1}',
                    b'{"name":"a","bytes":NaN,"sha256":"x","matchingCount":1}',
                    b'{"name":"a","bytes":1e999,"sha256":"x","matchingCount":1}'):
            with self.subTest(raw=raw),self.assertRaises(RuntimeError):decode('stored object',raw)

    def test_unhandled_exception_has_no_traceback_or_input_echo(self):
        result=subprocess.run([sys.executable,str(SOURCE)],input=b'not-json-synthetic-secret\n',capture_output=True,timeout=3)
        self.assertEqual(result.returncode,1)
        self.assertNotIn(b'Traceback',result.stderr);self.assertNotIn(b'synthetic-secret',result.stderr)
        self.assertIn(b'JSONDecodeError',result.stderr)

    def test_creation_failure_marks_unknown_before_docker_call(self):
        def fail(*args,**kwargs):raise RuntimeError('synthetic lost response')
        namespace={'same_daemon':lambda:None,'docker':fail,'verify':lambda *a:None,
                   'ENGINE':'engine','HELPER':'helper','base':'synthetic','label':'test','nonce':'abc',
                   'ENGINE_LAUNCH':'fixed','creation_uncertain':False,'owned':[],'re':__import__('re')}
        create=function('create',namespace)
        with self.assertRaisesRegex(RuntimeError,'lost response'):create('engine','none',[])
        self.assertIs(namespace['creation_uncertain'],True);self.assertEqual(namespace['owned'],[])

    def test_startup_log_classification_is_sanitized(self):
        namespace={'same_daemon':lambda:None,
                   'verify':lambda *args:{'State':{'Running':False,'ExitCode':1}},
                   'docker':lambda *args,**kwargs:subprocess.CompletedProcess(args,0,b'EACCES credential=synthetic-secret',b''),
                   'ENGINE':'engine'}
        diagnose=function('startup_diagnostic',namespace)
        result=diagnose('synthetic-id')
        self.assertEqual(result,{'phase':'engine_start','running':False,'exitCode':1,'logCategories':['EACCES']})
        self.assertNotIn('synthetic-secret',str(result))

    def test_fixed_source_and_probe_digests(self):
        archive=pinned_archive()
        probe=base64.b64decode(SOURCE.with_name('real_engine_bound_probe.b64').read_bytes(),validate=True)
        self.assertEqual(hashlib.sha256(archive).hexdigest(),'a92e9ee7130d6ca2e0218698ae82d5ba4cdebfdc728f2f2e76fcf77da6168993')
        self.assertEqual(hashlib.sha256(probe).hexdigest(),'63b033dd9f48152cd3959e01f2b252c5c82b87f4f8750d1a851e694c88fb304e')

    def test_pinned_archive_extracts_with_safe_modes(self):
        tree=ast.parse(SOURCE.read_text(encoding='utf-8'))
        texts=[item.value for item in ast.walk(tree) if isinstance(item,ast.Constant) and isinstance(item.value,str)
               and item.value.startswith('import hashlib,io,os,sys,tarfile\n')]
        self.assertEqual(len(texts),1);archive=pinned_archive()
        with tempfile.TemporaryDirectory() as temp:
            destination=str(Path(temp)/'src')
            script=texts[0].replace("'/work/src'",repr(destination))
            subprocess.run([sys.executable,'-c',script,hashlib.sha256(archive).hexdigest()],input=archive,check=True,timeout=10)
            source=Path(destination)/'processing'/'engine_ingress_guard.py'
            self.assertTrue(source.is_file());self.assertEqual(source.stat().st_mode & 0o777,0o644)

    def test_stored_inspection_uses_owned_container_tar(self):
        tree=ast.parse(SOURCE.read_text(encoding='utf-8'))
        calls=[node for node in ast.walk(tree) if isinstance(node,ast.Call) and isinstance(node.func,ast.Name)
               and node.func.id=='docker' and len(node.args)>2 and isinstance(node.args[0],ast.Constant)
               and node.args[0].value=='exec' and isinstance(node.args[2],ast.Constant) and node.args[2].value=='/bin/tar']
        self.assertEqual(len(calls),1)

    def test_stored_tar_checks_exact_file_and_rejects_ambiguity(self):
        import re
        verify=function('verify_stored_tar',{'re':re,'tarfile':tarfile,'io':io,'hashlib':hashlib})
        identifier='00000000-0000-4000-8000-000000000001';data=b'synthetic'
        expected={'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
        def archive(items):
            out=io.BytesIO()
            with tarfile.open(fileobj=out,mode='w:') as tar:
                for name,content in items:
                    info=tarfile.TarInfo(name)
                    if content is None:info.type=tarfile.SYMTYPE;info.linkname='elsewhere';tar.addfile(info)
                    elif content is True:info.type=tarfile.DIRTYPE;tar.addfile(info)
                    else:info.size=len(content);tar.addfile(info,io.BytesIO(content))
            return out.getvalue()
        directory=archive([(identifier,True),(identifier+'/synthetic.jpg',data),(identifier+'/body.json',b'{}')])
        self.assertEqual(verify(directory,identifier,expected)['matchingCount'],1)
        with self.assertRaisesRegex(RuntimeError,'stored_hash_mismatch'):
            verify(archive([(identifier,True),(identifier+'/synthetic.jpg',b'x'*len(data))]),identifier,expected)
        with self.assertRaisesRegex(RuntimeError,'stored_length_mismatch'):
            verify(archive([(identifier,True),(identifier+'/synthetic.jpg',b'x')]),identifier,expected)
        with self.assertRaisesRegex(RuntimeError,'stored_duplicate_or_missing'):
            verify(archive([(identifier,True),(identifier+'/synthetic.jpg',data),(identifier+'/synthetic_.jpg',data)]),identifier,expected)
        for bad in (archive([(identifier,None)]),archive([(identifier,True),(identifier+'/synthetic.jpg',None)]),
                    archive([(identifier,True),(identifier+'/synthetic.jpg',data),(identifier+'/synthetic.jpg',data)]),
                    archive([(identifier,True),(identifier+'/../synthetic.jpg',data)])):
            with self.assertRaises(RuntimeError):verify(bad,identifier,expected)
