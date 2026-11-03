import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
import unittest
from processing.large_paged_manifest_candidate import build,_digest

def pinned_module(node,module,*,required):
 if not node or not module:
  if required:raise RuntimeError('paired mode requires Node and canonicalize 2.1.0')
  return False
 if not os.path.isabs(module):raise RuntimeError('canonicalize module path must be absolute')
 script=r"""const fs=require('node:fs'),p=require('node:path');try{const root=fs.realpathSync(process.argv[1]),meta=JSON.parse(fs.readFileSync(p.join(root,'package.json'),'utf8')),entry=fs.realpathSync(require.resolve(root));if(meta.name!=='canonicalize'||meta.version!=='2.1.0'||!entry.startsWith(root+p.sep))process.exit(2)}catch(e){process.exit(2)}"""
 try:
  result=subprocess.run([node,'-e',script,module],capture_output=True,timeout=10)
 except (OSError,subprocess.TimeoutExpired):raise RuntimeError('canonicalize identity check failed') from None
 if result.returncode:raise RuntimeError('canonicalize identity check failed')
 return True

class PagedManifestCrossLanguageTests(unittest.TestCase):
 def test_missing_and_wrong_version_rejected_in_paired_mode(self):
  with self.assertRaises(RuntimeError):pinned_module(None,None,required=True)
  node=shutil.which('node')
  if not node:self.skipTest('Node unavailable for wrong-version test')
  with tempfile.TemporaryDirectory(prefix='paged-wrong-version-') as root:
   with self.assertRaises(RuntimeError):pinned_module(node,str(Path(root,'missing')),required=True)
   Path(root,'package.json').write_text('{"name":"canonicalize","version":"1.0.0","main":"index.js"}',encoding='utf8')
   Path(root,'index.js').write_text('module.exports=()=>"";',encoding='utf8')
   with self.assertRaises(RuntimeError):pinned_module(node,root,required=True)
 def test_node_canonicalize_unicode_escaping_and_page_boundary(self):
  module=os.environ.get('NODE_JCS_MODULE');node=shutil.which('node')
  if not pinned_module(node,module,required=os.environ.get('PAGED_MANIFEST_PAIRED_REQUIRED')=='1'):self.skipTest('pinned Node canonicalize module required')
  objects=[dict(id=f'i-{i:04}',name=f'photo_{i:04}.jpg',byteSize=1,sha256='a'*64,kind='image',mediaType='image/jpeg') for i in range(513)]
  objects[0]['mediaType']='image/"\u96ea\U0001f600\\test'
  closure,pages=build(objects)
  script=r"""const canon=require(process.argv[1]),crypto=require('node:crypto');let s='';process.stdin.setEncoding('utf8');process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>{const x=JSON.parse(s);function h(domain,v){return crypto.createHash('sha256').update(domain+'\0','utf8').update(canon(v),'utf8').digest('hex')}process.stdout.write(JSON.stringify({pages:x.pages.map(p=>h('ltds-paged-source-v1-page',p.body)),closure:h('ltds-paged-source-v1-closure',x.closure)}))});"""
  run=subprocess.run([node,'-e',script,module],input=json.dumps(dict(closure=closure,pages=pages),ensure_ascii=False),text=True,encoding='utf-8',capture_output=True,check=True,timeout=15)
  result=json.loads(run.stdout);self.assertEqual(result['pages'],[p['sha256'] for p in pages]);self.assertEqual(result['closure'],_digest(closure))

if __name__=='__main__':unittest.main()
