import json
import os
import shutil
import subprocess
import unittest
from processing.large_paged_manifest_candidate import build,_digest

class PagedManifestCrossLanguageTests(unittest.TestCase):
 def test_node_canonicalize_unicode_escaping_and_page_boundary(self):
  module=os.environ.get('NODE_JCS_MODULE');node=shutil.which('node')
  if not module or not node:self.skipTest('pinned Node canonicalize module required')
  objects=[dict(id=f'i-{i:04}',name=f'photo_{i:04}.jpg',byteSize=1,sha256='a'*64,kind='image',mediaType='image/jpeg') for i in range(513)]
  objects[0]['mediaType']='image/"\u96ea\U0001f600\\test'
  closure,pages=build(objects)
  script=r"""const canon=require(process.argv[1]),crypto=require('node:crypto');let s='';process.stdin.setEncoding('utf8');process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>{const x=JSON.parse(s);function h(domain,v){return crypto.createHash('sha256').update(domain+'\0','utf8').update(canon(v),'utf8').digest('hex')}process.stdout.write(JSON.stringify({pages:x.pages.map(p=>h('ltds-paged-source-v1-page',p.body)),closure:h('ltds-paged-source-v1-closure',x.closure)}))});"""
  run=subprocess.run([node,'-e',script,module],input=json.dumps(dict(closure=closure,pages=pages),ensure_ascii=False),text=True,encoding='utf-8',capture_output=True,check=True,timeout=15)
  result=json.loads(run.stdout);self.assertEqual(result['pages'],[p['sha256'] for p in pages]);self.assertEqual(result['closure'],_digest(closure))

if __name__=='__main__':unittest.main()
