"""Optional local cross-language comparison; pass Viewer planner JS path.

Synthetic budget calculations only; no network, files transferred, or authorization.
"""
import base64
import copy
import json
from pathlib import Path
import subprocess
import sys
from processing import reference_protocol as p
from processing.reference_input_budget_v2 import decode_budget_proposal

root = Path(__file__).resolve().parents[1]
vectors = json.loads((root / 'contracts/input-budget-v2-raw-vectors.json').read_text(encoding='utf-8'))
fixture = json.loads((root / 'contracts/input-budget-v2-fixture.json').read_text(encoding='utf-8'))
proposal = copy.deepcopy(fixture['input'])
proposal['objects'] = [{'id': f'photo-{i}', 'byteSize': (i % 8 + 1) * 1024 * 1024 + 1, 'sha256': 'a' * 64} for i in range(16831)] + [{'id': 'aux-gcp', 'byteSize': 123, 'sha256': 'b' * 64}]
vectors.append({'name': '16832-varied-plus-aux', 'rawBase64': base64.b64encode(p.canonical_json(proposal)).decode(), 'accepted': True})
script = """const fs=require('fs'),m=require(process.argv[1]);const v=JSON.parse(fs.readFileSync(0,'utf8'));console.log(JSON.stringify(v.map(x=>{try{return {ok:true,out:m.parseReferenceV2Bytes(Buffer.from(x.rawBase64,'base64'))}}catch(e){return {ok:false}}})));"""
result = subprocess.run(['node', '-e', script, str(Path(sys.argv[1]).resolve())], input=json.dumps(vectors), text=True, capture_output=True, check=True, timeout=30)
viewer_results = json.loads(result.stdout)
if len(viewer_results) != len(vectors):
    raise RuntimeError('result count mismatch')
for vector, observed in zip(vectors, viewer_results):
    try:
        local = decode_budget_proposal(base64.b64decode(vector['rawBase64'], validate=True))
        accepted = True
    except p.ProtocolError:
        local, accepted = None, False
    if accepted != vector['accepted'] or accepted != observed['ok'] or (accepted and local != observed['out']):
        raise RuntimeError('planner mismatch: ' + vector['name'])
print(f'{len(vectors)} synthetic cross-language cases matched')
