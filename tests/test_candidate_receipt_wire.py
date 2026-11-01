"""Cross-language wire conformance only; no auth, custody, or endpoint."""
import base64
import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import unittest
from processing import reference_protocol as p

ROOT=Path(__file__).resolve().parents[1]
PATH=ROOT/'contracts'/'viewer-receipt-binding-v1'
spec=importlib.util.spec_from_file_location('candidate_receipt_codec', PATH/'codec.py')
codec=importlib.util.module_from_spec(spec);spec.loader.exec_module(codec)
FIXTURE=json.loads((ROOT/'contracts'/'reference-v1.contract.json').read_text(encoding='utf8'))
BIND='00000000-0000-4000-8000-000000000001'
ATTEMPT='00000000-0000-4000-8000-000000000002'

def wrap(payload, **changes):
    env={'schemaVersion':1,'bindingId':BIND,'attemptId':ATTEMPT,
         'receiptSha256':hashlib.sha256(payload).hexdigest(),
         'receiptBase64url':base64.urlsafe_b64encode(payload).rstrip(b'=').decode('ascii')}
    env.update(changes)
    return p.canonical_json(env)

class CandidateReceiptWireTests(unittest.TestCase):
    def test_python_and_javascript_agree_on_golden_and_hostile_wires(self):
        payload=p.canonical_json(FIXTURE['acceptance'])
        valid=wrap(payload)
        golden=json.loads((PATH/'golden.json').read_text(encoding='utf8'))
        self.assertEqual(golden['receiptCanonicalUtf8'].encode('utf8'),payload)
        self.assertEqual(golden['receiptSha256'],hashlib.sha256(payload).hexdigest())
        self.assertEqual(golden['envelopeCanonicalUtf8'].encode('utf8'),valid)
        changed=payload.replace(b'receipt-1',b'receipt-2')
        # Deliberately reusing the old digest must not accept changed bytes.
        stale=wrap(changed,receiptSha256=hashlib.sha256(payload).hexdigest())
        reordered=dict(FIXTURE['acceptance']);reordered['acceptedObjectIds']=['result-2','result-1']
        different_order=wrap(p.canonical_json(reordered))
        biggest=dict(FIXTURE['acceptance'],receiptId='r'*128,storageReceiptId='s'*128,
            jobId='j'*128,generation=p.MAX_SAFE_INTEGER,environment='production',
            acceptedObjectIds=[str(i).zfill(5)+'x'*123 for i in range(p.MAX_OBJECTS)],
            acceptedAt='2000-01-01T00:00:00.123456+00:00')
        biggest_bytes=p.canonical_json(biggest)
        self.assertLess(len(biggest_bytes),codec.MAX_RECEIPT)
        self.assertLess(len(wrap(biggest_bytes)),codec.MAX_ENVELOPE)
        manifest={key:biggest[key] for key in ('jobId','generation','environment','pipelineDigest','resultManifestSha256')}
        manifest['objects']=[{'id':identifier} for identifier in biggest['acceptedObjectIds']]
        self.assertEqual(p.validate_acceptance(biggest,result_manifest=manifest,
            input_manifest_sha256=biggest['inputManifestSha256']),biggest)
        bound=dict(biggest,acceptedAt='x'*160) # conservative 40 Unicode scalars * four bytes
        self.assertEqual(len(p.canonical_json(bound)),1311009)
        self.assertEqual(len(wrap(p.canonical_json(bound))),1748238)
        vectors=[
            (valid,True), (valid,True), (different_order,True), (wrap(biggest_bytes),True), (stale,False),
            (wrap(payload,receiptSha256=hashlib.sha256(payload).hexdigest().upper()),False),
            (wrap(payload,receiptSha256='0'*63),False),
            (wrap(payload,receiptBase64url=base64.urlsafe_b64encode(payload).decode('ascii')),False),
            (wrap(b'{"a":1,"\\u0061":1}'),False),
            (wrap(b'{"a":"\\ud800"}'),False), (wrap(b'{"a":"\xff"}'),False),
            (wrap(b'{"a":-0}'),False), (wrap(b'{"a":1.0}'),False), (wrap(b'{"a":1e0}'),False),
            (wrap(b'{"a":9007199254740991}'),True), (wrap(b'{"a":9007199254740992}'),False),
            (wrap(payload,attemptId=[ATTEMPT]),False),
            (wrap(payload,attemptId=ATTEMPT+'\n'),False), (wrap(payload,bindingId=BIND+'\n'),False),
            (wrap(b'{"a":1}',receiptBase64url='e31='),False),
            (wrap(b'{}',receiptBase64url='e31'),False),
            (p.canonical_json({'attemptId':ATTEMPT,'bindingId':BIND,'schemaVersion':1,'receiptSha256':'0'*64}),False),
            (b'{"schemaVersion":1,"schemaVersion":1}',False),
            (b'{"schemaVersion":1,"\\u0073chemaVersion":1}',False),
            (wrap(p.canonical_json({'a':'\U0001f680'})),True),
            (wrap(p.canonical_json({'a':[[[[[[[1]]]]]]]})),True),
            (wrap(p.canonical_json({'a':[[[[[[[[1]]]]]]]]})),False),
            (wrap(p.canonical_json({'a':[1]*49998})),True),
            (wrap(p.canonical_json({'a':[1]*49999})),False),
            (b'x'*(codec.MAX_ENVELOPE+1),False),
            (wrap(b'{"x":"'+b'a'*codec.MAX_RECEIPT+b'"}'),False),
        ]
        python=[]
        for raw,_ in vectors:
            try: codec.decode(raw);python.append(True)
            except ValueError: python.append(False)
        self.assertEqual(python,[expected for _,expected in vectors])
        script="const c=require(process.argv[1]);let s='';process.stdin.setEncoding('utf8');process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>{let a=JSON.parse(s);process.stdout.write(JSON.stringify(a.map(x=>{try{c.decode(Buffer.from(x,'base64'));return true}catch{return false}})))});"
        proc=subprocess.run(['node','-e',script,str(PATH/'codec.cjs')],
            input=json.dumps([base64.b64encode(x).decode('ascii') for x,_ in vectors]),
            text=True,capture_output=True,timeout=15,check=True)
        self.assertEqual(json.loads(proc.stdout),python)
        self.assertNotEqual(codec.decode(valid)[2],codec.decode(different_order)[2])

    def test_codec_does_not_replace_v1_semantic_validation(self):
        env,receipt,_=codec.decode(wrap(p.canonical_json(FIXTURE['acceptance'])))
        checked=p.validate_acceptance(receipt,result_manifest=FIXTURE['resultManifest'],
                                      input_manifest_sha256=receipt['inputManifestSha256'])
        self.assertEqual(checked,receipt)
        self.assertEqual(env['attemptId'],ATTEMPT)
        # Syntactically valid but structurally/semantically meaningless data
        # requires separate v1 and server-owned identity checks.
        _,not_a_receipt,_=codec.decode(wrap(b'{"a":1}'))
        with self.assertRaises(ValueError):
            p.validate_acceptance(not_a_receipt,result_manifest=FIXTURE['resultManifest'],
                                  input_manifest_sha256=receipt['inputManifestSha256'])

if __name__=='__main__': unittest.main()
