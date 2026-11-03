import copy
import hashlib
import json
import os
from pathlib import Path
import subprocess
import unittest

from processing import prepared_envelope_jcs_candidate as candidate

ROOT = Path(__file__).resolve().parents[1]


class PreparedEnvelopeJcsTests(unittest.TestCase):
    def setUp(self):
        self.sub = json.loads((ROOT/'contracts/reference-v1.contract.json').read_text())['submission']
        self.env = dict(schemaVersion=3, protocol=candidate.PROTOCOL, submission=self.sub,
            viewerTaskId='10000000-0000-4000-8000-000000000001',
            viewerAttemptId='20000000-0000-4000-8000-000000000001',
            activationId='30000000-0000-4000-8000-000000000001')
        self.policy = dict(capabilityFingerprint='a'*64, optionsFingerprint='b'*64,
            availableEngines=['odm'], optionDefinitions={
                'resolution':dict(type='float',domain=dict(min=-100,max=100)),
                'count':dict(type='int',domain=dict(min=-3,max=10)),
                'quality':dict(type='enum',domain=['high','ultra']),
                'title':dict(type='string',domain=None), 'dsm':dict(type='bool',domain=None)})

    def run_candidate(self, payload=None, **kwargs):
        raw = json.dumps(self.env if payload is None else payload, ensure_ascii=True, separators=(',',':')).encode()
        return candidate.validate(raw, trusted_policy=self.policy, environment='production', **kwargs)

    def test_types_domains_and_replay(self):
        self.sub['pipeline']['odm']['options'] = dict(resolution=-1.25,count=-2,quality='ultra',title='café 🌳',dsm=True)
        canonical, digest, normalized = self.run_candidate()
        self.assertEqual(digest, hashlib.sha256(candidate.DOMAIN+canonical).hexdigest())
        self.assertEqual(self.run_candidate(previous_canonical=canonical)[0], canonical)
        for key, bad in [('resolution',True),('resolution',101),('count',1.5),('quality','unknown'),('title',2),('dsm',1)]:
            obj=copy.deepcopy(self.env); obj['submission']['pipeline']['odm']['options'][key]=bad
            with self.subTest(key=key,bad=bad),self.assertRaises(ValueError): self.run_candidate(obj)
        changed=copy.deepcopy(self.env); changed['submission']['pipeline']['odm']['options']['resolution']=2
        with self.assertRaisesRegex(ValueError,'replay changed'): self.run_candidate(changed,previous_canonical=canonical)

    def test_negative_zero_equivalence_preserves_server_digest(self):
        self.sub['pipeline']['odm']['options']={'resolution':-0.0}
        a=self.run_candidate(); self.sub['pipeline']['odm']['options']['resolution']=0
        b=self.run_candidate(previous_canonical=a[0]); self.assertEqual(a,b)
        self.sub['pipeline']['odm']['options']['resolution']=1.0
        a=self.run_candidate(); self.sub['pipeline']['odm']['options']['resolution']=1
        b=self.run_candidate(previous_canonical=a[0]); self.assertEqual(a,b)

    def test_candidate_hashes_normalized_submission_with_legacy_algorithm(self):
        self.sub['pipeline']['odm']['options']={'resolution':1.0}
        canonical, _, normalized=self.run_candidate()
        canonical_submission=json.loads(canonical)['submission']
        self.assertEqual(normalized['requestDigest'],candidate.p.digest(canonical_submission))
        self.assertEqual(normalized['pipelineDigest'],candidate.p.digest(canonical_submission['pipeline']))
        self.assertNotEqual(normalized['requestDigest'],candidate.p.digest(self.sub))
        self.assertNotEqual(normalized['pipelineDigest'],candidate.p.digest(self.sub['pipeline']))

    def test_trusted_float_list_normalization_and_bool_separation(self):
        self.policy['optionDefinitions']['resolution']['domain']=[1.0,-0.0]
        for value in (1,1.0,0,-0.0):
            self.sub['pipeline']['odm']['options']={'resolution':value}
            self.run_candidate()
        self.sub['pipeline']['odm']['options']={'resolution':True}
        with self.assertRaises(ValueError): self.run_candidate()
        for invalid in ([True],[float('nan')],[float('inf')],[]):
            self.policy['optionDefinitions']['resolution']['domain']=invalid
            self.sub['pipeline']['odm']['options']={'resolution':1}
            with self.subTest(invalid=invalid),self.assertRaises(ValueError): self.run_candidate()

    def test_enum_requires_valid_explicit_domain_and_strings_remain_available(self):
        self.sub['pipeline']['odm']['options']={'quality':'ultra','title':'custom'}
        self.run_candidate()
        for invalid in (None,[],['ultra',1],{'min':0,'max':1}):
            self.policy['optionDefinitions']['quality']['domain']=invalid
            with self.subTest(invalid=invalid),self.assertRaises(ValueError): self.run_candidate()
        self.policy['optionDefinitions']['quality']['domain']=['high']
        with self.assertRaises(ValueError): self.run_candidate()

    def test_bounded_duplicate_safe_parser(self):
        for raw in (b'{"a":1,"a":2}',b'['*25+b'0'+b']'*25,b'{"a":NaN}', b'{"a":1e999}', b'{"a":9007199254740992}', b'\xff', b' '*(candidate.p.MAX_BODY_BYTES+1)):
            with self.subTest(raw=raw[:50]),self.assertRaises(ValueError): candidate.validate(raw,trusted_policy=self.policy,environment='production')

    def test_actual_node_jcs_vectors(self):
        import rfc8785
        module=os.environ.get('NODE_JCS_MODULE')
        if not module: self.skipTest('set NODE_JCS_MODULE to pinned canonicalize package')
        values=[{'n':x} for x in [-0.0,0,1.0,-1,1.25,1e-6,1e-7,9007199254740991,5e-324,333333333.33333329,1e30]]
        values += [{'\ue000':'private','😀':'astral','message':'雪 café'}]
        self.sub['pipeline']['odm']['options']={'resolution':-1.25,'quality':'ultra'}
        values.append(self.env)
        script="const j=require(process.argv[1]);let s='';process.stdin.setEncoding('utf8');process.stdin.on('data',x=>s+=x);process.stdin.on('end',()=>console.log(JSON.stringify(JSON.parse(s).map(x=>Buffer.from(j(x)).toString('hex')))))"
        result=subprocess.run(['node','-e',script,module],input=json.dumps(values,ensure_ascii=True),text=True,capture_output=True,check=True)
        self.assertEqual(json.loads(result.stdout),[rfc8785.dumps(v).hex() for v in values])
        self.assertEqual(self.run_candidate()[0].hex(),json.loads(result.stdout)[-1])


if __name__ == '__main__': unittest.main()
