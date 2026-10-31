import copy
import hashlib
import json
from pathlib import Path
import unittest

from processing import reference_protocol as p
from processing import reference_source_proof_v2 as v


FIXTURE = Path(__file__).resolve().parents[1] / 'contracts/source-proof-v2-golden-draft.json'


def encoded(value, domain):
    raw = p.canonical_json(value)
    return raw, hashlib.sha256(domain + raw).hexdigest()


class SourceProofV2Tests(unittest.TestCase):
    def setUp(self):
        self.fixture = json.loads(FIXTURE.read_text(encoding='utf8'))
        self.manifest = self.fixture['inputManifest']
        self.docs = self.fixture['documents']

    def root(self, raw=None, expected=None, manifest=None):
        doc = self.docs['root']
        return v.validate_root(raw or doc['utf8'].encode(), expected_sha256=expected or doc['sha256'],
            job_id='job-1', generation=1, environment='staging', input_manifest=manifest or self.manifest)

    def object(self, key='object'):
        doc = self.docs[key]
        source = self.manifest['objects'][0 if key == 'emptyObject' else 1]
        return v.validate_object(doc['utf8'].encode(), expected_sha256=doc['sha256'], input_object=source)

    def test_shared_golden_documents_bind_complete_manifest_and_empty_object(self):
        root = self.root(); empty = self.object('emptyObject'); obj = self.object()
        self.assertEqual(root['objects'][0]['objectProofSha256'], self.docs['emptyObject']['sha256'])
        self.assertEqual(root['objects'][1]['objectProofSha256'], self.docs['object']['sha256'])
        self.assertEqual(empty['sha256'], hashlib.sha256(b'').hexdigest())
        self.assertEqual(v.validate_page(self.docs['page']['utf8'].encode(), object_descriptor=obj, page_index=0)
                         ['chunks'][0]['sha256'], hashlib.sha256(b'abc').hexdigest())
        with self.assertRaises(ValueError):
            v.validate_page(self.docs['page']['utf8'].encode(), object_descriptor=empty, page_index=0)

    def test_noncanonical_duplicate_negative_zero_and_invalid_utf8_rejected_before_hash(self):
        raw = self.docs['root']['utf8'].encode()
        for changed in (b' ' + raw, raw.replace(b'"version":2', b'"version":2,"version":2'),
                        raw.replace(b'"generation":1', b'"generation":1.0'),
                        raw.replace(b'"generation":1', b'"generation":-0'), raw + b'\xff'):
            with self.subTest(changed=changed[-30:]), self.assertRaises(ValueError):
                self.root(raw=changed, expected=hashlib.sha256(v.ROOT_DOMAIN + changed).hexdigest())

    def test_document_bounds_and_page_duplicate_keys_are_rejected(self):
        raw = b' ' * (v.MAX_ROOT_BYTES + 1)
        with self.assertRaises(ValueError):
            self.root(raw=raw, expected=hashlib.sha256(v.ROOT_DOMAIN + raw).hexdigest())
        raw = self.docs['page']['utf8'].encode().replace(b'"version":2', b'"version":2,"version":2')
        obj = dict(self.object(), pageDigests=[hashlib.sha256(v.PAGE_DOMAIN + raw).hexdigest()])
        with self.assertRaises(ValueError):
            v.validate_page(raw, object_descriptor=obj, page_index=0)

    def test_root_rejects_wrong_identity_and_object_closure_even_with_matching_hash(self):
        original = json.loads(self.docs['root']['utf8'])
        variants = []
        for key, bad in (('generation', True), ('generation', 2**53), ('jobId', 'bad/name'),
                         ('jobId', 'nonascii-é'), ('environment', 'production'),
                         ('chunkBytes', 1), ('inputManifestSha256', 'A' * 64)):
            value = copy.deepcopy(original); value[key] = bad; variants.append(value)
        value = copy.deepcopy(original); value['objects'].reverse(); variants.append(value)
        value = copy.deepcopy(original); value['objects'][0]['objectId'] = 'image-1'; variants.append(value)
        for value in variants:
            raw, digest = encoded(value, v.ROOT_DOMAIN)
            with self.subTest(value=value), self.assertRaises(ValueError): self.root(raw=raw, expected=digest)

    def test_descriptor_rejects_rebinding_empty_hash_and_inconsistent_pages(self):
        source = self.manifest['objects'][1]
        value = json.loads(self.docs['object']['utf8'])
        for key, bad in (('objectId', 'other'), ('byteSize', True), ('sha256', 'A' * 64),
                         ('chunkCount', 2), ('pageDigests', [])):
            changed = copy.deepcopy(value); changed[key] = bad
            raw, digest = encoded(changed, v.OBJECT_DOMAIN)
            with self.subTest(key=key), self.assertRaises(ValueError):
                v.validate_object(raw, expected_sha256=digest, input_object=source)
        bad = json.loads(self.docs['emptyObject']['utf8']); bad['sha256'] = '0' * 64
        source = dict(self.manifest['objects'][0], sha256='0' * 64)
        raw, digest = encoded(bad, v.OBJECT_DOMAIN)
        with self.assertRaises(ValueError): v.validate_object(raw, expected_sha256=digest, input_object=source)
        too_large = {'version':2, 'objectId':'huge', 'byteSize':p.MAX_SAFE_INTEGER,
                     'sha256':'0'*64, 'chunkCount':0, 'pageDigests':[]}
        raw, digest = encoded(too_large, v.OBJECT_DOMAIN)
        with self.assertRaises(ValueError):
            v.validate_object(raw, expected_sha256=digest,
                              input_object={'id':'huge', 'byteSize':p.MAX_SAFE_INTEGER, 'sha256':'0'*64})

    def test_page_index_and_full_final_chunk_layout_are_exact(self):
        count = v.PAGE_CHUNKS + 1; size = count * v.CHUNK_BYTES - 5
        chunks = [{'byteOffset': i * v.CHUNK_BYTES, 'byteSize': v.CHUNK_BYTES,
                   'sha256': 'a' * 64} for i in range(v.PAGE_CHUNKS)]
        first = {'version': 2, 'objectId': 'file-1', 'pageIndex': 0, 'chunks': chunks}
        last = {'version': 2, 'objectId': 'file-1', 'pageIndex': 1,
                'chunks': [{'byteOffset': v.PAGE_CHUNKS * v.CHUNK_BYTES,
                            'byteSize': v.CHUNK_BYTES - 5, 'sha256': 'b' * 64}]}
        a, ah = encoded(first, v.PAGE_DOMAIN); b, bh = encoded(last, v.PAGE_DOMAIN)
        descriptor = {'objectId': 'file-1', 'byteSize': size, 'chunkCount': count, 'pageDigests': [ah, bh]}
        self.assertEqual(len(v.validate_page(a, object_descriptor=descriptor, page_index=0)['chunks']), 256)
        self.assertEqual(v.validate_page(b, object_descriptor=descriptor, page_index=1)['chunks'][0]['byteSize'], v.CHUNK_BYTES - 5)
        for key, bad in (('byteOffset', 0), ('byteSize', v.CHUNK_BYTES), ('sha256', 'B' * 64)):
            changed = copy.deepcopy(last); changed['chunks'][0][key] = bad
            raw, digest = encoded(changed, v.PAGE_DOMAIN); bad_descriptor = dict(descriptor, pageDigests=[ah, digest])
            with self.subTest(key=key), self.assertRaises(ValueError):
                v.validate_page(raw, object_descriptor=bad_descriptor, page_index=1)
        with self.assertRaises(ValueError): v.validate_page(b, object_descriptor=descriptor, page_index=0)

    def test_shared_256_plus_one_page_vector(self):
        path = FIXTURE.with_name('source-proof-v2-page-boundaries-draft.json')
        docs = json.loads(path.read_text(encoding='utf8'))['documents']
        raw = docs['object']['utf8'].encode()
        descriptor = v.validate_object(raw, expected_sha256=docs['object']['sha256'],
            input_object={'id':'file-1', 'byteSize':257*v.CHUNK_BYTES-5, 'sha256':'c'*64})
        self.assertEqual(len(v.validate_page(docs['firstPage']['utf8'].encode(),
                         object_descriptor=descriptor, page_index=0)['chunks']), 256)
        self.assertEqual(len(v.validate_page(docs['lastPage']['utf8'].encode(),
                         object_descriptor=descriptor, page_index=1)['chunks']), 1)


if __name__ == '__main__': unittest.main()
