"""Draft cross-language hash fixture only; no broker capability or auth."""
import base64
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch
from processing import reference_transfer_v2_contract_candidate as candidate

class ReferenceTransferV2VectorTests(unittest.TestCase):
    def test_synthetic_request_hash_and_valid_length_encoding(self):
        vector=json.loads((Path(__file__).resolve().parents[1]/'contracts/reference-transfer-v2-hash-vector.json').read_text())
        raw=vector['canonicalRequest'].encode('ascii');value=json.loads(raw)
        self.assertEqual(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=True,allow_nan=False).encode('ascii'),raw)
        request_id=value['requestId']
        self.assertEqual(len(request_id),43)
        self.assertEqual(base64.urlsafe_b64encode(base64.urlsafe_b64decode(request_id+'=')).decode().rstrip('='),request_id)
        self.assertEqual(len(base64.urlsafe_b64decode(request_id+'=')),32)
        self.assertEqual(hashlib.sha256(bytes.fromhex(vector['domainHex'])+raw).hexdigest(),vector['requestSha256'])
        admission=json.loads(vector['canonicalAdmission'])
        delivery=admission.pop('deliveryId')
        self.assertEqual(len(delivery),43)
        self.assertEqual(len(base64.urlsafe_b64decode(delivery+'=')),32)
        self.assertEqual(admission,value)

    def test_draft_whole_object_admission_and_all_negative_vectors(self):
        root=Path(__file__).resolve().parents[1]/'contracts'
        vector=json.loads((root/'reference-transfer-v2-hash-vector.json').read_text())
        negative=json.loads((root/'reference-transfer-v2-negative-vectors.json').read_text())
        with self.assertRaises(candidate.V2ContractError):
            candidate.parse_admission(vector['canonicalAdmission'].encode())
        with patch.object(candidate,'QUALIFIED',True):
            _,digest=candidate.parse_admission(vector['canonicalAdmission'].encode())
            self.assertEqual(digest,vector['requestSha256'])
            with self.assertRaises(candidate.V2ContractError):candidate.parse_admission(b' '* (candidate.MAX_WIRE_BYTES+1))
            empty=json.loads(vector['canonicalAdmission'])
            empty['binding']['objectLength']=0
            empty['binding']['objectSha256']=hashlib.sha256(b'').hexdigest()
            empty['binding']['range']={'kind':'empty'}
            encoded=json.dumps(empty,sort_keys=True,separators=(',',':'),ensure_ascii=True).encode()
            self.assertEqual(candidate.parse_admission(encoded)[0],empty)
            for case in negative['vectors']:
                with self.subTest(case=case['name']),self.assertRaises(candidate.V2ContractError):
                    candidate.parse_admission(bytes.fromhex(case['wireHex']))
