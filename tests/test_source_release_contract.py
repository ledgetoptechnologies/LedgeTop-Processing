import copy
import json
from pathlib import Path
import unittest

from processing import reference_protocol as p
from processing.source_release_contract import verify


class SourceReleaseContractTests(unittest.TestCase):
    def test_shared_vectors(self):
        vectors=json.loads((Path(__file__).parents[1]/'contracts/fixtures/source-release-v1-vectors.json').read_text())
        positive=vectors['positive'];keys=vectors['trustedReleaseKeys']
        self.assertEqual(p.canonical_json(verify(positive['request'],keys)).decode(),positive['authorizationCanonical'])
        self.assertEqual(p.digest(positive['request']),positive['requestCanonicalSha256'])
        for case in vectors['negative']:
            with self.subTest(case=case['name']),self.assertRaises(ValueError):verify(case['request'],keys)

    def test_domain_signature_and_wrapper_are_closed(self):
        vectors=json.loads((Path(__file__).parents[1]/'contracts/fixtures/source-release-v1-vectors.json').read_text())
        for change in (lambda x:x.update(extra=True),lambda x:x.update(signature='A'*86),
                lambda x:x.update(authorizationSha256='0'*64)):
            request=copy.deepcopy(vectors['positive']['request']);change(request)
            with self.assertRaises(ValueError):verify(request,vectors['trustedReleaseKeys'])
