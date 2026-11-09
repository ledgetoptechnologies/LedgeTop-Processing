"""Cross-language synthetic policy vectors; not a runtime trust boundary.

The source vectors are shared with the Viewer draft. Passing this test proves
only that the two test oracles agree on the abstract admission examples. It
does not authenticate caller evidence, enable TreeDar, or qualify the worker.
"""
import copy
import hashlib
import json
from pathlib import Path
import re
import unittest


def synthetic_admission(value):
    capability, request, resolver = (value[key] for key in ("capability", "request", "resolver"))

    def identifier(item):
        return type(item) is str and re.fullmatch(r"[a-z0-9][a-z0-9-]{0,127}", item) is not None

    return (
        capability.get("advertised") is True
        and capability.get("current") is True
        and request.get("treedar") is True
        and request.get("gaussianSplat") is False
        and all(identifier(request.get(key)) and request.get(key) == resolver.get(key)
                for key in ("projectId", "assetId", "receiptId"))
        and type(request.get("sha256")) is str
        and re.fullmatch(r"[a-f0-9]{64}", request["sha256"]) is not None
        and request["sha256"] == resolver.get("sha256")
        and resolver.get("authenticated") is True
        and resolver.get("immutable") is True
        and resolver.get("genuineSensorLidar") is True
        and resolver.get("photogrammetryDerived") is False
    )


class SharedTreeDarProjectProvenanceVectors(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = Path(__file__).resolve().parents[1] / "contracts/treedar-project-provenance-vectors.json"
        raw = path.read_bytes()
        cls.fixture = json.loads(raw)
        # Match the shared fixture's canonical LF bytes across Windows checkouts.
        cls.fixture_sha256 = hashlib.sha256(raw.replace(b"\r\n", b"\n")).hexdigest()

    def test_vectors_match_viewer_draft_source_revision(self):
        self.assertEqual(self.fixture_sha256,
                         "1b9f951a6060f3fffd4bd4515d384bbb04f208f14ae5773e597890687b083c2d")

    def test_shared_fixture_covers_positive_and_fail_closed_cases(self):
        self.assertEqual(self.fixture["version"], 1)
        vectors = self.fixture["vectors"]
        self.assertTrue(any(item["accept"] is True for item in vectors))
        self.assertTrue(any(item["accept"] is False for item in vectors))
        self.assertEqual(len({item["name"] for item in vectors}), len(vectors))

    def test_python_policy_oracle_matches_every_shared_viewer_vector(self):
        for vector in self.fixture["vectors"]:
            with self.subTest(vector=vector["name"]):
                value = copy.deepcopy(self.fixture["base"])
                for key, patch in vector["patch"].items():
                    value[key].update(patch)
                self.assertIs(synthetic_admission(value), vector["accept"])


if __name__ == "__main__":
    unittest.main()
