import hashlib
import json
import os
from pathlib import Path
import subprocess
import unittest

from processing import reference_protocol as p
from processing.reference_source_intake_canonical import pipeline_digest


class V2PipelineDigestTests(unittest.TestCase):
    def test_exponent_fixed_point_negative_zero_and_integral_float(self):
        for value, encoded in ((1e-7, "1e-7"), (1e-6, "0.000001"), (-0.0, "0"), (1.0, "1"),
                               (0.1, "0.1"), (1.25, "1.25"), (9007199254740991, "9007199254740991")):
            with self.subTest(value=value):
                expected = hashlib.sha256(('{"option":' + encoded + '}').encode()).hexdigest()
                self.assertEqual(pipeline_digest({"option": value}), expected)

    def test_existing_v1_digest_is_not_reinterpreted(self):
        value = {"option": 1e-7}
        self.assertEqual(p.digest(value), hashlib.sha256(b'{"option":1e-07}').hexdigest())
        self.assertNotEqual(p.digest(value), pipeline_digest(value))

    def test_nonfinite_unsafe_integer_surrogate_or_nonobject_are_rejected(self):
        for value in ({"option": float("nan")}, {"option": float("inf")},
                      {"option": 2**53}, {"option": "\ud800"}, [], None):
            with self.subTest(value=repr(value)), self.assertRaises(p.ProtocolError):
                pipeline_digest(value)

    @unittest.skipUnless(os.environ.get("LTDS_VIEWER_REPO"), "actual Viewer checkout must be explicitly selected")
    def test_actual_viewer_canonicalizer_matches_numeric_option_vectors(self):
        repo = Path(os.environ["LTDS_VIEWER_REPO"]).resolve()
        values = [1e-7, 1e-6, -0.0, 1.0, 0.1, 1.25, 9007199254740991,
                  0.00001234, 333333333.3333333, 5e-324]
        pipelines = [{"odm": {"requested": True, "options": {"test-option": v},
                      "optionsFingerprint": "a" * 64}, "treedar": {"requested": False},
                      "gaussian_splat": {"requested": False}} for v in values]
        script = r'''
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto');
const {canonicalizeJcs}=require(path.join(process.argv[1],'apps/viewer/server/preparedSubmissionOutbox.js'));
const values=JSON.parse(fs.readFileSync(0,'utf8'));
process.stdout.write(JSON.stringify(values.map(v=>crypto.createHash('sha256').update(canonicalizeJcs(v)).digest('hex'))));
'''
        result = subprocess.run([os.environ.get("LTDS_NODE", "node"), "-e", script, str(repo)],
            input=json.dumps(pipelines), text=True, capture_output=True, timeout=30, check=True)
        self.assertEqual([pipeline_digest(v) for v in pipelines], json.loads(result.stdout))


if __name__ == "__main__":
    unittest.main()
