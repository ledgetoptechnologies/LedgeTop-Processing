"""Linux provisioning qualification in disposable test directories only."""
import contextlib
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from ops.provision_reference_config import provision as provision_config
from ops.provision_reference_tls import LEAVES, provision as provision_tls


@unittest.skipUnless(os.name == "posix", "requires Linux ownership and directory durability semantics")
class ReferenceConfigProvisioningTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.root.chmod(0o700)
        self.native = self.root / "native.env"
        self.legacy = "legacy-private-api-" + "a"*48
        self.native.write_text("PROCESSING_API_TOKEN=" + self.legacy + "\nPROCESSING_VIEWER_ENABLED=1\n")
        self.native.chmod(0o600)
        self.target = self.root / "new-reference"

    def test_exact_roles_scope_and_files_are_private_without_secret_stdout(self):
        before = self.native.read_bytes()
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            receipt = provision_config(str(self.target), str(self.native))
        values = json.loads((self.target / "credentials.json").read_text())
        runtime = json.loads((self.target / "runtime.json").read_text())
        self.assertEqual(set(values), {"viewerStaging", "clusterWorker", "mainWorker",
                                      "clusterResultReader", "mainResultReader"})
        self.assertEqual(len(set(values.values())), 5)
        self.assertNotIn(self.legacy, values.values())
        self.assertTrue(all(len(value) >= 32 for value in values.values()))
        self.assertEqual(runtime["viewerBearers"], {"staging": values["viewerStaging"]})
        for node in ("cluster", "main"):
            worker = runtime["workers"][node]
            self.assertEqual(worker["bearer"], values[node + "Worker"])
            self.assertEqual(worker["resultBearer"], values[node + "ResultReader"])
            self.assertEqual(worker["workerId"], node + "-reference")
            self.assertEqual(worker["instanceId"], node + "-reference-v1")
        self.assertEqual(runtime["listener"]["port"], 8443)
        self.assertEqual(runtime["listener"]["privateKey"], "/run/processing-reference/coordinator.key")
        self.assertEqual(self.native.read_bytes(), before)
        self.assertEqual(stat.S_IMODE(self.target.stat().st_mode), 0o700)
        self.assertEqual({path.name for path in self.target.iterdir()},
                         {"credentials.json", "runtime.json", "viewer-staging.token", "provisioning-receipt.json"})
        for path in self.target.iterdir():
            self.assertEqual(path.stat().st_uid, os.geteuid())
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
        public = output.getvalue() + json.dumps(receipt) + (self.target / "provisioning-receipt.json").read_text()
        for secret in (*values.values(), self.legacy):
            self.assertNotIn(secret, public)
        self.assertFalse(receipt["brokerConfigured"])
        self.assertFalse(receipt["workerIntakeConfigured"])
        self.assertFalse(receipt["servicesActivated"])

    def test_legacy_and_generated_collisions_are_regenerated(self):
        values = [char*64 for char in "BCDEF"]
        with patch("ops.provision_reference_config.secrets.token_urlsafe",
                   side_effect=[self.legacy, values[0], values[0], *values[1:]]) as generate:
            provision_config(str(self.target), str(self.native))
        actual = json.loads((self.target / "credentials.json").read_text())
        self.assertEqual(set(actual.values()), set(values))
        self.assertEqual(generate.call_count, 7)
        self.assertTrue(all(call.args == (48,) for call in generate.call_args_list))

    def test_existing_or_partial_output_is_refused_without_overwrite(self):
        provision_config(str(self.target), str(self.native))
        original = {path.name: path.read_bytes() for path in self.target.iterdir()}
        with self.assertRaises(FileExistsError):
            provision_config(str(self.target), str(self.native))
        self.assertEqual({path.name: path.read_bytes() for path in self.target.iterdir()}, original)
        partial = self.root / "partial"
        partial.mkdir(mode=0o700)
        marker = partial / "retained"
        marker.write_bytes(b"retained partial evidence")
        with self.assertRaises(FileExistsError):
            provision_config(str(partial), str(self.native))
        self.assertEqual(marker.read_bytes(), b"retained partial evidence")
        self.assertEqual(list(partial.iterdir()), [marker])

    def test_insecure_native_file_and_output_parent_are_refused_before_creation(self):
        self.native.chmod(0o644)
        with self.assertRaises(ValueError):
            provision_config(str(self.target), str(self.native))
        self.assertFalse(self.target.exists())
        self.native.chmod(0o600)
        parent = self.root / "shared-parent"
        parent.mkdir(mode=0o755)
        parent.chmod(0o755)
        with self.assertRaises(ValueError):
            provision_config(str(parent / "config"), str(self.native))
        self.assertFalse((parent / "config").exists())

    def test_symlink_and_hardlink_native_inputs_are_refused(self):
        linked = self.root / "linked-native.env"
        linked.symlink_to(self.native)
        with self.assertRaises(ValueError):
            provision_config(str(self.target), str(linked))
        linked.unlink()
        linked.hardlink_to(self.native)
        with self.assertRaises(ValueError):
            provision_config(str(self.target), str(linked))
        self.assertFalse(self.target.exists())

    def test_write_failure_retains_partial_evidence_and_retry_does_not_replace_it(self):
        with patch("ops.provision_reference_config.os.fsync", side_effect=OSError("synthetic disk failure")):
            with self.assertRaises(OSError):
                provision_config(str(self.target), str(self.native))
        self.assertTrue((self.target / "credentials.json").exists())
        retained = (self.target / "credentials.json").read_bytes()
        with self.assertRaises(FileExistsError):
            provision_config(str(self.target), str(self.native))
        self.assertEqual((self.target / "credentials.json").read_bytes(), retained)


@unittest.skipUnless(os.name == "posix" and Path("/usr/bin/openssl").is_file(),
                     "requires Linux and local OpenSSL; no package installation")
class ReferenceTLSProvisioningTests(unittest.TestCase):
    def test_real_scoped_certificates_chain_privacy_receipt_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            parent.chmod(0o700)
            target = parent / "tls"
            environment = dict(os.environ)
            output = io.StringIO()
            with contextlib.redirect_stdout(output):
                receipt = provision_tls(str(target))
            self.assertEqual(set(receipt["certificates"]), set(LEAVES))
            self.assertEqual(receipt["caSha256"], hashlib.sha256((target / "ca.crt").read_bytes()).hexdigest())
            self.assertFalse(receipt["systemTrustInstalled"])
            self.assertFalse(receipt["servicesActivated"])
            self.assertEqual(dict(os.environ), environment)
            self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o700)
            for path in target.iterdir():
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
                self.assertEqual(path.stat().st_uid, os.geteuid())
            for name, address in LEAVES.items():
                result = subprocess.run(["/usr/bin/openssl", "verify", "-CAfile", str(target / "ca.crt"),
                    "-purpose", "sslserver", "-verify_ip", address, str(target / (name + ".crt"))],
                    capture_output=True, timeout=10)
                self.assertEqual(result.returncode, 0)
            wrong = subprocess.run(["/usr/bin/openssl", "verify", "-CAfile", str(target / "ca.crt"),
                "-verify_ip", "192.168.50.89", str(target / "main-worker.crt")], capture_output=True, timeout=10)
            self.assertNotEqual(wrong.returncode, 0)
            public = output.getvalue() + json.dumps(receipt) + (target / "provisioning-receipt.json").read_text()
            self.assertNotIn("PRIVATE KEY", public)
            for path in target.glob("*.key"):
                self.assertNotIn(path.read_text(), public)
            old = {path.name: path.read_bytes() for path in target.iterdir()}
            with self.assertRaises(FileExistsError):
                provision_tls(str(target))
            self.assertEqual({path.name: path.read_bytes() for path in target.iterdir()}, old)

    def test_partial_directory_refuses_before_any_openssl_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory)
            parent.chmod(0o700)
            target = parent / "tls"
            target.mkdir(mode=0o700)
            marker = target / "retained"
            marker.write_bytes(b"retained")
            with patch("ops.provision_reference_tls.subprocess.run", side_effect=AssertionError("must not execute")):
                with self.assertRaises(FileExistsError):
                    provision_tls(str(target))
            self.assertEqual(list(target.iterdir()), [marker])


if __name__ == "__main__":
    unittest.main()
