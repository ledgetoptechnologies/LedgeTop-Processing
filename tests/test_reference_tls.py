import os
from pathlib import Path
import ssl
import tempfile
import unittest
from unittest.mock import patch

from processing.reference_tls import verified_client_context


class ReferenceTLSTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.system = ssl.create_default_context()
        # Reuse real public certificates already available locally. No network,
        # certificate/key generation, or system trust installation occurs.
        self.certificates = self.system.get_ca_certs(binary_form=True)

    def ca_file(self, certificate, name="additional-ca.pem"):
        target = self.root / name
        target.write_text(ssl.DER_cert_to_PEM_cert(certificate), encoding="ascii")
        return target

    def test_real_default_system_roots_are_preserved_with_configured_ca(self):
        if not self.certificates:
            self.skipTest("system trust store has no enumerable certificates")
        environment = dict(os.environ)
        context = verified_client_context(self.ca_file(self.certificates[0]))
        self.assertTrue(set(self.certificates) <= set(context.get_ca_certs(binary_form=True)))
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)
        self.assertGreaterEqual(context.minimum_version, ssl.TLSVersion.TLSv1_2)
        self.assertEqual(dict(os.environ), environment)
        self.assertIsNot(context, self.system)

    def test_real_ca_is_added_without_discarding_already_trusted_ca(self):
        if len(set(self.certificates)) < 2:
            self.skipTest("two local public CA certificates required")
        first, second = list(dict.fromkeys(self.certificates))[:2]
        original = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        original.load_verify_locations(cadata=ssl.DER_cert_to_PEM_cert(first))
        self.assertEqual(set(original.get_ca_certs(binary_form=True)), {first})
        with patch("processing.reference_tls.ssl.create_default_context", return_value=original) as defaults:
            context = verified_client_context(self.ca_file(second))
        defaults.assert_called_once_with()
        self.assertIs(context, original)
        self.assertEqual(set(context.get_ca_certs(binary_form=True)), {first, second})
        self.assertTrue(context.check_hostname)
        self.assertEqual(context.verify_mode, ssl.CERT_REQUIRED)

    def test_invalid_or_missing_ca_fails_closed(self):
        invalid = self.root / "invalid-ca.pem"
        invalid.write_text("not a certificate", encoding="ascii")
        for path in (invalid, self.root / "missing-ca.pem"):
            with self.subTest(path=path.name), self.assertRaises((ssl.SSLError, OSError)):
                verified_client_context(path)

    def test_stricter_existing_minimum_is_not_downgraded(self):
        if not self.certificates:
            self.skipTest("system trust store has no enumerable certificates")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        with patch("processing.reference_tls.ssl.create_default_context", return_value=context):
            result = verified_client_context(self.ca_file(self.certificates[0]))
        self.assertEqual(result.minimum_version, ssl.TLSVersion.TLSv1_3)


if __name__ == "__main__":
    unittest.main()
