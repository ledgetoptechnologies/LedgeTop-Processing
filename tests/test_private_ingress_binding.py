"""Pure configuration checks: no Docker, listener or service action."""
import unittest
from ops.node_ingress import configuration, GateError


class PrivateIngressBindingTests(unittest.TestCase):
    def test_missing_address_preserves_both_legacy_wildcards(self):
        text = configuration(24784, 24783, False)
        self.assertIn('listen 0.0.0.0:24784;', text)
        self.assertIn('listen [::]:24784 ipv6only=on;', text)

    def test_explicit_ipv4_loopback_has_no_other_listener(self):
        for closed in (False, True):
            text = configuration(24784, 24783, closed, listen_address='127.0.0.1')
            self.assertIn('listen 127.0.0.1:24784;', text)
            self.assertNotIn('listen [', text)
            self.assertNotIn('listen 0.0.0.0', text)

    def test_explicit_ipv6_loopback_has_no_ipv4_or_wildcard_listener(self):
        text = configuration(24784, 24783, True, listen_address='::1')
        self.assertIn('listen [::1]:24784 ipv6only=on;', text)
        self.assertNotIn('listen 0.0.0.0', text)
        self.assertNotIn('listen [::]:', text)

    def test_address_is_canonical_ip_not_hostname_or_nginx_syntax(self):
        for address in ('localhost', '127.0.0.1;', '127.0.0.1\n', '::01', 'fe80::1%eth0', '', True, 123):
            with self.subTest(address=address), self.assertRaises(GateError):
                configuration(24784, 24783, True, listen_address=address)


if __name__ == '__main__':
    unittest.main()
