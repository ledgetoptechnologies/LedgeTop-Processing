import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from processing.direct_worker_service_v2 import create_listener, load_listener_settings
from processing.reference_runtime import RuntimeConfigurationError


class WorkerListenerSettingsTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'worker.json'

    def write(self, value):
        self.path.write_text(json.dumps(value), encoding='utf-8')
        self.path.chmod(0o600)
        return str(self.path)

    def config(self, **changes):
        return dict(schemaVersion=1, enabled=True, host='127.0.0.1', port=18181, **changes)

    def test_no_configuration_and_explicit_disable_do_not_open_listener(self):
        self.assertIsNone(create_listener())
        value = self.config(); value['enabled'] = False
        self.assertIsNone(create_listener(self.write(value)))

    def test_invalid_or_nonprivate_configuration_fails_closed(self):
        for field, invalid in [('host', '0.0.0.0'), ('port', True), ('port', 0), ('enabled', 1)]:
            value = self.config(); value[field] = invalid
            with self.subTest(field=field, value=invalid):
                with self.assertRaises(RuntimeConfigurationError):
                    load_listener_settings(self.write(value))
        if os.name != 'nt':
            path = self.write(self.config()); self.path.chmod(0o644)
            with self.assertRaises(RuntimeConfigurationError):
                load_listener_settings(path)

    def test_symlink_and_unknown_settings_are_rejected(self):
        value = self.config(); value['extra'] = 'unexpected'
        with self.assertRaises(RuntimeConfigurationError):
            load_listener_settings(self.write(value))
        self.write(self.config())
        link = self.path.parent / 'alias.json'
        try:
            link.symlink_to(self.path)
        except (OSError, NotImplementedError):
            return
        with self.assertRaises(RuntimeConfigurationError):
            load_listener_settings(str(link))

    def test_opt_in_requires_trusted_dependencies_before_binding(self):
        path = self.write(self.config())
        with patch('processing.direct_worker_service_v2.create_direct_worker_server') as server:
            with self.assertRaises(RuntimeConfigurationError):
                create_listener(path)
            server.assert_not_called()

    def test_opt_in_builds_boundary_before_opening_server(self):
        path = self.write(self.config())
        with patch('processing.direct_worker_service_v2.DirectWorkerBoundaryV2') as boundary, \
             patch('processing.direct_worker_service_v2.create_direct_worker_server') as server:
            result = create_listener(path, boundary_kwargs={'ledger': object()}, tls_context=object())
            boundary.assert_called_once()
            server.assert_called_once_with(boundary.return_value, '127.0.0.1', 18181, ssl_context=unittest.mock.ANY)
            self.assertIs(result, server.return_value)
