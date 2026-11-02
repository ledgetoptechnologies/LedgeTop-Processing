"""Static safety contract for the inactive reference worker user unit."""
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
UNIT = ROOT / 'ops/systemd/user/ledgetop-processing-reference-worker.service'


class ReferenceWorkerUnitTests(unittest.TestCase):
    def test_unit_has_single_persistent_state_config_and_owned_daemon(self):
        text = UNIT.read_text(encoding='utf-8')
        self.assertIn('WorkingDirectory=%h/.local/share/ledgetop-processing-reference/current/source', text)
        self.assertIn('ExecStart=/usr/bin/python3 -B -m ops.reference_worker_daemon --config %h/.config/ledgetop-processing/reference-worker.json', text)
        self.assertIn('UMask=0077', text)
        self.assertIn('NoNewPrivileges=yes', text)
        self.assertNotIn('ExecStartPre=', text)
        self.assertNotIn('ExecStartPost=', text)

    def test_failure_restart_is_bounded_and_never_force_kills_unfinished_drain(self):
        text = UNIT.read_text(encoding='utf-8')
        self.assertIn('Restart=on-failure', text)
        self.assertIn('RestartSec=15s', text)
        self.assertIn('StartLimitIntervalSec=10min', text)
        self.assertIn('StartLimitBurst=20', text)
        self.assertIn('KillMode=mixed', text)
        self.assertIn('TimeoutStopSec=10min', text)
        self.assertIn('SendSIGKILL=no', text)

    def test_worker_command_is_not_parameterized_by_environment_or_arguments(self):
        text = UNIT.read_text(encoding='utf-8')
        start = re.search(r'^ExecStart=(.+)$', text, re.MULTILINE)
        self.assertIsNotNone(start)
        command = start.group(1)
        self.assertIn('ops.reference_worker_daemon --config ', command)
        self.assertNotRegex(command, r'%[iI]\b')
        self.assertNotRegex(command, r'\$\{?[^ }]+')


if __name__ == '__main__':
    unittest.main()
