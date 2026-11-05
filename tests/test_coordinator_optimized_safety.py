"""Optimized Python must retain the actual serialized updater safety gates."""
import ast
import inspect
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from ops import update_reference_coordinator as updater


class CoordinatorOptimizedSafetyTests(unittest.TestCase):
    def run_optimized(self, code):
        return subprocess.run([sys.executable, '-O', '-c', code], capture_output=True,
                              text=True, timeout=10, check=False)

    def test_every_embedded_safety_check_survives_optimized_compilation(self):
        root = Path(__file__).resolve().parents[1]
        count = 0
        for relative in ('ops/install_reference_coordinator.py', 'ops/update_reference_coordinator.py'):
            tree = ast.parse((root / relative).read_text())
            self.assertFalse(any(isinstance(node, ast.Assert) for node in ast.walk(tree)), relative)
            for node in ast.walk(tree):
                if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                    continue
                if 'def _require(condition):' not in node.value:
                    continue
                embedded = ast.parse(node.value)
                self.assertFalse(any(isinstance(item, ast.Assert) for item in ast.walk(embedded)))
                helper = next(item for item in embedded.body if isinstance(item, ast.FunctionDef)
                              and item.name == '_require')
                namespace = {}
                exec(compile(ast.Module(body=[helper], type_ignores=[]), '<actual-helper>', 'exec', optimize=2), namespace)
                with self.assertRaisesRegex(AssertionError, '^coordinator_safety_condition_failed$'):
                    namespace['_require'](False)
                namespace['_require'](True)
                count += 1
        self.assertEqual(count, 7)

    def test_actual_serialized_volume_owner_guard_refuses_under_dash_o(self):
        with tempfile.TemporaryDirectory() as directory:
            code = inspect.getsource(updater.volume_census) + '\nvolume_census(' + repr(directory) + ',uid=-1)\n'
            result = self.run_optimized(code)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('coordinator_safety_condition_failed', result.stderr)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_actual_backup_wrong_owner_fails_before_copy_under_dash_o(self):
        tree = ast.parse((Path(__file__).resolve().parents[1] / 'ops/install_reference_coordinator.py').read_text())
        script = next(node.value.value for node in tree.body if isinstance(node, ast.Assign)
                      and any(isinstance(target, ast.Name) and target.id == 'BACKUP' for target in node.targets))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source, target = root / 'source', root / 'backup'
            source.mkdir(); target.mkdir()
            (source / 'unchanged').write_bytes(b'synthetic-original')
            target.chmod(0o755)
            script = script.replace("Path('/source'),Path('/backup')", 'Path(' + repr(str(source)) + '),Path(' + repr(str(target)) + ')')
            result = self.run_optimized(script)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('coordinator_safety_condition_failed', result.stderr)
            self.assertEqual(list(target.iterdir()), [])
            self.assertEqual((source / 'unchanged').read_bytes(), b'synthetic-original')


if __name__ == '__main__':
    unittest.main()
