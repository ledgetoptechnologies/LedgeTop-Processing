"""Durable once-only claims. A claim is one exclusive file and is never replaced."""
import json
import os
import re
from pathlib import Path

from .errors import FrameRefused

_STEP = re.compile('[a-z]{1,16}')
_SHA = re.compile('[0-9a-f]{64}')
CODES = {'normalize': 'already_normalized', 'export': 'second_conversion'}


class Ledger:
    """`folder` must outlive the attempt. The operator owns it and never clears it."""

    def __init__(self, folder, *, run_nonce):
        self.folder = Path(folder)
        if not self.folder.is_dir():
            raise FrameRefused('durable_ledger_required')
        if not isinstance(run_nonce, str) or not _SHA.fullmatch(run_nonce):
            raise FrameRefused('run_nonce_required')
        self.run_nonce = run_nonce

    def _path(self, step, key):
        if not _STEP.fullmatch(step) or not isinstance(key, str) or not _SHA.fullmatch(key):
            raise FrameRefused('ledger_key_invalid')
        return self.folder / f'{step}-{key}.json'

    def claim(self, step, key):
        """Claim before acting, so a crash leaves a claim rather than a second run."""
        path = self._path(step, key)
        try:
            handle = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError as error:
            raise FrameRefused(CODES.get(step, 'already_claimed')) from error
        with os.fdopen(handle, 'w', encoding='utf-8') as stream:
            json.dump({'step': step, 'key': key, 'run_nonce': self.run_nonce}, stream,
                      sort_keys=True)
            stream.flush()
            os.fsync(stream.fileno())
        if os.name == 'posix':
            folder = os.open(self.folder, os.O_RDONLY)
            try:
                os.fsync(folder)
            finally:
                os.close(folder)

    def claimed(self, step, key):
        return self._path(step, key).exists()

    def entries(self):
        """Claims made by this run, for the receipt."""
        result = []
        for path in sorted(self.folder.glob('*.json')):
            try:
                entry = json.loads(path.read_text(encoding='utf-8'))
                if path.name != f"{entry['step']}-{entry['key']}.json":
                    raise ValueError
            except (ValueError, KeyError, TypeError) as error:
                raise FrameRefused('ledger_damaged') from error
            if entry['run_nonce'] == self.run_nonce:
                result.append({'step': entry['step'], 'key': entry['key']})
        return result
