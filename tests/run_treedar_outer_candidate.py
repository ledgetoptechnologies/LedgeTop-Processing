"""Owned isolated synthetic fixture entrypoint; intentionally retain temp roots."""
import shutil
import tempfile
import threading
import unittest


class RetainedTemporaryDirectory:
    def __init__(self, *args, **kwargs):
        self.name = tempfile.mkdtemp(*args, **kwargs)
        print('retained synthetic root:', self.name, flush=True)
    def cleanup(self):
        pass
    def __enter__(self):
        return self.name
    def __exit__(self, *_):
        pass


def main():
    if not shutil.which('openssl'):
        raise SystemExit('openssl unavailable; no install attempted')
    tempfile.TemporaryDirectory = RetainedTemporaryDirectory
    from tests.test_reference_treedar_publication import PublicationTests
    cases = unittest.TestSuite(PublicationTests(name) for name in (
        'test_fresh_outer_preview_binds_actual_bundle_and_preserves_sources',
        'test_outer_preview_holds_if_original_native_proof_changes'))
    result = unittest.TextTestRunner(verbosity=2).run(cases)
    remaining = [thread.name for thread in threading.enumerate() if thread is not threading.main_thread()]
    if remaining:
        print('unjoined owned threads:', remaining, flush=True)
        raise SystemExit(2)
    raise SystemExit(0 if result.wasSuccessful() else 1)


if __name__ == '__main__':
    main()
