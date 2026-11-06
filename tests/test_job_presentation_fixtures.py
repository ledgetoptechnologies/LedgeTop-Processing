"""The shared public fixtures: the coordinator produces them and a reference reader agrees.

reader() is the Viewer rule from docs/job-presentation-v1.md in the fewest lines. It
is a specification for the Viewer owner, not code the coordinator runs.
"""
from datetime import datetime
import hashlib
import json
from pathlib import Path
import re
import unittest

from processing import reference_presentation as presentation

PATH = Path(__file__).resolve().parent / 'fixtures' / 'job-presentation-v1.json'
FILE_SHA256 = '1977eab229b0cabb71aef94f82b142c8360ccd932a3bca84634a8f91c0716f71'
FIELDS = {'schemaVersion', 'protocol', 'state', 'processingConfirmedAt'}
TIME = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z\Z')


def valid_time(value):
    if type(value) is not str or TIME.fullmatch(value) is None:
        return False
    try:
        datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError:
        return False
    return True


def valid(value):
    if type(value) is not dict or set(value) != FIELDS or type(value['schemaVersion']) is not int:
        return False
    if value['schemaVersion'] != 1 or value['protocol'] != 'ltds-job-presentation-v1':
        return False
    state, confirmed = value['state'], value['processingConfirmedAt']
    if state not in (None, 'queued', 'preparing', 'processing'):
        return False
    if state == 'processing':
        return valid_time(confirmed)
    if state is None:
        return confirmed is None or valid_time(confirmed)
    return confirmed is None


def reader(case):
    """(display, confirmedAt). Concrete fields win, and missing data never means queued."""
    if not case['reachable']:
        return 'disconnected', None
    job = case['job']
    if job['status'] in ('failed', 'cancelled', 'succeeded'):
        return 'terminal', None
    if job['status'] == 'held':
        return 'held', None
    if job.get('queueReason') is not None:
        return 'queue_message', None
    if job['error'] is not None:
        return 'error', None
    if job['phase'] in ('waiting_for_delivery', 'awaiting_acceptance', 'completed'):
        return 'delivery', None
    view = job.get('presentation')
    if valid(view) and view['state'] == 'processing':
        return 'processing_confirmed', view['processingConfirmedAt']
    if valid(view) and view['state'] is not None:
        return view['state'], None
    retained = case['retained']
    if retained is not None and (retained['attemptId'], retained['generation']) == (job['attemptId'], job['generation']):
        return 'processing_confirmed', retained['processingConfirmedAt']
    return 'status_unavailable', None


class PublicFixtureTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw = PATH.read_bytes()
        cls.document = json.loads(cls.raw)
        cls.cases = cls.document['cases']

    def test_file_is_the_published_bytes(self):
        self.assertEqual(hashlib.sha256(self.raw).hexdigest(), FILE_SHA256)
        self.assertNotIn(b'\r', self.raw)

    def test_every_group_is_present_and_names_are_unique(self):
        self.assertEqual({case['group'] for case in self.cases}, {'precedence', 'malformed', 'attempt-generation', 'disconnected'})
        self.assertEqual(len({case['name'] for case in self.cases}), len(self.cases))

    def test_coordinator_cases_are_what_the_coordinator_produces(self):
        record = dict(witnessId='33333333-3333-4333-8333-333333333333', sequence=1, requestSha256='a' * 64,
            witnessSha256='b' * 64, executionUuid='11111111-1111-4111-8111-111111111111', generation=1,
            claimRequestId='driver-1', gateGeneration=1, gateReceiptId='c' * 32,
            workerObservedAt='2026-11-05T10:00:00.000Z', acceptedAt='2026-11-05T10:00:01.000Z')
        produced = [case for case in self.cases if case['source'] == 'coordinator']
        self.assertGreater(len(produced), 30)
        for case in produced:
            with self.subTest(case=case['name']):
                job = {key: value for key, value in case['job'].items() if key != 'presentation'}
                raw = json.dumps(record) if case['witness'] else None
                self.assertEqual(presentation.project(job, raw), case['job']['presentation'])

    def test_reference_reader_gives_every_expected_display(self):
        for case in self.cases:
            with self.subTest(case=case['name']):
                expected = case['expected']
                self.assertIn(expected['display'], self.document['displays'])
                self.assertEqual(reader(case), (expected['display'], expected['confirmedAt']))

    def test_no_missing_or_malformed_case_reads_as_queued_or_inferred_processing(self):
        for case in self.cases:
            if case['group'] == 'malformed':
                with self.subTest(case=case['name']):
                    self.assertNotIn(case['expected']['display'], ('queued', 'processing_confirmed'))

    def test_no_public_case_carries_the_worker_clock(self):
        self.assertNotIn(b'2026-11-05T10:00:00.000Z', self.raw)


if __name__ == '__main__':
    unittest.main()
