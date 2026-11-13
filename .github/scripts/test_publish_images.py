import copy
import json
import unittest
import urllib.error
from unittest.mock import patch
import publish_images
from publish_images import context, private_package, source_repository, REPOSITORY


class PublicationBoundaryTests(unittest.TestCase):
    def setUp(self):
        self.env = {'GITHUB_REPOSITORY': REPOSITORY, 'GITHUB_REF': 'refs/heads/main',
                    'GITHUB_EVENT_NAME': 'push', 'GITHUB_SHA': 'a' * 40,
                    'GITHUB_RUN_ID': '123', 'GITHUB_RUN_ATTEMPT': '1'}

    def test_pull_request_and_branch_dispatch_cannot_publish(self):
        for changes in ({'GITHUB_EVENT_NAME': 'pull_request'}, {'GITHUB_REF': 'refs/heads/feature'},
                        {'GITHUB_REPOSITORY': 'someone/fork'}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                context({**self.env, **changes})

    def test_new_run_or_attempt_never_reuses_an_earlier_tag(self):
        tag = context(self.env)
        self.assertNotEqual(tag, context({**self.env, 'GITHUB_RUN_ATTEMPT': '2'}))
        self.assertNotEqual(tag, context({**self.env, 'GITHUB_RUN_ID': '124'}))

    def test_public_or_wrong_repository_package_is_refused(self):
        for value in ({'visibility': 'public'}, {'visibility': 'internal'},
                      {'visibility': 'private', 'repository': {'full_name': 'other/repository'}}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                private_package(value)

    def test_new_package_is_allowed_but_published_link_must_be_verified(self):
        private_package(None)
        with self.assertRaises(ValueError):
            private_package(None, required=True)
        with self.assertRaises(ValueError):
            private_package({'visibility': 'private'}, required=True)
        with self.assertRaises(ValueError):
            private_package({'visibility': 'private'})
        private_package({'visibility': 'private', 'repository': {'full_name': REPOSITORY}}, required=True)

    def test_publication_targets_only_fixed_private_names(self):
        self.assertEqual(publish_images.PACKAGES,
                         ('ledgetop-processing-private-coordinator', 'ledgetop-processing-private-treedar'))
        self.assertNotIn('ledgetop-processing-orchestrator', publish_images.PACKAGES)

    def test_public_or_unlinked_package_stops_before_any_build_or_push(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'public'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        for metadata in ([{'visibility': 'public', 'repository': {'full_name': REPOSITORY}}],
                         [{'visibility': 'private'}], [linked, {'visibility': 'private'}]):
            with self.subTest(metadata=metadata), \
                 patch.dict(publish_images.os.environ, self.env, clear=True), \
                 patch.object(publish_images, 'github', side_effect=[repository, *metadata]), \
                 patch.object(publish_images, 'receipt'), \
                 patch.object(publish_images, 'command') as command:
                with self.assertRaises(SystemExit):
                    publish_images.main()
                command.assert_not_called()

    def test_metadata_forbidden_fails_closed_instead_of_treating_it_as_absent(self):
        failure = urllib.error.HTTPError('https://api.github.com/example', 403, 'forbidden', {}, None)
        with patch.dict(publish_images.os.environ, {'GITHUB_TOKEN': 'synthetic'}, clear=True), \
             patch.object(publish_images.urllib.request, 'urlopen', side_effect=failure):
            with self.assertRaisesRegex(ValueError, 'metadata permission'):
                publish_images.github('/example')

    def test_preflight_metadata_failure_stops_before_build(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'public'}
        records = []
        with patch.dict(publish_images.os.environ, self.env, clear=True), \
             patch.object(publish_images, 'github', side_effect=[repository, ValueError('metadata permission/read check failed')]), \
             patch.object(publish_images, 'receipt', side_effect=lambda x: records.append(copy.deepcopy(x))), \
             patch.object(publish_images, 'command') as command:
            with self.assertRaises(SystemExit):
                publish_images.main()
            command.assert_not_called()
        self.assertEqual(records[-1]['stage'], 'preflight_package_metadata')
        self.assertEqual(records[-1]['failureCode'], 'preflight_package_metadata_unavailable')

    def test_partial_push_public_metadata_records_digest_and_never_pushes_treedar(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'public'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        public = {'visibility': 'public', 'repository': {'full_name': REPOSITORY}}
        digest = 'sha256:' + 'b' * 64
        records, commands = [], []
        def fake_command(args, **kwargs):
            commands.append(args)
            if args[:3] == ['docker', 'image', 'inspect']:
                return json.dumps([{'Config': {'User': '10001:10001'}, 'Os': 'linux',
                                    'Architecture': 'amd64', 'Id': 'sha256:' + 'a' * 64}])
            if args[:4] == ['docker', 'buildx', 'imagetools', 'inspect']:
                return json.dumps({'digest': digest})
            return ''
        with patch.dict(publish_images.os.environ, self.env, clear=True), \
             patch.object(publish_images, 'github', side_effect=[repository, linked, linked, linked, public]), \
             patch.object(publish_images, 'receipt', side_effect=lambda x: records.append(copy.deepcopy(x))), \
             patch.object(publish_images, 'command', side_effect=fake_command):
            with self.assertRaises(SystemExit):
                publish_images.main()
        final = records[-1]
        self.assertEqual(final['stage'], 'postpush_package_metadata')
        self.assertEqual(final['failureCode'], 'postpush_package_not_private')
        self.assertEqual(final['images'][0]['status'], 'push_returned')
        self.assertEqual(final['images'][0]['digest'], digest)
        pushes = [args for args in commands if args[:2] == ['docker', 'push']]
        self.assertEqual(len(pushes), 1)
        self.assertIn('ledgetop-processing-private-coordinator:', pushes[0][2])
        self.assertEqual(final['images'][1]['status'], 'built')

    def test_preflight_checks_both_exact_package_names_before_build(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'public'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        with patch.dict(publish_images.os.environ, self.env, clear=True), \
             patch.object(publish_images, 'github', side_effect=[repository, linked, linked]) as github, \
             patch.object(publish_images, 'receipt'), \
             patch.object(publish_images, 'command', side_effect=RuntimeError('test boundary')) as command:
            with self.assertRaises(SystemExit):
                publish_images.main()
            paths = [call.args[0] for call in github.call_args_list]
            self.assertEqual(paths, ['/repos/' + REPOSITORY,
                '/orgs/ledgetoptechnologies/packages/container/ledgetop-processing-private-coordinator',
                '/orgs/ledgetoptechnologies/packages/container/ledgetop-processing-private-treedar'])
            self.assertEqual(command.call_count, 1)
            self.assertEqual(command.call_args.args[0][:2], ['docker', 'build'])

    def test_source_visibility_does_not_relax_private_package_requirement(self):
        for visibility in ('public', 'private'):
            self.assertEqual(source_repository({'full_name': REPOSITORY, 'visibility': visibility}), visibility)
        with self.assertRaises(ValueError):
            source_repository({'full_name': 'someone/other', 'visibility': 'public'})
        with self.assertRaises(ValueError):
            private_package({'visibility': 'public', 'repository': {'full_name': REPOSITORY}}, required=True)

    def run_publication(self, metadata, *, fail_push=False, fail_smoke=False, bad_digest=False):
        records, commands = [], []
        def fake_command(args, **kwargs):
            commands.append(args)
            if args[:3] == ['docker', 'image', 'inspect']:
                return json.dumps([{'Config': {'User': '10001:10001'}, 'Os': 'linux',
                                    'Architecture': 'amd64', 'Id': 'sha256:' + 'a' * 64}])
            if args[:2] == ['docker', 'run'] and fail_smoke:
                raise RuntimeError('synthetic smoke failure')
            if args[:2] == ['docker', 'push'] and fail_push:
                raise TimeoutError('synthetic lost push response')
            if args[:4] == ['docker', 'buildx', 'imagetools', 'inspect']:
                return json.dumps({'digest': 'bad' if bad_digest else 'sha256:' + 'b' * 64})
            return ''
        with patch.dict(publish_images.os.environ, self.env, clear=True), \
             patch.object(publish_images, 'github', side_effect=metadata), \
             patch.object(publish_images, 'receipt', side_effect=lambda x: records.append(copy.deepcopy(x))), \
             patch.object(publish_images, 'command', side_effect=fake_command):
            try:
                publish_images.main()
            except SystemExit as error:
                self.assertEqual(error.code, 1)
        return records, commands

    def test_authenticated_metadata_404_is_bootstrap_not_other_http_failures(self):
        for code in (404, 401, 403, 429, 500):
            failure = urllib.error.HTTPError('https://api.github.com/example', code, 'synthetic', {}, None)
            with self.subTest(code=code), \
                 patch.dict(publish_images.os.environ, {'GITHUB_TOKEN': 'synthetic'}, clear=True), \
                 patch.object(publish_images.urllib.request, 'urlopen', side_effect=failure):
                if code == 404:
                    self.assertIsNone(publish_images.github('/example'))
                else:
                    with self.assertRaises(ValueError): publish_images.github('/example')

    def test_fresh_private_pair_builds_and_smokes_before_first_push(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        records, commands = self.run_publication([repository, None, None, None, linked, None, linked])
        self.assertEqual(records[-1]['status'], 'succeeded')
        pushes = [i for i, args in enumerate(commands) if args[:2] == ['docker', 'push']]
        self.assertEqual(len(pushes), 2)
        self.assertEqual(sum(args[:2] == ['docker', 'build'] for args in commands[:pushes[0]]), 2)
        self.assertEqual(sum(args[:2] == ['docker', 'run'] for args in commands[:pushes[0]]), 2)
        for image in records[-1]['images']:
            self.assertEqual((image['visibility'], image['linkedRepository'], image['status']),
                             ('private', REPOSITORY, 'verified'))
            self.assertRegex(image['digest'], r'^sha256:[a-f0-9]{64}$')
        effects = json.dumps([args for args in commands if args[:2] in (['docker', 'build'], ['docker', 'push'])])
        for old in ('ledgetop-processing-coordinator:', 'ledgetop-processing-treedar:',
                    'ledgetop-processing-orchestrator:', ':latest', '@sha256:'):
            self.assertNotIn(old, effects)

    def test_changed_package_privacy_or_repository_before_push_is_fatal(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        for bad in ({'visibility': 'public', 'repository': {'full_name': REPOSITORY}},
                    {'visibility': 'internal', 'repository': {'full_name': REPOSITORY}},
                    {'visibility': 'private', 'repository': {'full_name': 'other/repo'}}):
            with self.subTest(bad=bad):
                records, commands = self.run_publication([repository, None, None, bad])
                self.assertEqual(records[-1]['stage'], 'prepush_package_metadata')
                self.assertFalse(any(args[:2] == ['docker', 'push'] for args in commands))

    def test_postpush_missing_or_wrong_repository_stops_pair_after_first_push(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        for bad in (None, {'visibility': 'private', 'repository': {'full_name': 'other/repo'}}):
            with self.subTest(bad=bad):
                records, commands = self.run_publication([repository, None, None, None, bad])
                self.assertEqual(records[-1]['stage'], 'postpush_package_metadata')
                self.assertEqual(records[-1]['status'], 'failed_or_uncertain')
                self.assertEqual(len([args for args in commands if args[:2] == ['docker', 'push']]), 1)
                self.assertIn('digest', records[-1]['images'][0])
                self.assertEqual(records[-1]['images'][1]['status'], 'built')

    def test_second_package_changed_before_push_preserves_first_verified_receipt(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        public = {'visibility': 'public', 'repository': {'full_name': REPOSITORY}}
        records, commands = self.run_publication([repository, None, None, None, linked, public])
        self.assertEqual(records[-1]['failureCode'], 'prepush_package_not_private')
        self.assertEqual(records[-1]['images'][0]['status'], 'verified')
        self.assertEqual(records[-1]['images'][1]['status'], 'built')
        self.assertEqual(len([args for args in commands if args[:2] == ['docker', 'push']]), 1)

    def test_lost_push_response_never_retries_and_retains_exact_uncertain_reference(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        records, commands = self.run_publication([repository, None, None, None], fail_push=True)
        final = records[-1]
        self.assertEqual(final['failureCode'], 'push_response_unconfirmed')
        self.assertEqual(final['images'][0]['status'], 'push_response_unconfirmed')
        pushes = [args for args in commands if args[:2] == ['docker', 'push']]
        self.assertEqual(len(pushes), 1)
        self.assertEqual(pushes[0][2], final['images'][0]['reference'])
        self.assertTrue(pushes[0][2].endswith(':' + context(self.env)))
        self.assertFalse(any(args[:4] == ['docker', 'buildx', 'imagetools', 'inspect'] for args in commands))

    def test_bad_digest_or_smoke_failure_cannot_complete_pair(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        records, commands = self.run_publication([repository, None, None, None], bad_digest=True)
        self.assertEqual(records[-1]['failureCode'], 'postpush_digest_unverified')
        self.assertEqual(len([args for args in commands if args[:2] == ['docker', 'push']]), 1)
        records, commands = self.run_publication([repository, None, None], fail_smoke=True)
        self.assertEqual(records[-1]['failureCode'], 'smoke_check_failed')
        self.assertFalse(any(args[:2] == ['docker', 'push'] for args in commands))


if __name__ == '__main__':
    unittest.main()
