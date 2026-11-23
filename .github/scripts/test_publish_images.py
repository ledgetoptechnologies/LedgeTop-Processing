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

    def test_private_package_requires_existing_linked_metadata(self):
        with self.assertRaisesRegex(ValueError, 'package metadata unavailable'):
            private_package(None)
        with self.assertRaises(ValueError):
            private_package({'visibility': 'private'})
        private_package({'visibility': 'private', 'repository': {'full_name': REPOSITORY}})

    def test_publication_targets_only_fixed_private_names(self):
        self.assertEqual(publish_images.PACKAGES,
                         ('ledgetop-processing-coordinator-v2', 'ledgetop-processing-treedar-v2'))
        for retired in ('ledgetop-processing-orchestrator', 'ledgetop-processing-private-coordinator',
                        'ledgetop-processing-coordinator', 'ledgetop-processing-private-treedar'):
            self.assertNotIn(retired, publish_images.PACKAGES)

    def test_unqualified_package_at_either_preflight_position_stops_before_build(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'public'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        for bad, code in self.bad_package_metadata():
            for index, package in enumerate(publish_images.PACKAGES):
                with self.subTest(package=package, bad=code):
                    metadata = [linked, linked]
                    metadata[index] = bad
                    records, commands = self.run_publication([repository, *metadata])
                    self.assertEqual(commands, [])
                    self.assertEqual(records[-1]['images'], [])
                    self.assertEqual(records[-1]['currentPackage'], package)
                    self.assertEqual(records[-1]['stage'], 'preflight_package_metadata')
                    self.assertEqual(records[-1]['failureCode'], 'preflight_package_' + code)

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
                return json.dumps([{'Config': {'User': '10001:10001', 'Labels': self.labels_for(args[3])},
                                    'Os': 'linux', 'Architecture': 'amd64', 'Id': 'sha256:' + 'a' * 64}])
            if args[:4] == ['docker', 'buildx', 'imagetools', 'inspect']:
                return json.dumps({'digest': digest})
            return ''
        with patch.dict(publish_images.os.environ, self.env, clear=True), \
             patch.object(publish_images, 'github', side_effect=[repository, *([linked] * 5), public]), \
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
        self.assertIn('ledgetop-processing-coordinator-v2:', pushes[0][2])
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
                '/orgs/ledgetoptechnologies/packages/container/ledgetop-processing-coordinator-v2',
                '/orgs/ledgetoptechnologies/packages/container/ledgetop-processing-treedar-v2'])
            self.assertEqual(command.call_count, 1)
            self.assertEqual(command.call_args.args[0][:2], ['docker', 'build'])

    def test_source_visibility_does_not_relax_private_package_requirement(self):
        for visibility in ('public', 'private'):
            self.assertEqual(source_repository({'full_name': REPOSITORY, 'visibility': visibility}), visibility)
        with self.assertRaises(ValueError):
            source_repository({'full_name': 'someone/other', 'visibility': 'public'})
        with self.assertRaises(ValueError):
            private_package({'visibility': 'public', 'repository': {'full_name': REPOSITORY}})

    @staticmethod
    def bad_package_metadata():
        return (
            (None, 'missing_or_inaccessible'),
            ({'visibility': 'public', 'repository': {'full_name': REPOSITORY}}, 'not_private'),
            ({'visibility': 'internal', 'repository': {'full_name': REPOSITORY}}, 'not_private'),
            ({'visibility': 'private'}, 'unlinked_or_wrong_repository'),
            ({'visibility': 'private', 'repository': {'full_name': 'other/repo'}}, 'unlinked_or_wrong_repository'),
            (ValueError('synthetic metadata permission failure'), 'metadata_unavailable'),
        )

    @staticmethod
    def labels_for(reference):
        for key, package in zip(publish_images.PACKAGE_KEYS, publish_images.PACKAGES):
            if '/' + package + ':' in reference or '/' + package + '@' in reference:
                return publish_images.labels(key)
        raise AssertionError('unexpected reference ' + reference)

    def run_publication(self, metadata, *, fail_push=False, fail_smoke=False, bad_digest=False, remote_change=None,
                        env=None, verified_digest=None):
        records, commands = [], []
        def fake_command(args, **kwargs):
            commands.append(args)
            if args[:3] == ['docker', 'image', 'inspect']:
                item = {'Config': {'User': '10001:10001', 'Labels': self.labels_for(args[3])}, 'Os': 'linux',
                        'Architecture': 'amd64', 'Id': 'sha256:' + 'a' * 64}
                if '@sha256:' in args[3] and remote_change:
                    remote_change(item)
                return json.dumps([item])
            if args[:2] == ['docker', 'run'] and fail_smoke:
                raise RuntimeError('synthetic smoke failure')
            if args[:2] == ['docker', 'push'] and fail_push:
                raise TimeoutError('synthetic lost push response')
            if args[:4] == ['docker', 'buildx', 'imagetools', 'inspect']:
                if args[4].endswith(':' + publish_images.VERIFIED_TAG) and verified_digest:
                    return json.dumps({'digest': verified_digest})
                return json.dumps({'digest': 'bad' if bad_digest else 'sha256:' + 'b' * 64})
            return ''
        with patch.dict(publish_images.os.environ, env or self.env, clear=True), \
             patch.object(publish_images, 'github', side_effect=metadata), \
             patch.object(publish_images, 'receipt', side_effect=lambda x: records.append(copy.deepcopy(x))), \
             patch.object(publish_images, 'command', side_effect=fake_command):
            try:
                publish_images.main()
            except SystemExit as error:
                self.assertEqual(error.code, 1)
        return records, commands

    def test_authenticated_metadata_404_means_required_package_is_unavailable(self):
        for code in (404, 401, 403, 429, 500):
            failure = urllib.error.HTTPError('https://api.github.com/example', code, 'synthetic', {}, None)
            with self.subTest(code=code), \
                 patch.dict(publish_images.os.environ, {'GITHUB_TOKEN': 'synthetic'}, clear=True), \
                 patch.object(publish_images.urllib.request, 'urlopen', side_effect=failure):
                if code == 404:
                    self.assertIsNone(publish_images.github('/example'))
                    with self.assertRaisesRegex(ValueError, 'package metadata unavailable'):
                        private_package(publish_images.github('/example'))
                else:
                    with self.assertRaises(ValueError): publish_images.github('/example')

    def test_existing_private_pair_builds_and_smokes_before_first_push(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        records, commands = self.run_publication([repository, *([linked] * 8), self.head(), linked, self.head(), linked])
        self.assertEqual(records[-1]['status'], 'succeeded')
        self.assertEqual(records[-1]['promotion'], {'status': 'promoted', 'tag': 'verified',
                                                  'promoted': list(publish_images.PACKAGES)})
        pushes = [i for i, args in enumerate(commands) if args[:2] == ['docker', 'push']]
        self.assertEqual(len(pushes), 4)
        # The moving `verified` tag is written only after both immutable
        # pushes have been pulled back by digest and identity-checked.
        identity = [i for i, args in enumerate(commands)
                    if args[:3] == ['docker', 'image', 'inspect'] and '@sha256:' in args[3]]
        self.assertEqual(len(identity), 2)
        for index in pushes[2:]:
            self.assertTrue(commands[index][2].endswith(':verified'))
            self.assertGreater(index, identity[-1])
        pushes = pushes[:2]
        self.assertEqual(sum(args[:2] == ['docker', 'build'] for args in commands[:pushes[0]]), 2)
        self.assertEqual(sum(args[:2] == ['docker', 'run'] for args in commands[:pushes[0]]), 2)
        self.assertEqual([row['currentPackage'] for row in records if row['stage'] == 'prepush_pair_metadata'],
                         list(publish_images.PACKAGES))
        for image in records[-1]['images']:
            self.assertEqual((image['visibility'], image['linkedRepository'], image['status']),
                             ('private', REPOSITORY, 'verified'))
            self.assertRegex(image['digest'], r'^sha256:[a-f0-9]{64}$')
            self.assertEqual(image['immutableReference'], image['reference'].rsplit(':', 1)[0] + '@' + image['digest'])
            self.assertEqual(image['remoteImageId'], image['localImageId'])
            self.assertTrue(image['imageIdentityVerified'])
            self.assertIn(['docker', 'pull', '--platform', 'linux/amd64', image['immutableReference']], commands)
            self.assertIn(['docker', 'image', 'inspect', image['immutableReference']], commands)
        effects = json.dumps([args for args in commands if args[:2] in (['docker', 'build'], ['docker', 'push'])])
        for old in ('ledgetop-processing-coordinator:', 'ledgetop-processing-treedar:',
                    'ledgetop-processing-private-coordinator', 'ledgetop-processing-private-treedar',
                    'ledgetop-processing-orchestrator:', ':latest', '@sha256:'):
            self.assertNotIn(old, effects)
        for image in records[-1]['images']:
            self.assertEqual(image['promotion'], 'verified')
            self.assertEqual(image['verifiedTag'], image['reference'].rsplit(':', 1)[0] + ':verified')

    def head(self, sha=None):
        return {'commit': {'sha': sha or self.env['GITHUB_SHA']}}

    def test_manual_run_publishes_but_never_moves_verified(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        records, commands = self.run_publication([repository, *([linked] * 8)],
                                                 env={**self.env, 'GITHUB_EVENT_NAME': 'workflow_dispatch'})
        self.assertEqual(records[-1]['status'], 'succeeded')
        self.assertEqual(records[-1]['promotion'], {'status': 'skipped_not_main_push'})
        self.assertFalse(any(':verified' in ' '.join(args) for args in commands))

    def test_superseded_main_commit_never_moves_verified_backwards(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        records, commands = self.run_publication([repository, *([linked] * 8), self.head('c' * 40)])
        self.assertEqual(records[-1]['status'], 'succeeded')
        self.assertEqual(records[-1]['promotion']['status'], 'skipped_superseded')
        self.assertFalse(any(':verified' in ' '.join(args) for args in commands))

    def test_unreadable_main_head_or_public_package_stops_before_verified_push(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        public = {'visibility': 'public', 'repository': {'full_name': REPOSITORY}}
        for tail, code in (([None], 'promotion_main_head_unavailable'),
                           ([self.head(), public], 'promotion_package_not_private')):
            with self.subTest(code=code):
                records, commands = self.run_publication([repository, *([linked] * 8), *tail])
                self.assertEqual(records[-1]['failureCode'], code)
                self.assertFalse(any(':verified' in ' '.join(args) for args in commands if args[:2] == ['docker', 'push']))

    def test_packages_activate_independently_when_main_moves_between_tags(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        records, commands = self.run_publication([repository, *([linked] * 8), self.head(), linked, self.head('c' * 40)])
        final = records[-1]
        self.assertEqual(final['status'], 'succeeded')
        self.assertEqual(final['promotion']['status'], 'partial_superseded')
        self.assertEqual(final['promotion']['promoted'], [publish_images.PACKAGES[0]])
        self.assertEqual([image['promotion'] for image in final['images']], ['verified', 'skipped_superseded'])
        verified = [args[2] for args in commands if args[:2] == ['docker', 'push'] and args[2].endswith(':verified')]
        self.assertEqual(len(verified), 1)
        self.assertIn('/' + publish_images.PACKAGES[0] + ':', verified[0])

    def test_failed_second_tag_leaves_first_package_independently_promoted(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        records, commands = [], []
        def fake_command(args, **kwargs):
            commands.append(args)
            if args[:3] == ['docker', 'image', 'inspect']:
                return json.dumps([{'Config': {'User': '10001:10001', 'Labels': self.labels_for(args[3])},
                                    'Os': 'linux', 'Architecture': 'amd64', 'Id': 'sha256:' + 'a' * 64}])
            if args[:2] == ['docker', 'push'] and args[2].endswith(publish_images.PACKAGES[1] + ':verified'):
                raise TimeoutError('synthetic lost push response')
            if args[:4] == ['docker', 'buildx', 'imagetools', 'inspect']:
                return json.dumps({'digest': 'sha256:' + 'b' * 64})
            return ''
        metadata = [repository, *([linked] * 8), self.head(), linked, self.head(), linked]
        with patch.dict(publish_images.os.environ, self.env, clear=True), \
             patch.object(publish_images, 'github', side_effect=metadata), \
             patch.object(publish_images, 'receipt', side_effect=lambda x: records.append(copy.deepcopy(x))), \
             patch.object(publish_images, 'command', side_effect=fake_command):
            with self.assertRaises(SystemExit):
                publish_images.main()
        final = records[-1]
        self.assertEqual(final['failureCode'], 'promotion_push_unconfirmed')
        self.assertEqual(final['promotion'], {'status': 'partial', 'promoted': [publish_images.PACKAGES[0]]})
        self.assertEqual([image['promotion'] for image in final['images']], ['verified', 'push_response_unconfirmed'])
        # Both images were already individually verified before any tag moved.
        self.assertEqual([image['status'] for image in final['images']], ['verified', 'verified'])

    def test_verified_tag_naming_another_digest_fails_publication(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        records, commands = self.run_publication([repository, *([linked] * 8), self.head(), linked],
                                                 verified_digest='sha256:' + 'd' * 64)
        self.assertEqual(records[-1]['failureCode'], 'promotion_digest_unverified')
        self.assertEqual(records[-1]['status'], 'failed_or_uncertain')
        self.assertEqual(len([args for args in commands if args[:2] == ['docker', 'push']
                              and args[2].endswith(':verified')]), 1)

    def test_build_carries_identity_labels_hosts_check(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        records, commands = self.run_publication([repository, *([linked] * 8), self.head(), linked, self.head(), linked])
        builds = [args for args in commands if args[:2] == ['docker', 'build']]
        self.assertEqual(records[-1]['status'], 'succeeded')
        for key, build in zip(publish_images.PACKAGE_KEYS, builds):
            text = ' '.join(build)
            for expected in ('com.ledgetop.processing.package=' + key, 'com.ledgetop.processing.publish-run-id=123',
                             'com.ledgetop.processing.publish-ref=refs/heads/main',
                             'com.ledgetop.processing.publish-event=push',
                             'org.opencontainers.image.revision=' + self.env['GITHUB_SHA']):
                self.assertIn(expected, text)

    def test_pair_recheck_stops_before_first_push_if_either_package_drifted(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        for bad, code in self.bad_package_metadata():
            for index, package in enumerate(publish_images.PACKAGES):
                with self.subTest(package=package, bad=code):
                    metadata = [linked, linked]
                    metadata[index] = bad
                    records, commands = self.run_publication([repository, linked, linked, *metadata])
                    self.assertEqual(records[-1]['stage'], 'prepush_pair_metadata')
                    self.assertEqual(records[-1]['currentPackage'], package)
                    self.assertEqual(records[-1]['failureCode'], 'prepush_package_' + code)
                    self.assertEqual([image['status'] for image in records[-1]['images']], ['built', 'built'])
                    self.assertFalse(any(args[:2] == ['docker', 'push'] for args in commands))

    def test_immediate_recheck_stops_first_push_after_valid_pair_read(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        records, commands = self.run_publication([repository, *([linked] * 4), None])
        self.assertEqual(records[-1]['stage'], 'prepush_package_metadata')
        self.assertEqual(records[-1]['failureCode'], 'prepush_package_missing_or_inaccessible')
        self.assertFalse(any(args[:2] == ['docker', 'push'] for args in commands))

    def test_postpush_missing_or_wrong_repository_stops_pair_after_first_push(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        for bad in (None, {'visibility': 'private', 'repository': {'full_name': 'other/repo'}}):
            with self.subTest(bad=bad):
                records, commands = self.run_publication([repository, *([linked] * 5), bad])
                self.assertEqual(records[-1]['stage'], 'postpush_package_metadata')
                self.assertEqual(records[-1]['status'], 'failed_or_uncertain')
                self.assertEqual(len([args for args in commands if args[:2] == ['docker', 'push']]), 1)
                self.assertIn('digest', records[-1]['images'][0])
                self.assertEqual(records[-1]['images'][1]['status'], 'built')

    def test_remote_identity_mismatch_stops_before_second_push(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        mutations = {
            'image_id': lambda item: item.update(Id='sha256:' + 'c' * 64),
            'platform': lambda item: item.update(Architecture='arm64'),
            'os': lambda item: item.update(Os='windows'),
            'user': lambda item: item['Config'].update(User='root'),
            'revision': lambda item: item['Config']['Labels'].update({'org.opencontainers.image.revision': 'c' * 40}),
            'source': lambda item: item['Config']['Labels'].update({'org.opencontainers.image.source': 'https://github.com/other/repo'}),
            'missing_labels': lambda item: item['Config'].update(Labels=None),
        }
        for name, mutation in mutations.items():
            with self.subTest(name=name):
                records, commands = self.run_publication([repository, *([linked] * 6)], remote_change=mutation)
                final = records[-1]
                self.assertEqual(final['failureCode'], 'postpush_image_identity_unverified')
                self.assertEqual(final['status'], 'failed_or_uncertain')
                self.assertNotIn('imageIdentityVerified', final['images'][0])
                self.assertNotIn('remoteImageId', final['images'][0])
                self.assertIn('@sha256:', final['images'][0]['immutableReference'])
                self.assertEqual(len([args for args in commands if args[:2] == ['docker', 'push']]), 1)
                self.assertEqual(final['images'][1]['status'], 'built')

    def test_second_package_changed_before_push_preserves_first_verified_receipt(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        public = {'visibility': 'public', 'repository': {'full_name': REPOSITORY}}
        records, commands = self.run_publication([repository, *([linked] * 6), public])
        self.assertEqual(records[-1]['failureCode'], 'prepush_package_not_private')
        self.assertEqual(records[-1]['images'][0]['status'], 'verified')
        self.assertEqual(records[-1]['images'][1]['status'], 'built')
        self.assertEqual(len([args for args in commands if args[:2] == ['docker', 'push']]), 1)

    def test_lost_push_response_never_retries_and_retains_exact_uncertain_reference(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'private'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        records, commands = self.run_publication([repository, *([linked] * 5)], fail_push=True)
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
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        records, commands = self.run_publication([repository, *([linked] * 5)], bad_digest=True)
        self.assertEqual(records[-1]['failureCode'], 'postpush_digest_unverified')
        self.assertEqual(len([args for args in commands if args[:2] == ['docker', 'push']]), 1)
        records, commands = self.run_publication([repository, linked, linked], fail_smoke=True)
        self.assertEqual(records[-1]['failureCode'], 'smoke_check_failed')
        self.assertNotIn('promotion', records[-1])
        self.assertFalse(any(args[:2] in (['docker', 'push'], ['docker', 'tag']) for args in commands))


if __name__ == '__main__':
    unittest.main()
