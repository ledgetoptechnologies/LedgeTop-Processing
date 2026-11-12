import unittest
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
        private_package({'visibility': 'private', 'repository': {'full_name': REPOSITORY}}, required=True)

    def test_publication_targets_only_new_coordinator_and_existing_treedar(self):
        self.assertEqual(publish_images.PACKAGES,
                         ('ledgetop-processing-coordinator', 'ledgetop-processing-treedar'))
        self.assertNotIn('ledgetop-processing-orchestrator', publish_images.PACKAGES)

    def test_missing_or_unverified_package_stops_before_any_build_or_push(self):
        repository = {'full_name': REPOSITORY, 'visibility': 'public'}
        linked = {'visibility': 'private', 'repository': {'full_name': REPOSITORY}}
        for metadata in ([None], [linked, None],
                         [{'visibility': 'public', 'repository': {'full_name': REPOSITORY}}],
                         [linked, {'visibility': 'private'}]):
            with self.subTest(metadata=metadata), \
                 patch.dict(publish_images.os.environ, self.env, clear=True), \
                 patch.object(publish_images, 'github', side_effect=[repository, *metadata]), \
                 patch.object(publish_images, 'receipt'), \
                 patch.object(publish_images, 'command') as command:
                with self.assertRaises(SystemExit):
                    publish_images.main()
                command.assert_not_called()

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
                '/orgs/ledgetoptechnologies/packages/container/ledgetop-processing-coordinator',
                '/orgs/ledgetoptechnologies/packages/container/ledgetop-processing-treedar'])
            self.assertEqual(command.call_count, 1)
            self.assertEqual(command.call_args.args[0][:2], ['docker', 'build'])

    def test_source_visibility_does_not_relax_private_package_requirement(self):
        for visibility in ('public', 'private'):
            self.assertEqual(source_repository({'full_name': REPOSITORY, 'visibility': visibility}), visibility)
        with self.assertRaises(ValueError):
            source_repository({'full_name': 'someone/other', 'visibility': 'public'})
        with self.assertRaises(ValueError):
            private_package({'visibility': 'public', 'repository': {'full_name': REPOSITORY}}, required=True)


if __name__ == '__main__':
    unittest.main()
