import unittest
from publish_images import context, private_package, REPOSITORY


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


if __name__ == '__main__':
    unittest.main()
