"""The one place that names the published Processing packages and labels.

The main publisher and the node-side auto-deploy step both read this module.
Change a package name here only after that package exists, is private and is
linked to this repository (see docs/private-package-publication.md).
"""
OWNER = 'ledgetoptechnologies'
REPOSITORY = OWNER + '/LedgeTop-Processing'
SOURCE_URL = 'https://github.com/' + REPOSITORY
REGISTRY = 'ghcr.io/' + OWNER

# Moved only by a main push whose checks, smoke tests and remote identity checks
# all passed, and only while that commit is still the head of main. Hosts read
# it to discover the newest digest; they never run the tag itself.
VERIFIED_TAG = 'verified'

PACKAGES = (
    {'key': 'coordinator', 'name': 'ledgetop-processing-coordinator-v2', 'buildRoot': '.'},
    {'key': 'treedar', 'name': 'ledgetop-processing-treedar-v2', 'buildRoot': 'engines/treedar'},
)

SOURCE_LABEL = 'org.opencontainers.image.source'
REVISION_LABEL = 'org.opencontainers.image.revision'
PACKAGE_LABEL = 'com.ledgetop.processing.package'
RUN_LABEL = 'com.ledgetop.processing.publish-run-id'
REF_LABEL = 'com.ledgetop.processing.publish-ref'
EVENT_LABEL = 'com.ledgetop.processing.publish-event'


def package(key):
    for item in PACKAGES:
        if item['key'] == key:
            return item
    raise ValueError('unknown Processing package')


def repository(key):
    return REGISTRY + '/' + package(key)['name']
