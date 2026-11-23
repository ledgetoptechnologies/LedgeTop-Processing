"""Per-kind service adapters for the drain-window runner.

`processing.container_update.execute` drives one planned replacement through
an adapter. Every adapter answers the same questions for its service kind:

- `inspect()` -> mapping with the currently selected `Image` ID;
- `image(id)` -> raises unless that immutable image ID exists locally;
- `prepare(id)` -> validates the exact resolved Compose configuration;
- `idle()` -> True only when the service is provably drained;
- `replace(id)` -> swaps to that image (raises `PreflightError` before any
  mutation, `Uncertain` when Docker may still be acting);
- `healthy(id)` -> True only when that image is serving correctly;
- `adoptable(id)` -> the health check used by the manual first adoption.

Rollback is the runner's own: `replace(old)` followed by `healthy(old)`.

Kinds:

- `odx`: a NodeODX/NodeODM engine. `ComposeNode` exactly as before; it is the
  default when `maintenance.json` declares no kind, so existing hosts keep
  their behaviour. There is no published odx package, so it is never an
  auto-deploy target.
- `coordinator`: the processing coordinator Compose service updating itself.
  Idle means the coordinator's own read-only `/v1/prototype/unsettled` count
  shows no queued, running or held job on any node (queued jobs are tolerated
  only once its self-update fence is up). `begin` carries `selfUpdate: true`,
  which atomically re-checks that, fences every claim on every node, and keeps
  the reservation across the coordinator's own restart so finish still works.
  Healthy means the exact container runs the target image, Docker health is
  present and healthy, `/healthz` answers ok, the authority ID is unchanged,
  the reservation and fence survived the restart, and a new image carries this
  repository's `coordinator` package labels. A coordinator without the
  unsettled API cannot update itself and is refused.
- `treedar`: the TreeDar CLI image, run on demand from a Compose service that
  is never left running. Idle means no running container of that service or of
  the pinned image. "Replacing" mutates no container: the swap is the pin the
  runner saves after success. Healthy means `treedar --version` succeeds in a
  locked-down, network-less container of that image (which imports the CLI
  and its native dependency stack) and a new image carries the `treedar`
  package labels. Rollback therefore leaves the old pin in place.

Unknown kinds are refused; nothing guesses.
"""
import json
from pathlib import Path
import re
from types import SimpleNamespace

from . import release_packages
from .container_update import ComposeNode, NAME, PreflightError, UpdateError, Uncertain
from .odx_smoke_worker import HTTPRefused, SmokeError

KINDS = ('odx', 'coordinator', 'treedar')
# The published package each kind may auto-deploy. odx has none.
PACKAGES = {'coordinator': 'coordinator', 'treedar': 'treedar'}
DEFAULT_KIND = 'odx'
UNSETTLED_JOB_STATES = ('queued', 'running', 'held')
ENGINE_ID = re.compile(r'[A-Za-z0-9][A-Za-z0-9:._-]{0,127}\Z')
COMMIT = re.compile(r'[0-9a-f]{40}\Z')
VERSION = re.compile(r'[0-9]+(?:\.[0-9]+){1,3}(?:[-+.][0-9A-Za-z.]{1,32})?\n?\Z')


def service_kind(settings):
    """The declared kind; `odx` when none is declared (existing hosts)."""
    kind = settings.get('kind', DEFAULT_KIND)
    if kind not in KINDS:
        raise UpdateError('unknown service kind in maintenance.json; refusing')
    return kind


def package_for(kind):
    """The package this kind auto-deploys, or None (odx)."""
    return PACKAGES.get(kind)


def image_labels(adapter, image):
    raw = adapter.docker(['docker', 'image', 'inspect', '--format', '{{json .Config.Labels}}', image])
    labels = json.loads(raw or 'null')
    return labels if isinstance(labels, dict) else {}


def package_labelled(labels, key):
    return (labels.get(release_packages.SOURCE_LABEL) == release_packages.SOURCE_URL
            and labels.get(release_packages.PACKAGE_LABEL) == key
            and isinstance(labels.get(release_packages.REVISION_LABEL), str)
            and bool(COMMIT.fullmatch(labels[release_packages.REVISION_LABEL])))


def service_args(root, settings, kind):
    project, service = settings.get('project'), settings.get('service')
    engine = settings.get('dockerEngineId')
    if (not isinstance(project, str) or not NAME.fullmatch(project)
            or not isinstance(service, str) or not NAME.fullmatch(service)):
        raise UpdateError('exact Compose project and service required')
    if not isinstance(engine, str) or not ENGINE_ID.fullmatch(engine):
        raise UpdateError(kind + ' service requires dockerEngineId in maintenance.json')
    port = settings.get('containerPort', 8080)
    if type(port) is not int or not 1 <= port <= 65535:
        raise UpdateError('containerPort must be a TCP port')
    return SimpleNamespace(project=project, service=service, compose_file=str(Path(root) / 'compose.json'),
                           docker_engine_id=engine, node_port=port)


class _PackageImage:
    """Label rule shared by the published-package kinds.

    The baseline is the image selected when the adapter was built (the old
    pin). It may be an adopted, unlabelled image, so rollback to it never
    requires labels; any other image must carry this package's labels.
    """
    package = None

    def labelled_or_baseline(self, image):
        return image == self.baseline or package_labelled(image_labels(self, image), self.package)


class CoordinatorService(_PackageImage, ComposeNode):
    """The coordinator Compose service on its own host."""
    package = 'coordinator'
    health_seconds = 120
    # Sent with `begin`: fence all claims and survive this service's restart.
    begin_fields = {'selfUpdate': True}

    def __init__(self, args, work, *, api=None, authority=None, baseline=None):
        # Deliberately not ComposeNode.__init__: no engine token or engine kind.
        self.args, self.root = args, Path(work)
        self.base = ['docker', 'compose', '-p', self.args.project, '-f',
                     str(Path(self.args.compose_file).resolve())]
        # ComposeNode.inspect checks the container's private port against this origin.
        self.node = api
        self.api = api
        self.authority, self.baseline = authority, baseline

    def unsettled(self):
        """The coordinator's bounded unsettled-job summary.

        A coordinator without this API predates self-update support: it would
        drop the reservation on restart, so it is refused, not guessed.
        """
        try:
            summary = self.api.json('GET', '/v1/prototype/unsettled')
        except HTTPRefused as error:
            if error.status == 404:
                raise UpdateError('coordinator lacks the self-update API; install this version manually once') from None
            raise
        jobs = summary.get('jobs') if isinstance(summary, dict) else None
        if (summary.get('schemaVersion') != 1 or not isinstance(jobs, dict)
                or any(type(jobs.get(state)) is not int or jobs[state] < 0 for state in UNSETTLED_JOB_STATES)
                or type(summary.get('selfUpdateFence')) is not bool):
            raise UpdateError('unreadable coordinator unsettled-job summary')
        return summary

    def jobs_drained(self):
        summary = self.unsettled()
        jobs = summary['jobs']
        # Queued work is persisted and cannot be claimed once the fence is up.
        return (jobs['running'] == 0 and jobs['held'] == 0
                and (jobs['queued'] == 0 or summary['selfUpdateFence']))

    def idle(self):
        item = self.inspect()
        if not item['State']['Running']:
            raise UpdateError('coordinator must be running for its own idle check')
        return self.jobs_drained()

    def serving(self, image):
        item = self.inspect()
        state = item['State']
        # The image declares a HEALTHCHECK; a container without one is not proof.
        health = state.get('Health')
        if (item['Image'] != image or not state['Running'] or state.get('OOMKilled')
                or not isinstance(health, dict) or health.get('Status') != 'healthy'):
            return False
        health = self.api.json('GET', '/healthz')
        if not isinstance(health, dict) or health.get('status') != 'ok':
            return False
        authority = self.api.json('GET', '/v1/prototype/authority')
        if self.authority is None or not isinstance(authority, dict) or authority.get('authorityId') != self.authority:
            # A coordinator that came up on another database is not this service.
            return False
        return True

    def healthy(self, image):
        # The restarted coordinator must still hold this update's reservation
        # and fence, or finish would be refused.
        return (self.serving(image) and self.unsettled()['selfUpdateFence'] is True
                and self.labelled_or_baseline(image))

    def adoptable(self, image):
        # Also proves the running coordinator can survive its own update.
        return self.serving(image) and self.unsettled()['selfUpdateFence'] is False


class TreeDarImage(_PackageImage, ComposeNode):
    """The on-demand TreeDar CLI image selected by one Compose service."""
    package = 'treedar'
    health_seconds = 120
    self_check_seconds = 90

    def __init__(self, args, work, *, api=None, authority=None, baseline=None):
        self.args, self.root = args, Path(work)
        self.base = ['docker', 'compose', '-p', self.args.project, '-f',
                     str(Path(self.args.compose_file).resolve())]
        self.baseline = baseline
        self.selected = None

    def pinned(self):
        return json.loads(Path(self.args.compose_file).read_text())['services'][self.args.service]['image']

    def inspect(self):
        # No long-running container: the selected image is the Compose pin.
        self.engine()
        return {'Image': self.pinned()}

    def running(self, *filters):
        command = ['docker', 'ps', '--quiet', '--no-trunc']
        for value in filters:
            command += ['--filter', value]
        return self.docker(command).split()

    def idle(self):
        self.engine()
        pin = self.pinned()
        return not (self.running('label=com.docker.compose.project=' + self.args.project,
                                 'label=com.docker.compose.service=' + self.args.service)
                    or self.running('ancestor=' + pin))

    def replace(self, image):
        # Validate the exact configuration with the new image; no container is
        # recreated. The runner saves the Compose pin only after health passes.
        try:
            self.prepare(image)
        except Uncertain:
            raise
        except UpdateError:
            raise PreflightError('replacement validation failed before container mutation') from None
        self.selected = image

    def self_check(self, image):
        self.image(image)
        output = self.docker(['docker', 'run', '--rm', '--pull', 'never', '--network', 'none',
                              '--read-only', '--tmpfs', '/tmp:rw,size=64m', '--cap-drop', 'ALL',
                              '--security-opt', 'no-new-privileges', '--user', '10001:10001',
                              '--entrypoint', 'treedar', image, '--version'],
                             timeout=self.self_check_seconds)
        return bool(VERSION.fullmatch(output))

    def healthy(self, image):
        return self.self_check(image) and self.labelled_or_baseline(image)

    def adoptable(self, image):
        return self.self_check(image)


ADAPTERS = {'coordinator': CoordinatorService, 'treedar': TreeDarImage}


def adapter(kind, root, settings, work, *, api=None, authority=None, baseline=None):
    """The adapter for a published-package kind; odx keeps `ComposeNode`.

    `root` is the operator root holding `compose.json`; `work` is the private
    receipt directory for resolved configuration files.
    """
    if kind not in ADAPTERS:
        raise UpdateError('no published-package adapter for this service kind')
    return ADAPTERS[kind](service_args(root, settings, kind), work, api=api, authority=authority,
                          baseline=baseline)


def safe(check, *args):
    """A check result where an unreadable answer is simply not healthy."""
    try:
        return bool(check(*args))
    except (UpdateError, SmokeError, KeyError, TypeError, ValueError):
        return False
