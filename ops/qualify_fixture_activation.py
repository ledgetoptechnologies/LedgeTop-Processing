"""Synthetic local-process activation seam for disposable cutover fixtures only.

No SSH, systemd, installed-tree receipt or production enrollment is implied.
The real activation journal still requires both ordered, durable observations.
"""
from .enrolled_coordinator_transport import digest, require
from .first_worker_activation import FirstWorkerActivation
from .qualify_enrolled_worker_preparation import readiness


class FixtureActivationBoundary:
    def __init__(self, controller, instance, worker):
        self.controller, self.instance, self.worker = controller, instance, worker

    def _frozen(self):
        state = self.controller.journal.read()
        require(state['phase'] == 'bootstrap-frozen' and state['pending'] is None
                and state['admissionOpened'] is False, 'fixture_bootstrap_not_frozen')

    def _packet(self):
        packet = readiness(self.instance, self.worker)
        require(packet.get('nodeId') == self.worker['nodeId'], 'fixture_activation_node_changed')
        return packet

    def preflight(self, expected):
        self._frozen(); self._packet(); return True

    def guard(self, expected, current, *, inspection):
        self._frozen(); self._packet(); return True

    def execute(self, expected, current):
        # The actual worker was already started by this disposable fixture.
        # Neither systemd nor SSH nor an installed host is touched.
        require(current['action'] in ('reload', 'start'), 'fixture_activation_action_changed')
        self._frozen(); self._packet()

    def observe(self, expected, current):
        self._frozen()
        return dict(scope='synthetic-local-process', action=current['action'],
                    requestId=current['requestId'], readiness=self._packet())

    def verify(self, expected, current, observation):
        self._frozen()
        if (type(observation) is not dict or set(observation) != {'scope','action','requestId','readiness'}
                or observation['scope'] != 'synthetic-local-process' or observation['action'] != current['action']
                or observation['requestId'] != current['requestId']): return False
        prior=observation['readiness']; fresh=self._packet()
        keys=('bootId','pid','nodeId','workerId','workerInstanceId','authorityId','engineId','gateGeneration',
              'engineVersion','configSha256','sourceSha256','driverBindingSha256','readerEpoch','activation',
              'optionsSha256','safeOptionsSha256','loadedSourceSha256')
        return (prior['nodeId']==fresh['nodeId'] and prior['settingsSha256']==fresh['settingsSha256']
                and prior['process']==fresh['process']
                and all(prior['readiness'][key]==fresh['readiness'][key] for key in keys))


def activate_fixture_workers(controller, driver, workers, startup_id, owner_sha):
    """Write only fixture-scoped journals; never create an installation receipt."""
    require(len(workers.instances) == len(driver.bundle['workers']) == 2,
            'complete_fixture_activation_inventory_required')
    states=[]
    for instance, worker in zip(workers.instances, driver.bundle['workers']):
        require(instance['node'] == worker['nodeId'], 'fixture_activation_order_changed')
        evidence=dict(scope='synthetic-preparation-only', installationQualified=False,
                      review=instance['review'])
        expected=dict(operationId=driver.plan['operationId'],cutoverPlanSha256=digest(driver.plan),
            nodeId=worker['nodeId'],workerSha256=digest(worker),manifestSha256=instance['review']['enrollment']['manifestSha256'],
            installReceiptSha256=digest(evidence),bootstrapStartupId=startup_id,ownerSha256=owner_sha,
            ingressPolicySha256=digest(dict(scope='synthetic-no-ingress')),
            nativeInventorySha256=digest(dict(scope='synthetic-native-fixture',nodeId=worker['nodeId'])))
        boundary=FixtureActivationBoundary(controller,instance,worker)
        activation=FirstWorkerActivation(driver.root,expected,boundary=boundary,driver=driver)
        activation.prepare();activation.advance();state=activation.advance()
        require(state['phase']=='ready' and state['pending'] is None,'fixture_activation_incomplete')
        states.append(state)
    return states
