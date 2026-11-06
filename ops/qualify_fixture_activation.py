"""Synthetic local-process activation seam for disposable cutover fixtures only.

No SSH, systemd, installed-tree receipt or production enrollment is implied.
The real activation journal still requires both ordered, durable observations.
"""
import os
import socket
import stat
from pathlib import Path
from .enrolled_coordinator_transport import digest, require
from .first_worker_activation import FirstWorkerActivation, activation_readiness_binding
from .qualify_enrolled_worker_preparation import readiness


def verify_fixture_ownership(driver, workers):
    operation=driver.plan['operationId']; outer=Path('/prototype-state')
    root=outer/operation
    require(Path(driver.root)==root/'operation' and root.resolve()==root and
            Path(driver.root).resolve()==Path(driver.root) and outer.is_mount(), 'fixture_operation_root_required')
    for path in (outer,root,Path(driver.root)):
        info=path.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid==os.geteuid() and
                stat.S_IMODE(info.st_mode)==0o700,'fixture_private_root_required')
    require(workers.root==root/'workers' and workers.layout['operationId']==operation,
            'fixture_worker_operation_changed')
    controller=driver.docker.inspect(workers.controller_id)
    require(controller['Id']==workers.controller_id and controller['Id'].startswith(socket.gethostname())
            and controller['Config']['Labels'].get('org.ledgetop.phase-controller')==operation
            and controller['State']['Running'] is True and controller['HostConfig']['NetworkMode']=='bridge'
            and controller['Image']==workers.layout['candidateImageId'], 'owned_fixture_controller_required')
    mounts=[m for m in controller['Mounts'] if m.get('Destination')=='/prototype-state']
    require(len(mounts)==1 and mounts[0].get('Type')=='volume' and mounts[0].get('RW') is True
            and mounts[0].get('Name') in ('ltds-phase-forward-artifacts-'+operation.replace('-','')[:16],
                                          'ltds-phase-rollback-artifacts-'+operation.replace('-','')[:16]),
            'owned_fixture_artifact_volume_required')
    for instance,worker in zip(workers.instances,driver.bundle['workers']):
        node=worker['nodeId']
        require(instance['node']==node and instance['root']==workers.root/node
                and instance['entry']==workers.layout['installations'][node]
                and instance['manifest']==worker,'fixture_worker_binding_changed')


class FixtureActivationBoundary:
    def __init__(self, controller, driver, workers, instance, worker):
        self.controller, self.driver, self.workers = controller, driver, workers
        self.instance, self.worker = instance, worker

    def _frozen(self):
        verify_fixture_ownership(self.driver,self.workers)
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
    verify_fixture_ownership(driver,workers)
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
        boundary=FixtureActivationBoundary(controller,driver,workers,instance,worker)
        activation=FirstWorkerActivation(driver.root,expected,boundary=boundary,driver=driver)
        activation.prepare();activation.advance();state=activation.advance()
        require(state['phase']=='ready' and state['pending'] is None,'fixture_activation_incomplete')
        packet=boundary._packet()
        activation_readiness_binding(driver,worker,packet['readiness'],packet['process'])
        states.append(state)
    return states
