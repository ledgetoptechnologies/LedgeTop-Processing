"""Read-only staged coordinator/worker configuration cross-check, no activation.

Run where all explicitly staged file paths resolve. Actual runtime loaders check
each file; this tool compares their bindings, not installed services or custody.
"""
import argparse
import hmac
import ipaddress
import json
from urllib.parse import urlsplit

from processing import reference_protocol as p
from processing.config import from_environment
from processing.reference_result_proxy import _origin
from processing.reference_runtime import load_reference_settings
from ops.reference_worker_daemon import load_config, _service_profile, _option_evidence
from ops.bootstrap_worker_input_accounting import input_profile


class DeploymentPreflightHeld(ValueError):
    """Fixed diagnostic only; never include private configuration values."""


def inspect_deployment(coordinator_path, worker_paths, *, config, authority_id,
                       coordinator_origin, broker_origin):
    """Load real private configs; never construct Queue/worker/network clients."""
    try:
        p._uuid(authority_id,'authority')
        coordinator_origin=_origin(coordinator_origin);broker_origin=_origin(broker_origin)
        if type(worker_paths) is not dict or set(worker_paths)!={'cluster','main'}:
            raise ValueError('paired workers required')
        settings=load_reference_settings(coordinator_path,config)
        if set(settings.workers)!=set(worker_paths):raise ValueError('paired identities required')
        workers={node:load_config(worker_paths[node]) for node in worker_paths}
        secrets=list(settings.viewer_bearers)
        for worker in settings.workers.values():secrets.extend((worker['bearer'],worker['resultBearer']))
        identities=[]
        for node,worker in workers.items():
            bound=settings.workers[node]
            # Mirror the actual factory's pure namespace-local engine rule.
            # Coordinator LAN URLs cannot be compared across namespaces.
            parsed=urlsplit(worker['engineOrigin'])
            if (parsed.scheme not in ('http','https') or not ipaddress.ip_address(parsed.hostname).is_loopback
                    or parsed.username is not None or parsed.password is not None or parsed.path not in ('','/')
                    or parsed.query or parsed.fragment or parsed.port == 0):
                raise ValueError('fixed literal loopback engine origin required')
            secrets.extend(worker[key] for key in ('workerBearer','resultReadBearer','engineToken') if worker[key])
            if (worker['nodeId'],worker['authorityId'],worker['workerId'],worker['workerInstanceId']) != (
                    node,authority_id,bound['workerId'],bound['instanceId']):raise ValueError('worker identity differs')
            if not hmac.compare_digest(worker['workerBearer'],bound['bearer']) or not hmac.compare_digest(worker['resultReadBearer'],bound['resultBearer']):
                raise ValueError('private channel differs')
            native_token=config.local_node_token if node=='cluster' else config.main_node_token
            if not hmac.compare_digest(worker['engineToken'],native_token or ''):raise ValueError('native channel differs')
            if _origin(worker['coordinatorOrigin'])!=coordinator_origin or _origin(worker['brokerOrigin'])!=broker_origin:
                raise ValueError('fixed endpoint differs')
            if not set(settings.viewer_bearers.values())<=set(worker['allowedEnvironments']):raise ValueError('environment coverage differs')
            if _service_profile(worker) is None or not _option_evidence(worker) or not input_profile(worker):
                raise ValueError('qualified worker profiles required')
            if worker['inputCopyAccounting']['schemaVersion']!=3:raise ValueError('three-retained profile required')
            if worker['cleanupEnabled'] or worker['engineCleanupEnabled']:raise ValueError('pilot cleanup must remain disabled')
            if settings.stream_budget['maxObjectBytes']<worker['outputBudget']:raise ValueError('result stream budget cannot cover worker output')
            if settings.initial_grant_delivery is not None:
                initial=settings.initial_grant_delivery.payload()
                if worker['transferTtl']*1000>min(initial['maxLifetimeMs'],initial['brokerCapabilities']['maxEpochTtlMs']):
                    raise ValueError('initial transfer lifetime differs')
            routing=settings.routing_bindings[node] if settings.routing_bindings is not None else None
            recovery=settings.recovery_bindings[node] if settings.recovery_bindings is not None else None
            if routing is not None and (routing.engine_id,routing.gate_generation,routing.engine_version)!=(worker['engineId'],worker['gateGeneration'],worker['engineVersion']):
                raise ValueError('routing identity differs')
            if recovery is not None and (recovery.authority_id,recovery.engine_id,recovery.gate_generation,recovery.coordinator_origin,recovery.output_budget)!=(
                    authority_id,worker['engineId'],worker['gateGeneration'],worker['gateCoordinatorOrigin'],worker['outputBudget']):raise ValueError('recovery binding differs')
            identities.append(dict(nodeId=node,workerId=worker['workerId'],workerInstanceId=worker['workerInstanceId'],
                engineId=worker['engineId'],gateGeneration=worker['gateGeneration'],engineVersion=worker['engineVersion'],
                inputAccountingVersion=3,resultOrigin=bound['resultOrigin'],allowedEnvironments=sorted(worker['allowedEnvironments'])))
        if len({w['engineId'] for w in workers.values()})!=2:raise ValueError('independent engines required')
        result=dict(schemaVersion=1,scope='staged-configuration-only',configurationBindingsMatch=True,
            authorityId=authority_id,coordinatorOrigin=coordinator_origin,brokerOrigin=broker_origin,
            workers=identities,initialGrantDelivery=settings.initial_grant_delivery is not None,
            grantReconciliation=settings.grant_reconciliation or settings.initial_grant_delivery is not None,
            servicesActivated=False,deploymentApproved=False,
            outstanding=['installed source/image identity','actual TLS and port mapping','physical gate and fresh bootstrap',
                         'authenticated worker health and capacity','Viewer issuer/recoverable bearer/source custody',
                         'Viewer durable result publication and acceptance','user-controlled small pilot'])
        encoded=json.dumps(result,sort_keys=True)
        if any(secret in encoded for secret in secrets):raise ValueError('public identity overlaps private credential')
        return result
    except Exception:
        raise DeploymentPreflightHeld('staged reference configuration preflight held') from None


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('coordinator-config','cluster-config','main-config','authority-id','coordinator-origin','broker-origin'):
        parser.add_argument('--'+name,required=True)
    args=parser.parse_args(argv)
    try:
        result=inspect_deployment(args.coordinator_config,{'cluster':args.cluster_config,'main':args.main_config},
            config=from_environment(),authority_id=args.authority_id,coordinator_origin=args.coordinator_origin,broker_origin=args.broker_origin)
    except Exception:
        print(json.dumps({'error':'staged_configuration_preflight_held'}));return 1
    print(json.dumps(result,sort_keys=True));return 0


if __name__=='__main__':raise SystemExit(main())
