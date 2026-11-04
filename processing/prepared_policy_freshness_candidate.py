"""Unmounted, held-only fresh NodeODM policy reconciliation candidate.

Catalog inputs and configuration must eventually be tied to authenticated fixed
node identities and a durable activation epoch. This module deliberately cannot
admit, activate, reserve, issue grants or replace the scheduler's node checks.
"""
import copy

from . import reference_protocol as p
from .options import catalog
from .prepared_envelope_jcs_candidate import _validate_definition
from .reference_runtime import discovered_policy


class PreparedPolicyHeld(ValueError):
    pass


def inspect_policy_held(*, nodes, config, configured_policy, inventory=False):
    """Re-read all configured engines; return held, never admission authority."""
    try:
        if type(inventory) is not bool or type(nodes) not in (list,tuple) or not nodes:
            raise ValueError('fixed fleet required')
        nodes=tuple(tuple(node) for node in nodes)
        if any(len(node)!=3 or not node[1] for node in nodes):raise ValueError('configured nodes required')
        policy=copy.deepcopy(configured_policy)
        p._closed(policy,('capabilityFingerprint','optionsFingerprint','availableEngines','optionDefinitions'),'prepared policy')
        if policy['availableEngines'] != ['odm'] or type(policy['optionDefinitions']) is not dict:
            raise ValueError('unsupported policy')
        snapshot=catalog(nodes,inventory=inventory)
        expected={node[0] for node in nodes}
        if (snapshot.get('ready') is not True or snapshot.get('degraded') is not False
                or len(snapshot.get('nodes',[]))!=len(expected)
                or {node.get('role') for node in snapshot['nodes']}!=expected
                or any(node.get('configured') is not True or node.get('available') is not True for node in snapshot['nodes'])):
            raise ValueError('full fleet unavailable')
        capability, options_fingerprint, allowed=discovered_policy(snapshot,config)
        if (policy['capabilityFingerprint']!=capability or policy['optionsFingerprint']!=options_fingerprint):
            raise ValueError('configured capability changed')
        definitions=policy['optionDefinitions']; common={option['name']:option for option in snapshot['commonOptions']}
        if not set(definitions)<=set(allowed):raise ValueError('option unavailable')
        for name,definition in definitions.items():
            _validate_definition(definition)
            fresh=common[name]
            # No domain is inferred from defaults or help text. An actual
            # explicit engine-provided domain must match type and shape exactly.
            if ('domain' not in fresh or fresh.get('type')!=definition['type']
                    or p.canonical_json(fresh['domain'])!=p.canonical_json(definition['domain'])):
                raise ValueError('option type or domain unproved')
        return {'state':'held','reason':'prepared_policy_admission_unqualified',
                'capabilityFingerprint':capability,'optionsFingerprint':options_fingerprint}
    except (KeyError,TypeError,ValueError,AttributeError) as exc:
        raise PreparedPolicyHeld('prepared policy freshness unavailable') from exc
