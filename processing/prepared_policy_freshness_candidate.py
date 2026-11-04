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


def _numeric_domain_supported(option, definition):
    """Accept only reviewed string grammar and a provable configured subset.

    Positive is interpreted strictly (>0), conservatively, even if a producer
    might use it to include zero. Unknown grammar and unbounded positive policy
    are held; no option names are encoded here.
    """
    kind=definition['type']; domain=option.get('domain')
    if option.get('type') != kind:
        return False
    if type(domain) is not str:
        # Preserve exact structured-fixture comparison; never infer a domain.
        return 'domain' in option and p.canonical_json(domain)==p.canonical_json(definition['domain'])
    grammar={
        ('int','integer'):(None,None,False),
        ('int','positive integer'):(1,None,False),
        ('int','integer: 1 <= x <= 14'):(1,14,False),
        ('float','float'):(None,None,False),
        ('float','positive float'):(0,None,True),
        ('float','float > 0.0'):(0,None,True),
    }
    bounds=grammar.get((kind,domain))
    if bounds is None:
        return False
    minimum,maximum,strict_minimum=bounds
    configured=definition['domain']
    if configured is None:
        return minimum is None and maximum is None
    if type(configured) is list:
        return all((minimum is None or (value>minimum if strict_minimum else value>=minimum))
                   and (maximum is None or value<=maximum) for value in configured)
    if type(configured) is not dict:
        return False
    return ((minimum is None or (configured['min']>minimum if strict_minimum else configured['min']>=minimum))
            and (maximum is None or configured['max']<=maximum))


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
            # Configured values must be a provable subset of the explicit
            # engine domain; unknown producer language never becomes authority.
            if not _numeric_domain_supported(fresh,definition):
                raise ValueError('option type or domain unproved')
        return {'state':'held','reason':'prepared_policy_admission_unqualified',
                'capabilityFingerprint':capability,'optionsFingerprint':options_fingerprint}
    except (KeyError,TypeError,ValueError,AttributeError) as exc:
        raise PreparedPolicyHeld('prepared policy freshness unavailable') from exc
