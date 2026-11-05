"""Closed private transport inputs for the existing first-cutover plan.

This is configuration validation, not qualification or activation. The reviewed
plan's deployment digest binds these inputs; none of their values grants new
authority. Credentials remain in existing private files, never this bundle.
"""
import hashlib
import re
from urllib.parse import urlsplit
import uuid

from .coordinator_startup_policy import SOURCES, _origin as engine_origin, build_startup_policy
from .enrolled_coordinator_cutover import validate_plan, PREDECESSOR_ID, PREDECESSOR_IMAGE, PREDECESSOR_SOURCE, NAME
from .enrolled_coordinator_transport import (TransportHeld, require, canonical, digest, decode,
    private_bytes, environment_file, _absolute_path, validate_recipe, create_arguments)


PROFILE = 'ltds-enrolled-cutover-private-deployment-v1'
FILES = frozenset(('bootstrapConfig','finalConfig','bootstrapEnvironment','finalEnvironment',
    'holderEnvironment','policy','predecessorSchema','censusKey','reviewedCounts'))
WORKER_SETTINGS = frozenset(('schemaVersion','unitName','unitFile','unitSha256','pythonExecutable',
    'configPath','sourceRoot','sourceSha256','readinessOrigin','readinessCAFile'))
WORKER_EXPECTED = frozenset(('nodeId','workerId','workerInstanceId','authorityId','engineId',
    'gateGeneration','engineVersion','configSha256','sourceSha256','activation'))


def closed(value, fields, reason):
    require(type(value) is dict and set(value) == set(fields), reason)


def hash_value(value):
    require(type(value) is str and re.fullmatch('[a-f0-9]{64}',value), 'reviewed_digest_required')
    return value


def identifier(value):
    try: valid = type(value) is str and str(uuid.UUID(value)) == value
    except (ValueError,AttributeError): valid = False
    require(valid,'reviewed_identifier_required'); return value


def slug(value):
    require(type(value) is str and re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9_-]{0,127}',value),
            'reviewed_worker_identity_required')
    return value


def origin(value):
    require(type(value) is str and len(value)<=2048,'reviewed_tls_origin_required')
    parsed=urlsplit(value)
    require(parsed.scheme=='https' and parsed.hostname and parsed.username is None and parsed.password is None
            and parsed.path in ('','/') and not parsed.query and not parsed.fragment,
            'reviewed_tls_origin_required')
    try: port=parsed.port
    except ValueError: raise TransportHeld('reviewed_tls_origin_required') from None
    require(port is None or 1<=port<=65535,'reviewed_tls_origin_required')
    return value


def artifact(value):
    closed(value,('path','sha256'),'closed_private_artifact_required')
    _absolute_path(value['path']);hash_value(value['sha256'])


def worker(value, authority):
    closed(value,('nodeId','sshHost','settings','expected'),'closed_worker_readiness_reference_required')
    slug(value['nodeId'])
    # The existing SSH identity is selected by the operator; no password, key or
    # arbitrary SSH options may be smuggled into a host argument.
    require(type(value['sshHost']) is str and re.fullmatch('[a-zA-Z0-9][a-zA-Z0-9.-]{0,252}',value['sshHost']),
            'fixed_existing_worker_host_required')
    settings=value['settings'];closed(settings,WORKER_SETTINGS,'existing_worker_handover_settings_required')
    require(type(settings['schemaVersion']) is int and settings['schemaVersion']==1,'worker_settings_version_required')
    require(type(settings['unitName']) is str and re.fullmatch(r'[A-Za-z0-9_.-]{1,128}\.service',settings['unitName']),
            'existing_worker_unit_required')
    for name in ('unitFile','pythonExecutable','configPath','sourceRoot','readinessCAFile'):_absolute_path(settings[name])
    for name in ('unitSha256','sourceSha256'):hash_value(settings[name])
    origin(settings['readinessOrigin'])
    expected=value['expected'];closed(expected,WORKER_EXPECTED,'existing_worker_readiness_expectations_required')
    require(expected['nodeId']==value['nodeId'] and expected['authorityId']==authority
            and expected['sourceSha256']==settings['sourceSha256'],'worker_expected_identity_changed')
    for name in ('nodeId','workerId','workerInstanceId','engineId'):slug(expected[name])
    require(type(expected['gateGeneration']) is int and expected['gateGeneration']>=0,'worker_generation_required')
    require(type(expected['engineVersion']) is str and 1<=len(expected['engineVersion'])<=128
            and all(0x21<=ord(c)<=0x7e for c in expected['engineVersion']),'worker_version_required')
    for name in ('configSha256','sourceSha256'):hash_value(expected[name])
    activation=expected['activation']
    if activation is not None:
        closed(activation,('schemaVersion','activationId','planId','imageId','engineVersion'),'existing_worker_activation_required')
        require(type(activation['schemaVersion']) is int and activation['schemaVersion']==1,'worker_activation_version_required')
        identifier(activation['activationId']);identifier(activation['planId'])
        require(type(activation['imageId']) is str and activation['imageId'].startswith('sha256:'),'worker_activation_image_required')
        hash_value(activation['imageId'][7:])
        require(activation['engineVersion']==expected['engineVersion'],'worker_activation_version_changed')


def validate_bundle(value, plan):
    """Pure validation; performs no Docker, SSH, file reads or side effects."""
    plan=validate_plan(plan)
    closed(value,('schemaVersion','profile','operationId','authorityId','host','predecessor','candidateSourceCommit','reviewedSourceRoot',
                  'recipes','holderSettings','artifacts','sourceHashes','nodeEndpoints','workers','qualifications'),
           'closed_private_deployment_required')
    require(type(value['schemaVersion']) is int and value['schemaVersion']==1 and value['profile']==PROFILE
            and value['operationId']==plan['operationId'] and value['authorityId']==plan['authorityId'],
            'deployment_plan_identity_changed')
    require(value['candidateSourceCommit']==plan['candidate']['sourceCommit'],'candidate_source_pin_changed')
    _absolute_path(value['reviewedSourceRoot'])
    host=value['host'];closed(host,('dockerEngineId','bootId','dockerBinary','dockerBinarySha256','dockerSocket'),'closed_original_host_required')
    require(type(host['dockerEngineId']) is str and 1<=len(host['dockerEngineId'])<=128
            and re.fullmatch('[a-zA-Z0-9:-]+',host['dockerEngineId']),'docker_engine_identity_required')
    identifier(host['bootId']);_absolute_path(host['dockerBinary'])
    hash_value(host['dockerBinarySha256'])
    require(host['dockerSocket']=='unix:///run/docker.sock','reviewed_docker_socket_required')
    old=value['predecessor'];closed(old,('containerId','imageId','sourceCommit','name','stateVolume','leaseDirectory'),
                                 'closed_original_deployment_required')
    require((old['containerId'],old['imageId'],old['sourceCommit'],old['name'])==
            (PREDECESSOR_ID,PREDECESSOR_IMAGE,PREDECESSOR_SOURCE,NAME)
            and old['stateVolume']==NAME+'-state','reviewed_original_deployment_changed')
    _absolute_path(old['leaseDirectory'])
    recipes=value['recipes'];closed(recipes,('bootstrap','final','holder'),'closed_candidate_recipes_required')
    for role in ('bootstrap','final'):
        validate_recipe(recipes[role],bootstrap=role=='bootstrap')
        require(recipes[role]['imageId']==plan['candidate']['imageId']
                and digest(recipes[role])==plan['candidate'][role+'RecipeSha256'],'candidate_recipe_pin_changed')
    # The full plan digest includes deploymentSha256. It cannot itself live in
    # this bundle without creating an impossible hash fixed point. Derive the
    # holder's required planSha256 from the already reviewed plan at execution.
    holder=value['holderSettings']
    closed(holder,('schemaVersion','operationId','authorityId','stateRoot','leaseDirectory'),
           'closed_derived_holder_binding_required')
    settings=dict(holder,planSha256=digest(plan))
    create_arguments(recipes['holder'],operation_id=plan['operationId'],role='lease-holder',
                     intent_id=plan['operationId'],holder_settings=settings)
    require(settings['authorityId']==plan['authorityId']
            and settings['leaseDirectory']==old['leaseDirectory']
            and recipes['holder']['imageId']==plan['candidate']['imageId'],'holder_plan_pin_changed')
    for recipe in recipes.values():
        state=[m for m in recipe['mounts'] if m['target']=='/prototype-state']
        lease=[m for m in recipe['mounts'] if m['target']==old['leaseDirectory']]
        require(len(state)==len(lease)==1 and state[0]['type']=='volume' and state[0]['source']==old['stateVolume']
                and lease[0]['type']=='bind' and lease[0]['readOnly'] is False,'original_physical_mount_binding_changed')
    files=value['artifacts'];closed(files,FILES,'closed_private_artifact_map_required')
    for entry in files.values():artifact(entry)
    for role in ('bootstrap','final'):
        require(files[role+'Config']['sha256']==plan['candidate'][role+'ConfigSha256']
                and files[role+'Environment']['path']==recipes[role]['environmentFile']
                and files[role+'Environment']['sha256']==recipes[role]['environmentSha256'],
                'private_recipe_file_binding_changed')
    require(files['holderEnvironment']['path']==recipes['holder']['environmentFile']
            and files['holderEnvironment']['sha256']==recipes['holder']['environmentSha256']
            and files['policy']['sha256']==plan['candidate']['declaredTransformSha256']
            and files['censusKey']['sha256']==plan['bindings']['censusKeySha256']
            and files['reviewedCounts']['sha256']==plan['bindings']['reviewedCountsSha256'],
            'existing_review_authority_changed')
    hashes=value['sourceHashes'];closed(hashes,SOURCES,'existing_policy_source_inventory_required')
    for item in hashes.values():hash_value(item)
    endpoints=value['nodeEndpoints'];require(type(endpoints) is list and len(endpoints)==2,'exact_enrolled_node_inventory_required')
    nodes=set()
    for endpoint in endpoints:
        closed(endpoint,('id','url'),'closed_node_endpoint_required');slug(endpoint['id']);engine_origin(endpoint['url'])
        require(endpoint['id'] not in nodes,'duplicate_node_endpoint');nodes.add(endpoint['id'])
    require(nodes=={'cluster','main'},'exact_enrolled_node_inventory_required')
    require(type(value['workers']) is list and len(value['workers'])==len(nodes),'complete_worker_readiness_inventory_required')
    seen=set()
    for item in value['workers']:
        worker(item,plan['authorityId']);require(item['nodeId'] not in seen,'duplicate_worker_reference');seen.add(item['nodeId'])
    require(seen==nodes,'worker_node_inventory_changed')
    closed(value['qualifications'],plan['gates'],'closed_existing_qualification_map_required')
    for name,pin in plan['gates'].items():
        proof=value['qualifications'][name]
        if pin is None:require(proof is None,'unreviewed_qualification_cannot_be_promoted')
        else:
            artifact(proof);require(proof['sha256']==pin,'qualification_review_changed')
    require(digest(value)==plan['bindings']['deploymentSha256'],'reviewed_deployment_digest_changed')
    return decode(canonical(value))


def read_bundle(path,plan):
    return validate_bundle(decode(private_bytes(path,maximum=262144),262144),plan)


def derived_holder_settings(bundle,plan):
    bundle=validate_bundle(bundle,plan)
    return dict(bundle['holderSettings'],planSha256=digest(validate_plan(plan)))


def verify_private_artifacts(bundle, plan, *, require_all_qualifications=False):
    """Hash existing private files. This is not live health or qualification."""
    bundle=validate_bundle(bundle,plan)
    require(not require_all_qualifications or all(bundle['qualifications'].values()),
            'reviewed_activation_qualification_missing')
    result={};policy_inputs={}
    for name,entry in bundle['artifacts'].items():
        maximum=32 if name=='censusKey' else 65536 if name.endswith('Environment') or name=='predecessorSchema' else 1024*1024
        raw=private_bytes(entry['path'],maximum=maximum)
        require(hashlib.sha256(raw).hexdigest()==entry['sha256'],'private_reviewed_artifact_changed')
        require(name!='censusKey' or len(raw)==32,'existing_census_key_required')
        if name.endswith('Environment'):environment_file(entry['path'])
        if name in ('bootstrapConfig','finalConfig','policy','predecessorSchema'):policy_inputs[name]=raw
        result[name]=dict(sha256=entry['sha256'],bytes=len(raw))
    generated=build_startup_policy(bundle['reviewedSourceRoot'],expected_source_hashes=bundle['sourceHashes'],
        candidate_image_id=bundle['recipes']['bootstrap']['imageId'],candidate_source_commit=bundle['candidateSourceCommit'],
        bootstrap_config_bytes=policy_inputs['bootstrapConfig'],final_config_bytes=policy_inputs['finalConfig'],
        node_endpoints=bundle['nodeEndpoints'],authority_id=bundle['authorityId'],
        reviewed_schema_bytes=policy_inputs['predecessorSchema'])
    require(generated==policy_inputs['policy'],'independently_generated_startup_policy_changed')
    for name,entry in bundle['qualifications'].items():
        if entry is None:
            require(not require_all_qualifications,'reviewed_activation_qualification_missing')
            result[name]=None;continue
        raw=private_bytes(entry['path'],maximum=1024*1024)
        require(hashlib.sha256(raw).hexdigest()==entry['sha256'],'qualification_artifact_changed')
        result[name]=dict(sha256=entry['sha256'],bytes=len(raw))
    return result
