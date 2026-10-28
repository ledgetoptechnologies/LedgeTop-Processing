"""Fixed-policy image discovery and staging, never qualification or deployment.

The selected linux/amd64 manifest digest is the registry pull identity. Its
config digest is the expected *different semantic identity* of the local image;
RepoDigests are never treated as image IDs. Docker's existing credential helper
is used without reading/exporting credentials. No tag pulls, registry writes,
pruning, TLS overrides, global context changes, or service commands exist here.

Verbose Docker CLI does not expose an index's authoritative digest. We compare
its complete descriptor set before/after immutable child re-fetch; this detects
changed content mappings, not transient ABA tag changes or index-only annotations.
Only immutable selected-child metadata authorizes staging. Compressed layer size
does NOT bound extracted storage: the operator must reserve a trusted worst-case
amount; the post-pull Size check cannot enforce a hard extraction quota. Unknown
pull outcomes retain a durable intent and may only be reconciled by inspection.
"""
import base64
from contextlib import contextmanager
from dataclasses import asdict,dataclass
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import sqlite3
import stat
import subprocess
import threading
import time


class RegistryError(ValueError):
    """Bounded reason only: never include registry/credential-helper output."""


class RegistryHeld(RegistryError):
    """A possibly completed pull is fenced; inspect instead of replaying it."""


DIGEST=re.compile(r'sha256:[a-f0-9]{64}\Z')
TAG=re.compile(r'[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}\Z')
NAME=re.compile(r'[a-z0-9][a-z0-9_.-]{0,62}\Z')
IMAGE_MEDIA={'application/vnd.oci.image.manifest.v1+json','application/vnd.docker.distribution.manifest.v2+json'}
CONFIG_MEDIA={'application/vnd.oci.image.config.v1+json','application/vnd.docker.container.image.v1+json'}
LAYER_MEDIA={'application/vnd.oci.image.layer.v1.tar','application/vnd.oci.image.layer.v1.tar+gzip',
    'application/vnd.oci.image.layer.v1.tar+zstd','application/vnd.docker.image.rootfs.diff.tar.gzip'}


def _json(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)


def _sha(value):
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _digest(value):
    if type(value) is not str or not DIGEST.fullmatch(value):raise RegistryError('invalid_digest')
    return value


def _integer(value):
    if type(value) is not int or not 0<=value<=2**53-1:raise RegistryError('invalid_size')
    return value


def _repository(value,*,canonical=False):
    if type(value) is not str or len(value)>255 or any(c in value for c in '@?#\\ \t\n\r') or '://' in value:
        raise RegistryError('fixed_repository_required')
    parts=value.split('/')
    if not ('.' in parts[0] or ':' in parts[0] or parts[0]=='localhost'):
        if canonical:raise RegistryError('fully_qualified_repository_required')
        parts=['docker.io']+(['library'] if len(parts)==1 else [])+parts
    if len(parts)<2 or not re.fullmatch(r'[a-z0-9][a-z0-9.-]*(?::[0-9]{1,5})?',parts[0]):
        raise RegistryError('fixed_repository_required')
    if any(not re.fullmatch(r'[a-z0-9]+(?:[._-][a-z0-9]+)*',part) for part in parts[1:]):
        raise RegistryError('fixed_repository_required')
    return '/'.join(parts)


@dataclass(frozen=True)
class RegistryChannel:
    name:str
    repository:str
    tag:str

    def __post_init__(self):
        if type(self.name) is not str or type(self.tag) is not str or not NAME.fullmatch(self.name) or not TAG.fullmatch(self.tag):
            raise RegistryError('invalid_fixed_channel')
        _repository(self.repository,canonical=True)


@dataclass(frozen=True)
class RegistryPolicy:
    docker_executable:str
    docker_endpoint:str
    engine_id:str
    channels:tuple[RegistryChannel,...]
    staging_reservation_bytes:int
    timeout_seconds:int=30
    pull_timeout_seconds:int=1800
    max_output_bytes:int=4*1024**2

    def __post_init__(self):
        if not Path(self.docker_executable).is_absolute():raise RegistryError('fixed_docker_executable_required')
        endpoint=self.docker_endpoint
        if not (endpoint.startswith('unix:///') and len(endpoint)>8 and not any(c in endpoint for c in '\r\n\x00')
                or endpoint=='npipe:////./pipe/docker_engine'):
            raise RegistryError('fixed_local_docker_endpoint_required')
        if type(self.engine_id) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}',self.engine_id):
            raise RegistryError('fixed_engine_identity_required')
        if (type(self.channels) is not tuple or not self.channels or len(self.channels)>16
                or any(type(channel) is not RegistryChannel for channel in self.channels)
                or len({channel.name for channel in self.channels})!=len(self.channels)):
            raise RegistryError('explicit_fixed_channels_required')
        for value in (self.staging_reservation_bytes,self.timeout_seconds,self.pull_timeout_seconds,self.max_output_bytes):
            if _integer(value)<1:raise RegistryError('positive_policy_bound_required')
        if self.max_output_bytes>16*1024**2:raise RegistryError('bounded_output_required')


def _run(command,*,timeout,max_bytes):
    """Drain both pipes concurrently with bounded retention and fixed deadline."""
    env=os.environ.copy()
    for key in ('DOCKER_CONTEXT','DOCKER_HOST','DOCKER_TLS_VERIFY','DOCKER_CERT_PATH'):
        env.pop(key,None)
    try:
        process=subprocess.Popen(command,stdin=subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=subprocess.PIPE,
            env=env,start_new_session=os.name!='nt')
    except Exception:raise RegistryError('docker_command_unavailable') from None
    overflow=threading.Event()
    chunks=[bytearray(),bytearray()]
    def drain(index,stream):
        try:
            while chunk:=stream.read(65536):
                if len(chunks[index])+len(chunk)>max_bytes:overflow.set();return
                chunks[index].extend(chunk)
        except Exception:overflow.set()
        finally:stream.close()
    threads=[threading.Thread(target=drain,args=(i,stream),daemon=True,name='registry-command-output')
             for i,stream in enumerate((process.stdout,process.stderr))]
    for thread in threads:thread.start()
    deadline=time.monotonic()+timeout
    reason=None
    # Credential helpers can outlive the CLI and inherit its pipes. The command
    # is complete only when both the process and readers are finished; a parent
    # exit must not disable the deadline or leave its owned POSIX group alive.
    while process.poll() is None or any(thread.is_alive() for thread in threads):
        if overflow.is_set():reason='docker_output_limit';break
        if time.monotonic()>=deadline:reason='docker_command_timeout';break
        time.sleep(.01)
    if reason:
        try:
            if os.name!='nt':os.killpg(process.pid,signal.SIGKILL)
            else:process.kill()
        except OSError:pass
    try:process.wait(timeout=2)
    except subprocess.TimeoutExpired:reason=reason or 'docker_command_timeout'
    for thread in threads:thread.join(timeout=2)
    if reason or overflow.is_set() or any(thread.is_alive() for thread in threads):
        raise RegistryError(reason or 'docker_output_limit')
    return subprocess.CompletedProcess(command,process.returncode,bytes(chunks[0]),bytes(chunks[1]))


def _no_secrets(value,depth=0):
    if depth>24:raise RegistryError('metadata_depth_limit')
    if isinstance(value,dict):
        for key,item in value.items():
            if (type(key) is not str or re.search(r'password|secret|credential|authorization|(?:^|[_.-])token(?:$|[_.-])',key,re.I)):
                raise RegistryError('secret_bearing_metadata')
            _no_secrets(item,depth+1)
    elif isinstance(value,list):
        for item in value:_no_secrets(item,depth+1)
    elif isinstance(value,str):
        if (re.search(r'(?i)bearer\s|basic\s+[A-Za-z0-9+/=]+|https?://[^/\s]*@',value)
                or re.search(r'(?i)(?:password|secret|access_token|authorization)\s*[:=]|[?&](?:token|sig|signature|x-amz-signature|x-amz-credential)=',value)
                or any(char in value for char in '\x00\r')):
            raise RegistryError('secret_bearing_metadata')


def _load(raw):
    def unique(pairs):
        result={}
        for key,value in pairs:
            if key in result:raise RegistryError('ambiguous_metadata')
            result[key]=value
        return result
    try:
        value=json.loads(raw,object_pairs_hook=unique,parse_constant=lambda _:(_ for _ in ()).throw(ValueError()))
        _no_secrets(value)
        return value
    except RegistryError:raise
    except Exception:raise RegistryError('invalid_registry_metadata') from None


class DockerRegistry:
    """Durable candidate discovery and one-shot immutable staging for a host."""
    def __init__(self,path,policy,*,capacity_admit,runner=None):
        if type(policy) is not RegistryPolicy or not callable(capacity_admit):raise RegistryError('fixed_policy_and_capacity_required')
        self.path=Path(path).absolute();self.policy=policy
        self.capacity_admit,self.runner=capacity_admit,runner or _run
        self.channels={channel.name:channel for channel in policy.channels}
        self.path.parent.mkdir(parents=True,mode=0o700,exist_ok=True)
        if any(part.is_symlink() for part in (self.path,*self.path.parents)):raise RegistryError('private_registry_journal_required')
        info=self.path.parent.stat()
        if os.name!='nt' and (info.st_uid!=os.geteuid() or info.st_mode & 0o077):raise RegistryError('private_registry_journal_required')
        fd=os.open(self.path,os.O_CREAT|os.O_RDWR|getattr(os,'O_NOFOLLOW',0),0o600);os.close(fd)
        with self._db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS registry_binding (id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS registry_candidates (id TEXT PRIMARY KEY,value TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS registry_staging (id TEXT PRIMARY KEY,state TEXT NOT NULL,metadata TEXT)')
            binding=_json(asdict(policy));old=db.execute('SELECT value FROM registry_binding WHERE id=1').fetchone()
            if old and old[0]!=binding:raise RegistryError('registry_policy_changed')
            db.execute('INSERT OR IGNORE INTO registry_binding VALUES(1,?)',(binding,))
        if os.name!='nt':
            fd=os.open(self.path.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
            try:os.fsync(fd)
            finally:os.close(fd)

    @contextmanager
    def _db(self):
        info=self.path.lstat()
        if (self.path.is_symlink() or not stat.S_ISREG(info.st_mode) or info.st_nlink!=1
                or (os.name!='nt' and (info.st_uid!=os.geteuid() or info.st_mode & 0o077))):
            raise RegistryError('private_registry_journal_required')
        db=sqlite3.connect(self.path,timeout=10);db.row_factory=sqlite3.Row
        db.execute('PRAGMA synchronous=FULL')
        try:
            with db:
                db.execute('BEGIN IMMEDIATE');yield db
        finally:db.close()

    def _command(self,args,*,pull=False):
        command=[self.policy.docker_executable,'--host',self.policy.docker_endpoint,*args]
        try:
            result=self.runner(command,timeout=self.policy.pull_timeout_seconds if pull else self.policy.timeout_seconds,
                max_bytes=self.policy.max_output_bytes)
            for stream in (result.stdout,result.stderr):
                if not isinstance(stream,(str,bytes)) or len(stream.encode() if isinstance(stream,str) else stream)>self.policy.max_output_bytes:
                    raise RegistryError('docker_output_limit')
            if result.returncode!=0:raise RegistryError('docker_command_failed')
            return result.stdout.decode('utf-8') if isinstance(result.stdout,bytes) else result.stdout
        except RegistryError:raise
        except Exception:raise RegistryError('docker_command_uncertain') from None

    def _entries(self,reference,repository):
        value=_load(self._command(['manifest','inspect','--verbose',reference]))
        entries=value if isinstance(value,list) else [value]
        if not 1<=len(entries)<=64:raise RegistryError('manifest_count_limit')
        parsed=[]
        for entry in entries:
            if not isinstance(entry,dict) or set(entry)-{'Ref','Descriptor','Raw','OCIManifest','SchemaV2Manifest'}:
                raise RegistryError('unsupported_manifest_schema')
            if not {'Ref','Descriptor','Raw'}<=set(entry):raise RegistryError('raw_manifest_required')
            desc=entry['Descriptor']
            if not isinstance(desc,dict) or set(desc)-{'mediaType','digest','size','platform','annotations'}:
                raise RegistryError('unsupported_manifest_descriptor')
            if desc.get('mediaType') not in IMAGE_MEDIA:raise RegistryError('unsupported_manifest_schema')
            digest=_digest(desc.get('digest'));size=_integer(desc.get('size'))
            platform=desc.get('platform')
            if (not isinstance(platform,dict) or set(platform)-{'architecture','os','variant','os.version','os.features'}
                    or type(platform.get('architecture')) is not str or type(platform.get('os')) is not str):
                raise RegistryError('explicit_manifest_platform_required')
            try:
                ref,refdigest=entry['Ref'].rsplit('@',1)
                if ':' in ref.rsplit('/',1)[-1]:ref=ref.rsplit(':',1)[0]
                if _repository(ref)!=repository or refdigest!=digest:raise ValueError()
                raw=base64.b64decode(entry['Raw'],validate=True)
            except Exception:raise RegistryError('manifest_reference_changed') from None
            if len(raw)!=size or 'sha256:'+hashlib.sha256(raw).hexdigest()!=digest:raise RegistryError('manifest_digest_mismatch')
            manifest=_load(raw)
            attestation=(platform['os']==platform['architecture']=='unknown'
                and isinstance(manifest,dict) and manifest.get('artifactType')=='application/vnd.docker.attestation.manifest.v1+json')
            extra={'artifactType','subject'} if attestation else set()
            if (not isinstance(manifest,dict) or set(manifest)-({'schemaVersion','mediaType','config','layers','annotations'}|extra)
                    or type(manifest.get('schemaVersion')) is not int or manifest['schemaVersion']!=2
                    or manifest.get('mediaType')!=desc['mediaType']):raise RegistryError('unsupported_manifest_schema')
            shown=entry.get('OCIManifest',entry.get('SchemaV2Manifest'))
            if shown is None or shown!=manifest:raise RegistryError('manifest_representation_changed')
            config=manifest.get('config')
            config_keys={'mediaType','digest','size'}|({'data'} if attestation else set())
            if (not isinstance(config,dict) or set(config)!=config_keys
                    or config['mediaType'] not in (CONFIG_MEDIA|({'application/vnd.oci.empty.v1+json'} if attestation else set()))):
                raise RegistryError('unsupported_config_schema')
            _digest(config['digest']);_integer(config['size'])
            if attestation:
                try:data=base64.b64decode(config['data'],validate=True)
                except Exception:raise RegistryError('invalid_attestation_config') from None
                subject=manifest.get('subject')
                if (data!=b'{}' or config['size']!=len(data) or config['digest']!='sha256:'+hashlib.sha256(data).hexdigest()
                        or not isinstance(subject,dict) or set(subject)!={'mediaType','digest','size'}
                        or subject['mediaType'] not in IMAGE_MEDIA):raise RegistryError('invalid_attestation_config')
                _digest(subject['digest']);_integer(subject['size'])
            layers=manifest.get('layers')
            if not isinstance(layers,list) or not 1<=len(layers)<=256:raise RegistryError('bounded_image_layers_required')
            total=config['size']
            for layer in layers:
                if not isinstance(layer,dict) or set(layer)-{'mediaType','digest','size','annotations'}:raise RegistryError('unsupported_layer_schema')
                allowed=LAYER_MEDIA|({'application/vnd.in-toto+json'} if platform['os']==platform['architecture']=='unknown' else set())
                if layer.get('mediaType') not in allowed:raise RegistryError('unsupported_layer_schema')
                _digest(layer.get('digest'));total+=_integer(layer.get('size'));_integer(total)
            parsed.append(dict(descriptor=desc,configDigest=config['digest'],compressedBytes=total))
        return parsed

    def discover(self,channel):
        if channel not in self.channels:raise RegistryError('unconfigured_channel')
        source=self.channels[channel];reference=source.repository+':'+source.tag
        entries=self._entries(reference,source.repository)
        selected=[entry for entry in entries if entry['descriptor']['platform']=={'os':'linux','architecture':'amd64'}]
        if len(selected)!=1:raise RegistryError('unambiguous_linux_amd64_required')
        item=selected[0];digest=item['descriptor']['digest']
        immutable=self._entries(source.repository+'@'+digest,source.repository)
        if immutable!=[item]:raise RegistryError('immutable_manifest_changed')
        after=self._entries(reference,source.repository)
        fingerprint=lambda entries:_sha(sorted(entries,key=lambda item:_json(item)))
        if fingerprint(after)!=fingerprint(entries):raise RegistryError('tag_changed_during_discovery')
        candidate=dict(schemaVersion=1,channel=channel,repository=source.repository,tag=source.tag,platform='linux/amd64',
            manifestDigest=digest,configDigest=item['configDigest'],localImageId=item['configDigest'],
            compressedBytes=item['compressedBytes'],descriptorSetSha256=fingerprint(entries))
        candidate['candidateId']=_sha(candidate)
        with self._db() as db:db.execute('INSERT OR IGNORE INTO registry_candidates VALUES(?,?)',(candidate['candidateId'],_json(candidate)))
        return candidate

    def _candidate(self,candidate):
        fields={'schemaVersion','channel','repository','tag','platform','manifestDigest','configDigest',
            'localImageId','compressedBytes','descriptorSetSha256','candidateId'}
        if type(candidate) is not dict or set(candidate)!=fields or type(candidate.get('candidateId')) is not str:
            raise RegistryError('persisted_candidate_required')
        source=self.channels.get(candidate['channel']) if type(candidate['channel']) is str else None
        if (source is None or type(candidate['schemaVersion']) is not int or candidate['schemaVersion']!=1
                or candidate['repository']!=source.repository or candidate['tag']!=source.tag or candidate['platform']!='linux/amd64'
                or candidate['localImageId']!=candidate['configDigest']
                or candidate['candidateId']!=_sha({key:value for key,value in candidate.items() if key!='candidateId'})):
            raise RegistryError('persisted_candidate_required')
        _digest(candidate['manifestDigest']);_digest(candidate['configDigest']);_integer(candidate['compressedBytes'])
        if not re.fullmatch(r'[a-f0-9]{64}',candidate['descriptorSetSha256']):raise RegistryError('persisted_candidate_required')
        with self._db() as db:row=db.execute('SELECT value FROM registry_candidates WHERE id=?',(candidate['candidateId'],)).fetchone()
        if not row or row[0]!=_json(candidate):raise RegistryError('persisted_candidate_required')
        return candidate['candidateId']

    def _engine(self):
        if self._command(['info','--format','{{.ID}}']).strip()!=self.policy.engine_id:raise RegistryError('docker_engine_changed')

    def _pull_id(self,candidate):
        # Tag/channel/attestation changes cannot create a fresh pull identity
        # for an already uncertain immutable target.
        return _sha(dict(repository=candidate['repository'],manifestDigest=candidate['manifestDigest'],
            configDigest=candidate['configDigest'],platform=candidate['platform'],engineId=self.policy.engine_id))

    def _inspect(self,candidate):
        reference=candidate['repository']+'@'+candidate['manifestDigest']
        template='{"Id":{{json .Id}},"Os":{{json .Os}},"Architecture":{{json .Architecture}},"RepoDigests":{{json .RepoDigests}},"Size":{{json .Size}}}'
        image=_load(self._command(['image','inspect','--format',template,reference]))
        if not isinstance(image,dict) or set(image)!={'Id','Os','Architecture','RepoDigests','Size'}:raise RegistryError('invalid_local_image_metadata')
        if image['Id']!=candidate['configDigest'] or (image['Os'],image['Architecture'])!=('linux','amd64'):
            raise RegistryError('local_config_or_platform_changed')
        repos=image['RepoDigests']
        if not isinstance(repos,list) or not 1<=len(repos)<=64:raise RegistryError('local_repository_digest_missing')
        normalized=[]
        for ref in repos:
            try:repo,digest=ref.rsplit('@',1);normalized.append(_repository(repo)+'@'+_digest(digest))
            except Exception:raise RegistryError('invalid_local_repository_digest') from None
        if reference not in normalized:raise RegistryError('local_repository_digest_missing')
        if _integer(image['Size'])>self.policy.staging_reservation_bytes:raise RegistryError('local_image_exceeds_reservation')
        return dict(candidateId=candidate['candidateId'],repository=candidate['repository'],manifestDigest=candidate['manifestDigest'],
            imageId=image['Id'],platform='linux/amd64',localSize=image['Size'],engineId=self.policy.engine_id)

    def _finish(self,identifier,state,metadata=None):
        with self._db() as db:db.execute('UPDATE registry_staging SET state=?,metadata=? WHERE id=?',(state,_json(metadata) if metadata else None,identifier))

    def stage(self,candidate):
        """Capacity callback must durably reserve, idempotently by candidate ID.

        Its reservation remains caller-owned across restart/unknown responses;
        this component never releases budget or prunes images to make room.
        """
        self._candidate(candidate);identifier=self._pull_id(candidate)
        self._engine()
        with self._db() as db:
            old=db.execute('SELECT state FROM registry_staging WHERE id=?',(identifier,)).fetchone()
            unresolved=db.execute("SELECT 1 FROM registry_staging WHERE state!='staged'").fetchone()
        if old:
            if old['state']=='staged':return self.reconcile(candidate)
            raise RegistryHeld('pull_outcome_requires_reconciliation')
        if unresolved:raise RegistryHeld('pull_outcome_requires_reconciliation')
        if candidate['compressedBytes']>self.policy.staging_reservation_bytes:raise RegistryError('compressed_image_exceeds_reservation')
        try:admitted=self.capacity_admit(dict(candidate),self.policy.staging_reservation_bytes)
        except Exception:raise RegistryError('capacity_admission_failed') from None
        if admitted is not True:raise RegistryError('capacity_admission_required')
        self._engine()
        with self._db() as db:
            if db.execute("SELECT 1 FROM registry_staging WHERE id=? OR state!='staged'",(identifier,)).fetchone():
                raise RegistryHeld('pull_outcome_requires_reconciliation')
            db.execute("INSERT INTO registry_staging VALUES(?,'pull_pending',NULL)",(identifier,))
        try:
            self._command(['pull','--platform','linux/amd64',candidate['repository']+'@'+candidate['manifestDigest']],pull=True)
            self._engine();metadata=self._inspect(candidate);self._engine()
            self._finish(identifier,'staged',metadata)
            return metadata
        except Exception:
            try:self._finish(identifier,'held')
            except Exception:pass  # Original durable intent still fences replay.
            raise RegistryHeld('pull_outcome_requires_reconciliation') from None

    def reconcile(self,candidate):
        self._candidate(candidate);identifier=self._pull_id(candidate)
        with self._db() as db:
            if db.execute('SELECT 1 FROM registry_staging WHERE id=?',(identifier,)).fetchone() is None:
                raise RegistryHeld('no_prior_pull_intent')
        try:self._engine();metadata=self._inspect(candidate);self._engine()
        except Exception:raise RegistryHeld('pull_outcome_requires_reconciliation') from None
        self._finish(identifier,'staged',metadata)
        return metadata
