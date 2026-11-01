"""Local native bundle publication. No parent completion or outer transport."""
from contextlib import contextmanager,closing
from dataclasses import asdict,dataclass
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import stat
import struct
import threading
import zipfile

from . import reference_protocol as p,reference_treedar_bundle as b
from .reference_parent_guard import ProductionParentGuard,_private,_private_json,_readonly_sqlite,exact
from .reference_treedar_stage import TreeDarNativeStage
from .treedar_engine import _file,_sync_directory,_write_new


class PublicationHeld(ValueError):pass


def require(condition,reason):
    if not condition:raise PublicationHeld(reason)


def encoded(value):return p.canonical_json(value).decode()


@dataclass(frozen=True)
class PublicationPolicy:
    limits:b.BundleLimits
    control_budget:int=131072
    attempt_cap:int=3

    def __post_init__(self):
        require(type(self.limits) is b.BundleLimits,'fixed_bundle_limits_required')
        require(type(self.control_budget) is int and 4096<=self.control_budget<=262144,'bounded_publication_control_required')
        require(type(self.attempt_cap) is int and 1<=self.attempt_cap<=3,'bounded_copy_attempts_required')


SCHEMA=(
 'CREATE TABLE publication_binding(id INTEGER PRIMARY KEY CHECK(id=1),value TEXT NOT NULL)',
 'CREATE TABLE publications(job TEXT PRIMARY KEY,intent TEXT NOT NULL,source TEXT NOT NULL,control_base INTEGER NOT NULL)',
 '''CREATE TABLE publication_copies(job TEXT NOT NULL,number INTEGER NOT NULL,plan TEXT NOT NULL,
 phase TEXT NOT NULL,partial_identity TEXT,writer_intent TEXT,receipt TEXT,PRIMARY KEY(job,number))''')
PHASES=('planned','owned','abandoned_preparation','streaming','ready','link_pending','linked','unlink_pending','published')


class _Guard:
    """Concrete internal guard; not a caller-supplied authority callback."""
    def __init__(self,publication,job,number,policy,expected):
        self.publication,self.job,self.number,self.policy,self.expected=publication,job,number,policy,expected
        self.binding=dict(policySha256=p.digest(asdict(policy)),expectedSha256=p.digest(expected),
            publicationId=policy.publication_id,sourceStageProofSha256=policy.source_stage_proof_sha256)

    def check(self,context,*,mutation):
        host=self.publication;host._check_active();row=host._copy(self.job,self.number)
        require(row['phase'] in ('owned','streaming'),'copy_not_writable')
        source=host._live(self.job,mutation=mutation)
        require(exact(context,dict(expected=self.expected,publicationId=self.policy.publication_id,
            reservationId=self.policy.reservation_id,allocationBase=self.policy.allocation_base,
            remainingReservedBytes=self.policy.remaining_reserved_bytes,sourceStageProofSha256=self.policy.source_stage_proof_sha256,
            manifestSha256=context['manifestSha256'])),'copy_guard_context_changed')
        require(context['manifestSha256'] in (None,source['manifest']['manifestSha256']),'terminal_native_manifest_changed')
        reservation=host.stage._reservation(host._intent(self.job)['parentBinding'])
        require((reservation['allocated'],reservation['remaining'])==(self.policy.allocation_base,self.policy.remaining_reserved_bytes),'copy_reservation_changed')


class NativeBundlePublication:
    def __init__(self,path,policy,*,stage):
        require(os.name=='posix' and type(policy) is PublicationPolicy and type(stage) is TreeDarNativeStage
            and type(stage.guard) is ProductionParentGuard,'concrete_native_publication_dependencies_required')
        self.path=Path(path).absolute();self.root=self.path.parent;require(self.root==stage.root,'shared_state_owner_required')
        self.stage,self.policy=stage,policy;self._initial=(stage,stage.journal,stage.ledger,stage.owner,stage.guard)
        self._pid=os.getpid();self._mutex=threading.Lock();self._active=None;self._root_identity=_private(self.root,directory=True)[1]
        self.lock_path=self.path.with_name(self.path.name+'.lock')
        known=(stage.path,stage.lock_path,stage.journal.path,stage.journal.lock_path,Path(stage.ledger.path),stage.owner.root/'worker-daemon.lock',stage.prior.path,stage.prior.lock_path)
        require(all(x not in known and x not in {Path(str(k)+suffix) for k in known for suffix in ('-wal','-shm','-journal','.lock')} for x in (self.path,self.lock_path)),'dedicated_publication_paths_required')
        self._files={}
        for path in (self.path,self.lock_path):
            require(path.resolve()==path,'canonical_publication_paths_required')
            fd=os.open(path,os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW|os.O_NONBLOCK,0o600)
            try:
                self._files[path]=_private(path)[1];require((os.fstat(fd).st_dev,os.fstat(fd).st_ino)==self._files[path],'publication_inode_changed')
            finally:os.close(fd)
        self._binding=encoded(dict(protocol='ltds-native-bundle-publication-v1',policy=asdict(policy),stageBinding=stage._binding,
            rootIdentity=self._root_identity,files={str(k):v for k,v in self._files.items()}))
        with self._operation(),self._db() as db:
            if not db.execute("SELECT 1 FROM sqlite_master WHERE name NOT GLOB 'sqlite_*'").fetchone():
                for sql in SCHEMA:db.execute(sql)
            self._schema(db);row=db.execute('SELECT value FROM publication_binding WHERE id=1').fetchone()
            require(not row or row[0]==self._binding,'publication_binding_changed')
            require(row or not db.execute('SELECT 1 FROM publications').fetchone(),'publication_binding_missing')
            db.execute('INSERT OR IGNORE INTO publication_binding VALUES(1,?)',(self._binding,))
        _sync_directory(self.root);_sync_directory(self.root.parent);self._initialized=True

    def _check(self):
        require(os.getpid()==self._pid and all(a is c for a,c in zip(self._initial,(self.stage,self.stage.journal,self.stage.ledger,self.stage.owner,self.stage.guard))),'publication_owner_changed')
        self.stage._check();require(_private(self.root,directory=True)[1]==self._root_identity,'publication_root_changed')
        require(all(_private(path)[1]==identity for path,identity in self._files.items()),'publication_inode_changed')
        require(encoded(dict(protocol='ltds-native-bundle-publication-v1',policy=asdict(self.policy),stageBinding=self.stage._binding,
            rootIdentity=self._root_identity,files={str(k):v for k,v in self._files.items()}))==self._binding,'publication_policy_changed')

    @contextmanager
    def _operation(self):
        self._check();require(self._mutex.acquire(blocking=False),'publication_busy')
        fd=None
        try:
            import fcntl
            fd=os.open(self.lock_path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
            require((os.fstat(fd).st_dev,os.fstat(fd).st_ino)==self._files[self.lock_path],'publication_lock_changed')
            try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:raise PublicationHeld('publication_busy') from None
            self._active=(fd,threading.get_ident())
            yield;self._check()
        finally:
            self._active=None
            if fd is not None:os.close(fd)
            self._mutex.release()

    def _check_active(self):
        self._check();require(self._active is not None and self._active[1]==threading.get_ident() and self._mutex.locked(),'publication_writer_lease_required')
        fd=self._active[0];require((os.fstat(fd).st_dev,os.fstat(fd).st_ino)==self._files[self.lock_path],'publication_writer_lock_changed')

    @staticmethod
    def _schema(db):
        from .reference_odm_stage import NativeODMStage
        with closing(sqlite3.connect(':memory:')) as expected:
            for sql in SCHEMA:expected.execute(sql)
            require(NativeODMStage._inventory(db)==NativeODMStage._inventory(expected),'publication_schema_changed')

    @contextmanager
    def _db(self,*,readonly=False):
        self._check()
        if self.path.stat().st_size:
            with _readonly_sqlite(self.path):pass
        db=sqlite3.connect(self.path.as_uri()+'?mode=ro',uri=True) if readonly else sqlite3.connect(self.path)
        db.row_factory=sqlite3.Row
        try:
            if not readonly:db.execute('PRAGMA synchronous=FULL')
            db.execute('BEGIN' if readonly else 'BEGIN IMMEDIATE')
            if getattr(self,'_initialized',False):
                self._schema(db);row=db.execute('SELECT value FROM publication_binding WHERE id=1').fetchone()
                require(row is not None and row[0]==self._binding,'publication_binding_changed')
            size=db.execute('PRAGMA page_count').fetchone()[0]*db.execute('PRAGMA page_size').fetchone()[0]
            require(2*size<=self.policy.control_budget,'publication_sql_control_budget_exceeded')
            yield db;self._check()
            if not readonly:
                # Reserve the whole shared DB plus its rollback copy for this
                # attempt. Conservative double accounting is intentional.
                size=db.execute('PRAGMA page_count').fetchone()[0]*db.execute('PRAGMA page_size').fetchone()[0]
                require(2*size<=self.policy.control_budget,'publication_sql_control_budget_exceeded')
            if not readonly:db.commit()
        except BaseException:
            if not readonly:db.rollback()
            raise
        finally:db.close()

    def _intent(self,job):
        with self._db(readonly=True) as db:row=db.execute('SELECT intent FROM publications WHERE job=?',(job,)).fetchone()
        require(row is not None,'unknown_publication');return _private_json(row[0])

    def _copy(self,job,number=None):
        with self._db(readonly=True) as db:
            row=db.execute('SELECT * FROM publication_copies WHERE job=? '+('AND number=?' if number else 'ORDER BY number DESC LIMIT 1'),(job,number) if number else (job,)).fetchone()
        if row is None:return None
        row=dict(row);require(row['phase'] in PHASES,'publication_phase_changed');return row

    def _phase(self,row,phase,**values):
        require(phase in PHASES and set(values)<={'partial_identity','writer_intent','receipt'},'closed_publication_checkpoint_required')
        with self._db() as db:
            db.execute('UPDATE publication_copies SET phase=?'+''.join(','+key+'=?' for key in values)+' WHERE job=? AND number=?',
                [phase,*[encoded(value) for value in values.values()],row['job'],row['number']])
            current=db.execute('SELECT plan,partial_identity,writer_intent,receipt FROM publication_copies WHERE job=? AND number=?',(row['job'],row['number'])).fetchone()
            require(sum(len(value.encode()) for value in current if value is not None)<=self.policy.control_budget,'publication_metadata_budget_exceeded')

    def _live(self,job,*,mutation):
        intent=self._intent(job);stage=self.stage
        issued,plan,prior_intent,prior_proof=stage._intent(intent);parent=issued['parentBinding']
        stage._guard(parent,mutation);stage._local(parent);stage._host();stage._provenance(intent,plan)
        row=stage._row(job);require(row is not None and row['intent']==encoded(intent),'native_attempt_changed')
        evidence=stage.read_evidence(intent);require(evidence is not None and evidence['exitCode']==0,'qualified_exited_native_required')
        observed=stage._inspect(row)
        require(observed['State']['Status']=='exited' and observed['State']['Running'] is False and observed['State']['ExitCode']==0,'native_container_not_exited_successfully')
        require(evidence['containerId']==observed['Id'],'native_container_changed')
        with self._db(readonly=True) as db:source=_private_json(db.execute('SELECT source FROM publications WHERE job=?',(job,)).fetchone()[0])
        require(evidence['evidenceSha256']==source['evidenceSha256'],'native_evidence_changed')
        prep=_private_json(row['preparation']);proof=Path(prep['output'])/'proof.json'
        _file(proof,stage.policy.native_proof_budget,expected_bytes=source['proofBytes'],expected_sha=source['proofSha256'])
        _file(Path(prep['control']),stage.policy.control_budget,expected_bytes=source['controlBytes'],expected_sha=source['controlSha256'])
        return source

    def _qualify(self,intent,*,include_admission=False):
        stage=self.stage;intent,plan,prior_intent,prior_proof=stage._intent(intent);parent=intent['parentBinding'];job=parent['jobId']
        stage._guard(parent,False);stage._local(parent);stage._host();stage._provenance(intent,plan);stage.prior.verify(prior_intent,prior_proof)
        row=stage._row(job);require(row is not None and row['intent']==encoded(intent),'native_attempt_changed')
        evidence=stage.read_evidence(intent);require(evidence is not None and evidence['exitCode']==0,'qualified_exited_native_required')
        observed=stage._inspect(row);require(observed['State']['Status']=='exited' and observed['State']['Running'] is False and observed['State']['ExitCode']==0 and evidence['containerId']==observed['Id'],'native_container_not_exited_successfully')
        prep=_private_json(row['preparation']);obj=intent['stage']['inputObjects'][0]
        _file(Path(prep['input']),obj['byteSize'],expected_bytes=obj['byteSize'],expected_sha=obj['sha256'])
        supplied=stage.inputs.resolve(intent);require(type(supplied) is dict and set(supplied)=={obj['id']},'original_native_input_changed')
        stage.ledger._owned_path(parent['reservationId'],supplied[obj['id']])
        directory=stage.ledger.root/'jobs'/parent['reservationId']/('tree-stage-'+intent['attemptId'])
        require(prep['input']==str(stage.ledger.root/supplied[obj['id']]) and prep['output']==str(directory)
            and prep['control']==str(directory.parent/('tree-'+intent['attemptId']+'.json')),'native_preparation_changed')
        odm_inputs=stage.prior.inputs.resolve(prior_intent);originals=prior_intent['stage']['inputObjects']
        require(type(odm_inputs) is dict and set(odm_inputs)=={v['id'] for v in originals},'original_parent_inputs_changed')
        for original in originals:
            relative=odm_inputs[original['id']];stage.ledger._owned_path(parent['reservationId'],relative)
            _file(stage.ledger.root/relative,original['byteSize'],expected_bytes=original['byteSize'],expected_sha=original['sha256'])
        require(sum(v['byteSize'] for v in originals)+obj['byteSize']==parent['inputBytes'],'whole_original_input_accounting_required')
        control_path=Path(prep['control']);_private(control_path);require(control_path.stat().st_size<=stage.policy.control_budget,'native_control_changed')
        control=_private_json(control_path.read_text());job_fields={k:parent[k] for k in ('jobId','generation','environment','manifestSha256','pipelineDigest')}
        require(exact(control,dict(job=job_fields,reservationId=parent['reservationId'],inputObject=obj,inputRelativePath=supplied[obj['id']],
            outputRelativePath=str(directory.relative_to(stage.ledger.root)),provenance=intent['provenance'],allowSynthetic=False,
            policy=asdict(stage.policy.admission),outputBudget=stage.policy.max_raw_bytes,sourceSha256=stage.policy.config.source_sha256,
            engineVersion=stage.policy.config.engine_version,timeoutSeconds=stage.policy.config.timeout_seconds,maxFiles=stage.policy.max_files)),'pinned_native_control_changed')
        proof_path=Path(prep['output'])/'proof.json';proof_bytes,proof_sha=_file(proof_path,stage.policy.native_proof_budget)
        proof=_private_json(proof_path.read_text());p._closed(proof,('schemaVersion','job','engineVersion','admission','objects'),'native bootstrap proof')
        require(type(proof['schemaVersion']) is int and proof['schemaVersion']==1 and exact(proof['job'],job_fields) and proof['engineVersion']==stage.policy.config.engine_version,'native_bootstrap_identity_changed')
        admission=proof['admission'];fields=('admissionVersion','jobId','generation','environment','objectId','objectSha256','byteSize','sourceKind','provenanceReceiptId','genuineAcquisition','pointCount','groundPointCount','groundCoverage','crs','crsWktSha256','pointCountAssurance','verticalUnits','verticalDatum','verticalDatumAssurance','rasterGridCells')
        p._closed(admission,fields,'native admission');receipt=intent['provenance'];policy=stage.policy.admission
        expected=dict(admissionVersion=p.TREEDAR_ADMISSION_VERSION,jobId=job,generation=parent['generation'],environment=parent['environment'],objectId=obj['id'],objectSha256=obj['sha256'],byteSize=obj['byteSize'],sourceKind='lidar',provenanceReceiptId=receipt['receiptId'],genuineAcquisition=True,verticalUnits='metre',verticalDatum=receipt['verticalDatum'],verticalDatumAssurance='authenticated_declaration_no_conversion')
        require(all(exact(admission[k],v) for k,v in expected.items()) and receipt['sourceKind']=='lidar','genuine_original_native_admission_required')
        for key in ('pointCount','groundPointCount','rasterGridCells'):p._integer(admission[key],key)
        require(policy.min_ground_points<=admission['groundPointCount']<=admission['pointCount']<=policy.max_points and admission['rasterGridCells']<=policy.max_grid_cells,'native_admission_policy_changed')
        coverage=admission['groundCoverage'];require(type(coverage) in (int,float) and math.isfinite(coverage) and policy.min_ground_coverage<=coverage<=1,'native_ground_coverage_changed')
        p._hash(admission['crsWktSha256'],'native CRS');require(type(admission['crs']) is str and 1<=len(admission['crs'])<=80 and admission['pointCountAssurance'] in ('authenticated_source_count','uncompressed_byte_layout_and_stream'),'native_crs_or_count_assurance_changed')
        if 'pointCount' in receipt:require(admission['pointCount']==receipt['pointCount'],'authenticated_native_count_changed')
        objects=proof['objects'];require(type(objects) is list and 12<=len(objects)<=min(stage.policy.max_files,self.policy.limits.maxFiles),'bounded_native_proof_required')
        files=[];names=set();total=0
        for value in objects:
            p._closed(value,('id','role','byteSize','sha256','relativePath'),'native proof object');relative=value['relativePath']
            require(type(relative) is str and relative.startswith('run/'),'native_object_path_changed');name=relative[4:]
            require(name in b.NATIVE_FILES and name not in names and value['role']==b.NATIVE_FILES[name][0]
                and value['id']=='treedar-'+hashlib.sha256(relative.encode()).hexdigest()[:32],'native_object_semantics_changed')
            names.add(name);p._integer(value['byteSize'],'native bytes');p._hash(value['sha256'],'native SHA');total+=value['byteSize']
            require(total<=self.policy.limits.maxRawBytes,'native_raw_budget_exceeded')
            _file(Path(prep['output'])/relative,value['byteSize'],expected_bytes=value['byteSize'],expected_sha=value['sha256'])
            files.append(dict(nativePath=name,role=value['role'],format=b.NATIVE_FILES[name][1],byteSize=value['byteSize'],sha256=value['sha256']))
        require(b.REQUIRED_FILES<=names,'native_required_outputs_missing')
        expected_evidence=[dict(relativePath='proof.json',byteSize=proof_bytes,sha256=proof_sha)]+[dict(relativePath='run/'+v['nativePath'],byteSize=v['byteSize'],sha256=v['sha256']) for v in files]
        require(exact(sorted(expected_evidence,key=lambda v:v['relativePath']),evidence['objects']),'native_evidence_inventory_changed')
        # Exact closure, bounded before materialization. Current pinned bootstrap
        # emits flat run files; no additional directory is accepted here.
        for directory,allowed in ((Path(prep['output']),{'proof.json','run'}),(Path(prep['output'])/'run',names)):
            seen=set()
            with os.scandir(directory) as entries:
                for entry in entries:
                    require(entry.name in allowed and entry.name not in seen,'unlisted_native_output');seen.add(entry.name)
            require(seen==allowed,'native_output_missing')
        require(b._directory(str(Path(prep['output'])/'run')) is not None,'private_native_raw_required')
        binding=dict(jobId=job,generation=parent['generation'],environment=parent['environment'],inputManifestSha256=parent['manifestSha256'],pipelineDigest=parent['pipelineDigest'],sourceLidar=dict(objectId=obj['id'],byteSize=obj['byteSize'],sha256=obj['sha256'],provenanceReceiptId=receipt['receiptId']))
        manifest=dict(schemaVersion=1,format=b.FORMAT,binding=binding,engineVersion=stage.policy.config.engine_version,files=sorted(files,key=lambda v:v['nativePath']),rawBytes=total)
        manifest['manifestSha256']=b.manifest_sha256(manifest);manifest=b.validate_manifest(manifest,dict(binding=binding,engineVersion=manifest['engineVersion']),self.policy.limits)
        reservation=stage._reservation(parent)
        require(reservation['allocated']>=prep['baseline']+control_path.stat().st_size+proof_bytes+total+len(p.canonical_json(evidence)),'native_allocation_not_retained')
        control_bytes,control_sha=_file(control_path,stage.policy.control_budget)
        source=dict(evidenceSha256=evidence['evidenceSha256'],proofSha256=proof_sha,proofBytes=proof_bytes,controlBytes=control_bytes,controlSha256=control_sha,rawRoot=str(Path(prep['output'])/'run'),manifest=manifest)
        return (source,admission) if include_admission else source

    def begin(self,intent):
        with self._operation():
            source=self._qualify(intent);job=intent['parentBinding']['jobId'];self.stage._guard(intent['parentBinding'],True)
            reservation=self.stage._reservation(intent['parentBinding']);limits=self.policy.limits
            require(limits.maxArchiveBytes+limits.maxManifestBytes+limits.maxControlProofBytes+self.policy.control_budget<=reservation['remaining'],'retained_copy_capacity_insufficient')
            with self._db(readonly=True) as db:require(not db.execute('SELECT 1 FROM publications WHERE job=?',(job,)).fetchone(),'publication_never_replayed')
            control_base=reservation['allocated']+self.policy.control_budget
            self.stage.ledger.record_allocated(intent['parentBinding']['reservationId'],control_base)
            with self._db() as db:
                require(not db.execute('SELECT 1 FROM publications WHERE job=?',(job,)).fetchone(),'publication_never_replayed')
                db.execute('INSERT INTO publications VALUES(?,?,?,?)',(job,encoded(intent),encoded(source),control_base))
            return self._new_copy(job)

    def _new_copy(self,job):
        intent=self._intent(job);source=self._qualify(intent);require(exact(source,self._live(job,mutation=True)),'native_source_changed')
        self._retained(job)
        previous=self._copy(job);number=previous['number']+1 if previous else 1;require(number<=self.policy.attempt_cap,'copy_attempt_cap_reached')
        parent=intent['parentBinding'];reservation=self.stage._reservation(parent);limits=self.policy.limits
        control_cost=self.policy.control_budget if previous else 0
        if not previous:
            with self._db(readonly=True) as db:control_base=db.execute('SELECT control_base FROM publications WHERE job=?',(job,)).fetchone()[0]
            require(type(control_base) is int and reservation['allocated']>=control_base,'publication_control_allocation_lost')
        require(limits.maxArchiveBytes+limits.maxManifestBytes+limits.maxControlProofBytes+control_cost<=reservation['remaining'],'retained_copy_capacity_insufficient')
        publication_id='bundle-'+intent['attemptId']+'-'+str(number);base=reservation['allocated']+control_cost
        owner=self.stage.ledger.root/'jobs'/parent['reservationId'];_private(owner,directory=True)
        output=owner/(publication_id+'-output');state=owner/(publication_id+'-state')
        for path in (output,state):self.stage.ledger._owned_path(parent['reservationId'],str(path.relative_to(self.stage.ledger.root)))
        owner_identity=b._directory(str(owner))
        for path in (output,state):require(self._absent(path),'copy_paths_must_initially_be_absent')
        policy=b.BundlePolicy(source['rawRoot'],str(output),str(state),publication_id,parent['reservationId'],base,reservation['remaining']-control_cost,p.digest(source),limits)
        authorization=dict(owner=dict(path=str(owner),device=str(owner_identity[0]),inode=str(owner_identity[1])),initiallyAbsent=[str(output),str(state)])
        plan=dict(policy=asdict(policy),source=source,expected=dict(binding=source['manifest']['binding'],engineVersion=source['manifest']['engineVersion']),preparationAuthorization=authorization)
        require(len(p.canonical_json(plan))<self.policy.control_budget,'publication_plan_budget_exceeded')
        with self._db() as db:db.execute('INSERT INTO publication_copies VALUES(?,?,?,?,NULL,NULL,NULL)',(job,number,encoded(plan),'planned'))
        row=self._copy(job);self.stage.ledger.record_allocated(parent['reservationId'],base)
        self._live(job,mutation=True);self._paths(row)
        for directory in (output,state):directory.mkdir(mode=0o700);_sync_directory(directory);_sync_directory(directory.parent)
        fd=os.open(output/(publication_id+'.zip.partial'),os.O_RDWR|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
        try:
            info=os.fstat(fd);identity=dict(device=str(info.st_dev),inode=str(info.st_ino))
            os.fsync(fd);_sync_directory(output)
            self._phase(row,'owned',partial_identity=identity)
            writer=b.BundlePartialWriter(policy,plan['expected'],guard=_Guard(self,job,number,policy,plan['expected']))
            writer_intent=writer.intent_for(fd);_write_new(writer.intent_path,writer_intent);self._phase(row,'streaming',writer_intent=writer_intent)
            receipt=writer.write_to(fd);self._phase(row,'ready',receipt=receipt)
        finally:
            os.close(fd);self._charge_copy(self._copy(job))
        return self._publish(self._copy(job))

    def _paths(self,row):
        plan=_private_json(row['plan']);value=dict(plan['policy']);value['limits']=b.BundleLimits.from_mapping(value['limits']);policy=b.BundlePolicy(**value)
        require(exact(asdict(policy.limits),asdict(self.policy.limits)) and plan['expected']==dict(binding=plan['source']['manifest']['binding'],engineVersion=plan['source']['manifest']['engineVersion']),'copy_configuration_changed')
        b.validate_manifest(plan['source']['manifest'],plan['expected'],policy.limits)
        intent=self._intent(row['job']);parent=intent['parentBinding'];publication_id='bundle-'+intent['attemptId']+'-'+str(row['number'])
        owner=self.stage.ledger.root/'jobs'/parent['reservationId']
        owner_identity=b._directory(str(owner));authorization=plan.get('preparationAuthorization')
        require(exact(authorization,dict(owner=dict(path=str(owner),device=str(owner_identity[0]),inode=str(owner_identity[1])),
            initiallyAbsent=[str(owner/(publication_id+'-output')),str(owner/(publication_id+'-state'))])),'copy_preparation_authorization_unavailable')
        require(type(row['number']) is int and 1<=row['number']<=self.policy.attempt_cap and policy.publication_id==publication_id
            and policy.reservation_id==parent['reservationId'] and policy.output_root==str(owner/(publication_id+'-output'))
            and policy.state_root==str(owner/(publication_id+'-state')) and policy.raw_root==plan['source']['rawRoot']
            and policy.source_stage_proof_sha256==p.digest(plan['source'])
            and policy.allocation_base+policy.remaining_reserved_bytes==parent['inputBytes']+parent['outputBudget'],'copy_plan_binding_changed')
        return plan,policy,Path(policy.output_root)/(policy.publication_id+'.zip.partial'),Path(policy.output_root)/(policy.publication_id+'.zip')

    @staticmethod
    def _absent(path):
        try:path.lstat()
        except FileNotFoundError:return True
        return False

    def _preparation_census(self,row,*,sync=False):
        """Retain only: unknown pre-streaming bytes can never become a result."""
        plan,policy,partial,final=self._paths(row);observations=[];total=0
        owner_before=Path(policy.output_root).parent.lstat()
        require(row['writer_intent'] is None,'preparation_already_streaming')
        original_phase=row['phase']
        if original_phase=='abandoned_preparation':original_phase=self._abandoned_receipt(row)['originalPhase']
        require(original_phase in ('planned','owned'),'early_preparation_phase_required')
        require(original_phase!='owned' or row['partial_identity'] is not None,'owned_partial_identity_missing')
        partial_seen=False
        for root,allowed in ((Path(policy.output_root),partial.name),(Path(policy.state_root),policy.publication_id+'.intent.json')):
            if self._absent(root):
                observations.append(dict(path=str(root),present=False));continue
            root_identity=b._directory(str(root));root_before=root.lstat()
            root_observation=dict(path=str(root),present=True,device=str(root_identity[0]),inode=str(root_identity[1]),files=[])
            with os.scandir(root) as entries:
                for entry in entries:
                    require(entry.name==allowed and not root_observation['files'],'unknown_preparation_artifact')
                    path=root/entry.name;before=b._regular(path,private=True)
                    is_partial=path==partial
                    require(is_partial or original_phase=='owned','intent_without_owned_checkpoint')
                    maximum=0 if is_partial else policy.limits.maxManifestBytes+policy.limits.maxControlProofBytes
                    require(before.st_size<=maximum,'early_preparation_bytes_exceeded')
                    identity=dict(device=str(before.st_dev),inode=str(before.st_ino))
                    if is_partial:
                        partial_seen=True
                        require(row['partial_identity'] is None or exact(identity,_private_json(row['partial_identity'])),'publication_inode_changed')
                    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
                    try:
                        require(b._snapshot(before)==b._snapshot(os.fstat(fd)),'preparation_artifact_changed')
                        count=0;digest=hashlib.sha256()
                        while True:
                            block=os.read(fd,min(1024**2,before.st_size-count+1,maximum-count+1))
                            if not block:break
                            count+=len(block);require(count<=before.st_size and count<=maximum,'preparation_artifact_changed');digest.update(block)
                        require(count==before.st_size and b._snapshot(before)==b._snapshot(os.fstat(fd))==b._snapshot(path.lstat()),'preparation_artifact_changed')
                        if sync:os.fsync(fd)
                    finally:os.close(fd)
                    total+=count;root_observation['files'].append(dict(name=entry.name,**identity,byteSize=count,sha256=digest.hexdigest()))
            require(b._directory(str(root))==root_identity and b._snapshot(root_before)==b._snapshot(root.lstat()),'preparation_directory_changed')
            if sync:_sync_directory(root)
            observations.append(root_observation)
        require(original_phase!='owned' or partial_seen,'owned_partial_disappeared')
        require(b._snapshot(owner_before)==b._snapshot(Path(policy.output_root).parent.lstat()),'preparation_parent_changed_during_census')
        if sync:_sync_directory(Path(policy.output_root).parent)
        value=dict(state='abandoned_preparation',originalPhase=original_phase,authorizationSha256=p.digest(plan['preparationAuthorization']),
            observations=observations,retainedBytes=total,chargedAllowance=policy.limits.maxArchiveBytes+policy.limits.maxManifestBytes+policy.limits.maxControlProofBytes)
        value['receiptSha256']=p.digest(value);return value

    @staticmethod
    def _abandoned_receipt(row):
        receipt=_private_json(row['receipt'])
        p._closed(receipt,('state','originalPhase','authorizationSha256','observations','retainedBytes','chargedAllowance','receiptSha256'),'abandoned preparation receipt')
        require(receipt['state']=='abandoned_preparation' and receipt['receiptSha256']==p.digest({k:v for k,v in receipt.items() if k!='receiptSha256'}),'abandoned_preparation_receipt_changed')
        return receipt

    def _abandon_preparation(self,row):
        self._check_active();self._live(row['job'],mutation=True)
        receipt=self._preparation_census(row,sync=True);_,policy,_,_=self._paths(row)
        current=self.stage._reservation(self._intent(row['job'])['parentBinding'])
        maximum=policy.allocation_base+receipt['chargedAllowance']
        self.stage.ledger.record_allocated(policy.reservation_id,max(current['allocated'],maximum))
        self._phase(row,'abandoned_preparation',receipt=receipt)

    def _census(self,row):
        plan,policy,partial,final=self._paths(row);size=0;seen=set()
        for root,allowed in ((Path(policy.output_root),{partial.name,final.name}),(Path(policy.state_root),{policy.publication_id+'.intent.json'})):
            identity=b._directory(str(root))
            if row['writer_intent']:
                original=_private_json(row['writer_intent'])['roots'][str(root)]
                require(exact(original,dict(device=str(identity[0]),inode=str(identity[1]))),'publication_directory_identity_changed')
            with os.scandir(root) as entries:
                for entry in entries:
                    require(entry.name in allowed,'unknown_publication_artifact');path=root/entry.name;info=path.lstat()
                    require(stat.S_ISREG(info.st_mode) and info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o600 and info.st_nlink in (1,2),'publication_artifact_changed')
                    identity=(info.st_dev,info.st_ino)
                    if root==Path(policy.output_root):
                        require(row['partial_identity'] is not None and exact(dict(device=str(info.st_dev),inode=str(info.st_ino)),_private_json(row['partial_identity'])),'publication_inode_changed')
                        require(info.st_size<=policy.limits.maxArchiveBytes,'publication_archive_budget_exceeded')
                        if info.st_nlink==2:
                            require(row['phase'] in ('link_pending','linked','unlink_pending') and partial.exists() and final.exists()
                                and partial.stat().st_ino==final.stat().st_ino,'unowned_publication_hardlink')
                        if path==final:require(row['phase'] in ('link_pending','linked','unlink_pending','published'),'final_without_link_intent')
                    else:require(info.st_nlink==1 and info.st_size<=policy.limits.maxManifestBytes+policy.limits.maxControlProofBytes,'publication_intent_budget_exceeded')
                    if identity not in seen:size+=info.st_size;seen.add(identity)
        return size

    def _charge_copy(self,row):
        _,policy,_,_=self._paths(row);size=self._census(row);parent=self._intent(row['job'])['parentBinding'];current=self.stage._reservation(parent)
        require(size<=policy.remaining_reserved_bytes,'retained_copy_capacity_exceeded')
        self.stage.ledger.record_allocated(parent['reservationId'],max(current['allocated'],policy.allocation_base+size))

    def _retained(self,job):
        with self._db(readonly=True) as db:rows=[dict(row) for row in db.execute('SELECT * FROM publication_copies WHERE job=? ORDER BY number',(job,))]
        require(len(rows)<=self.policy.attempt_cap and [row['number'] for row in rows]==list(range(1,len(rows)+1)),'copy_history_changed')
        for row in rows:
            if row['phase']=='abandoned_preparation':
                receipt=self._abandoned_receipt(row);require(exact(self._preparation_census(row),receipt),'abandoned_preparation_census_changed')
                _,policy,_,_=self._paths(row);current=self.stage._reservation(self._intent(job)['parentBinding'])
                self.stage.ledger.record_allocated(policy.reservation_id,max(current['allocated'],policy.allocation_base+receipt['chargedAllowance']))
                continue
            self._charge_copy(row)
            if row['receipt']:
                receipt=_private_json(row['receipt'])
                if receipt.get('state')=='retained_incomplete':
                    _,policy,partial,final=self._paths(row);require(not final.exists(),'retained_partial_became_final')
                    _file(partial,policy.limits.maxArchiveBytes,expected_bytes=receipt['archive']['byteSize'],expected_sha=receipt['archive']['sha256'])

    def _archive(self,row):
        plan,policy,partial,final=self._paths(row);self._census(row);path=final if final.exists() else partial
        require(path.exists(),'publication_archive_missing');info=path.lstat();fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK)
        try:
            require((info.st_dev,info.st_ino)==(os.fstat(fd).st_dev,os.fstat(fd).st_ino),'publication_inode_changed')
            if row['writer_intent'] is None:raise PublicationHeld('copy_intent_unknown')
            intent_path=Path(policy.state_root)/(policy.publication_id+'.intent.json');_private(intent_path)
            require(exact(_private_json(intent_path.read_text()),_private_json(row['writer_intent'])),'publication_intent_changed')
            # Verification does not invoke a writer or repeat its mutation.
            verifier=object.__new__(b.BundlePartialWriter);verifier._manifest=plan['source']['manifest']
            expected=[(b.MANIFEST_NAME,len(p.canonical_json(verifier._manifest)))]+[('artifacts/'+v['nativePath'],v['byteSize']) for v in verifier._manifest['files']]
            if not self._zip_census(fd,info.st_size,[name for name,_ in expected]):
                prefix=os.pread(fd,4,0)
                require(path==partial and row['phase']=='streaming' and (not info.st_size or prefix==b'PK\x03\x04'),'corrupt_publication_archive')
                return None
            try:
                with os.fdopen(os.dup(fd),'rb') as stream,zipfile.ZipFile(stream) as archive:
                    infos=archive.infolist()
            except zipfile.BadZipFile:
                prefix=os.pread(fd,4,0)
                require(path==partial and row['phase']=='streaming' and (not info.st_size or prefix==b'PK\x03\x04'),'corrupt_publication_archive')
                return None
            require(len(infos)==len(expected),'corrupt_publication_inventory')
            for value,(name,size) in zip(infos,expected):
                require(value.filename==name and value.file_size==size and value.compress_size==size and value.compress_type==zipfile.ZIP_STORED
                    and not value.flag_bits&1 and value.date_time==(1980,1,1,0,0,0) and value.external_attr>>16==stat.S_IFREG|0o600,'corrupt_publication_metadata')
            try:verifier._verify_archive(fd,p.canonical_json(verifier._manifest))
            except Exception:raise PublicationHeld('corrupt_publication_archive') from None
            sha=hashlib.sha256();offset=0
            while offset<=info.st_size:
                block=os.pread(fd,min(1024**2,info.st_size-offset+1),offset)
                if not block:break
                offset+=len(block);require(offset<=info.st_size,'publication_archive_changed');sha.update(block)
            require(offset==info.st_size and b._snapshot(info)==b._snapshot(os.fstat(fd))==b._snapshot(path.lstat()),'publication_archive_changed')
            os.fsync(fd)
            result=dict(state='local_native_bundle_published',archive=dict(byteSize=offset,sha256=sha.hexdigest()),manifestSha256=verifier._manifest['manifestSha256'],
                binding=plan['expected']['binding'],engineVersion=plan['expected']['engineVersion'],nativeFiles=verifier._manifest['files'],sourceStageProofSha256=policy.source_stage_proof_sha256)
            if row['receipt']:
                old=_private_json(row['receipt']);require(exact(old['archive'],result['archive']) and old['manifestSha256']==result['manifestSha256'],'published_receipt_changed')
            return result
        finally:os.close(fd)

    @staticmethod
    def _zip_census(fd,size,names):
        """Bound central-directory allocation before ZipFile materializes it."""
        if size<22:return False
        tail=os.pread(fd,22,size-22)
        if tail[:4]!=b'PK\x05\x06':return False
        _,disk,central_disk,count_disk,count,central_size,offset,comment=struct.unpack('<4s4H2LH',tail)
        require(disk==central_disk==comment==0,'corrupt_publication_directory')
        end=size-22
        if count==65535 or central_size==0xffffffff or offset==0xffffffff:
            require(size>=98,'corrupt_publication_zip64')
            locator=os.pread(fd,20,size-42);signature,disk,record_offset,disks=struct.unpack('<4sLQL',locator)
            require(signature==b'PK\x06\x07' and disk==0 and disks==1 and record_offset==size-98,'corrupt_publication_zip64')
            record=os.pread(fd,56,record_offset)
            signature,length,_,_,disk,central_disk,count_disk,count,central_size,offset=struct.unpack('<4sQ2H2L4Q',record)
            require(signature==b'PK\x06\x06' and length==44 and disk==central_disk==0,'corrupt_publication_zip64');end=record_offset
        require(count==count_disk==len(names) and central_size<=sum(46+len(name.encode())+28 for name in names)
            and offset+central_size==end and offset>=0,'bounded_publication_directory_required')
        cursor=offset
        for name in names:
            header=os.pread(fd,46,cursor);require(len(header)==46 and header[:4]==b'PK\x01\x02','corrupt_publication_directory')
            name_size,extra_size,comment_size=struct.unpack_from('<3H',header,28)
            require(name_size==len(name.encode()) and extra_size<=28 and comment_size==0,'bounded_publication_member_metadata_required')
            require(os.pread(fd,name_size,cursor+46)==name.encode(),'corrupt_publication_member_name')
            cursor+=46+name_size+extra_size
        require(cursor==end,'corrupt_publication_directory');return True

    def _publish(self,row):
        job=row['job'];source=self._qualify(self._intent(job));require(exact(source,self._live(job,mutation=True)),'native_source_changed')
        plan,policy,partial,final=self._paths(row);receipt=self._archive(row);require(receipt is not None,'incomplete_copy_requires_recovery')
        self._charge_copy(row)
        if row['phase'] in ('streaming','ready'):
            self._phase(row,'link_pending',receipt=receipt);row=self._copy(job)
        if row['phase']=='link_pending':
            self._live(job,mutation=True)
            if not final.exists():require(partial.exists(),'partial_missing_before_link');os.link(partial,final,follow_symlinks=False)
            self._census(row);_sync_directory(final.parent);self._phase(row,'linked',receipt=receipt);row=self._copy(job)
        if row['phase']=='linked':self._phase(row,'unlink_pending',receipt=receipt);row=self._copy(job)
        if row['phase']=='unlink_pending':
            self._live(job,mutation=True);self._census(row);require(final.exists(),'linked_final_missing')
            if partial.exists():os.unlink(partial)
            _sync_directory(final.parent);self._phase(row,'published',receipt=receipt);row=self._copy(job)
        require(row['phase']=='published' and final.exists() and not partial.exists() and final.stat().st_nlink==1,'publication_not_complete')
        return self.read_receipt(job)

    def reconcile(self,job):
        with self._operation():
            source=self._qualify(self._intent(job));require(exact(source,self._live(job,mutation=False)),'native_source_changed')
            row=self._copy(job)
            if row is None:return self._new_copy(job)
            if row['phase'] in ('planned','owned'):
                self._abandon_preparation(row);return self._new_copy(job)
            if row['phase']=='abandoned_preparation':return self._new_copy(job)
            self._retained(job)
            # The real daemon and publication flock are both held. A previous
            # process cannot still be writing when a new copy is authorized.
            if self._archive(row) is None:
                self._charge_copy(row);_,policy,partial,_=self._paths(row)
                size,sha=_file(partial,policy.limits.maxArchiveBytes)
                self._phase(row,'streaming',receipt=dict(state='retained_incomplete',archive=dict(byteSize=size,sha256=sha)))
                return self._new_copy(job)
            if row['phase']=='published':return self.read_receipt(job)
            return self._publish(row)

    def preview_outer_manifest(self,job):
        """Fresh local-only observation; never a transport or custody receipt."""
        from .reference_treedar_outer_candidate import make_preview
        with self._operation():
            intent=self._intent(job)
            source,admission=self._qualify(intent,include_admission=True)
            require(exact(source,self._live(job,mutation=False)),'native_source_changed')
            row=self._copy(job)
            require(row is not None and row['phase']=='published','published_native_bundle_required')
            archive=self._archive(row)
            require(archive is not None and exact(archive,_private_json(row['receipt'])),'published_receipt_changed')
            result=make_preview(source,admission,archive,self.stage.policy.config,self.policy.limits)
            require(exact(source,self._live(job,mutation=False)),'native_source_changed')
            return result

    def preview_spatial_candidate(self,job,grant,spatial):
        """Local evidence correlation only; never a custody or execution grant."""
        from . import reference_treedar_spatial_candidate as s
        require(s.QUALIFIED is True and type(spatial) is s.LocalSpatialReceipt
            and spatial.stage is self.stage,'exact_qualified_spatial_observer_required')
        intent=self._intent(job['jobId'])
        observation=spatial.qualify(intent,job,grant)
        preview=self.preview_outer_manifest(job['jobId'])
        observation_after=spatial.qualify(intent,job,grant)
        require(exact(observation,observation_after),'spatial_observation_changed')
        record=observation['record'];admission=dict(record['admission'])
        wkt=admission.pop('crsWkt',None)
        require(exact(admission,preview['sourceEvidence']['admission']),'native_spatial_admission_changed')
        require(type(wkt) is str and hashlib.sha256(wkt.encode('utf-8')).hexdigest()==preview['spatial']['crsWktSha256'],
            'native_spatial_wkt_changed')
        preview['spatial']['wkt']=wkt
        preview['spatial']['definitionStatus']='local_revalidated_not_remote_authority'
        preview['spatial']['localObservationSha256']=observation['proofSha256']
        preview['format']='ltds-treedar-spatial-outer-preview-v1'
        return preview

    def read_receipt(self,job):
        """Historical SQL-only read; no authority, Docker or file verification."""
        row=self._copy(job)
        if row is None:return None
        receipt=_private_json(row['receipt']) if row['receipt'] else None
        if row['phase']=='abandoned_preparation':receipt=self._abandoned_receipt(row)
        if row['phase']=='published':
            p._closed(receipt,('state','archive','manifestSha256','binding','engineVersion','nativeFiles','sourceStageProofSha256'),'local publication receipt')
            plan,policy,_,_=self._paths(row);manifest=plan['source']['manifest']
            require(receipt['state']=='local_native_bundle_published' and exact(receipt['binding'],plan['expected']['binding'])
                and receipt['manifestSha256']==manifest['manifestSha256'] and exact(receipt['nativeFiles'],manifest['files'])
                and receipt['engineVersion']==plan['expected']['engineVersion'] and receipt['sourceStageProofSha256']==policy.source_stage_proof_sha256,'publication_receipt_binding_changed')
            p._closed(receipt['archive'],('byteSize','sha256'),'publication archive');p._integer(receipt['archive']['byteSize'],'archive bytes');p._hash(receipt['archive']['sha256'],'archive SHA')
            require(receipt['archive']['byteSize']<=policy.limits.maxArchiveBytes,'publication_archive_budget_exceeded')
        return dict(phase=row['phase'],copyNumber=row['number'],receipt=receipt,
            verification='historical_sql_only',outerTransport='unintegrated',parentSettled=False)
