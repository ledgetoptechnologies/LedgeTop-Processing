"""Deterministic review artifacts only. Not a wire or authority validator."""
import base64
import copy
import hashlib
import io
import json
from pathlib import Path
import struct
import zipfile

ROOT=Path(__file__).resolve().parent
PINS={'contract-proposal-v3.json':'23918d2263a91ca6c91501fd35bdf52fb60826c2ab6c33e06596b37688671be5',
      'metadata-reauthorization-addendum-v1.json':'502491feee2f8e52df49e87ce51e0f26a2fa3c2e0b6ed9431b8423f794358242'}
NATIVE_PIN='0b11c0b8958fbfcb170b42fe6e61a07d95b17a7140d905137f760e74d6ec8b34'
MAX=9007199254740991
PROTOCOL='ltds-reference-outer-v2-proposed'
META='ltds-original-metadata-reauthorization-v1-proposed'
TIME='2026-10-28T12:00:00.000Z'

def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(value):return hashlib.sha256(value).hexdigest()
def digest(value):return sha(canonical(value))
def seal(value,key):value=copy.deepcopy(value);value[key]=digest(value);return value
def write(path,value):
    target=ROOT/path;target.parent.mkdir(parents=True,exist_ok=True)
    target.write_bytes((json.dumps(value,sort_keys=True,indent=2,ensure_ascii=False,allow_nan=False)+'\n').encode())
def ref(name):return {'$ref':'#/$defs/'+name}
def nullable(value):return {'anyOf':[value,{'type':'null'}]}
def enum(*values):return {'enum':list(values)}
def const(value):return {'const':value}
def array(value,minimum=0,maximum=10000):return {'type':'array','items':value,'minItems':minimum,'maxItems':maximum,'uniqueItems':True}
def obj(properties):return {'type':'object','properties':properties,'required':list(properties),'additionalProperties':False}
def integer(minimum=0,maximum=MAX):return {'type':'integer','minimum':minimum,'maximum':maximum}

D={
 'Slug':{'type':'string','pattern':'^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$','maxLength':128},
 'WorkerId':{'type':'string','pattern':'^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$','maxLength':64},
 'UUID':{'type':'string','pattern':'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'},
 'SubmissionUUID':{'type':'string','pattern':'^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'},
 'Sha256':{'type':'string','pattern':'^[0-9a-f]{64}$'},
 'Positive':integer(1),'Natural':integer(),
 'Time':{'type':'string','pattern':'^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{3}Z$','format':'date-time'},
 'Environment':enum('production','staging'),'Engine':enum('odm','treedar'),
 'EngineVersion':{'type':'string','minLength':1,'maxLength':128,'pattern':'^[A-Za-z0-9][A-Za-z0-9_.+-]*$'},
 'Basename':{'type':'string','minLength':3,'maxLength':255,'pattern':'^[!-~]+\\.[!-~]+$','x-semanticRules':['BASENAME_SAFETY']},
 'ArchivePath':{'type':'string','minLength':1,'maxLength':1024,'pattern':'^[A-Za-z0-9_][A-Za-z0-9_./-]*$','x-semanticRules':['ARCHIVE_PATH_SAFETY']},
 'Product':enum('orthophoto','pointcloud','mesh','dsm','dtm','report','native_bundle'),
 'Purpose':enum('next_stage_admission','custody_observation','publication_authority_check'),
 'JsonValue':{'anyOf':[{'type':'null'},{'type':'boolean'},{'type':'number'},
    {'type':'string','maxLength':4096},array(ref('JsonValue'),maximum=256),
    {'type':'object','maxProperties':256,'additionalProperties':ref('JsonValue')}],
    'x-note':'Finite values only; strict lexical policy rejects unsafe keys/invalid Unicode/depth before schema evaluation.'},
 'Options':{'type':'object','maxProperties':256,'patternProperties':{'^[a-z][a-z0-9-]{0,79}$':ref('JsonValue')},'additionalProperties':False,
    'x-semanticRules':['SERVER_APPROVED_OPTIONS','OPAQUE_PIPELINE_DIGEST']}}
S,H,P,N,T,E,G,B=ref('Slug'),ref('Sha256'),ref('Positive'),ref('Natural'),ref('Time'),ref('Environment'),ref('Engine'),{'type':'boolean'}
def a(name,minimum=0):return array(ref(name),minimum)
def define(typename,**fields):D[typename]=obj(fields)

define('Binding',jobId=ref('UUID'),generation=P,environment=E,inputManifestSha256=H,pipelineDigest=H,parentClaimId=S)
define('StageBinding',binding=ref('Binding'),stageId=S,stageOrdinal=integer(1,2),engine=G,workerId=ref('WorkerId'),workerImageDigest=H,reservationId=S,reservationReceiptId=S,continuationId=S)
define('Submission',schemaVersion=const(2),protocol=const(PROTOCOL),submissionId=ref('SubmissionUUID'),inputManifest=ref('InputManifest'),pipeline=ref('Pipeline'),capabilityFingerprint=H,contractFingerprint=H)
define('Pipeline',mode=enum('odm_only','treedar_only','combined'),odm=ref('OdmRequest'),treedar=ref('TreeDarRequest'),gaussian_splat=obj({'requested':const(False)}))
define('OdmRequest',requested=B,options=ref('Options'),optionsFingerprint=H,requestedProducts=a('ProductRequest'))
define('ProductRequest',product=enum('orthophoto','pointcloud','mesh','dsm','dtm','report'),required=B)
define('TreeDarRequest',requested=B,inputObjectIds=a('Slug'),provenanceReceiptId=nullable(S),bundleContract=nullable(const('ltds-treedar-bundle-v1')),requestedProducts=array(const('native_bundle'),maximum=1))
define('InputCounts',images=N,lidarObjects=N,points=N)
define('InputObject',id=S,name=ref('Basename'),byteSize=P,sha256=H,kind=enum('image','lidar','auxiliary'),mediaType=enum('image/jpeg','image/png','image/tiff','application/vnd.las','application/vnd.laz','application/octet-stream','application/json','text/plain','text/csv'))
define('InputManifest',id=S,revision=P,sha256=H,totalBytes=P,objectCount=integer(1,10000),inputCounts=ref('InputCounts'),objects=a('InputObject',1))
define('CustodyObject',objectId=S,controlledObjectId=S,byteSize=P,sha256=H,mediaType=enum('application/vnd.las','application/vnd.laz'),acquisitionReceiptId=S,physicalReceiptId=S,genuineLasLaz=const(True),photogrammetryDerived=const(False))
define('OriginalLidarCustodyProof',schemaVersion=const(2),receiptType=const('original_lidar_custody'),receiptId=S,storageReceiptId=S,binding=ref('Binding'),issuerId=S,objects=a('CustodyObject',1),retentionGuarantee=const('until_explicit_release_ack'),retainedAt=T,validationVersion=S)
define('OriginalObjectBinding',objectId=S,byteSize=P,sha256=H,controlledObjectId=S,physicalReceiptId=S)
define('SourceMetadataReauthorization',schemaVersion=const(2),receiptType=const('original_source_reauthorization'),receiptId=S,stage=ref('StageBinding'),originalCustodyReceiptId=S,originalObjects=a('OriginalObjectBinding',1),checkedAt=T,metadataExpiresAt=T,metadataOperationId=S,payloadBudgetLedgerId=S)
D['SourceMetadataReauthorization']['x-status']='Superseded compact v3 summary; not an accepted authority message. Full addendum AuthorizedResponse is required; paired owners must decide whether to remove this redundant type.'
define('Artifact',id=S,stage=ref('StageBinding'),role=enum('odm_archive','treedar_bundle','orthophoto','pointcloud','mesh','dem','report'),format=enum('zip','geotiff','glb','pdf'),byteSize=P,sha256=H,artifactManifestSha256=nullable(H),physicalReceiptId=S,sourceObjectIds=a('Slug',1),productKeys=a('Product',1),physicalKind=const('file'))
define('ArchiveMemberReference',memberId=S,relativePath=ref('ArchivePath'),byteSize=N,sha256=H,closureManifestSha256=H,closureId=S)
define('ProductTarget',artifactId=S,artifactSha256=H,artifactByteSize=P,closureManifestSha256=nullable(H),memberRefs=a('ArchiveMemberReference'),validationReceiptId=S)
define('ProductOutcome',engine=G,product=ref('Product'),state=enum('available','not_produced','failed'),required=B,evidenceReceiptId=nullable(S),targets=a('ProductTarget'))
define('ResultManifest',schemaVersion=const(2),binding=ref('Binding'),stage=ref('StageBinding'),resultSetId=S,engineVersions={'oneOf':[obj({'odm':ref('EngineVersion')}),obj({'treedar':ref('EngineVersion')})]},objects=a('Artifact',1),products=a('ProductOutcome',1),resultManifestSha256=H)
define('ProducerCustodyReceipt',schemaVersion=const(2),receiptType=const('producer_result_custody'),receiptId=S,stage=ref('StageBinding'),issuerId=S,objectId=S,byteSize=P,sha256=H,physicalObjectId=S,reservationId=S,retainedAt=T,retainUntil=const('accepted_copy_ack_or_authorized_release'),verificationReceiptId=S)
define('AcceptedArtifact',id=S,sha256=H,byteSize=P,stageId=S,workerId=ref('WorkerId'),reservationId=S,physicalReceiptId=S,validationReceiptId=S,producerCustodyReceiptId=S)
define('PartialAcceptance',schemaVersion=const(2),receiptType=const('partial_result_acceptance'),receiptId=S,storageReceiptId=S,binding=ref('Binding'),resultSetId=S,resultManifestSha256=H,acceptedArtifacts=a('AcceptedArtifact',1),acceptedAt=T,validationVersion=S,ackOperationId=S)
define('StageResultReference',stage=ref('StageBinding'),resultSetId=S,resultManifestSha256=H,requiredArtifactIds=a('Slug',1),acceptanceReceiptIds=a('Slug',1))
define('ParentCompletion',schemaVersion=const(2),binding=ref('Binding'),mode=enum('odm_only','treedar_only','combined'),stageResults=a('StageResultReference',1),requestedProductsDigest=H,publicationReceiptIds=a('Slug',1),terminalReceiptId=S,completionSha256=H)
define('StageContinuation',schemaVersion=const(2),binding=ref('Binding'),continuationId=S,previousStageId=nullable(S),nextStage=ref('StageBinding'),previousResultManifestSha256=nullable(H),originalCustodyReceiptId=nullable(S),reauthorizationReceiptId=nullable(S),payloadBudgetLedgerId=S,state=enum('prepared','dispatched','outcome_unknown','result_observed','accepted','held'),operationId=S)
define('CleanupAck',schemaVersion=const(2),receiptType=const('producer_cleanup_ack'),receiptId=S,binding=ref('Binding'),resultManifestSha256=H,acceptanceReceiptIds=a('Slug',1),producerObjectIds=a('Slug',1),physicalReceiptIds=a('Slug',1),terminalReceiptId=S,cleanupOperationId=S,acknowledgedAt=T)
define('EptClosureEntry',relativePath=ref('ArchivePath'),byteSize=N,sha256=H)
define('PointcloudZipManifest',schemaVersion=const(1),format=const('ltds-ept-bundle-v1-proposed'),stage=ref('StageBinding'),inputManifestSha256=H,files=a('EptClosureEntry',1),rawBytes=P,manifestSha256=H)
define('OdmArchiveClosureEntry',memberId=S,relativePath=ref('ArchivePath'),byteSize=P,sha256=H,closureId=S)
define('OdmArchiveClosureManifest',schemaVersion=const(1),format=const('ltds-odm-closure-v1-proposed'),stage=ref('StageBinding'),inputManifestSha256=H,files=a('OdmArchiveClosureEntry',1),rawBytes=P,manifestSha256=H)
define('SettledStage',stage=ref('StageBinding'),engineExecutionId=S,terminalState=enum('succeeded','failed','cancelled'),engineStopReceiptId=S,resultCustodyReceiptIds=a('Slug',1),settledAt=T)
define('ExecutionSettledReceipt',schemaVersion=const(2),receiptType=const('execution_settled'),receiptId=S,binding=ref('Binding'),requestedStageIds=array(S,1,2),settledStages=array(ref('SettledStage'),1,2),producerCustodyReceiptIds=a('Slug',1),retainedDiskReservationIds=a('Slug',1),diskBudgetReceiptId=S,coordinatorSettlementOperationId=S,settledAt=T)
define('HeavyCapacityReleaseReceipt',schemaVersion=const(2),receiptType=const('heavy_capacity_release'),receiptId=S,binding=ref('Binding'),executionSettledReceiptId=S,releasedCpuGpuSlotIds=a('Slug',1),remainingDiskHoldIds=a('Slug',1),operationId=S,releasedAt=T)

define('OriginalReceiptIdentity',receiptId=S,receiptSha256=H,issuerId=S,issuerPolicySha256=H)
define('OriginalObject',objectId=S,sha256=H,byteSize=P,controlledObjectId=S,acquisitionReceiptId=S,custodyPhysicalReceiptId=S,mediaType=enum('application/vnd.las','application/vnd.laz'))
common=dict(schemaVersion=const(1),protocol=const(META))
source=dict(parentBinding=ref('Binding'),planSha256=H,stageDefinitionSha256=H,predecessorProofSha256=nullable(H),originalAcquisition=ref('OriginalReceiptIdentity'),originalCustody=ref('OriginalReceiptIdentity'),originalObjects=a('OriginalObject',1),originalTransferDigest=H,payloadBudgetLedgerId=S,payloadBudgetLedgerRevision=N,issuerPolicySha256=H)
define('ImmutableMetadataIntent',**common,receiptType=const('immutable_metadata_intent'),operationId=S,round=P,purpose=ref('Purpose'),**source,stage=ref('StageBinding'),controlBudgetLedgerId=S,controlChargeId=S,requestedAt=T,intentSha256=H)
response=dict(**common,receiptType=const('metadata_authorization_receipt'),receiptId=S,operationId=S,round=P,intentSha256=H,purpose=ref('Purpose'),**source,authenticatedActorBindingSha256=H,custodyRevision=N,revocationRevision=N,checkedAt=T,controlChargeId=S,receiptSha256=H)
define('AuthorizedResponse',**response,decision=const('authorized'),metadataExpiresAt=T)
define('DeniedResponse',**response,decision=const('denied'),metadataExpiresAt=const(None),denialCode=enum('source_revoked','source_missing','custody_unverified','actor_denied','policy_changed','binding_mismatch','budget_exhausted','issuer_unavailable'))
define('ControlBudgetPolicy',policySha256=H,maxRounds=P,maxAttemptsPerRound=P,maxIntentBytes=integer(1,4194304),maxResponseBytes=integer(1,4194304),maxHistoryRecords=P,maxHistoryBytes=P,maxConcurrentRequests=P,metadataFreshnessSeconds=integer(1,300),reservedControlDiskReceiptId=S)
define('ControlJournalEntry',schemaVersion=const(1),operationId=S,round=P,intentSha256=H,intentBytes=P,chargeId=S,state=enum('prepared','issued','outcome_unknown','authorized','denied','not_issued_reconciled','held'),attemptCount=N,responseReceiptId=nullable(S),responseSha256=nullable(H),responseBytes=N,actorVerificationReceiptId=nullable(S),policySha256=H,previousRoundReceiptSha256=nullable(H))
define('SealedStageAdmission',schemaVersion=const(1),receiptType=const('sealed_stage_metadata_admission'),receiptId=S,stage=ref('StageBinding'),planSha256=H,stageDefinitionSha256=H,intentSha256=H,metadataAuthorizationReceiptId=S,metadataAuthorizationReceiptSha256=H,originalAcquisition=ref('OriginalReceiptIdentity'),originalCustody=ref('OriginalReceiptIdentity'),authenticatedActorBindingSha256=H,custodyRevision=N,revocationRevision=N,admittedAt=T,admissionSha256=H)
for typename in ('AuthorizedResponse','SealedStageAdmission'):
    D[typename]['properties'].update(custodyRevision=P,revocationRevision=P)
define('TreeDarBundleLimits',maxArchiveBytes=P,maxRawBytes=P,maxFiles=integer(1,64),maxManifestBytes=integer(1,1048576),maxControlProofBytes=P,maxValidationDiskBytes=P)
define('ArchiveAdmissionLimits',maxArchiveBytes=P,maxRawBytes=P,maxExpandedMembers=integer(1,10000),maxExpandedBytes=P,maxManifestBytes=integer(1,4194304),maxControlProofBytes=P,maxValidationDiskBytes=P,maxRetainedCopyBytes=P)
for typename,length in [('Sha256',64),('UUID',36),('SubmissionUUID',36),('Time',24)]:D[typename].update(minLength=length,maxLength=length)
for typename,characters in [('Slug','A-Za-z0-9_-'),('WorkerId','A-Za-z0-9_-'),('EngineVersion','A-Za-z0-9_.+-'),('Basename','!-~'),('ArchivePath','A-Za-z0-9_./-')]:
    D[typename]['not']={'pattern':'[^'+characters+']'}
def when(field,value,properties):return {'if':{'properties':{field:const(value)}},'then':{'properties':properties}}
D['Artifact']['allOf']=[when('role',role,{'format':const(fmt)}) for role,fmt in {'odm_archive':'zip','treedar_bundle':'zip','orthophoto':'geotiff','pointcloud':'zip','mesh':'glb','dem':'geotiff','report':'pdf'}.items()]
D['Artifact']['allOf'] += [when('format','zip',{'artifactManifestSha256':H})]+[when('format',fmt,{'artifactManifestSha256':const(None)}) for fmt in ('geotiff','glb','pdf')]
D['ProductOutcome']['allOf']=[when('state','available',{'targets':a('ProductTarget',1),'evidenceReceiptId':S}),when('state','failed',{'targets':array(ref('ProductTarget'),maximum=0),'evidenceReceiptId':S}),when('state','not_produced',{'targets':array(ref('ProductTarget'),maximum=0)}),when('engine','treedar',{'product':const('native_bundle')}),when('engine','odm',{'product':enum('orthophoto','pointcloud','mesh','dsm','dtm','report')})]
D['TreeDarRequest']['allOf']=[when('requested',False,{'inputObjectIds':array(S,maximum=0),'provenanceReceiptId':const(None),'bundleContract':const(None),'requestedProducts':array(const('native_bundle'),maximum=0)}),when('requested',True,{'inputObjectIds':array(S,1),'provenanceReceiptId':S,'bundleContract':const('ltds-treedar-bundle-v1'),'requestedProducts':array(const('native_bundle'),1,1)})]
D['OdmRequest']['allOf']=[when('requested',False,{'options':{'maxProperties':0},'requestedProducts':array(ref('ProductRequest'),maximum=0)}),when('requested',True,{'requestedProducts':a('ProductRequest',1)})]
D['Pipeline']['allOf']=[when('mode',mode,{'odm':{'properties':{'requested':const(odm)}},'treedar':{'properties':{'requested':const(tree)}}}) for mode,odm,tree in [('odm_only',True,False),('treedar_only',False,True),('combined',True,True)]]
D['InputObject']['allOf']=[when('kind',kind,{'mediaType':enum(*types)}) for kind,types in {'image':('image/jpeg','image/png','image/tiff'),'lidar':('application/vnd.las','application/vnd.laz'),'auxiliary':('application/octet-stream','application/json','text/plain','text/csv')}.items()]
D['ResultManifest']['allOf']=[{'if':{'properties':{'stage':{'properties':{'engine':const(engine)}}}},'then':{'properties':{'engineVersions':obj({engine:ref('EngineVersion')})}}} for engine in ('odm','treedar')]


def schemas():
    sources={}
    for name,pin in PINS.items():
        source=ROOT/'sources'/name
        data=source.read_bytes();assert sha(data)==pin
        sources[name]=json.loads(data)
    native_source=ROOT/'sources/treedar-bundle-v1.contract.json'
    native=native_source.read_bytes();assert sha(native)==NATIVE_PIN
    for value in sources.values():
        for name,fields in value['fieldsets'].items():assert set(D[name]['properties'])==set(fields),(name,set(D[name]['properties'])^set(fields))
    public=['Submission','OriginalLidarCustodyProof','ProducerCustodyReceipt','ResultManifest','PartialAcceptance','ParentCompletion','StageContinuation','CleanupAck','PointcloudZipManifest','ExecutionSettledReceipt','HeavyCapacityReleaseReceipt','ImmutableMetadataIntent','AuthorizedResponse','DeniedResponse','SealedStageAdmission']
    schema={'$schema':'https://json-schema.org/draft/2020-12/schema','$id':'urn:ltds:reference-v2:candidate:review-only',
        'title':'Unagreed reference outer-v2 candidate plus metadata addendum','description':'Schema-style review artifact, not installed validation or authority. Lexical/semantic/authentication rules are additionally mandatory.',
        'oneOf':[ref(name) for name in public],'$defs':D,'x-status':['UNAGREED','NOTWIRECOMPATIBLE','NOTACTIVATED'],'x-sourcePins':PINS,
        'x-wholeMessageBytes':4194304,'x-maximumDepth':32,'x-unsafeKeys':['__proto__','prototype','constructor']}
    write('contract.schema.json',schema)


def zip_bytes(entries):
    target=io.BytesIO()
    with zipfile.ZipFile(target,'w',compression=zipfile.ZIP_STORED) as archive:
        for name,body in sorted(entries.items()):
            info=zipfile.ZipInfo(name,(1980,1,1,0,0,0));info.create_system=3;info.external_attr=0o100600<<16
            archive.writestr(info,body)
    return target.getvalue()


def fixture(mode,index,*,optional=False,odm_product='report'):
    odm=mode!='treedar_only';tree=mode!='odm_only';job=f'00000000-0000-4000-8000-{index:012d}'
    raw={};inputs=[]
    for name,kind in ([('image-1.jpg','image')] if odm else [])+([('original-1.las','lidar')] if tree else []):
        body=('SYNTHETIC-NOT-SENSOR:'+name).encode();identity='photo-1' if kind=='image' else 'lidar-1';raw[identity]=body
        inputs.append(dict(id=identity,name=name,byteSize=len(body),sha256=sha(body),kind=kind,mediaType='image/jpeg' if kind=='image' else 'application/vnd.las'))
    manifest=seal(dict(id='inputs-'+mode,revision=1,totalBytes=sum(len(x) for x in raw.values()),objectCount=len(inputs),inputCounts=dict(images=int(odm),lidarObjects=int(tree),points=10 if tree else 0),objects=inputs),'sha256')
    pipeline=dict(mode=mode,odm=dict(requested=odm,options={},optionsFingerprint=sha(b'fixture-catalog'),requestedProducts=[dict(product=odm_product,required=True)] if odm else []),treedar=dict(requested=tree,inputObjectIds=['lidar-1'] if tree else [],provenanceReceiptId='acquisition-1' if tree else None,bundleContract='ltds-treedar-bundle-v1' if tree else None,requestedProducts=['native_bundle'] if tree else []),gaussian_splat=dict(requested=False))
    if optional:pipeline['odm']['requestedProducts'].insert(0,dict(product='orthophoto',required=False))
    binding=dict(jobId=job,generation=1,environment='staging',inputManifestSha256=manifest['sha256'],pipelineDigest=sha(('OPAQUE-SERVER-FIXTURE-IDENTITY:'+mode+':'+odm_product+':'+str(optional)).encode()),parentClaimId='original-parent-claim-'+mode)
    stages=[dict(binding=binding,stageId='stage-'+engine,stageOrdinal=i+1,engine=engine,workerId='worker-main',workerImageDigest=sha(('image:'+engine).encode()),reservationId='parent-reservation',reservationReceiptId='reservation-proof',continuationId='continuation-'+engine) for i,engine in enumerate((['odm'] if odm else [])+(['treedar'] if tree else []))]
    stage_definitions=[dict(stage=stage,inputObjects=[v for v in inputs if v['kind']==('image' if stage['engine']=='odm' else 'lidar')],requestedProducts=pipeline[stage['engine']]['requestedProducts']) for stage in stages]
    approved_plan=dict(schemaVersion=1,binding=binding,mode=mode,stages=stage_definitions)
    docs=[dict(type='Submission',value=dict(schemaVersion=2,protocol=PROTOCOL,submissionId=job,inputManifest=manifest,pipeline=pipeline,capabilityFingerprint=sha(b'UNADVERTISED-SYNTHETIC-CAPABILITY'),contractFingerprint=sha(b'UNAGREED-NOT-A-CONTRACT-AUTHORIZATION')))]
    artifacts=[];closures=[];results=[];acceptances=[];custodies=[];metadata=[];settled=[]
    for stage in stages:
        engine=stage['engine'];artifact_id=engine+'-file';product=odm_product if engine=='odm' else 'native_bundle'
        if engine=='odm':
            body=b'<html><body>Synthetic report fixture; no actual ODM execution.</body></html>' if product=='report' else b'0 0 0\n1 0 1\n0 1 2\n'
            path='odm_report/report.html' if product=='report' else 'odm_georeferencing/odm_georeferenced_model.xyz'
            closure=seal(dict(schemaVersion=1,format='ltds-odm-closure-v1-proposed',stage=stage,inputManifestSha256=manifest['sha256'],files=[dict(memberId=product+'-member',relativePath=path,byteSize=len(body),sha256=sha(body),closureId=product+'-closure')],rawBytes=len(body)),'manifestSha256')
            entries={path:body};member=[dict(memberId=product+'-member',relativePath=path,byteSize=len(body),sha256=sha(body),closureManifestSha256=closure['manifestSha256'],closureId=product+'-closure')]
        else:
            native=json.loads((ROOT/'sources/treedar-bundle-v1.contract.json').read_text());native_manifest=copy.deepcopy(native['manifest'])
            lidar=next(v for v in inputs if v['kind']=='lidar')
            native_manifest['binding']=dict(jobId=job,generation=1,environment='staging',inputManifestSha256=manifest['sha256'],pipelineDigest=binding['pipelineDigest'],sourceLidar=dict(objectId=lidar['id'],byteSize=lidar['byteSize'],sha256=lidar['sha256'],provenanceReceiptId='acquisition-1'))
            native_manifest.pop('manifestSha256');native_manifest=seal(native_manifest,'manifestSha256');closure=native_manifest
            entries={'artifacts/'+name:base64.b64decode(body) for name,body in native['payloadsBase64'].items()};entries['treedar-bundle.json']=canonical(native_manifest)
            member=[dict(memberId='native-'+str(i),relativePath='artifacts/'+v['nativePath'],byteSize=v['byteSize'],sha256=v['sha256'],closureManifestSha256=closure['manifestSha256'],closureId='native-bundle-closure') for i,v in enumerate(native_manifest['files'])]
            member.sort(key=lambda v:v['relativePath'])
        blob=zip_bytes(entries);artifact=dict(id=artifact_id,stage=stage,role='odm_archive' if engine=='odm' else 'treedar_bundle',format='zip',byteSize=len(blob),sha256=sha(blob),artifactManifestSha256=closure['manifestSha256'],physicalReceiptId='producer-'+engine,sourceObjectIds=[v['id'] for v in inputs if v['kind']==('image' if engine=='odm' else 'lidar')],productKeys=[product],physicalKind='file')
        target=dict(artifactId=artifact_id,artifactSha256=artifact['sha256'],artifactByteSize=len(blob),closureManifestSha256=closure['manifestSha256'],memberRefs=member,validationReceiptId='worker-product-validation-'+engine)
        outcomes=[dict(engine=engine,product=product,state='available',required=True,evidenceReceiptId='worker-outcome-'+engine,targets=[target])]
        if optional and engine=='odm':outcomes.insert(0,dict(engine='odm',product='orthophoto',state='not_produced',required=False,evidenceReceiptId=None,targets=[]))
        result=seal(dict(schemaVersion=2,binding=binding,stage=stage,resultSetId='results-'+engine,engineVersions={engine:'3.8.5' if engine=='odm' else '0.0.1'},objects=[artifact],products=outcomes),'resultManifestSha256')
        custody=dict(schemaVersion=2,receiptType='producer_result_custody',receiptId='producer-'+engine,stage=stage,issuerId='fixture-worker-custody-issuer',objectId=artifact_id,byteSize=len(blob),sha256=sha(blob),physicalObjectId='producer-object-'+engine,reservationId='parent-reservation',retainedAt=TIME,retainUntil='accepted_copy_ack_or_authorized_release',verificationReceiptId='worker-custody-check-'+engine)
        accepted=dict(id=artifact_id,sha256=sha(blob),byteSize=len(blob),stageId=stage['stageId'],workerId=stage['workerId'],reservationId=stage['reservationId'],physicalReceiptId='nas-copy-'+engine,validationReceiptId='nas-ingestion-validation-'+engine,producerCustodyReceiptId='producer-'+engine)
        acceptance=dict(schemaVersion=2,receiptType='partial_result_acceptance',receiptId='acceptance-'+engine,storageReceiptId='nas-storage-'+engine,binding=binding,resultSetId=result['resultSetId'],resultManifestSha256=result['resultManifestSha256'],acceptedArtifacts=[accepted],acceptedAt=TIME,validationVersion='candidate-fixture-only',ackOperationId='acceptance-ack-'+engine)
        artifacts.append(dict(id=artifact_id,bytesBase64=base64.b64encode(blob).decode(),byteSize=len(blob),sha256=sha(blob),semanticScope='Synthetic bytes; ZIP transport/member/hash consistency only, not genuine processing or geospatial quality.'))
        closures.append(dict(artifactId=artifact_id,descriptor=closure));results.append(result);acceptances.append(acceptance);custodies.append(custody)
        settled.append(dict(stage=stage,engineExecutionId='synthetic-execution-'+engine,terminalState='succeeded',engineStopReceiptId='synthetic-stop-'+engine,resultCustodyReceiptIds=[custody['receiptId']],settledAt=TIME))
        docs.extend([dict(type='ProducerCustodyReceipt',value=custody),dict(type='ResultManifest',value=result),dict(type='PartialAcceptance',value=acceptance)])
    if tree:
        stage=stages[-1];lidar=next(v for v in inputs if v['kind']=='lidar');original=dict(objectId=lidar['id'],controlledObjectId='nas-original-lidar',byteSize=lidar['byteSize'],sha256=lidar['sha256'],mediaType=lidar['mediaType'],acquisitionReceiptId='acquisition-1',physicalReceiptId='nas-original-physical',genuineLasLaz=True,photogrammetryDerived=False)
        source_custody=dict(schemaVersion=2,receiptType='original_lidar_custody',receiptId='original-custody',storageReceiptId='original-storage',binding=binding,issuerId='synthetic-nas-source-issuer',objects=[original],retentionGuarantee='until_explicit_release_ack',retainedAt='2026-10-28T10:00:00.000Z',validationVersion='synthetic-expectation-not-authority')
        acquisition=dict(schemaVersion=1,receiptId='acquisition-1',jobId=job,generation=1,environment='staging',objectId=lidar['id'],objectSha256=lidar['sha256'],byteSize=lidar['byteSize'],sourceKind='lidar',verticalUnits='metre',verticalDatum='navd88')
        original_identity=dict(receiptId='acquisition-1',receiptSha256=digest(acquisition),issuerId='synthetic-acquisition-issuer',issuerPolicySha256=sha(b'FIXTURE-ACQUISITION-POLICY'))
        custody_identity=dict(receiptId='original-custody',receiptSha256=digest(source_custody),issuerId=source_custody['issuerId'],issuerPolicySha256=sha(b'FIXTURE-CUSTODY-POLICY'))
        auth_source=dict(parentBinding=binding,planSha256=digest(approved_plan),stageDefinitionSha256=digest(stage_definitions[-1]),predecessorProofSha256=sha(b'SYNTHETIC-ODM-STOP-PROOF') if odm else None,originalAcquisition=original_identity,originalCustody=custody_identity,originalObjects=[dict(objectId=lidar['id'],sha256=lidar['sha256'],byteSize=lidar['byteSize'],controlledObjectId='nas-original-lidar',acquisitionReceiptId='acquisition-1',custodyPhysicalReceiptId='nas-original-physical',mediaType=lidar['mediaType'])],originalTransferDigest=sha(b'ORIGINAL-EXPIRED-TRANSFER-REQUEST'),payloadBudgetLedgerId='unchanged-payload-ledger',payloadBudgetLedgerRevision=7,issuerPolicySha256=sha(b'FIXTURE-METADATA-POLICY'))
        intent=seal(dict(schemaVersion=1,protocol=META,receiptType='immutable_metadata_intent',operationId='metadata-operation-1',round=1,purpose='next_stage_admission',**auth_source,stage=stage,controlBudgetLedgerId='control-ledger',controlChargeId='control-charge-1',requestedAt=TIME),'intentSha256')
        response=seal(dict(schemaVersion=1,protocol=META,receiptType='metadata_authorization_receipt',decision='authorized',receiptId='metadata-response-1',operationId=intent['operationId'],round=1,intentSha256=intent['intentSha256'],purpose=intent['purpose'],**auth_source,authenticatedActorBindingSha256=sha(b'SYNTHETIC-ACTOR-EXPECTATION-NOT-A-MAC'),custodyRevision=3,revocationRevision=2,checkedAt=TIME,metadataExpiresAt='2026-10-28T12:05:00.000Z',controlChargeId=intent['controlChargeId']),'receiptSha256')
        admission=seal(dict(schemaVersion=1,receiptType='sealed_stage_metadata_admission',receiptId='sealed-admission',stage=stage,planSha256=auth_source['planSha256'],stageDefinitionSha256=auth_source['stageDefinitionSha256'],intentSha256=intent['intentSha256'],metadataAuthorizationReceiptId=response['receiptId'],metadataAuthorizationReceiptSha256=response['receiptSha256'],originalAcquisition=original_identity,originalCustody=custody_identity,authenticatedActorBindingSha256=response['authenticatedActorBindingSha256'],custodyRevision=3,revocationRevision=2,admittedAt='2026-10-28T12:00:01.000Z'),'admissionSha256')
        docs.extend([dict(type='OriginalLidarCustodyProof',value=source_custody),dict(type='ImmutableMetadataIntent',value=intent),dict(type='AuthorizedResponse',value=response),dict(type='SealedStageAdmission',value=admission)])
    execution=dict(schemaVersion=2,receiptType='execution_settled',receiptId='execution-settled',binding=binding,requestedStageIds=[v['stageId'] for v in stages],settledStages=settled,producerCustodyReceiptIds=[v['receiptId'] for v in custodies],retainedDiskReservationIds=['parent-reservation','delivery-disk-hold'],diskBudgetReceiptId='synthetic-current-disk-budget',coordinatorSettlementOperationId='execution-settlement-operation',settledAt='2026-10-28T13:00:00.000Z')
    release=dict(schemaVersion=2,receiptType='heavy_capacity_release',receiptId='heavy-release',binding=binding,executionSettledReceiptId=execution['receiptId'],releasedCpuGpuSlotIds=['global-heavy-slot'],remainingDiskHoldIds=execution['retainedDiskReservationIds'],operationId='heavy-release-operation',releasedAt='2026-10-28T13:00:01.000Z')
    completion=seal(dict(schemaVersion=2,binding=binding,mode=mode,stageResults=[dict(stage=v['stage'],resultSetId=v['resultSetId'],resultManifestSha256=v['resultManifestSha256'],requiredArtifactIds=[x['id'] for x in v['objects']],acceptanceReceiptIds=[acceptances[i]['receiptId']]) for i,v in enumerate(results)],requestedProductsDigest=digest([dict(stageId=v['stageId'],engine=v['engine'],requestedProducts=pipeline[v['engine']]['requestedProducts']) for v in stages]),publicationReceiptIds=['synthetic-catalog-'+v['engine'] for v in stages],terminalReceiptId='synthetic-parent-delivery-terminal'),'completionSha256')
    docs.extend([dict(type='ExecutionSettledReceipt',value=execution),dict(type='HeavyCapacityReleaseReceipt',value=release),dict(type='ParentCompletion',value=completion)])
    for result,acceptance in zip(results,acceptances):
        docs.append(dict(type='CleanupAck',value=dict(schemaVersion=2,receiptType='producer_cleanup_ack',receiptId='cleanup-ack-'+result['stage']['engine'],binding=binding,resultManifestSha256=result['resultManifestSha256'],acceptanceReceiptIds=[acceptance['receiptId']],producerObjectIds=[v['id'] for v in result['objects']],physicalReceiptIds=[v['physicalReceiptId'] for v in result['objects']],terminalReceiptId=completion['terminalReceiptId'],cleanupOperationId='cleanup-operation-'+result['stage']['engine'],acknowledgedAt='2026-10-28T14:00:00.000Z')))
    return dict(fixtureVersion=1,status='SYNTHETIC_EXPECTED_CONTEXT_NOT_AUTHORITY',mode=mode,
        scope='Golden structural/hash/lifecycle relationships only; no acquisition, engine execution, custody, issuer authentication, semantic processing, NAS acceptance or publication actually occurred.',
        expectedContext=dict(originalBinding=binding,originalSubmission=docs[0]['value'],admittedStages=stages,syntheticApprovedPlan=approved_plan,
            planDigestScope='Canonical fixture plan/stage metadata is supplied for consistency checks only; production plan digests remain authenticated original references, not a new public client derivation.',pipelineDigestOwnership='Opaque server-provided fixed digest, not computed from options.',
            trustModel='IDs/digests represent expected externally authenticated facts ONLY. Absent real trusted endpoints/receipts these records authorize nothing.',payload=dict(ledgerId='unchanged-payload-ledger',revision=7,consumedBytes=manifest['totalBytes'],cumulativeByteBudget=manifest['totalBytes']*3,originalPayloadExpiresAt='2026-10-28T11:00:00.000Z'),
            expectedActorBindingSha256=sha(b'SYNTHETIC-ACTOR-EXPECTATION-NOT-A-MAC'),authenticationEvidence=None,
            workerValidationReceiptIds=['worker-outcome-'+v['engine'] for v in stages]+['worker-product-validation-'+v['engine'] for v in stages],
            nasAcceptedValidationReceiptIds=['nas-ingestion-validation-'+v['engine'] for v in stages],
            limitsScope='Explicit invented finite TEST CONTEXT values, not approved deployment policy.',
            archiveLimits=dict(maxArchiveBytes=1048576,maxRawBytes=1048576,maxExpandedMembers=64,maxExpandedBytes=1048576,maxManifestBytes=65536,maxControlProofBytes=65536,maxValidationDiskBytes=8388608,maxRetainedCopyBytes=8388608),
            treeDarLimits=dict(maxArchiveBytes=1048576,maxRawBytes=1048576,maxFiles=64,maxManifestBytes=65536,maxControlProofBytes=65536,maxValidationDiskBytes=8388608),
            authorityPolicy=dict(atStart='Fresh metadata and current parent/revocation check required before next heavy engine.',historicalValidation='Sealed at-start admission remains evidence after metadata TTL; source receipt and bundle identity unchanged.',publication='Separate CURRENT publication/catalog authority required; historical source authorization is insufficient.',outage='Retain all disk/custody/outbox holds; release heavy capacity only after ALL requested execution settled.')),
        originalPayloads=[dict(objectId=k,bytesBase64=base64.b64encode(v).decode()) for k,v in sorted(raw.items())],artifacts=artifacts,closureDescriptors=closures,documents=docs,
        originalAcquisitionRecord=acquisition if tree else None,
        outageSnapshot=dict(executionSettledReceipt=execution,heavyCapacityReleaseReceipt=release,parentDeliveryState='pending',outboxRetained=True,originalSourceCustodyRetained=tree,diskHoldsRetained=True,viewerAccepted=False),
        timingScope='Documents are separate lifecycle snapshots, not evidence of one instantaneous state; fixture trust is modeled, never real.')


RULES={
 'STRICT_JSON':('lexical','Bound raw bytes to4194304 and depth32 before decode; reject malformed UTF8/BOM, duplicate or escaped duplicate keys, unpaired Unicode surrogates, trailing data, nonfinite numbers, negative zero and unsafe keys at every depth.'),
 'TIME_CANONICAL':('semantic','Times must parse and round-trip as real UTC Gregorian timestamps at exact millisecond precision; format/date annotations alone are not authority. Fresh metadata expiry is later than checkedAt and within admitted freshness policy; no clock rollback or expired next-stage authority.'),
 'INTEGER_TOKENS':('lexical','Integer fields require unsigned decimal JSON integer tokens within safe bounds; reject bool,1.0,1e0,-0 and values>9007199254740991. Options may use separately admitted finite floating numbers; they are not a public digest domain.'),
 'CLOSED_FIELDS':('schema','Every record requires all named fields and rejects unknown fields. Nullable fields are explicit. Unknown protocol/version and downgrade are rejected.'),
 'BASENAME_SAFETY':('semantic','Original basenames are ASCII printable1..255 with nonempty stem/extension; reject separators/:<>"|?*, leading dot/space, trailing dot/space and reserved CON/PRN/AUX/NUL/COM1..9/LPT1..9 stems, case-insensitively.'),
 'ARCHIVE_PATH_SAFETY':('semantic','Archive paths are relative forward-slash paths; no empty/dot/dotdot segments, absolute/drive/UNC/control forms, links/special entries, duplicate/case collisions or unlisted members.'),
 'MANIFEST_CLOSURE':('semantic','Original IDs unique; basenames unique case-insensitively; objectCount/counts/positive safe sum exact. Original order/IDs/names/hashes/byte sizes are immutable. Requested LiDAR subset is exact, genuine and positive; derived ODM cloud cannot replace it.'),
 'OPAQUE_PIPELINE_DIGEST':('authority','pipelineDigest is opaque server-approved request identity copied from authenticated original context. No caller-computed options canonicalization, float algorithm or client self-hash can establish it.'),
 'SERVER_APPROVED_OPTIONS':('authority','Options and vocabulary fingerprint must match the authenticated admitted engine catalog and server-approved pipeline. A schema-shaped options object is not admission.'),
 'MODE_PLAN':('semantic','odm_only requests one ODM stage; treedar_only requests one TreeDar stage; combined requests ODM then TreeDar under one original parent. Exact sorted products/inputs, stages/ordinals and Gaussian false must agree with original plan.'),
 'ORIGINAL_BINDING':('authority','Every nested binding equals trusted original job/generation/environment/input-manifest/pipeline/parent-claim identity. Credential establishes environment/actor. Stage binding equals admitted worker/image/reservation/receipt/continuation; IDs supplied by worker are not proof.'),
 'ROLE_FORMAT':('semantic','odm_archive:zip, treedar_bundle:zip, orthophoto:geotiff, pointcloud:zip, mesh:glb, dem:geotiff, report:pdf. Exactly one positive ODM archive on ODM success and one TreeDar bundle on TreeDar success; unrequested engines produce none. No tree_inventory/CHM relabel.'),
 'RESULT_ENGINE':('semantic','A stage result has exactly that stage engine version and original products only, not both requested engines or another stage results. IDs unique/ASCII ordered; requested products match required flags and outcomes.'),
 'PRODUCT_TARGETS':('semantic','available has nonempty exact validated targets; failed/not_produced have no targets; not_produced optional may have null evidence. Required unavailable prevents parent completion. Every target/container/hash/size/member/closure matches its SAME stage immutable manifest. No mandatory standalone duplicate product artifact.'),
 'MEMBER_CLOSURE':('semantic','ZIP member references match actual admitted closure descriptor, expanded bytes/hash/path and exact dependencies. Synthetic transport bytes do not prove report/pointcloud/mesh/geospatial semantics. Plain-file targets have null closure and no members; zip products require a real agreed closure descriptor.'),
 'ZERO_EXPANDED_MEMBER':('semantic','Original sources and physical artifacts remain positive. Zero-byte expanded EPT/archive metadata entries require explicit semantic closure/policy justification; no blanket zero allowance and no empty product/ZIP success.'),
 'PRODUCT_EVIDENCE_PHASE':('authority','ProductOutcome.evidenceReceiptId and ProductTarget.validationReceiptId refer to authenticated PREUPLOAD worker semantic validation, not future NAS ingestion acceptance. NAS accepted-copy validation is separate.'),
 'CUSTODY_AUTHORITY':('authority','Original NAS custody is independently authenticated immutable-object/purge-pin evidence for exact original subset. Matching strings/hash/stat or claimed genuine flags alone authorize nothing. Producer result custody exists before manifest/upload and binds exact retained physical bytes.'),
 'DISTINCT_RECEIPT_ROLES':('semantic','Producer custody, NAS accepted-copy custody, acquisition, acceptance/storage/ACK and terminal receipt roles remain distinct. AcceptedArtifact.producerCustodyReceiptId is original Artifact.physicalReceiptId; its physicalReceiptId is a different managed accepted-copy receipt.'),
 'PARTIAL_SAME_MANIFEST':('semantic','Partial acceptance references one exact binding/resultSetId/resultManifestSha256. Every accepted ID/hash/size/stage/worker/reservation matches that manifest and original producer receipt. Never union receipts from different result manifests/stages to invent closure.'),
 'ACCEPTANCE_AUTHORITY':('authority','NAS acceptance follows durable exact-copy byte/directory barriers and semantic validation, plus committed receipt/outbox. Upload/HTTP success alone is not acceptance. IDs must resolve through fixed authenticated trusted services.'),
 'SOURCE_REAUTH_INTENT':('semantic','Metadata request retains original parent/plan/stage/predecessor/acquisition/custody/objects/transfer/ledger identities; stable operation+round+intent bytes and purpose. Changed body under same key conflicts; no new round while outcome unknown.'),
 'SOURCE_REAUTH_AUTHORITY':('authority','Distinct metadata-only fixed issuer/actor authorization; do not forward expired payload bearer. Exact authenticated response binding, current custody/revocation/policy and valid bounded metadata deadline before NEXT heavy stage. No new payload permission/expiry/budget/claim.'),
 'SOURCE_REAUTH_DENIAL':('semantic','Denied response has null expiry and exact original request bindings; no old-green fallback. Zero custody/revocation revision only with issuer_unavailable. Immutable round histories/counts/bytes remain charged through restart.'),
 'HISTORICAL_ADMISSION':('lifecycle','Seal authorized at-start receipt/intent/source identities before engine mutation. Later metadata TTL expiry does not invalidate truthful sealed execution/output or rewrite native bundle/source identity. Ongoing current revocation/parent policy remains separate.'),
 'CURRENT_PUBLICATION':('authority','Catalog/publication requires separate CURRENT authenticated publication authority and accepted product closure. Historical source admission alone cannot authorize publication after current denial/revocation. Metadata publication_authority_check is not itself a catalog publish credential.'),
 'FINITE_CONTROL_BUDGET':('budget','Preissue reserve/charge exact finite rounds/attempts/canonical bytes/history/concurrency/control disk holds; retry/lookup same operation counts monotonically. Unknown issuer outcome uses exact lookup or explicitly proven identical replay; no counter reset or new key.'),
 'EXECUTION_SETTLEMENT':('lifecycle','ALL original requested heavy stages must actually stop/settle with verified stop, custody and safe disk-budget receipts before authoritative heavy/global CPU/GPU release. Intermediate ODM alone in combined is insufficient. Requested cancel/timeouts are not stop evidence.'),
 'DELIVERY_SEPARATE':('lifecycle','Execution settled/heavy released is distinct from Viewer acceptance/publication/parent delivery completion. Viewer outage retains bytes, original custody, disk/control/outbox holds while safely settled heavy capacity may release.'),
 'PARENT_COMPLETION':('authority','Parent completion requires exact original stage result references and all required products durably accepted and published. Optional unavailable outputs do not fabricate success; stage/partial/worker receipts cannot independently manufacture parent completion.'),
 'CLEANUP_AUTHORITY':('authority','Cleanup ACK requires exact authenticated parent terminal delivery, accepted closure and pin-free producer subset. Never delete originals, accepted managed copies, unaccepted output or unknown paths; missing ACK retains data. Original-source release is a separate issuer decision.'),
 'SAFE_CAPACITY':('budget','Positive explicit per-admission archive/raw/expanded member/count/control/disk policies must account all simultaneous original, producer RAW/ZIP, transfer/expansion/accepted copies and outboxes by actual filesystem; no compressed-size substitute or reservation reset.'),
 'STRUCTURED_DIGEST':('semantic','Canonical safe-integer metadata uses UTF8/ASCII-key sorting/no whitespace/array order; omit ONLY the designated own top-level self-digest. Never omit nested physical sha256. Only named domains apply.'),
 'SUMMARY_NOT_AUTHORITY':('authority','Superseded v3 SourceMetadataReauthorization compact summary is excluded from root message union; full addendum receipt is required. Owners must remove or explicitly pair any future derived view.')}

def declarations():
    write('semantic-rules.json',{'status':'UNAGREED_DECLARATIVE_NO_VALIDATOR','rules':[dict(id=k,phase=v[0],requirement=v[1]) for k,v in RULES.items()],
        'requiredOrdering':{'inputObjects':'preserve original manifest order','resultObjects':'ASCII id','custodyObjects':'ASCII objectId','requestedProducts':'ASCII product','productOutcomes':'ASCII engine/product','memberRefs':'ASCII relativePath','eptEntries':'ASCII relativePath','allOtherIdentitySets':'ASCII full identity'},
        'reviewOnlyLimits':dict(metadataBytes=4194304,maximumDepth=32,maxArrayMembers=10000,maximumSafeInteger=MAX,maximumStageCount=2),
        'notExpressibleBySchemaAlone':['strict JSON token/UTF8 decoding','cross-record identity and membership','safe cumulative byte arithmetic','semantic geospatial/product closure','trusted issuer/actor/receipt authenticity','live revocation/custody/capacity and OS ownership','durable lifecycle/replay state']})
    write('hash-domains.json',{'status':'UNAGREED_NO_NEW_PIPELINE_HASH_ALGORITHM','canonicalMetadata':{'encoding':'UTF8','keyOrder':'ASCII lexicographic','whitespace':False,'arrayOrder':'preserved','unicode':'valid scalar values; no normalization; standard JSON escaping','numbers':'finite nonnegative safe integer metadata only; booleans remain booleans; negative zero forbidden'},
        'selfDigests':[dict(type=t,field=f,omission='ONLY this own top-level field',domain='complete canonical structured record, preserving every nested digest') for t,f in [('InputManifest','sha256'),('ResultManifest','resultManifestSha256'),('ParentCompletion','completionSha256'),('PointcloudZipManifest','manifestSha256'),('OdmArchiveClosureManifest','manifestSha256'),('ImmutableMetadataIntent','intentSha256'),('AuthorizedResponse','receiptSha256'),('DeniedResponse','receiptSha256'),('SealedStageAdmission','admissionSha256')]],
        'physicalDigests':{'fields':['InputObject.sha256','Artifact.sha256','CustodyObject.sha256','OriginalObject.sha256','ArchiveMemberReference.sha256','EptClosureEntry.sha256','AcceptedArtifact.sha256'],'domain':'Exactly byteSize bytes of that one physical file; ZIP hashes compressed container bytes. No decompression/canonicalization/directory digest.'},
        'opaqueAuthenticatedReferences':['Binding.pipelineDigest','Submission.capabilityFingerprint','Submission.contractFingerprint','OdmRequest.optionsFingerprint','OriginalReceiptIdentity.receiptSha256','OriginalReceiptIdentity.issuerPolicySha256','ControlBudgetPolicy.policySha256','authenticatedActorBindingSha256','originalTransferDigest','planSha256','stageDefinitionSha256','predecessorProofSha256'],
        'newCandidateTechnicalChoice':{'ParentCompletion.requestedProductsDigest':'Canonical array in original stage order of closed {stageId,engine,requestedProducts}; no ODM option values included. Still requires paired acceptance.'},
        'authenticity':'Self-digests/fixture strings are never MACs, signatures or issuer authorization.'})


def supplements(combined):
    docs=combined['documents'];get=lambda name:copy.deepcopy(next(v['value'] for v in docs if v['type']==name))
    stage=get('ResultManifest')['stage'];intent=get('ImmutableMetadataIntent');authorized=get('AuthorizedResponse')
    intent.update(operationId='metadata-operation-2',round=2,purpose='publication_authority_check',controlChargeId='control-charge-2',requestedAt='2026-10-28T15:00:00.000Z')
    intent.pop('intentSha256');intent=seal(intent,'intentSha256')
    denied=copy.deepcopy(authorized);denied.update(receiptId='metadata-denial-2',operationId=intent['operationId'],round=2,purpose=intent['purpose'],intentSha256=intent['intentSha256'],controlChargeId=intent['controlChargeId'],decision='denied',checkedAt='2026-10-28T15:00:01.000Z',metadataExpiresAt=None,denialCode='source_revoked',revocationRevision=3)
    denied.pop('receiptSha256');denied=seal(denied,'receiptSha256')
    policy=dict(policySha256=sha(b'SYNTHETIC-EXTERNALLY-APPROVED-CONTROL-POLICY'),maxRounds=3,maxAttemptsPerRound=4,maxIntentBytes=65536,maxResponseBytes=65536,maxHistoryRecords=24,maxHistoryBytes=524288,maxConcurrentRequests=1,metadataFreshnessSeconds=300,reservedControlDiskReceiptId='synthetic-control-disk-hold')
    journal=dict(schemaVersion=1,operationId=intent['operationId'],round=2,intentSha256=intent['intentSha256'],intentBytes=len(canonical(intent)),chargeId=intent['controlChargeId'],state='denied',attemptCount=1,responseReceiptId=denied['receiptId'],responseSha256=denied['receiptSha256'],responseBytes=len(canonical(denied)),actorVerificationReceiptId='synthetic-authenticated-actor-check',policySha256=policy['policySha256'],previousRoundReceiptSha256=authorized['receiptSha256'])
    continuation=dict(schemaVersion=2,binding=stage['binding'],continuationId=stage['continuationId'],previousStageId='stage-odm',nextStage=stage,previousResultManifestSha256=next(v['value']['resultManifestSha256'] for v in docs if v['type']=='ResultManifest' and v['value']['stage']['engine']=='odm'),originalCustodyReceiptId='original-custody',reauthorizationReceiptId=authorized['receiptId'],payloadBudgetLedgerId='unchanged-payload-ledger',state='prepared',operationId='original-stage-continuation-operation')
    ept_payloads={'ept.json':b'{}','ept-data/0-0-0-0.laz':b'SYNTHETIC-NOT-A-LAZ-FILE!!'}
    ept_files=[dict(relativePath=name,byteSize=len(body),sha256=sha(body)) for name,body in sorted(ept_payloads.items())]
    # These describe invented bytes only, not usable EPT or geospatial success.
    ept=seal(dict(schemaVersion=1,format='ltds-ept-bundle-v1-proposed',stage=next(v['value']['stage'] for v in docs if v['type']=='ResultManifest' and v['value']['stage']['engine']=='odm'),inputManifestSha256=stage['binding']['inputManifestSha256'],files=ept_files,rawBytes=sum(v['byteSize'] for v in ept_files)),'manifestSha256')
    return dict(status='SYNTHETIC_EXPECTED_CONTEXT_NOT_AUTHORITY',scope='Independent typed-record examples, not additional successful actions in the combined completed workflow. EPT is structural metadata only; no usable EPT claim.',documents=[dict(type=t,value=v) for t,v in [('ImmutableMetadataIntent',intent),('DeniedResponse',denied),('ControlBudgetPolicy',policy),('ControlJournalEntry',journal),('StageContinuation',continuation),('PointcloudZipManifest',ept)]],
        eptPayloads=[dict(relativePath=name,bytesBase64=base64.b64encode(body).decode()) for name,body in sorted(ept_payloads.items())],
        expectedContext=dict(previousRoundReceipt=authorized,request=intent,currentPublicationAuthorized=False,denialRequiresRetention=True,payloadLedgerUnchanged=True,notAnEngineReplay=True))


def negative_vectors(fixtures):
    combined=fixtures['combined'];entries=[]
    def doc(kind,engine=None):return copy.deepcopy(next(v['value'] for v in combined['documents'] if v['type']==kind and (engine is None or v['value'].get('stage',{}).get('engine')==engine)))
    def negative(name,kind,value,rule,*,raw=None,context='valid/combined.json',note='',phase=None):
        path='invalid/'+name+('.raw' if raw is not None else '.json');target=ROOT/path;target.parent.mkdir(exist_ok=True)
        data=raw if raw is not None else canonical(value);target.write_bytes(data)
        entries.append(dict(id=name,inputFile=path,documentType=kind,inputSha256=sha(data),byteSize=len(data),expectedContext=context,expected='reject',rule=rule,phase=phase or RULES[rule][0],note=note))
    binding=doc('ResultManifest')['binding'];wire=canonical(binding)
    for name,data in [
        ('duplicate-key',wire[:-1]+b',"generation":1}'),
        ('escaped-duplicate-key',wire[:-1]+b',"gener\\u0061tion":1}'),
        ('invalid-utf8',wire[:-1]+b',"extra":"\xff"}'),
        ('lone-high-surrogate',wire.replace(b'"parentClaimId":"original-parent-claim-combined"',b'"parentClaimId":"\\ud800"')),
        ('lone-low-surrogate',wire.replace(b'"parentClaimId":"original-parent-claim-combined"',b'"parentClaimId":"\\udc00"')),
        ('integer-float',wire.replace(b'"generation":1',b'"generation":1.0')),
        ('integer-exponent',wire.replace(b'"generation":1',b'"generation":1e0')),
        ('negative-zero',wire.replace(b'"generation":1',b'"generation":-0')),
        ('unsafe-integer',wire.replace(b'"generation":1',b'"generation":9007199254740992')),
        ('nonfinite-number',wire.replace(b'"generation":1',b'"generation":NaN')),
        ('trailing-data',wire+b' false'),('utf8-bom',b'\xef\xbb\xbf'+wire),
        ('unsafe-prototype-key',wire[:-1]+b',"__proto__":{}}'),
        ('depth-overflow',b'['*33+b'0'+b']'*33)]:
        negative(name,'Binding',None,'INTEGER_TOKENS' if name in ('integer-float','integer-exponent','negative-zero','unsafe-integer') else 'STRICT_JSON',raw=data,phase='lexical')
    value=copy.deepcopy(binding);value['generation']=True;negative('boolean-integer','Binding',value,'INTEGER_TOKENS')
    value=copy.deepcopy(binding);value['unexpected']=1;negative('unknown-field','Binding',value,'CLOSED_FIELDS')
    value=doc('Submission');value['schemaVersion']=1;negative('version-downgrade','Submission',value,'CLOSED_FIELDS')
    value=doc('Submission');value['pipeline']['gaussian_splat']['requested']=True;negative('gaussian-enabled','Submission',value,'MODE_PLAN',phase='schema')
    value=doc('Submission');value['inputManifest']['objects'][-1]['name']='../original.las';negative('source-traversal','Submission',value,'BASENAME_SAFETY')
    value=doc('Submission');value['inputManifest']['objects'][-1]['name']='CON.las';negative('reserved-basename','Submission',value,'BASENAME_SAFETY')
    value=doc('Submission');value['inputManifest']['objects'][-1]['name']='image-1.JPG';negative('source-case-collision','Submission',value,'MANIFEST_CLOSURE')
    value=doc('Submission');value['inputManifest']['totalBytes']+=1;value['inputManifest'].pop('sha256');value['inputManifest']=seal(value['inputManifest'],'sha256');negative('input-sum-mismatch','Submission',value,'MANIFEST_CLOSURE')
    value=doc('ResultManifest','odm');value['objects'][0]['role']='tree_inventory';value.pop('resultManifestSha256');value=seal(value,'resultManifestSha256');negative('bundle-v1-fallback-role','ResultManifest',value,'ROLE_FORMAT',phase='schema')
    value=doc('ResultManifest','odm');value['objects'][0]['format']='geotiff';value.pop('resultManifestSha256');value=seal(value,'resultManifestSha256');negative('archive-role-format','ResultManifest',value,'ROLE_FORMAT',phase='schema')
    value=doc('ResultManifest','odm');value['engineVersions']['treedar']='0.0.1';value.pop('resultManifestSha256');value=seal(value,'resultManifestSha256');negative('intermediate-fabricated-engine-version','ResultManifest',value,'RESULT_ENGINE',phase='schema')
    value=doc('ResultManifest','odm')
    def change_claim(item):
        if isinstance(item,dict):
            for key in item:
                if key=='parentClaimId':item[key]='replacement-parent-claim'
                else:change_claim(item[key])
        elif isinstance(item,list):
            for child in item:change_claim(child)
    change_claim(value);value.pop('resultManifestSha256');value=seal(value,'resultManifestSha256');negative('coherent-replacement-parent-claim','ResultManifest',value,'ORIGINAL_BINDING')
    value=doc('OriginalLidarCustodyProof');value['objects'][0]['sha256']='f'*64;negative('custody-original-file-mismatch','OriginalLidarCustodyProof',value,'CUSTODY_AUTHORITY')
    value=doc('OriginalLidarCustodyProof');value['objects'][0]['photogrammetryDerived']=True;negative('derived-cloud-as-original','OriginalLidarCustodyProof',value,'CUSTODY_AUTHORITY',phase='schema')
    negative('self-declared-custody-no-authentication','OriginalLidarCustodyProof',doc('OriginalLidarCustodyProof'),'CUSTODY_AUTHORITY',note='Bytes are otherwise unchanged; expected external authenticated custody context is absent, so claim cannot authorize.',context='invalid-context/no-authentication.json')
    value=doc('PartialAcceptance');value['acceptedArtifacts'][0]['physicalReceiptId']=value['acceptedArtifacts'][0]['producerCustodyReceiptId'];negative('producer-nas-receipt-alias','PartialAcceptance',value,'DISTINCT_RECEIPT_ROLES')
    value=doc('PartialAcceptance');value['resultManifestSha256']='f'*64;negative('partial-other-manifest','PartialAcceptance',value,'PARTIAL_SAME_MANIFEST')
    value=doc('PartialAcceptance');value['acceptedArtifacts'][0]['stageId']='stage-foreign';negative('partial-other-stage','PartialAcceptance',value,'PARTIAL_SAME_MANIFEST')
    value=doc('PartialAcceptance');value['acceptedArtifacts'][0]['sha256']='f'*64;negative('partial-byte-digest-mismatch','PartialAcceptance',value,'PARTIAL_SAME_MANIFEST')
    value=doc('PartialAcceptance');extra=copy.deepcopy(value['acceptedArtifacts'][0]);extra['sha256']='e'*64;value['acceptedArtifacts'].append(extra);negative('partial-duplicate-id','PartialAcceptance',value,'PARTIAL_SAME_MANIFEST')
    value=doc('ResultManifest','odm');value['products'][0]['targets'][0]['memberRefs'][0]['sha256']='f'*64;value.pop('resultManifestSha256');value=seal(value,'resultManifestSha256');negative('archive-member-digest-mismatch','ResultManifest',value,'MEMBER_CLOSURE')
    value=doc('ResultManifest','odm');value['products'][0]['targets'][0]['memberRefs'][0]['relativePath']='../outside.html';value.pop('resultManifestSha256');value=seal(value,'resultManifestSha256');negative('archive-member-traversal','ResultManifest',value,'ARCHIVE_PATH_SAFETY')
    value=doc('ResultManifest','odm');value['products'][0]['evidenceReceiptId']='acceptance-odm';value.pop('resultManifestSha256');value=seal(value,'resultManifestSha256');negative('preupload-outcome-uses-future-acceptance','ResultManifest',value,'PRODUCT_EVIDENCE_PHASE')
    value=doc('ExecutionSettledReceipt');value['settledStages']=value['settledStages'][:1];negative('combined-intermediate-releases-heavy','ExecutionSettledReceipt',value,'EXECUTION_SETTLEMENT')
    value=doc('HeavyCapacityReleaseReceipt');value['remainingDiskHoldIds']=[];negative('heavy-release-drops-delivery-disk-holds','HeavyCapacityReleaseReceipt',value,'DELIVERY_SEPARATE',phase='schema')
    value=doc('ImmutableMetadataIntent');value['payloadBudgetLedgerId']='replacement-ledger';value.pop('intentSha256');value=seal(value,'intentSha256');negative('metadata-payload-ledger-reset','ImmutableMetadataIntent',value,'SOURCE_REAUTH_INTENT')
    value=doc('ImmutableMetadataIntent');value['purpose']='publication_authority_check';value.pop('intentSha256');value=seal(value,'intentSha256');negative('same-operation-changed-intent','ImmutableMetadataIntent',value,'SOURCE_REAUTH_INTENT')
    value=doc('AuthorizedResponse');value['purpose']='publication_authority_check';value.pop('receiptSha256');value=seal(value,'receiptSha256');negative('metadata-purpose-cross-replay','AuthorizedResponse',value,'SOURCE_REAUTH_AUTHORITY')
    value=doc('AuthorizedResponse');value['metadataExpiresAt']='2026-10-28T11:00:00.000Z';value.pop('receiptSha256');value=seal(value,'receiptSha256');negative('expired-next-stage-admission','AuthorizedResponse',value,'SOURCE_REAUTH_AUTHORITY')
    value=doc('AuthorizedResponse');value['authenticatedActorBindingSha256']='f'*64;value.pop('receiptSha256');value=seal(value,'receiptSha256');negative('self-hashed-foreign-actor','AuthorizedResponse',value,'SOURCE_REAUTH_AUTHORITY')
    value=doc('AuthorizedResponse');value['custodyRevision']=0;value.pop('receiptSha256');value=seal(value,'receiptSha256');negative('authorized-unresolved-custody','AuthorizedResponse',value,'SOURCE_REAUTH_AUTHORITY',phase='schema')
    negative('historical-admission-as-current-publication','SealedStageAdmission',doc('SealedStageAdmission'),'CURRENT_PUBLICATION',context='invalid-context/publication-revoked.json')
    support=supplements(combined);denied=copy.deepcopy(next(v['value'] for v in support['documents'] if v['type']=='DeniedResponse'))
    denied['metadataExpiresAt']='2026-10-28T15:05:00.000Z';denied.pop('receiptSha256');denied=seal(denied,'receiptSha256');negative('denial-grants-expiry','DeniedResponse',denied,'SOURCE_REAUTH_DENIAL',phase='schema')
    denied=copy.deepcopy(next(v['value'] for v in support['documents'] if v['type']=='DeniedResponse'));denied['custodyRevision']=0;denied.pop('receiptSha256');denied=seal(denied,'receiptSha256');negative('denial-unresolved-state-wrong-reason','DeniedResponse',denied,'SOURCE_REAUTH_DENIAL')
    second=next(v['value'] for v in support['documents'] if v['type']=='ImmutableMetadataIntent');negative('new-round-before-unknown-resolved','ImmutableMetadataIntent',second,'SOURCE_REAUTH_INTENT',context='invalid-context/prior-round-unknown.json')
    journal=copy.deepcopy(next(v['value'] for v in support['documents'] if v['type']=='ControlJournalEntry'));journal['attemptCount']=0;negative('metadata-attempt-counter-reset','ControlJournalEntry',journal,'FINITE_CONTROL_BUDGET',context='invalid-context/control-counter-rollback.json')
    value=doc('CleanupAck');value['terminalReceiptId']='execution-settled';negative('execution-settled-is-not-cleanup-delivery-terminal','CleanupAck',value,'CLEANUP_AUTHORITY')
    write('invalid-context/no-authentication.json',dict(status='SYNTHETIC_NEGATIVE_CONTEXT',base='valid/combined.json',trustedCustodyIssuerAuthenticated=False,physicalRetentionVerified=False))
    write('invalid-context/publication-revoked.json',dict(status='SYNTHETIC_NEGATIVE_CONTEXT',base='valid/combined.json',historicalAdmissionValid=True,currentPublicationAuthorized=False,revocationState='denied',retainResultsAndOutbox=True))
    write('invalid-context/prior-round-unknown.json',dict(status='SYNTHETIC_NEGATIVE_CONTEXT',base='valid/combined.json',originalOperationId='metadata-operation-1',originalRound=1,priorState='outcome_unknown',authoritativeResolution=None))
    write('invalid-context/control-counter-rollback.json',dict(status='SYNTHETIC_NEGATIVE_CONTEXT',base='valid/support-records.json',durableMinimumAttemptCount=1,ledgerResetAllowed=False))
    def bad_zip(name,blob,rule):
        path='invalid-artifacts/'+name+'.zip';target=ROOT/path;target.parent.mkdir(exist_ok=True);target.write_bytes(blob)
        entries.append(dict(id=name,inputFile=path,documentType='ArtifactZipBytes',inputSha256=sha(blob),byteSize=len(blob),expectedContext='valid/combined.json',expected='reject',rule=rule,phase='archive',
            note='Validate this physical ZIP under a synthetic expected-custody context for these exact bytes. No actual producer/NAS authority is claimed. Rejection is archive structure/closure, not invented geospatial evidence.'))
    bad_zip('zip-traversal',zip_bytes({'../outside.html':b'bad'}),'ARCHIVE_PATH_SAFETY')
    bad_zip('zip-unlisted-member',zip_bytes({'odm_report/report.html':b'expected','unlisted.bin':b'x'}),'MEMBER_CLOSURE')
    bad_zip('zip-case-collision',zip_bytes({'odm_report/report.html':b'x','odm_report/REPORT.html':b'y'}),'ARCHIVE_PATH_SAFETY')
    target=io.BytesIO()
    with zipfile.ZipFile(target,'w') as archive:
        info=zipfile.ZipInfo('odm_report/report.html',(1980,1,1,0,0,0));info.create_system=3;info.external_attr=0o120777<<16;archive.writestr(info,b'../outside')
    bad_zip('zip-symlink',target.getvalue(),'ARCHIVE_PATH_SAFETY')
    encrypted=bytearray(zip_bytes({'odm_report/report.html':b'x'}));local=encrypted.find(b'PK\x03\x04');central=encrypted.find(b'PK\x01\x02')
    struct.pack_into('<H',encrypted,local+6,struct.unpack_from('<H',encrypted,local+6)[0]|1)
    struct.pack_into('<H',encrypted,central+8,struct.unpack_from('<H',encrypted,central+8)[0]|1)
    bad_zip('zip-encryption-flag',bytes(encrypted),'ARCHIVE_PATH_SAFETY')
    bad_zip('zip-empty-product',zip_bytes({}),'MEMBER_CLOSURE')
    write('invalid/index.json',dict(status='EXPECTED_REJECTION_CASES_NO_VALIDATOR_RUN',cases=entries))


def candidate_index():
    write('candidate.json',dict(candidateVersion=1,status=['UNAGREED','NOTWIRECOMPATIBLE','NOTACTIVATED'],sourcePins=PINS,nativeBundleSourcePin=NATIVE_PIN,schema='contract.schema.json',semanticRules='semantic-rules.json',hashDomains='hash-domains.json',validFixtures=['valid/odm_only.json','valid/treedar_only.json','valid/combined.json','valid/odm_optional_unavailable.json','valid/odm_pointcloud_only.json','valid/support-records.json'],invalidIndex='invalid/index.json',canonicalVectors='canonical-vectors.json',
        invariant='No existing v1 schema, implementation, claim, receipt, config, capability or deployed runtime is modified.',
        technicalCandidateChoices=dict(metadataMaximumDepth=32,archiveMemberPathCharacters=1024,producerRetainUntilSpelling='accepted_copy_ack_or_authorized_release',odmClosureDescriptor='ltds-odm-closure-v1-proposed sidecar supplied in fixture context; actual transport unresolved',requestedProductsDigest='Canonical original stage-ordered {stageId,engine,requestedProducts} array; no options included.',supersededSummary='SourceMetadataReauthorization kept as annotated definition for source coverage; excluded from accepted root message union.'),
        unimplementedGates=['paired schema/lexical/semantic validator acceptance and conformance','actual fixed issuer and actor authentication/endpoints/reconciliation APIs','actual immutable original LAS/LAZ custody and purge-pin enforcement/release policy','actual preupload producer custody/worker semantic evidence integration','actual accepted-copy ingestion/fsync/receipt/outbox factory','separate current publication/catalog authorization and cleanup ACK API','real engine/product/COG/CRS/pointcloud/mesh/bundle semantic qualification','finite deployed archive/raw/expansion/control/history/disk coexistence limits and ledger enforcement','durable all-heavy execution settlement vs pending delivery disk hold integration','migration/no-upgrade/capability negotiation/rollback'],
        semanticReviewQuestions=['Approve technical candidate constants/grammars and requestedProductsDigest domain?','Remove superseded compact reauthorization summary type or explicitly pair a non-authoritative view?','Pair ODM closure descriptor transport and exact EPT/mesh dependency semantics without mandatory standalone duplicates?','Confirm preupload worker ProductOutcome evidence and producer custody are distinct from NAS accepted-copy validation/custody?','Confirm fresh next-stage metadata admission versus immutable historical stage validation and separate current publication authority?','Pair exact trusted issuer/actor/custody/capacity evidence and finite deployment policy before any activation?']))


def main():
    schemas()
    fixtures={mode:fixture(mode,i+1) for i,mode in enumerate(('odm_only','treedar_only','combined'))}
    for mode,value in fixtures.items():write('valid/'+mode+'.json',value)
    write('valid/odm_optional_unavailable.json',fixture('odm_only',4,optional=True))
    write('valid/odm_pointcloud_only.json',fixture('odm_only',5,odm_product='pointcloud'))
    write('valid/support-records.json',supplements(fixtures['combined']))
    declarations();negative_vectors(fixtures);candidate_index()
    write('canonical-vectors.json',{'status':'CODEC_ONLY_NOT_AUTHORITY','vectors':[dict(value=value,canonicalUtf8Base64=base64.b64encode(canonical(value)).decode(),sha256=digest(value)) for value in [dict(label='é🌲',nested=dict(sha256='a'*64)),dict(count=MAX,ordered=[3,2,1]),dict(sha256='outer-field-is-not-omitted-without-domain',child=dict(sha256='b'*64))]]})
    files=[dict(path=str(path.relative_to(ROOT)).replace('\\','/'),byteSize=path.stat().st_size,sha256=sha(path.read_bytes())) for path in sorted(ROOT.rglob('*')) if path.is_file() and path.name!='inventory.json']
    write('inventory.json',dict(status='REVIEW_INVENTORY_NOT_NEGOTIATED_CONTRACT_FINGERPRINT',files=files,inventoryDigest=digest(files)))
    # A generator asserts source fieldset coverage and deterministic bytes. It
    # intentionally supplies no wire parser, semantic validator or authority API.


if __name__=='__main__':main()
