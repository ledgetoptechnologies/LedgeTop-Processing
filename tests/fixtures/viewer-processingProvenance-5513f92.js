'use strict';
const crypto=require('node:crypto');
const path=require('node:path');
const stable=value=>Array.isArray(value)?value.map(stable):value&&typeof value==='object'?Object.fromEntries(Object.keys(value).sort().map(key=>[key,stable(value[key])])):value;
const sha=value=>crypto.createHash('sha256').update(JSON.stringify(stable(value))).digest('hex');
const secretKey=/(?:token|password|secret|credential|authorization|api[-_]?key)/i;
function safe(value,key=''){
  if(secretKey.test(key)||(typeof value==='string'&&(/https?:\/\/[^\s/]+@/i.test(value)||/[?&](?:token|key|secret|password)=/i.test(value))))return{redacted:true,sha256:sha(value)};
  if(Array.isArray(value))return value.map(item=>safe(item));
  if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([name,item])=>[name,safe(item,name)]));
  return value;
}
function inputInventory(datasetFiles=[],gcpSnapshot=null){
  const sourceFiles=datasetFiles.map(file=>({relativePath:file.relativePath,byteSize:file.byteSize,sha256:file.sha256,processingRole:file.processingRole||'auto'}));
  const photo=/\.(?:jpe?g|png|tiff?|dng|raw|heic)$/i;
  let selected=sourceFiles.filter(file=>!['gcp_source','administrative'].includes(file.processingRole)&&(file.processingRole!=='auto'||photo.test(file.relativePath)));
  if(gcpSnapshot){selected=selected.filter(file=>path.posix.basename(file.relativePath).toLowerCase()!=='gcp_list.txt');selected.push({relativePath:'gcp_list.txt',byteSize:Buffer.byteLength(gcpSnapshot.content),sha256:gcpSnapshot.sha256});}
  const inputFiles=[...selected.filter(file=>photo.test(file.relativePath)),...selected.filter(file=>!photo.test(file.relativePath))].map(({relativePath,byteSize,sha256})=>({relativePath,byteSize,sha256}));
  return{sourceFiles,sourceFilesSha256:sha(sourceFiles),inputFiles,inputManifestSha256:crypto.createHash('sha256').update(JSON.stringify(inputFiles)).digest('hex')};
}
function submissionProvenance(attempt,provider,createdAt,{dataset,gcpSnapshot}={}){
  const caps=provider.capabilities||{},schema=Array.isArray(caps.options)?caps.options.map(option=>Object.fromEntries(['name','type','domain','help','value','rawDefault'].filter(key=>Object.hasOwn(option,key)).map(key=>[key,key==='value'||key==='rawDefault'?safe(option[key],option.name):safe(option[key])]))):[];
  const capabilities=Object.fromEntries(['apiVersion','engine','engineVersion','providerType','detectionMethod','maxImages','maxParallelTasks'].filter(key=>Object.hasOwn(caps,key)).map(key=>[key,safe(caps[key])]));
  const defaults=Object.fromEntries(schema.filter(option=>typeof option.name==='string'&&Object.hasOwn(option,'value')&&option.value!==null).map(option=>[option.name,option.value]));
  const submittedOptions=safe(attempt.options||{}),snapshot={schemaVersion:1,capturedAt:createdAt,attemptId:attempt.id,datasetId:attempt.datasetId,datasetManifestSha256:dataset?.manifestSha256||null,gcpSnapshotSha256:gcpSnapshot?.sha256||null,providerId:provider.id,providerTaskId:attempt.providerTaskId,providerType:provider.type,
    capabilityFingerprint:attempt.capabilityFingerprint||null,capturedCapabilityFingerprint:provider.capabilityFingerprint||null,
    capabilityFingerprintMatches:Boolean(attempt.capabilityFingerprint&&attempt.capabilityFingerprint===provider.capabilityFingerprint),
    capabilities,schema,schemaSha256:sha(schema),submittedOptions,submittedOptionsSha256:sha(attempt.options||{}),
    resolvedOptionsFromAdvertisedDefaults:{...defaults,...submittedOptions},
    defaultsAuthority:provider.type==='clusterodm'?'cluster-reference-worker-advertisement':'provider-advertisement',
    producingEngineVerified:false,...(Array.isArray(dataset?.files)?inputInventory(dataset.files,gcpSnapshot):{})};
  return{...snapshot,snapshotSha256:sha(snapshot)};
}
function providerResultEvidence(attempt,status,observedAt){
  if(status?.uuid!==attempt.providerTaskId||status.status!=='completed')return null;
  const identity=value=>typeof value==='string'&&/^[A-Za-z0-9._+ -]{1,120}$/.test(value)?value:null;
  return{schemaVersion:1,observedAt,providerTaskId:status.uuid,status:'completed',statusCode:Number.isSafeInteger(status.statusCode)?status.statusCode:null,
    imagesCount:Number.isSafeInteger(status.imagesCount)&&status.imagesCount>=0?status.imagesCount:null,
    engine:identity(status.engine),engineVersion:identity(status.engineVersion),engineEvidence:'task-info-response',producingEngineVerified:false};
}
module.exports={submissionProvenance,providerResultEvidence,inputInventory};
