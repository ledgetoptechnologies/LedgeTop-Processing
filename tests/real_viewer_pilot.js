'use strict';
// A single owned pilot, with persistent state. This is not a production worker.
// Credential is consumed from stdin, never argv, environment, receipt or logs.
const fs=require('node:fs');
const path=require('node:path');
const crypto=require('node:crypto');
const assert=require('node:assert/strict');
const source=process.env.VIEWER_SOURCE;
if(!source||!path.isAbsolute(source)) throw new Error('Viewer source required');
const srv=path.join(source,'apps/viewer/server');
const {openDatabase}=require(path.join(srv,'database'));
const {ProcessingRepository}=require(path.join(srv,'processingRepository'));
const {ViewerRepository}=require(path.join(srv,'repository'));
const {StorageManager,hashFile}=require(path.join(srv,'storageManager'));
const {NodeOdmProvider}=require(path.join(srv,'nodeOdmProvider'));
const {processOne}=require(path.join(srv,'processingWorker'));
const {processOneDerivative}=require(path.join(srv,'derivativeWorker'));

function saveExclusive(file, value){
  const fd=fs.openSync(file,fs.constants.O_WRONLY|fs.constants.O_CREAT|fs.constants.O_EXCL|fs.constants.O_NOFOLLOW,0o600);
  try{fs.writeFileSync(fd,JSON.stringify(value));fs.fsyncSync(fd);}finally{fs.closeSync(fd);}
  const parent=fs.openSync(path.dirname(file),fs.constants.O_RDONLY|fs.constants.O_DIRECTORY);
  try{fs.fsyncSync(parent);}finally{fs.closeSync(parent);}
}
function safeBasename(name){return typeof name==='string'&&name.length>0&&name===path.basename(name)&&
  !name.includes('\\')&&!name.includes('/')&&/\.(jpe?g)$/i.test(name);}
async function main(){
  const root=process.env.PILOT_ROOT, endpoint=process.env.PILOT_ENDPOINT;
  const mode=process.env.PILOT_MODE, deadlineSeconds=Number(process.env.PILOT_DEADLINE_SECONDS||'7200');
  if(!root||!path.isAbsolute(root)||!fs.statSync(root).isDirectory()||fs.realpathSync(root)!==root||
     !Number.isInteger(deadlineSeconds)||deadlineSeconds<30||deadlineSeconds>21600) throw new Error('invalid_pilot_config');
  const url=new URL(endpoint);
  if(url.protocol!=='http:'||url.username||url.password||url.pathname!=='/'||url.search||url.hash||
     !((mode==='live'&&url.hostname==='192.168.50.89'&&url.port==='3005')||
       (mode==='fake'&&url.hostname==='127.0.0.1'&&Number(url.port)>0))) throw new Error('invalid_pilot_endpoint');
  const token=fs.readFileSync(0,'utf8').trim();
  if(token.length<16||token.length>1024||/[\r\n\0]/.test(token)) throw new Error('invalid_credential');
  const receiptPath=path.join(root,'pilot-receipt.json');
  if(fs.existsSync(receiptPath)||fs.existsSync(path.join(root,'viewer.sqlite'))) throw new Error('pilot_already_started');
  const manifest=JSON.parse(fs.readFileSync(path.join(root,'input-manifest.json'),'utf8'));
  const owner=JSON.parse(fs.readFileSync(path.join(root,'owner.json'),'utf8'));
  if(!/^[a-f0-9]{32}$/.test(manifest.owner)||manifest.owner!==owner.owner||
     manifest.schemaVersion!==1||!Array.isArray(manifest.files)||manifest.files.length<20||manifest.files.length>25) throw new Error('invalid_input_manifest');
  if(new Set(manifest.files.map(f=>String(f.relativePath).toLowerCase())).size!==manifest.files.length) throw new Error('duplicate_input');
  for(const file of manifest.files){
    if(!safeBasename(file.relativePath)||!Number.isSafeInteger(file.byteSize)||file.byteSize<1||!/^[a-f0-9]{64}$/.test(file.sha256)) throw new Error('invalid_input');
    const p=path.join(root,'input',file.relativePath),st=fs.lstatSync(p);
    if(!st.isFile()||st.isSymbolicLink()||st.size!==file.byteSize||await hashFile(p)!==file.sha256) throw new Error('input_changed');
  }
  // The launcher must mount the same trusted, private registry for every pilot
  // on this host. This is not a cross-host coordinator fence. Do not delete
  // this marker on timeout, error, completion or process death.
  const registry=process.env.PILOT_REGISTRY;
  if(registry!=='/pilot-registry'||!fs.statSync(registry).isDirectory()||fs.realpathSync(registry)!==registry) throw new Error('invalid_pilot_registry');
  const inventorySha256=crypto.createHash('sha256').update(JSON.stringify(manifest.files)).digest('hex');
  saveExclusive(path.join(registry,'active.json'),{schemaVersion:1,owner:manifest.owner,inventorySha256,mode});
  // Both records precede provider access. A failure retains them; a second
  // local pilot cannot initialize another remote task without manual review.
  saveExclusive(path.join(root,'pilot-started.json'),{schemaVersion:1,owner:manifest.owner,mode});
  const adapter=new NodeOdmProvider({endpoint,token,providerType:'nodeodm',transferTimeoutMs:6*3600_000});
  const cap=(await adapter.capabilities()).capabilities;
  if(cap.engine!=='ledgetop-processing'||cap.maxParallelTasks!==(mode==='live'?1:2)||!['gltf','pc-ept'].every(n=>cap.options.some(o=>o.name===n&&o.type==='bool'))) throw new Error('provider_preflight_failed');
  for(const n of ['datasets','models','cache','trash','imports']) fs.mkdirSync(path.join(root,n),{mode:0o700});
  const db=openDatabase(path.join(root,'viewer.sqlite'));
  try{
    const processing=new ProcessingRepository(db),repo=new ViewerRepository(db);
    const storage=new StorageManager({datasetsMount:path.join(root,'datasets'),modelsMount:path.join(root,'models'),cacheMount:path.join(root,'cache'),trashMount:path.join(root,'trash'),datasetImportMount:path.join(root,'imports'),storageReserveBytes:20*1024**3,storageReservePercent:10});
    storage.initialize();
    const project=processing.createProject({displayName:'Owned staging pilot'}),relativePath=crypto.randomUUID();
    const draft=processing.createDataset({projectId:project.id,displayName:'Owned staging pilot',storageMode:'managed',rootKey:'datasets',relativePath});
    const dir=path.join(root,'datasets',relativePath);fs.mkdirSync(dir,{mode:0o700});
    for(const file of manifest.files){const from=path.join(root,'input',file.relativePath),to=path.join(dir,file.relativePath);fs.copyFileSync(from,to,fs.constants.COPYFILE_EXCL);const fd=fs.openSync(to,'r');try{fs.fsyncSync(fd);}finally{fs.closeSync(fd);}if(await hashFile(to)!==file.sha256)throw new Error('dataset_copy_mismatch');}
    const files=manifest.files.map(f=>({relativePath:f.relativePath,byteSize:f.byteSize,sha256:f.sha256}));
    processing.finalizeDataset(draft.id,files,crypto.createHash('sha256').update(JSON.stringify(files)).digest('hex'));
    const task=processing.createTask({projectId:project.id,datasetId:draft.id,displayName:'Owned staging pilot'});
    const provider=processing.upsertProvider({type:'nodeodm',displayName:'Owned staging pilot',endpoint,enabled:true});
    const attempt=processing.createAttempt({taskId:task.id,providerId:provider.id,options:{gltf:true,'pc-ept':true}});
    // Persist identity before any initialize/upload/commit call.
    saveExclusive(receiptPath,{schemaVersion:1,owner:manifest.owner,mode,projectId:project.id,datasetId:draft.id,taskId:task.id,attemptId:attempt.id,providerTaskId:attempt.providerTaskId});
    console.log(JSON.stringify({event:'created',owner:manifest.owner,attemptId:attempt.id,providerTaskId:attempt.providerTaskId}));
    const config={meshDerivativesEnabled:true,processingProviderTransferTimeoutMs:6*3600_000,opsBaseUrl:'http://pilot.invalid',obj2TilesBin:'/opt/obj2tiles/Obj2Tiles',localDerivativesEnabled:false};
    const deps={processing,repository:repo,storage,config,providerCredentials:{resolve:()=>token}};
    const until=Date.now()+deadlineSeconds*1000;let last='';
    while(Date.now()<until){
      await processOne(deps,'owned-pilot');
      const state=processing.getAttempt(attempt.id),submission=processing.getAttemptSubmission(attempt.id);
      const status=state.status+':'+submission.submissionPhase;
      if(status!==last){console.log(JSON.stringify({event:'status',status:state.status,submission:submission.submissionPhase}));last=status;}
      if(state.status==='derivatives'){
        const oldInfo=console.info;console.info=(...args)=>console.error(...args);
        try{await processOneDerivative({processing,storage,config},'owned-pilot-derivative');}finally{console.info=oldInfo;}
      }
      const after=processing.getAttempt(attempt.id);
      if(after.status==='ready_for_review'){
        const proof=processing.getAttemptTransferProvenance(attempt.id)?.archive;
        assert.ok(proof&&Array.isArray(proof.archiveFiles)&&proof.archiveFiles.length>0);
        for(const file of proof.archiveFiles){const p=path.join(root,'models',task.id,attempt.id,...file.relativePath.split('/'));assert.equal((await hashFile(p)),file.sha256);assert.equal(fs.statSync(p).size,file.byteSize);}
        const model=repo.getModelVersion(after.resultModelId,after.resultModelVersionId);
        const tiles=model?.activeVersion?.assets?.find(a=>a.kind==='tiles');assert.ok(tiles?.sha256);
        const tilePath=storage.resolve('models',tiles.relativePath,{mustExist:true});assert.equal(await hashFile(tilePath),tiles.sha256);
        saveExclusive(path.join(root,'pilot-complete.json'),{schemaVersion:1,owner:manifest.owner,attemptId:attempt.id,providerTaskId:attempt.providerTaskId,archiveManifestSha256:proof.archiveManifestSha256,tilesSha256:tiles.sha256,files:proof.archiveFiles.length});
        console.log(JSON.stringify({event:'ready_for_review',files:proof.archiveFiles.length}));return;
      }
      if(['failed','cancelled'].includes(after.status))throw new Error('pilot_terminal_failure');
      await new Promise(resolve=>setTimeout(resolve,2000));
    }
    throw new Error('monitor_deadline_reached');
  } finally {db.close();}
}
main().catch(error=>{if(process.env.PILOT_MODE==='fake')console.error(error.stack);else console.error(JSON.stringify({event:'held',code:/^[a-z_]+$/.test(error.message)?error.message:'pilot_failed'}));process.exitCode=1;});
