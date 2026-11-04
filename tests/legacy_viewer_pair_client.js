'use strict';
// Test-only client. Run with a separately supervised loopback fixture server.
const assert=require('node:assert/strict');
const crypto=require('node:crypto');
const fs=require('node:fs');
const path=require('node:path');
const readline=require('node:readline');
const root=process.env.VIEWER_SOURCE;
if (!root || !path.isAbsolute(root)) throw new Error('absolute VIEWER_SOURCE required');
const server=path.join(root,'apps/viewer/server');
const {openDatabase}=require(path.join(server,'database'));
const {ProcessingRepository}=require(path.join(server,'processingRepository'));
const {StorageManager}=require(path.join(server,'storageManager'));
const {ViewerRepository}=require(path.join(server,'repository'));
const {NodeOdmProvider}=require(path.join(server,'nodeOdmProvider'));
const {processSubmit,processReconcile,processIngest}=require(path.join(server,'processingWorker'));
const {processOneDerivative}=require(path.join(server,'derivativeWorker'));

async function main() {
  const dataRoot=process.argv[2], port=Number(process.argv[3]), mode=process.argv[4];
  if (!path.isAbsolute(dataRoot)||!Number.isSafeInteger(port)||port<1||port>65535||!['clean','lost','partial','overlap','result','result-retry'].includes(mode)) throw new Error('bad fixture arguments');
  fs.mkdirSync(dataRoot,{mode:0o700});
  for(const name of ['datasets','models','cache','trash','imports'])fs.mkdirSync(path.join(dataRoot,name));
  const db=openDatabase(path.join(dataRoot,'viewer.sqlite'));
  try {
    const processing=new ProcessingRepository(db);
    const storage=new StorageManager({datasetsMount:path.join(dataRoot,'datasets'),modelsMount:path.join(dataRoot,'models'),cacheMount:path.join(dataRoot,'cache'),trashMount:path.join(dataRoot,'trash'),datasetImportMount:path.join(dataRoot,'imports'),storageReserveBytes:0,storageReservePercent:0});
    storage.initialize();
    const project=processing.createProject({displayName:'Synthetic'}), relativePath=crypto.randomUUID();
    const draft=processing.createDataset({projectId:project.id,displayName:'Synthetic',storageMode:'managed',rootKey:'datasets',relativePath});
    const dir=path.join(dataRoot,'datasets',relativePath); fs.mkdirSync(dir);
    const files=Array.from({length:3},(_,i)=>{const relativePath=`photo-${i}.jpg`,body=Buffer.from(`synthetic-${i}`);fs.writeFileSync(path.join(dir,relativePath),body);return {relativePath,byteSize:body.length,sha256:crypto.createHash('sha256').update(body).digest('hex')};});
    processing.finalizeDataset(draft.id,files,crypto.createHash('sha256').update(JSON.stringify(files)).digest('hex'));
    const task=processing.createTask({projectId:project.id,datasetId:draft.id,displayName:'Synthetic'});
    const provider=processing.upsertProvider({type:'nodeodm',displayName:'Fixture',endpoint:`http://127.0.0.1:${port}`,enabled:true});
    const attempt=processing.createAttempt({taskId:task.id,providerId:provider.id,options:{}}),job=processing.claimJob('synthetic-owner');
    // The fixture does not assert real filesystem capacity; all payloads are bytes long.
    storage.requireProcessingHeadroom=()=>({});
    let dropped=false;
    let aborted=false;
    const fetchImpl=async (url,init)=>{if(mode==='overlap'&&!aborted&&url.pathname.includes('/task/new/upload/')){aborted=true;init={...init,signal:AbortSignal.any([init.signal,AbortSignal.timeout(1500)])};}const response=await fetch(url,init);if(mode==='lost'&&!dropped&&url.pathname.includes('/task/new/upload/')){dropped=true;await response.arrayBuffer();throw new Error('synthetic lost upload response');}return response;};
    const deps={processing,storage,config:{},signal:AbortSignal.timeout(15000),adapterFactory:()=>new NodeOdmProvider({endpoint:provider.endpoint,token:'synthetic-viewer-'+'b'.repeat(32),fetchImpl,transferTimeoutMs:5000})};
    if(mode==='lost'||mode==='partial'||mode==='overlap'){
      await assert.rejects(processSubmit(job,deps),mode==='lost'?/synthetic lost upload response/:
        mode==='partial'?error=>error.name==='TypeError' && /fetch failed/i.test(error.message):
        error=>error.name==='TimeoutError');
      assert.equal(processing.getAttemptSubmission(attempt.id).submissionPhase,'uploading');
      assert.equal(processing.getAttemptSubmission(attempt.id).uploadedFileCount,0);
      if(mode==='partial'||mode==='overlap') {
        console.log(JSON.stringify({event:'interrupted',uuid:attempt.providerTaskId,files}));
        const input=readline.createInterface({input:process.stdin})[Symbol.asyncIterator]();
        const command=await input.next();
        assert.equal(command.value,'resume');
      }
    }
    await processSubmit(job,deps);
    assert.equal(processing.getAttemptSubmission(attempt.id).submissionPhase,'committed');
    if(mode==='result'||mode==='result-retry') {
      console.log(JSON.stringify({event:'submitted',uuid:attempt.providerTaskId,files}));
      const input=readline.createInterface({input:process.stdin})[Symbol.asyncIterator]();
      assert.equal((await input.next()).value,'finished');
      // Eliminate the ordinary 5s polling delay only in this synthetic DB.
      db.prepare("UPDATE processing_jobs SET available_at=? WHERE attempt_id=? AND job_type='reconcile' AND status='pending'").run(new Date(0).toISOString(),attempt.id);
      const reconcile=processing.claimJob('synthetic-reconcile');
      assert.equal(reconcile.job_type,'reconcile');
      const credentials={resolve:()=> 'synthetic-viewer-'+'b'.repeat(32)};
      const config={meshDerivativesEnabled:true,processingProviderTransferTimeoutMs:5000,opsBaseUrl:'http://synthetic.invalid'};
      await processReconcile(reconcile,{processing,config,providerCredentials:credentials,signal:AbortSignal.timeout(15000)});
      const ingest=processing.claimJob('synthetic-ingest');
      assert.equal(ingest.job_type,'ingest');
      // Only synthetic bytes are written; simulate enough available storage for ingestion.
      const actualSpace=storage.space.bind(storage);
      storage.space=(name)=>name==='models'?{...actualSpace(name),available:16*1024**3,reserve:0}:actualSpace(name);
      const ingestDeps={processing,repository:new ViewerRepository(db),storage,config,providerCredentials:credentials,signal:AbortSignal.timeout(15000)};
      if(mode==='result-retry') {
        await assert.rejects(processIngest(ingest,ingestDeps),error=>error.code==='provider_request_failed' && error.status===503);
        assert.equal(processing.getAttemptTransferProvenance(attempt.id)?.archive||null,null);
        assert.equal(fs.existsSync(path.join(dataRoot,'models',task.id,attempt.id)),false);
        console.log(JSON.stringify({event:'download-interrupted',uuid:attempt.providerTaskId}));
        assert.equal((await input.next()).value,'retry');
      }
      await processIngest(ingest,ingestDeps);
      const receipt=processing.getAttemptTransferProvenance(attempt.id)?.archive;
      const final=processing.getAttempt(attempt.id);
      assert.ok(receipt && Array.isArray(receipt.archiveFiles) && receipt.archiveFiles.length===6);
      const expectedNames=new Set(['odm_texturing/odm_textured_model_geo.glb','odm_texturing/odm_textured_model_geo.obj','odm_texturing/model.mtl','odm_texturing/model.png','entwine_pointcloud/ept.json','3d_tiles/model/tileset.json']);
      for(const file of receipt.archiveFiles){
        assert.ok(expectedNames.delete(file.relativePath));
        const bytes=fs.readFileSync(path.join(dataRoot,'models',task.id,attempt.id,file.relativePath));
        assert.equal(file.byteSize,bytes.length);
        assert.equal(file.sha256,crypto.createHash('sha256').update(bytes).digest('hex'));
      }
      assert.equal(expectedNames.size,0);
      assert.equal(receipt.archiveManifestSha256,crypto.createHash('sha256').update(JSON.stringify(receipt.archiveFiles)).digest('hex'));
      assert.ok(final.resultModelId && final.resultModelVersionId);
      assert.equal(final.status,'derivatives');
      const repo=new ViewerRepository(db);
      const derivativeConfig={...config,obj2TilesBin:'/opt/obj2tiles/Obj2Tiles',localDerivativesEnabled:false};
      storage.requireDerivativeSpace=()=>({inodeReserve:0});
      const priorInfo=console.info; console.info=(...args)=>console.error(...args);
      try { assert.equal(await processOneDerivative({processing,storage,config:derivativeConfig},'synthetic-derivative'),true); }
      finally { console.info=priorInfo; }
      const completed=processing.getAttempt(attempt.id);
      if(completed.status!=='ready_for_review') console.error('derivative diagnostic',JSON.stringify(processing.listDerivativeJobs(10)));
      assert.equal(completed.status,'ready_for_review');
      assert.ok(repo.getModelVersion(completed.resultModelId,completed.resultModelVersionId));
      console.log(JSON.stringify({event:'ingested',uuid:attempt.providerTaskId,receipt,attemptStatus:completed.status,modelId:completed.resultModelId,versionId:completed.resultModelVersionId}));
      return;
    }
    console.log(JSON.stringify({event:'complete',uuid:attempt.providerTaskId,files,mode,dropped}));
  } finally { db.close(); }
}
main().catch(error=>{console.error(error.stack);process.exitCode=1;});
