'use strict';
// Processing-owned, opt-in cross-app fixture. Production Viewer admission,
// custody, writer fencing and model publication are NOT enabled or proved.
const fs=require('node:fs'),path=require('node:path'),https=require('node:https');
const crypto=require('node:crypto'),readline=require('node:readline');
const PIN='21cb9e40a4ee5bcee0ea97827a393978ac8d5714';
const bad=code=>Object.assign(new Error(code),{code});
const sha=b=>crypto.createHash('sha256').update(b).digest('hex');
const assert=(value,code)=>{if(!value)throw bad(code)};
function privateDirectory(directory){
 assert(path.isAbsolute(directory)&&fs.realpathSync(directory)===directory,'FIXTURE_ROOT');
 let cursor=path.parse(directory).root;
 for(const part of directory.slice(cursor.length).split(path.sep)){if(!part)continue;cursor=path.join(cursor,part);const s=fs.lstatSync(cursor);assert(s.isDirectory()&&!s.isSymbolicLink(),'FIXTURE_ROOT');}
 const s=fs.statSync(directory);assert(s.uid===process.getuid()&&(s.mode&0o077)===0,'FIXTURE_ROOT');
}
function appJobSnapshot(db){
 return db.prepare('SELECT attempt_id,job_id,lease_owner,lease_generation,lease_expires_ms FROM viewer_reference_attempt_journal ORDER BY attempt_id').all().map(r=>({attempt_id:r.attempt_id,job_id:r.job_id,lease_owner:r.lease_owner,lease_generation:r.lease_generation,lease_expires_ms:r.lease_expires_ms}));
}
async function main(){
 assert(process.env.LTDS_VIEWER_APP_SYNTHETIC==='1','SYNTHETIC_FIXTURE_REQUIRED');
 assert(process.platform==='linux'&&Number(process.versions.node.split('.')[0])===24,'LINUX_NODE24_REQUIRED');
 assert(process.env.NODE_TLS_REJECT_UNAUTHORIZED!=='0','TLS_VALIDATION_REQUIRED');
 const closers=[];let active=true;
 try{
 assert(fs.statSync(process.argv[2]).size<=65536,'FIXTURE_CONFIG');
 const config=JSON.parse(fs.readFileSync(process.argv[2],'utf8'));
 assert(config.scope==='processing-viewer-app-joint-synthetic-v1'&&config.viewerCommit===PIN&&config.environment==='staging','FIXTURE_CONFIG');
 const origin=new URL(config.coordinatorOrigin);
 assert(origin.protocol==='https:'&&origin.hostname==='127.0.0.1'&&origin.pathname==='/'&&!origin.username&&!origin.password&&!origin.search&&!origin.hash,'FIXTURE_ORIGIN');
 privateDirectory(config.root);
 assert(fs.statSync(config.viewerManifest).size<=1048576,'FIXTURE_SOURCE_MANIFEST');
 const sourceManifestBytes=fs.readFileSync(config.viewerManifest);
 assert(sourceManifestBytes.length<=1048576&&sha(sourceManifestBytes)===config.viewerManifestSha256,'FIXTURE_SOURCE_MANIFEST');
 const sourceManifest=JSON.parse(sourceManifestBytes),sourcePins=new Map(sourceManifest.files.map(x=>[x.path,x.sha256]));
 assert(sourceManifest.commit===PIN,'FIXTURE_SOURCE_MANIFEST');
 const repositoryRoot=path.resolve(config.viewerRoot,'../..');
 function loadedModules(){
  const loaded=[];
  for(const filename of Object.keys(require.cache)){
   if(filename===__filename)continue;
   const relative=path.relative(repositoryRoot,filename).split(path.sep).join('/');
   assert(!relative.startsWith('../')&&!path.isAbsolute(relative)&&sourcePins.has(relative),'FIXTURE_UNPINNED_MODULE');
   const s=fs.lstatSync(filename);assert(s.isFile()&&!s.isSymbolicLink()&&s.nlink===1&&s.size<=4194304&&fs.realpathSync(filename)===filename,'FIXTURE_UNPINNED_MODULE');
   assert(sha(fs.readFileSync(filename))===sourcePins.get(relative),'FIXTURE_UNPINNED_MODULE');
   loaded.push({path:relative,sha256:sourcePins.get(relative)});
  }
  return loaded.sort((a,b)=>a.path.localeCompare(b.path));
 }
 const marker=path.join(config.root,'synthetic-owner.json');
 const identity=JSON.stringify({scope:config.scope,viewerCommit:PIN,environment:'staging',coordinatorSha256:sha(origin.origin)});
 function atomic(filename,bytes){
  const temp=filename+'.tmp-'+crypto.randomUUID(),fd=fs.openSync(temp,'wx',0o600);
  try{fs.writeFileSync(fd,bytes);fs.fsyncSync(fd);}finally{fs.closeSync(fd)}
  fs.renameSync(temp,filename);const dir=fs.openSync(path.dirname(filename),'r');try{fs.fsyncSync(dir)}finally{fs.closeSync(dir)}
 }
 if(fs.existsSync(marker))assert(fs.readFileSync(marker,'utf8')===identity,'FIXTURE_REOPEN_IDENTITY');
 else{assert(fs.readdirSync(config.root).length===0,'FIXTURE_ROOT_NOT_EMPTY');atomic(marker,identity)}
 const serverRoot=path.join(config.viewerRoot,'server');
 const load=name=>require(path.join(serverRoot,name));
 const {DatabaseSync}=require('node:sqlite');
 const {applyMigrations}=load('database.js'),p=load('referenceProtocol.js');
 const {prepareReferenceSubmission}=load('referenceSubmissionDraft.js');
 const {ReferenceProvider}=load('referenceProvider.js');
 const {ReferenceAppJournal,createSyntheticReferenceDispatcher,processReferenceOne}=load('referenceAppDispatch.js');
 const {ReferenceTransferBroker}=load('referenceTransferBroker.js');
 const {createReferenceBrokerHandler}=load('referenceHttp.js');
 const {ReferenceResultJournal,validateResultBinding}=load('referenceResultJournal.js');
 const {ReferenceAcceptanceLedger}=load('referenceAcceptance.js');
 const {handleReferenceResult}=load('referenceResultBridge.js');
 const {handleTerminal}=load('referenceTerminalDraft.js');
 const dirs={};for(const name of ['input','results','ingested','receipts']){dirs[name]=path.join(config.root,name);if(!fs.existsSync(dirs[name]))fs.mkdirSync(dirs[name],{mode:0o700});privateDirectory(dirs[name])}
 const input=path.join(dirs.input,'Survey_001.JPG');
 if(!fs.existsSync(input))atomic(input,Buffer.from('abc'));
 assert(fs.readFileSync(input).equals(Buffer.from('abc')),'FIXTURE_INPUT_CHANGED');
 const db=new DatabaseSync(path.join(config.root,'application.sqlite'));closers.push(()=>db.close());applyMigrations(db);
 db.exec('PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;');
 const journal=new ReferenceAppJournal(db);
 const provider=new ReferenceProvider({endpoint:origin.origin,token:config.viewerBearer,environment:'staging',enabled:true,
  privateCaPem:fs.readFileSync(config.caFile,'utf8'),maxObjectBytes:4194304});
 const broker=new ReferenceTransferBroker({databasePath:path.join(config.root,'broker.sqlite'),managedRoot:dirs.input,environment:'staging'});
 closers.push(()=>broker.close());
 const handler=createReferenceBrokerHandler({broker,enabled:true});
 const brokerServer=https.createServer({cert:fs.readFileSync(config.caFile),key:fs.readFileSync(config.keyFile)},async(req,res)=>{
  try{if(!await handler(req,res)){res.statusCode=404;res.end()}}catch{res.destroy()}
 });
 closers.push(async()=>{if(brokerServer.listening)await new Promise(resolve=>brokerServer.close(resolve))});
 await new Promise((resolve,reject)=>{brokerServer.once('error',reject);brokerServer.listen(0,'127.0.0.1',resolve)});
 const rootFd=fs.openSync(dirs.results,fs.constants.O_RDONLY|fs.constants.O_DIRECTORY),rootStat=fs.fstatSync(rootFd,{bigint:true});
 closers.push(()=>fs.closeSync(rootFd));
 // EXPLICIT fixture authority: FD/inode checks only, not an issued production
 // custody capability or a cross-process old-writer-drain proof.
 const authority={rootFd,device:rootStat.dev,inode:rootStat.ino,assertCurrent:()=>active};
 const resultJournal=new ReferenceResultJournal({authority,maxBytes:4194304});
 closers.push(()=>resultJournal.db.close());
 const ledger=new ReferenceAcceptanceLedger({databasePath:path.join(config.root,'acceptance.sqlite'),managedRoot:dirs.results});
 closers.push(()=>ledger.db.close());
 let custodyChecks=0,ingestCalls=0;
 const custody={assertCurrent:async({environment,manifest})=>{assert(environment==='staging'&&manifest.objects.every(x=>x.sha256===sha(Buffer.from('abc'))),'SYNTHETIC_CUSTODY');custodyChecks++}};
 const transfer={dispatchOnce:async({provider,job,expected,manifest,signal})=>{
  const caps=await provider.capabilities({signal});
  const minted=broker.mintGrant({jobId:job.jobId,generation:job.generation,manifest,reservation:job.transferRequest,
   reservationAvailable:true,objectFiles:Object.fromEntries(manifest.objects.map(x=>[x.id,input])),
   capabilities:caps,capabilityFingerprint:caps.capabilityFingerprint});
  assert(minted.tokenRecoverable===true,'SYNTHETIC_GRANT_UNCERTAIN');
  await provider.postTransferGrant(job.jobId,minted.grant,expected,{signal});
 }};
 const result={handleResult:async({provider,expected,signal})=>{
  const manifest=await provider.results(expected.jobId,expected,{signal}),object=manifest.objects[0];
  const binding=validateResultBinding(expected,manifest,object.id),storageKey=p.sha256(p.canonicalBytes(binding));
  const filename=path.join(dirs.receipts,expected.jobId+'.json');let acceptance;
  if(fs.existsSync(filename))acceptance=JSON.parse(fs.readFileSync(filename,'utf8'));
  else{acceptance={schemaVersion:1,receiptId:'synthetic-app-'+crypto.randomUUID(),storageReceiptId:storageKey,
   jobId:expected.jobId,generation:expected.generation,environment:expected.environment,
   inputManifestSha256:expected.inputManifestSha256,pipelineDigest:expected.pipelineDigest,resultManifestSha256:manifest.resultManifestSha256,
   acceptedObjectIds:[object.id],acceptedAt:new Date().toISOString(),validationVersion:'ltds-reference-v1'};atomic(filename,JSON.stringify(acceptance))}
  const target=path.join(dirs.ingested,storageKey+'.zip');
  const verify=async({retained})=>fs.existsSync(target)&&sha(fs.readFileSync(target))===object.sha256&&fs.statSync(target).size===object.byteSize&&retained.storageKey===storageKey;
  await handleReferenceResult({provider,expected,journal:resultJournal,ledger,acceptance,signal,
   assertAuthority:()=>active,
   ingestRetained:async({retained})=>{ingestCalls++;const bytes=fs.readFileSync(path.join(dirs.results,retained.receipt.relativePath));assert(sha(bytes)===object.sha256,'FIXTURE_INGEST_DIGEST');if(fs.existsSync(target))assert(fs.readFileSync(target).equals(bytes),'FIXTURE_INGEST_CHANGED');else atomic(target,bytes)},
   verifyIngest:verify});
 }};
 let receiptFactories=0;
 const terminal={handleTerminal:async({provider,expected,signal})=>handleTerminal({provider,ledger,expected,signal,
  receiptFactory:outcome=>{receiptFactories++;return {schemaVersion:1,receiptId:'terminal-'+outcome.jobId,
   storageReceiptId:'terminal-storage-'+outcome.jobId,jobId:outcome.jobId,generation:outcome.generation,
   environment:outcome.environment,inputManifestSha256:outcome.inputManifestSha256,pipelineDigest:outcome.pipelineDigest,
   failureSha256:outcome.failureSha256,acceptedAt:new Date().toISOString(),validationVersion:'ltds-reference-v1'}}})};
 const dispatch=createSyntheticReferenceDispatcher({scope:'synthetic-fixture-only',journal,provider,custody,transfer,result,terminal});
 const productionDisabled=await processReferenceOne({journal,provider},'joint-owner')===false;
 assert(productionDisabled,'PRODUCTION_LANE_MUST_STAY_OFF');
 function snapshot(){return {jobs:appJobSnapshot(db),
  grantCount:broker.db.prepare('SELECT COUNT(*) n FROM reference_grants').get().n,
  terminalReceipts:ledger.db.prepare('SELECT job_id,payload,acked FROM terminal_acceptances ORDER BY job_id').all().map(r=>({jobId:r.job_id,payloadSha256:sha(r.payload),acked:r.acked})),receiptFactories,
  acceptanceCount:ledger.db.prepare('SELECT COUNT(*) n FROM acceptances').get().n,
  acknowledged:ledger.db.prepare('SELECT COUNT(*) n FROM ack_outbox WHERE acked=1').get().n,
  retainedFiles:fs.readdirSync(dirs.results).filter(x=>x.endsWith('.zip')).length,
  ingestedFixtureFiles:fs.readdirSync(dirs.ingested).filter(x=>x.endsWith('.zip')).length,
  sourcePresent:fs.readFileSync(input).equals(Buffer.from('abc')),custodyChecks,ingestCalls,
  modelCount:db.prepare('SELECT COUNT(*) n FROM models').get().n,published:false,productionLaneEnabled:!productionDisabled,
  loadedModules:loadedModules()}}
 const guardNames=['viewer_reference_attempt_admission_unavailable','viewer_reference_provider_unavailable','viewer_reference_protocol_unavailable','viewer_reference_genesis_unavailable','viewer_reference_genesis_no_existing'];
 function guardCatalog(){return guardNames.map(name=>{
  const row=db.prepare("SELECT sql FROM sqlite_master WHERE type='trigger' AND name=?").get(name);assert(row,'FIXTURE_GUARD_MISSING');return {name,sql:row.sql}
 })}
 async function seed(count,{failAfterGuardDrop=false}={}){
  assert(Number.isInteger(count)&&count>=1&&count<=3,'FIXTURE_COUNT');
  const caps=await provider.capabilities(),options=await provider.options();
  // These are NEW synthetic records only. Dropping the explicit hard-off
  // admission triggers in this private fixture is NOT a production migration.
  const guards=guardCatalog();
  db.exec('PRAGMA foreign_keys=OFF');db.exec('BEGIN IMMEDIATE');
  try{
  for(const guard of guards)db.exec('DROP TRIGGER '+guard.name);
  if(failAfterGuardDrop)throw bad('INJECTED_SEED_FAILURE');
  db.exec("UPDATE reference_catalog_projection_state SET scan_complete=1;INSERT INTO projects(id,display_name,status,created_at,updated_at) VALUES('joint-project','Synthetic','active','t','t');INSERT INTO viewer_reference_providers VALUES('joint-reference','Synthetic reference','"+origin.origin+"',0)");
  for(let i=0;i<count;i++){
   const id='joint-'+i;
   db.prepare("INSERT INTO datasets(id,project_id,display_name,storage_mode,root_key,relative_path,status,created_at,updated_at) VALUES(?,'joint-project','Synthetic','managed','datasets',?,'finalized','t','t')").run(id,id);
   db.prepare("INSERT INTO processing_tasks(id,project_id,dataset_id,display_name,status,created_at,updated_at) VALUES(?,'joint-project',?,'Synthetic','draft','t','t')").run(id,id);
   db.prepare("INSERT INTO processing_attempts(id,task_id,dataset_id,attempt_number,options_json,status,created_at,updated_at) VALUES(?,?,?,1,'{}','pending','t','t')").run(id,id,id);
   db.prepare('UPDATE processing_tasks SET active_attempt_id=? WHERE id=?').run(id,id);
   db.prepare("INSERT INTO viewer_reference_attempt_genesis VALUES(?,'joint-reference')").run(id);
   db.prepare("INSERT INTO viewer_reference_attempt_protocols VALUES(?,'joint-reference','reference-v1')").run(id);
   const request=prepareReferenceSubmission({enabled:true,dataset:{id,status:'finalized',storageMode:'managed',files:[{id:'image-1',relativePath:'Survey_001.JPG',byteSize:3,sha256:sha(Buffer.from('abc'))}]},submissionId:crypto.randomUUID(),capabilities:caps,optionsCatalog:options,odmOptions:{}}).submission;
   const text=JSON.stringify(request);db.prepare('INSERT INTO viewer_reference_attempt_journal(attempt_id,submission_id,environment,request_json,request_sha256,input_manifest_sha256) VALUES(?,?,?,?,?,?)').run(id,request.submissionId,'staging',text,sha(text),request.manifest.sha256);
  }
  // Restore the exact original migration footprint before any dispatch/reopen.
  // This private seeding exception is not an app admission implementation.
  for(const guard of guards)db.exec(guard.sql);
  assert(p.canonicalBytes(guardCatalog()).equals(p.canonicalBytes(guards)),'FIXTURE_GUARD_CHANGED');
  assert(db.prepare('PRAGMA foreign_key_check').all().length===0,'FIXTURE_FOREIGN_KEY');db.exec('COMMIT');
  }catch(error){db.exec('ROLLBACK');throw error}finally{db.exec('PRAGMA foreign_keys=ON')}
  for(const row of snapshot().jobs)await dispatch({attemptId:row.attempt_id,owner:'joint-owner'});
  return snapshot();
 }
 process.stdout.write(JSON.stringify({ready:true,brokerOrigin:'https://127.0.0.1:'+brokerServer.address().port,
  guardHashes:guardCatalog().map(x=>({name:x.name,sha256:sha(x.sql)})),loadedModules:loadedModules(),scope:'synthetic-app-draft-real-wire-not-production-admission'})+'\n');
  const lines=readline.createInterface({input:process.stdin,crlfDelay:Infinity});
  closers.push(()=>lines.close());
  for await(const line of lines){
   assert(Buffer.byteLength(line)<=1024,'FIXTURE_COMMAND');const command=JSON.parse(line);
   if(command.action==='close'){process.stdout.write(JSON.stringify({closed:true,...snapshot()})+'\n');break}
   let value;
   if(command.action==='seed')value=await seed(command.count);
   else if(command.action==='seed-failure')value=await seed(command.count,{failAfterGuardDrop:true});
   else if(command.action==='pump'){for(const row of snapshot().jobs)await dispatch({attemptId:row.attempt_id,owner:'joint-owner'});value=snapshot()}
   else if(command.action==='terminal-pump'){
    let uncertain=null;
    for(const row of snapshot().jobs)try{await dispatch({attemptId:row.attempt_id,owner:'joint-owner'})}catch(error){uncertain=error.code||'TERMINAL_UNCERTAIN';break}
    value={...snapshot(),uncertain};
   }
   else if(command.action==='terminal-replay'){
    // Direct saved-ledger replay only: NEVER claim or release an app attempt.
    const before=snapshot().jobs;
    for(const row of before){const record=db.prepare('SELECT request_json FROM viewer_reference_attempt_journal WHERE attempt_id=?').get(row.attempt_id),request=JSON.parse(record.request_json);
     const job=await provider.status(row.job_id,{jobId:row.job_id,environment:'staging',inputManifestSha256:request.manifest.sha256});
     if(['failed','cancelled'].includes(job.status))await terminal.handleTerminal({provider,expected:{jobId:job.jobId,generation:job.generation,environment:job.environment,inputManifestSha256:job.manifestSha256,pipelineDigest:job.pipelineDigest}});
    }
    assert(p.canonicalBytes(snapshot().jobs).equals(p.canonicalBytes(before)),'APP_LEASE_MUST_NOT_CHANGE');value=snapshot();
   }
   else if(command.action==='snapshot')value=snapshot();else throw bad('FIXTURE_COMMAND');
   process.stdout.write(JSON.stringify(value)+'\n');
  }
 }finally{
  active=false;let failed=false;
  for(const close of closers.reverse())try{await close()}catch{failed=true}
  assert(!failed,'FIXTURE_CLEANUP_FAILED');
 }
}
module.exports={appJobSnapshot};
if(require.main===module)main().catch(error=>{process.stderr.write(JSON.stringify({error:error.code||'SYNTHETIC_FIXTURE_FAILED'})+'\n');process.exitCode=1});
