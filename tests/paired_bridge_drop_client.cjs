'use strict';
// Synthetic-only: no extraction, catalog publication or cleanup authority.
const fs=require('node:fs'),path=require('node:path'),crypto=require('node:crypto'),assert=require('node:assert/strict');
const {ReferenceProvider}=require('/viewer/referenceProvider');
const {ReferenceAcceptanceLedger}=require('/viewer/referenceAcceptance');
const {ReferenceResultJournal,validateResultBinding}=require('/viewer/referenceResultJournal');
const {handleReferenceResult}=require('/viewer/referenceResultBridge');
const protocol=require('/viewer/referenceProtocol');
const mode=process.argv[2],port=Number(process.argv[3]),root='/owned/bridge-state',models=path.join(root,'models');
if(!['first','retry'].includes(mode)||!Number.isInteger(port)||port<1||port>65535)throw Error('fixture arguments refused');
// Fail closed if application code tries any JS-level cleanup. This does not
// intercept native SQLite sidecar behavior and is not a host writer fence.
for(const name of ['unlinkSync','rmSync','rmdirSync'])fs[name]=()=>{throw Error('cleanup forbidden in synthetic fixture');};
for(const name of ['unlink','rm','rmdir'])fs.promises[name]=async()=>{throw Error('cleanup forbidden in synthetic fixture');};
const fixture=JSON.parse(fs.readFileSync('/owned/bridge-fixture.json','utf8'));
const expected=fixture.expected,manifest=fixture.resultManifest,receipt=fixture.acceptance,obj=manifest.objects[0];
const key=protocol.sha256(protocol.canonicalBytes(validateResultBinding(expected,manifest,obj.id)));
assert.equal(key,receipt.storageReceiptId);
const origin='https://fixture.invalid';
const allowed=new Set([`/ltds/v1/jobs/${expected.jobId}/results`,`/ltds/v1/jobs/${expected.jobId}/results/${obj.id}`,`/ltds/v1/jobs/${expected.jobId}/acceptance`]);
const provider=new ReferenceProvider({endpoint:origin,token:'synthetic-viewer-token-only-00001',environment:'staging',enabled:true,maxObjectBytes:1024*1024,
  fetchImpl:async(url,init)=>{const u=new URL(url);if(u.origin!==origin||!allowed.has(u.pathname)||u.search||u.hash)throw Error('fixture route refused');return fetch(`http://127.0.0.1:${port}${u.pathname}`,{...init,redirect:'error'});}});
// The bridge's missing-file fallback invokes its source callback before any
// staging creation. Refuse all later downloads, so a missing pre-staged file
// holds before retainReferenceResult can create/link/unlink anything.
let permitInitialDownload=mode==='first';const download=provider.downloadObject.bind(provider);
provider.downloadObject=async(...args)=>{if(!permitInitialDownload)throw Error('bridge staging creation forbidden');return download(...args);};
function syncDir(dir){const fd=fs.openSync(dir,fs.constants.O_RDONLY|fs.constants.O_DIRECTORY|fs.constants.O_NOFOLLOW);try{fs.fsyncSync(fd);}finally{fs.closeSync(fd);}}
function writeExclusive(target,data){const fd=fs.openSync(target,fs.constants.O_WRONLY|fs.constants.O_CREAT|fs.constants.O_EXCL|fs.constants.O_NOFOLLOW,0o600);try{for(let i=0;i<data.length;){const n=fs.writeSync(fd,data,i,data.length-i);if(n<=0)throw Error('synthetic write failed');i+=n;}fs.fsyncSync(fd);}finally{fs.closeSync(fd);}syncDir(path.dirname(target));}
function sameBytes(file,sha,size){const fd=fs.openSync(file,fs.constants.O_RDONLY|fs.constants.O_NOFOLLOW);try{const st=fs.fstatSync(fd);if(!st.isFile()||st.nlink!==1||st.size!==size)return false;const data=fs.readFileSync(fd);return data.length===size&&crypto.createHash('sha256').update(data).digest('hex')===sha;}finally{fs.closeSync(fd);}}
if(mode==='first'){fs.mkdirSync(root,{mode:0o700});fs.mkdirSync(models,{mode:0o700});syncDir('/owned');syncDir(root);}
const rootFd=fs.openSync(models,fs.constants.O_RDONLY|fs.constants.O_DIRECTORY|fs.constants.O_NOFOLLOW),stat=fs.fstatSync(rootFd,{bigint:true});
// Mock authority only: the named volume is isolated for this test, but this
// callback does not prove production writer exclusion or future custody.
const authority={rootFd,device:stat.dev,inode:stat.ino,assertCurrent:()=>true};
const ledger=new ReferenceAcceptanceLedger({databasePath:path.join(root,'acceptance.sqlite'),managedRoot:models});
const journal=new ReferenceResultJournal({authority,maxBytes:1024*1024});
const managed=path.join(root,'managed-copy.zip'),record=path.join(root,'managed-copy-receipt.json');
const evidence={version:1,jobId:expected.jobId,generation:expected.generation,resultManifestSha256:expected.resultManifestSha256,objectSha256:obj.sha256,objectBytes:obj.byteSize};
const verifyIngest=async()=>{
  try {return JSON.stringify(JSON.parse(fs.readFileSync(record,'utf8')))===JSON.stringify(evidence)&&sameBytes(managed,obj.sha256,obj.byteSize);}
  catch{return false;}
};
const ingestRetained=async({retained})=>{
  if(mode!=='first')throw Error('unexpected duplicate synthetic ingest');
  const source=path.join(models,retained.receipt.relativePath);
  if(!sameBytes(source,obj.sha256,obj.byteSize))throw Error('retained synthetic archive changed');
  writeExclusive(managed,fs.readFileSync(source));writeExclusive(record,Buffer.from(JSON.stringify(evidence),'utf8'));
};
const args={provider,expected,journal,ledger,acceptance:receipt,ingestRetained,verifyIngest,assertAuthority:()=>true};
(async()=>{
  if(mode==='first'){
    // Explicit pre-stage: exercise actual HTTP binary path once, then create
    // the exact journal key. Journal.retain must only verify this existing file.
    await provider.results(expected.jobId,expected);const parts=[];let size=0;
    for await(const part of await provider.downloadObject(expected.jobId,obj.id,{expectedBinding:expected,maxBytes:1024*1024})){size+=part.length;if(size>1024*1024)throw Error('archive bound exceeded');parts.push(part);}
    const bytes=Buffer.concat(parts);assert.equal(bytes.length,obj.byteSize);assert.equal(crypto.createHash('sha256').update(bytes).digest('hex'),obj.sha256);
    writeExclusive(path.join(models,key+'.zip'),bytes);
    permitInitialDownload=false;
    await assert.rejects(handleReferenceResult(args),{code:'UNCERTAIN'});
    assert.equal(ledger.pendingOutbox(Date.now()+15000).length,1);
    fs.writeSync(1,'SYNTHETIC_BRIDGE_PARTIAL_UNCERTAIN\n');process.exit(86);
  }
  // Respect the actual outbox backoff rather than claiming scheduler behavior.
  const deadline=Date.now()+15000;while(!ledger.pendingOutbox().length){if(Date.now()>=deadline)throw Error('outbox backoff deadline');await new Promise(r=>setTimeout(r,100));}
  const pending=ledger.pendingOutbox();assert.equal(pending.length,1);assert.deepEqual(pending[0].payload,receipt);
  const result=await handleReferenceResult(args);assert.equal(result.receiptId,receipt.receiptId);assert.equal(result.fullyAccepted,true);
  assert.equal(ledger.pendingOutbox().length,0);assert.equal(await verifyIngest(),true);
  fs.writeSync(1,'SYNTHETIC_BRIDGE_REOPEN_REPLAY_ACKED\n');process.exit(87);
})().catch(e=>{process.stderr.write(String(e.code||e.name||'fixture failure'));process.exit(1);});
