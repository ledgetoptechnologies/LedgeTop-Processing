'use strict';
// Disposable container only: actual index.js HTTP server and taskNew module.
const fs=require('fs'), http=require('http'), child=require('child_process'), assert=require('assert');
const root='/var/www/tests/data', tmp='/var/www/tmp', port=3189;
const mode=process.env.SUBMIT_SCENARIO;
if(!['init-existing','init-retry','commit-existing','async-error','commit-lost-ack'].includes(mode)) throw Error('scenario');
const id='11111111-2222-4333-8444-555555555555', src=tmp+'/'+id, dest=root+'/'+id;
fs.writeFileSync('/tmp/proof-config.json',JSON.stringify({test:true,port,logger:{logDirectory:'/tmp'},maxImages:1}));
const proc=child.spawn(process.execPath,['/var/www/index.js','--config','/tmp/proof-config.json'],{cwd:'/var/www',stdio:'ignore'});
const stop=()=>{if(proc.exitCode===null)proc.kill('SIGTERM');};
const request=(method,path,headers={},body=Buffer.alloc(0))=>new Promise((resolve,reject)=>{
 const req=http.request({host:'127.0.0.1',port,method,path,headers:{...headers,'Content-Length':body.length},timeout:3000},res=>{const chunks=[];res.on('data',x=>chunks.push(x));res.on('end',()=>{try{resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));}catch(e){reject(e);}});});
 req.on('error',reject);req.on('timeout',()=>req.destroy(Error('timeout')));req.end(body);
});
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
async function main(){
 for(let i=0;;i++){try{await request('GET','/info');break;}catch(e){if(i===120)throw Error('server not ready');await sleep(250);}}
 const init=()=>request('POST','/task/new/init',{'set-uuid':id});
 if(mode==='init-existing'){
  fs.mkdirSync(dest);fs.writeFileSync(dest+'/sentinel','original');const r=await init();assert(r.error);assert.strictEqual(fs.readFileSync(dest+'/sentinel','utf8'),'original');
 } else if(mode==='init-retry'){
  const first=await init();assert.strictEqual(first.uuid,id);const body=fs.readFileSync(src+'/body.json');fs.writeFileSync(src+'/sentinel','upload');const second=await init();assert(second.error);assert(fs.readFileSync(src+'/body.json').equals(body));assert.strictEqual(fs.readFileSync(src+'/sentinel','utf8'),'upload');
 } else if(mode==='commit-existing'){
  fs.mkdirSync(src);fs.writeFileSync(src+'/body.json','{}');fs.writeFileSync(src+'/sentinel','upload');fs.mkdirSync(dest);fs.writeFileSync(dest+'/sentinel','original');const r=await request('POST','/task/new/commit/'+id);assert(r.error);assert.strictEqual(fs.readFileSync(dest+'/sentinel','utf8'),'original');assert.strictEqual(fs.readFileSync(src+'/body.json','utf8'),'{}');assert.strictEqual(fs.readFileSync(src+'/sentinel','utf8'),'upload');
 } else {
  const first=await init();assert.strictEqual(first.uuid,id);
  const boundary='synthetic-boundary';const body=Buffer.from('--'+boundary+'\r\nContent-Disposition: form-data; name="images"; filename="one.jpg"\r\nContent-Type: image/jpeg\r\n\r\none\r\n--'+boundary+'\r\nContent-Disposition: form-data; name="images"; filename="two.jpg"\r\nContent-Type: image/jpeg\r\n\r\ntwo\r\n--'+boundary+'--\r\n');
  const up=await request('POST','/task/new/upload/'+id,{'Content-Type':'multipart/form-data; boundary='+boundary},body);assert.strictEqual(up.success,true);
  if(mode==='commit-lost-ack'){
    await new Promise(resolve=>{const r=http.request({host:'127.0.0.1',port,method:'POST',path:'/task/new/commit/'+id,headers:{'Content-Length':0}});r.on('error',()=>{});r.on('finish',()=>{r.destroy();resolve();});r.end();});
  } else {const committed=await request('POST','/task/new/commit/'+id);assert.strictEqual(committed.uuid,id);}
  let info;for(let i=0;i<100;i++){info=await request('GET','/task/'+id+'/info');if(info.status&&info.status.code===30)break;await sleep(100);}
  assert(info.status&&info.status.code===30);assert(fs.existsSync(dest+'/images/one.jpg'));assert(fs.existsSync(dest+'/images/two.jpg'));
  // Model a lost commit acknowledgement: a repeated commit cannot remove or
  // replace the already-created destination, regardless of its response shape.
  const retry=await request('POST','/task/new/commit/'+id);assert(retry.error);assert.strictEqual(fs.readFileSync(dest+'/images/one.jpg','utf8'),'one');assert.strictEqual(fs.readFileSync(dest+'/images/two.jpg','utf8'),'two');
 }
 console.log(JSON.stringify({mode,pass:true}));stop();await new Promise(r=>proc.once('exit',r));
}
main().catch(e=>{console.error('synthetic test failed:',e.message);stop();setTimeout(()=>process.exit(1),500);});
