'use strict';
const test=require('node:test'),assert=require('node:assert/strict'),fs=require('node:fs'),vm=require('node:vm'),path=require('node:path');
const sourceRoot=process.env.RETENTION_SOURCE_ROOT;if(!sourceRoot)throw new Error('RETENTION_SOURCE_ROOT must point to independently materialized patched source variants');
for(const variant of ['nodeodx-main','nodeodm-small']) test(variant+' scheduled cleanup and operator removal',()=>{
 const source=fs.readFileSync(path.join(sourceRoot,variant,'libs/TaskManager.js'),'utf8');
 function create(disabled){
  const removed=[],scheduled=[],status={FAILED:1,COMPLETED:2,CANCELED:3,QUEUED:4,RUNNING:5};
  const ids=['failed','complete','cancel'];
  class Task{constructor(id){this.uuid=id;this.status={code:status[id==='failed'?'FAILED':id==='complete'?'COMPLETED':'CANCELED']};this.dateCreated=0;this.processingTime=0;this.initialized=true}getStatus(){return this.status.code}isCanceled(){return true}cleanup(cb){removed.push(this.uuid);cb()}serialize(){return {uuid:this.uuid}}static CreateFromSerialized(x,cb){cb(null,new Task(x.uuid))}}
  const asyncStub={series(steps,done){let i=0;function next(e){if(e||i===steps.length)return done&&done(e);steps[i++](next)}next()},eachSeries(items,fn,done){let i=0;function next(e){if(e||i===items.length)return done&&done(e);fn(items[i++],next)}next()},each(items,fn,done){this.eachSeries(items,fn,done)}};
  const virtualFS={readFile(p,cb){cb(null,Buffer.from(JSON.stringify(ids.map(uuid=>({uuid})))) )},writeFile(p,d,cb){cb(null)},readdir(p,cb){cb(null,[p==='tmp'?'staleupload-1-2-3-4':'orphan-1-2-3-4'])},statSync(){return {isDirectory:()=>true}},stat(p,cb){cb(null,{isDirectory:()=>true,ctime:new Date(0)})}};
  const modules={'assert':assert,'../config':{disableScheduledCleanup:disabled,cleanupTasksAfter:1,cleanupUploadsAfter:1,parallelQueueProcessing:1,maxRuntime:0},rimraf:(p,cb)=>{removed.push(p);cb&&cb()},fs:virtualFS,path,'./logger':{info(){},debug(){},warn(){},error(){}},'./Task':Task,'./statusCodes':status,async:asyncStub,'node-schedule':{scheduleJob(cron,cb){scheduled.push(cb)}},'./Directories':{data:'/fake'},'./ProgressReceiver':class{addListener(){}}};
  const module={exports:{}};vm.runInNewContext(source,{require:n=>{if(!(n in modules))throw Error(n);return modules[n]},module,Buffer,Date,console},{filename:variant+'/TaskManager.js'}); module.exports.initialize(()=>{});return {manager:module.exports.singleton(),removed,scheduled};
 }
 const a=create(true);assert.deepEqual(a.removed,[]);assert.deepEqual(Object.keys(a.manager.tasks).sort(),['cancel','complete','failed']);a.scheduled[0]();assert.deepEqual(a.removed,[]);let callback=false;a.manager.remove('complete',()=>callback=true);assert.equal(callback,true);assert.deepEqual(a.removed,['complete']);
 const b=create(false);assert.deepEqual(b.removed.slice(0,3).sort(),['cancel','complete','failed']);assert(b.removed.some(x=>x.includes('orphan')));assert(b.removed.some(x=>x.includes('staleupload')));
});


for(const variant of ['nodeodx-main','nodeodm-small']) test(variant+' explicit configuration only',()=>{
 const source=fs.readFileSync(path.join(sourceRoot,variant,'config.js'),'utf8');
 function read(argv,file={}){const module={exports:{}};const mocks={fs:{readFileSync:()=>Buffer.from(JSON.stringify(file))},minimist:()=>({...argv}),'./libs/utils':{get:(o,k,d)=>Object.hasOwn(o,k)?o[k]:d},'./libs/apps':{sevenZ:'7z',unzip:'unzip'},child_process:{spawnSync:()=>({status:0})}};vm.runInNewContext(source,{module,require:n=>mocks[n],process:{argv:[],env:{},exit(){throw Error('exit')}},console},{filename:variant+'/config.js'});return module.exports.disableScheduledCleanup}
 assert.equal(read({}),false);assert.equal(read({'disable-scheduled-cleanup':true}),true);assert.equal(read({disable_scheduled_cleanup:true}),true);assert.throws(()=>read({disable_scheduled_cleanup:'false'}),/must be a boolean/);assert.equal(read({}, {disableScheduledCleanup:true}),true);assert.throws(()=>read({}, {disableScheduledCleanup:'true'}),/must be a boolean/);
});
