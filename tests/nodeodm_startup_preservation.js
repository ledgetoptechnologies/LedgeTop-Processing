'use strict';
// Run only inside a disposable NodeODM-derived test container with tmpfs data.
const fs = require('fs');
const root = '/var/www/data';
const sourcePath = '/var/www/libs/TaskManager.js';
const id = 'aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee';
const other = '11111111-2222-3333-4444-555555555555';
const scenario = process.env.GUARD_SCENARIO;
const allowed = ['fresh','missing','malformed','wrongshape','duplicate','incomplete','pathid','uppercase','deserialize','validold','oldplusorphan','running','queued','queuedstarted','statusmissing','swapindex','swapdir'];
if (!allowed.includes(scenario)) throw Error('bad scenario');
process.argv = ['node','index.js','--cleanup_tasks_after','0','--cleanup_uploads_after','0'];
const mkdirTask = (uuid) => {
  fs.mkdirSync(root+'/'+uuid); fs.writeFileSync(root+'/'+uuid+'/sentinel','synthetic');
  fs.mkdirSync(root+'/'+uuid+'/images'); fs.mkdirSync(root+'/'+uuid+'/gcp');
};
const task = (uuid) => ({uuid, name:'synthetic', options:[], outputs:[], skipPostProcessing:true,
    dateCreated:1, processingTime:1, status:{code:40}, initialized:true});
if (scenario !== 'fresh') mkdirTask(id);
if (scenario === 'malformed') fs.writeFileSync(root+'/tasks.json','{bad');
if (scenario === 'wrongshape') fs.writeFileSync(root+'/tasks.json','{}');
if (scenario === 'duplicate') fs.writeFileSync(root+'/tasks.json',JSON.stringify([task(id),task(id)]));
if (scenario === 'incomplete') fs.writeFileSync(root+'/tasks.json','[]');
if (scenario === 'pathid') fs.writeFileSync(root+'/tasks.json',JSON.stringify([task('../escape')]));
if (scenario === 'uppercase') fs.writeFileSync(root+'/tasks.json',JSON.stringify([task(id.toUpperCase())]));
if (['deserialize','validold','oldplusorphan','swapindex','swapdir'].includes(scenario)) fs.writeFileSync(root+'/tasks.json',JSON.stringify([task(id)]));
if (scenario === 'running') fs.writeFileSync(root+'/tasks.json',JSON.stringify([{...task(id),status:{code:20}}]));
if (scenario === 'queued') fs.writeFileSync(root+'/tasks.json',JSON.stringify([{...task(id),status:{code:10},dateStarted:0}]));
if (scenario === 'queuedstarted') fs.writeFileSync(root+'/tasks.json',JSON.stringify([{...task(id),status:{code:10},dateStarted:123}]));
if (scenario === 'statusmissing') {const t=task(id);delete t.status;fs.writeFileSync(root+'/tasks.json',JSON.stringify([t]));}
if (scenario === 'oldplusorphan') mkdirTask(other);
require.cache[require.resolve('/var/www/libs/logger.js')] = {exports:{debug(){},warn(){},info(){},error(){}}};
const schedules=[];
require.cache[require.resolve('/var/www/node_modules/node-schedule')] = {exports:{scheduleJob(_when, fn){schedules.push(fn);}}};
const Task = require('/var/www/libs/Task.js');
let attempts = 0;
const original = Task.CreateFromSerialized;
Task.CreateFromSerialized = (value, done) => {
  attempts++;
  if (scenario === 'deserialize') return process.nextTick(() => done(new Error('synthetic restore failure')));
  if (scenario === 'swapindex') fs.writeFileSync(root+'/tasks.json','[]');
  if (scenario === 'swapdir') mkdirTask(other);
  return original.call(Task,value,done);
};
const m=require(sourcePath);
m.initialize(err=>{
  const shouldFail=!['fresh','validold'].includes(scenario);
  const retained=scenario==='fresh'||fs.existsSync(root+'/'+id+'/sentinel');
  const retainedOther=scenario!=='oldplusorphan'||fs.existsSync(root+'/'+other+'/sentinel');
  const noRestoreOnPreflight=!['missing','malformed','wrongshape','duplicate','incomplete','pathid','uppercase','oldplusorphan','running','queued','queuedstarted','statusmissing'].includes(scenario)||attempts===0;
  let scheduledSafe=true;
  if (scenario==='validold' && !err) {
    const manager=m.singleton();
    manager.removeOldTasks=()=>{throw Error('scheduled age deletion');};
    manager.removeStaleUploads=()=>{throw Error('scheduled stale upload deletion');};
    try { if (schedules.length!==1) throw Error('missing schedule'); schedules[0](); }
    catch (_) { scheduledSafe=false; }
  }
  const pass=(!!err===shouldFail)&&retained&&retainedOther&&noRestoreOnPreflight&&scheduledSafe&&(scenario!=='validold'||attempts===1);
  console.log(JSON.stringify({scenario,failed:!!err,retained,retainedOther,attempts,scheduledSafe,pass})); process.exit(pass?0:1);
});
setTimeout(()=>{console.error('timeout');process.exit(3)},12000);
