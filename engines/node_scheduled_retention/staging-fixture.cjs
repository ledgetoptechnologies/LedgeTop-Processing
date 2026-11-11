'use strict';
// REVIEW ONLY. Run only with the documented isolated synthetic mounts.
const fs=require('node:fs'),path=require('node:path'),assert=require('assert').strict,crypto=require('node:crypto'),Module=require('node:module');
process.exitCode=1;const deadline=setTimeout(()=>{console.error('fixture timed out');process.exit(1)},30000);
assert.equal(process.cwd(),'/var/www');
const mode=process.env.FIXTURE_MODE;assert(['on-first','on-restart','off'].includes(mode));
const disabled=mode!=='off';
const nonce=process.env.FIXTURE_NONCE;assert(/^[0-9a-f]{32}$/.test(nonce||''));
const mounts=fs.readFileSync('/proc/self/mountinfo','utf8').split('\n').map(line=>line.split(' '));
for(const [target,expected] of [['/var/www/data','rw'],['/var/www/tmp','rw'],['/var/www/config.js','ro'],['/var/www/libs/TaskManager.js','ro'],['/fixture.cjs','ro']]){const matches=mounts.filter(parts=>parts[4]===target);assert.equal(matches.length,1,`missing isolated mount ${target}`);assert(matches[0][5].split(',').includes(expected),`incorrect mount mode ${target}`)}
const RealDate=Date, future=RealDate.now()+365*24*3600*1000;
global.Date=class extends RealDate{constructor(...args){super(...(args.length?args:[future]))}static now(){return future}};
const originalLoad=Module._load;let periodic;
Module._load=function(name,parent,isMain){
 if(parent?.filename?.startsWith('/var/www/libs/')&&name==='./logger')return {info(){},warn(){},error(){},debug(){},verbose(){}};
 if(parent?.filename==='/var/www/libs/TaskManager.js'&&name==='./ProgressReceiver')return class{addListener(){}};
 if(parent?.filename==='/var/www/libs/TaskManager.js'&&name==='node-schedule')return {scheduleJob(_cron,cb){periodic=cb}};
 return originalLoad.apply(this,arguments);
};
const config=require('/var/www/config');assert.equal(config.disableScheduledCleanup,disabled);
const status=require('/var/www/libs/statusCodes');assert.deepEqual([status.FAILED,status.COMPLETED,status.CANCELED],[30,40,50]);
const ids=['00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000002','00000000-0000-0000-0000-000000000003'];
const orphan='00000000-0000-0000-0000-000000000004',stale='00000000-0000-0000-0000-000000000005';
const data='/var/www/data',tmp='/var/www/tmp';
function sentinel(dir){return path.join(dir,'sentinel.bin')}
function hash(p){return crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex')}
for(const dir of [data,tmp])assert.equal(fs.readFileSync(path.join(dir,'.fixture-marker'),'utf8'),`node-retention-${nonce}\n`);
function seed(){
 assert.deepEqual(fs.readdirSync(data),['.fixture-marker']);assert.deepEqual(fs.readdirSync(tmp),['.fixture-marker']);
 for(const id of ids){for(const sub of ['images','gcp'])fs.mkdirSync(path.join(data,id,sub),{recursive:true});fs.writeFileSync(sentinel(path.join(data,id)),`fixture-${nonce}-${id}`)}
 fs.mkdirSync(path.join(data,orphan));fs.writeFileSync(sentinel(path.join(data,orphan)),`orphan-${nonce}`);fs.mkdirSync(path.join(tmp,stale));fs.writeFileSync(sentinel(path.join(tmp,stale)),`upload-${nonce}`);
 fs.writeFileSync(path.join(data,'tasks.json'),JSON.stringify(ids.map((uuid,i)=>({uuid,name:'synthetic',options:[],outputs:[],skipPostProcessing:true,dateCreated:1,processingTime:1,status:{code:[30,40,50][i]}}))));
}
if(mode!=='on-restart')seed();
const paths=[...ids.map(id=>sentinel(path.join(data,id))),sentinel(path.join(data,orphan)),sentinel(path.join(tmp,stale))];
function verifySeed(){
 assert.deepEqual(fs.readdirSync(data).sort(),[...ids,orphan,'tasks.json','.fixture-marker'].sort());assert.deepEqual(fs.readdirSync(tmp).sort(),['.fixture-marker',stale].sort());
 for(const id of ids){assert.deepEqual(fs.readdirSync(path.join(data,id)).sort(),['gcp','images','sentinel.bin']);for(const sub of ['images','gcp'])assert.deepEqual(fs.readdirSync(path.join(data,id,sub)),[]);assert.equal(fs.readFileSync(sentinel(path.join(data,id)),'utf8'),`fixture-${nonce}-${id}`)}
 for(const [dir,expected] of [[path.join(data,orphan),`orphan-${nonce}`],[path.join(tmp,stale),`upload-${nonce}`]]){assert.deepEqual(fs.readdirSync(dir),['sentinel.bin']);assert.equal(fs.readFileSync(sentinel(dir),'utf8'),expected)}
 const tasks=JSON.parse(fs.readFileSync(path.join(data,'tasks.json'),'utf8'));assert.equal(tasks.length,3);for(let i=0;i<3;i++){const row=tasks.find(t=>t.uuid===ids[i]);assert(row);assert.equal(row.status.code,[30,40,50][i]);assert.equal(row.name,'synthetic')}
}
verifySeed();const before=paths.map(hash);
// Observe completion and error of the scheduler's own asynchronous dump.
const realWrite=fs.writeFile;let dumpCompletion;
fs.writeFile=function(file,body,cb){if(path.resolve(file)===path.resolve(data,'tasks.json'))return realWrite.call(fs,file,body,error=>{cb(error);dumpCompletion?.(error)});return realWrite.apply(fs,arguments)};
function scheduledDump(){return new Promise((resolve,reject)=>{dumpCompletion=error=>error?reject(error):resolve();assert.equal(typeof periodic,'function');periodic()})}

require('/var/www/libs/TaskManager').initialize(async err=>{try{
 if(err)throw err;const mgr=require('/var/www/libs/TaskManager').singleton();
 if(disabled){assert.deepEqual(paths.map(hash),before);assert.deepEqual(Object.keys(mgr.tasks).sort(),ids.slice().sort());await scheduledDump();verifySeed();assert.deepEqual(paths.map(hash),before);if(mode==='on-restart')await new Promise((resolve,reject)=>mgr.remove(ids[2],e=>e?reject(e):resolve()));if(mode==='on-restart'){assert(!fs.existsSync(paths[2]));assert.deepEqual(paths.slice(0,2).map(hash),before.slice(0,2))}clearTimeout(deadline);process.exitCode=0;console.log('fixture passed',mode)}
 else{assert.equal(Object.keys(mgr.tasks).length,0);for(const p of paths)assert(!fs.existsSync(p));clearTimeout(deadline);process.exitCode=0;console.log('fixture passed',mode)}
 }catch(e){console.error(e);process.exitCode=1}});
