'use strict';
// Fixed in-container fixture code. Never accepts an origin, token or user data.
const fs=require('node:fs'),crypto=require('node:crypto'),http=require('node:http');
const [mode,task,owner]=process.argv.slice(2);
if(!/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(task||'')||!/^[a-f0-9]{32}$/.test(owner||''))throw new Error('invalid synthetic identity');
const deadline=setTimeout(()=>process.exit(2),5000);
function request(method,route,body=Buffer.alloc(0),headers={}){
  return new Promise((resolve,reject)=>{
    const req=http.request({host:'127.0.0.1',port:3000,method,path:route,headers:{...headers,'Content-Length':body.length}},res=>{
      let size=0;const parts=[];res.on('data',data=>{size+=data.length;if(size>65536){req.destroy();reject(new Error('response limit'));}else parts.push(data);});
      res.on('end',()=>{try{if(res.statusCode<200||res.statusCode>=300)throw new Error('HTTP status');resolve(JSON.parse(Buffer.concat(parts).toString('utf8')));}catch(e){reject(e);}});
    });req.setTimeout(3000,()=>req.destroy(new Error('timeout')));req.on('error',reject);req.end(body);
  });
}
function checked(value){if(!value||typeof value!=='object'||Array.isArray(value)||value.error||value.success===false)throw new Error('API error');return value;}
function form(name,filename,bytes){const boundary='ltds-'+owner;const filenamePart=filename?`; filename="${filename}"`:'';return {body:Buffer.concat([Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="${name}"${filenamePart}\r\n\r\n`),bytes,Buffer.from(`\r\n--${boundary}--\r\n`)]),headers:{'Content-Type':`multipart/form-data; boundary=${boundary}`}};}
function markerPath(){if(fs.realpathSync('/var/www/data')!=='/var/www/data')throw new Error('unexpected volume path');return '/var/www/data/.ltds-retention-'+owner;}
async function main(){
  if(mode==='device'){
    if(fs.readdirSync('/dev').some(name=>/^(nvidia|dri$|kfd$)/i.test(name)))throw new Error('device visible');
    const status=fs.readFileSync('/proc/self/status','utf8');
    const caps=/^CapEff:\s*([0-9a-f]+)$/mi.exec(status);
    if(!caps||!/^0+$/.test(caps[1]))throw new Error('effective capabilities present');
    const identity=path=>{const s=fs.statSync(path,{bigint:true});return {dev:String(s.dev),ino:String(s.ino)};};
    clearTimeout(deadline);console.log(JSON.stringify({ok:true,mode,userns:identity('/proc/self/ns/user'),
      cgroupns:identity('/proc/self/ns/cgroup'),volume:identity('/var/www/data')}));return;
  } else if(mode==='empty'){
    const info=checked(await request('GET','/info'));
    if(typeof info.version!=='string')throw new Error('invalid info');
    const tasks=await request('GET','/task/list');if(!Array.isArray(tasks)||tasks.length!==0)throw new Error('task list not empty');
  } else if(mode==='init'){
    const input=form('options',null,Buffer.from('[]'));
    const result=checked(await request('POST','/task/new/init',input.body,{...input.headers,'set-uuid':task}));
    if(result.uuid!==task)throw new Error('task identity mismatch');
  } else if(mode==='upload'){
    // Four-byte synthetic placeholder; never committed to reconstruction.
    const input=form('images','fixture.jpg',Buffer.from([255,216,255,217]));
    checked(await request('POST','/task/new/upload/'+task,input.body,input.headers));
  } else if(mode==='info'){
    const value=checked(await request('GET','/task/'+task+'/info'));
    if(value.uuid!==task||value.imagesCount!==1)throw new Error('task or input count differs');
  } else if(mode==='marker-put'){
    const file=markerPath(),fd=fs.openSync(file,fs.constants.O_WRONLY|fs.constants.O_CREAT|fs.constants.O_EXCL|fs.constants.O_NOFOLLOW,0o600);
    try{fs.writeFileSync(fd,'ltds-synthetic-marker-v1');fs.fsyncSync(fd);}finally{fs.closeSync(fd);}
    const dir=fs.openSync('/var/www/data',fs.constants.O_RDONLY|fs.constants.O_DIRECTORY|fs.constants.O_NOFOLLOW);try{fs.fsyncSync(dir);}finally{fs.closeSync(dir);}
  } else if(mode==='marker-check'){
    const file=markerPath(),fd=fs.openSync(file,fs.constants.O_RDONLY|fs.constants.O_NOFOLLOW);
    try{const s=fs.fstatSync(fd);if(!s.isFile()||s.nlink!==1||s.size!==24||fs.readFileSync(fd,'utf8')!=='ltds-synthetic-marker-v1')throw new Error('marker changed');}finally{fs.closeSync(fd);}
  } else if(mode==='remove'){
    const body=Buffer.from('uuid='+encodeURIComponent(task));
    checked(await request('POST','/task/remove',body,{'Content-Type':'application/x-www-form-urlencoded'}));
    const tasks=await request('GET','/task/list');
    // The volume started empty and only this task was created. Refuse every
    // unexpected shape or remaining entry until the real API is qualified.
    if(!Array.isArray(tasks)||tasks.length!==0)throw new Error('task list not empty');
  } else throw new Error('invalid operation');
  clearTimeout(deadline);console.log(JSON.stringify({ok:true,mode}));
}
main().catch(()=>{clearTimeout(deadline);console.error('synthetic probe failed');process.exitCode=1;});
