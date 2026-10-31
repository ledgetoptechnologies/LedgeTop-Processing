'use strict';
// Build-time exact-source patcher. Never accept other upstream file versions.
const fs=require('fs'); const crypto=require('crypto');
const sourcePath='/var/www/libs/TaskManager.js';
const originals={'89ff48a399f87fdcec78ad49fa48fc8f7dd233c5421645bd816e9e0510916bbd':'da380f38690cfe40625aff19fa76dedba18c5b692833f07d6bcb94e34c4c9d45','9f43fff4882e7fba113c69f73328daa927ebb7e5716414eea5d9225b15d50efb':'892aae3d9d7b7fab3de2d0666e2ba72442283a4ff770ebf2eaaa4aa50a32815a'};
const digest=x=>crypto.createHash('sha256').update(x).digest('hex');
const before=digest(fs.readFileSync(sourcePath)); if (!originals[before]) throw Error('unsupported TaskManager source');
let src = fs.readFileSync(sourcePath,'utf8');
function replaceOnce(old, replacement) {
  if (src.split(old).length !== 2) throw Error('source mismatch');
  src=src.replace(old,replacement);
}
replaceOnce(`        const progressReceiver = new ProgressReceiver();
        progressReceiver.addListener(this.onProgressUpdate.bind(this));`, '');
replaceOnce(`        async.series([
            cb => this.restoreTaskListFromDump(cb),
            cb => this.removeOldTasks(cb),
            cb => this.removeOrphanedDirectories(cb),
            cb => this.removeStaleUploads(cb),`, `        async.series([
            cb => {
                try {
                    const fingerprint=s=>[s.dev,s.ino,s.size,s.mtimeMs,s.ctimeMs].join(':');
                    const rootStat=fingerprint(fs.lstatSync(Directories.data));
                    const entries=fs.readdirSync(Directories.data);
                    if (entries.length>100002) throw new Error('too many entries');
                    const re=/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
                    const onDisk=entries.filter(e => re.test(e));
                    const suspicious=entries.filter(e => /^[0-9a-fA-F-]{36}$/.test(e) && !re.test(e));
                    if (suspicious.length) throw new Error('noncanonical task path');
                    const directoryStates=new Map();
                    for (const name of onDisk) { const st=fs.lstatSync(path.join(Directories.data,name)); if (!st.isDirectory()) throw new Error('invalid task path'); directoryStates.set(name,fingerprint(st)); }
                    let raw, indexState=null;
                    try {
                        const before=fs.lstatSync(TASKS_DUMP_FILE);
                        if (!before.isFile() || before.size>64*1024*1024) throw new Error('invalid index file');
                        const fd=fs.openSync(TASKS_DUMP_FILE,fs.constants.O_RDONLY|fs.constants.O_NOFOLLOW);
                        try {const opened=fs.fstatSync(fd);if(fingerprint(opened)!==fingerprint(before)) throw new Error('changed index');raw=fs.readFileSync(fd,'utf8');if(fingerprint(fs.fstatSync(fd))!==fingerprint(before)) throw new Error('changed index');} finally {fs.closeSync(fd);}
                        if(fingerprint(fs.lstatSync(TASKS_DUMP_FILE))!==fingerprint(before)) throw new Error('changed index');
                        indexState=fingerprint(before);
                    } catch (e) { if(e.code==='ENOENT' && onDisk.length===0) raw='[]'; else throw e; }
                    const tasks=JSON.parse(raw);
                    if (!Array.isArray(tasks)) throw new Error('invalid index');
                    const ids=tasks.map(t=>t && t.uuid);
                    if (ids.some(x=>typeof x!=='string'||!re.test(x)) || new Set(ids).size!==ids.length) throw new Error('invalid ids');
                    const found=new Set(onDisk);
                    if (ids.length!==onDisk.length || ids.some(x=>!found.has(x))) throw new Error('index mismatch');
                    if (tasks.some(t=>!t.status || ![30,40,50].includes(t.status.code))) throw new Error('active or uncertain task');
                    this._guardIndexRaw=raw;
                    this._guardCheck=()=>{
                        if(fingerprint(fs.lstatSync(Directories.data))!==rootStat) throw new Error('root changed');
                        if(indexState!==null) {if(fingerprint(fs.lstatSync(TASKS_DUMP_FILE))!==indexState) throw new Error('index changed');}
                        else {try {fs.lstatSync(TASKS_DUMP_FILE);throw new Error('index appeared');} catch(e) {if(e.code!=='ENOENT') throw e;}}
                        for (const [name,state] of directoryStates) if(fingerprint(fs.lstatSync(path.join(Directories.data,name)))!==state) throw new Error('task directory changed');
                    };
                    this._guardCheck();
                    cb();
                } catch(e) { cb(new Error('task index requires inspection')); }
            },
            cb => this.restoreTaskListFromDump(cb),
            cb => { try {this._guardCheck();cb();} catch(e) {cb(new Error('task state changed; inspection required'));} },
            cb => {
                const progressReceiver=new ProgressReceiver();
                progressReceiver.addListener(this.onProgressUpdate.bind(this));
                cb();
            },`);
replaceOnce('if (done !== undefined) done();\n                    return;\n                }\n\n                async.each(tasks,',
    "if (done !== undefined) done(new Error('task index invalid'));\n                    return;\n                }\n\n                async.each(tasks,");
replaceOnce('fs.readFile(TASKS_DUMP_FILE, (err, data) => {',
    'process.nextTick(() => { const err = null, data = this._guardIndexRaw;');
replaceOnce('                async.each(tasks, (taskJson, done) => {',
    '                async.eachSeries(tasks, (taskJson, done) => {');
replaceOnce("logger.info(`Initialized ${tasks.length} tasks`);\n                    if (done !== undefined) done();",
    "if (err) return done(err);\n                    logger.info(`Initialized ${tasks.length} tasks`);\n                    if (done !== undefined) done();");
replaceOnce(`                    this.removeOldTasks();
                    this.dumpTaskList();
                    this.removeStaleUploads();`, `                    this.dumpTaskList();`);

if (digest(src)!==originals[before]) throw Error('patched TaskManager differs');
fs.writeFileSync(sourcePath,src);
if (digest(fs.readFileSync(sourcePath))!==originals[before]) throw Error('patch persistence failed');
