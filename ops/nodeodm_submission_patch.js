'use strict';
// Build-time patch only for the exact pinned upstream engine sources.
const fs=require('fs'), crypto=require('crypto');
const digest=x=>crypto.createHash('sha256').update(x).digest('hex');
const taskPath='/var/www/libs/taskNew.js', indexPath='/var/www/index.js';
const expected={
  'dc1fa27ec7b94189e98edc44838f54bea604539a13588c1d1c37b6196775bee3':{index:'67e023f8e38c6a72d3b93967241c5e8811a490cbf5005ed9fa27e654f9f8968b',taskOut:'a950e07b2509465b165575e80308351eadfb3ece285865ad5444ffe4bd8244ab',indexOut:'b2931f3f5235f5bc298d6fc6f9481264fbeae49156f8cd961163a71de10fdb2b'},
  '1b803713301db91dd11b8d2b54816fe3827cf5211edbc488e8f81104736f1f38':{index:'a31bc1c4282bf2614982e81e4974a783171916a379fa146900b1dfad8dd2cd6e',taskOut:'b9796fb8f31de5b383136a388723be9428429d9e76462cbb2fcc7cb19547207a',indexOut:'97d67720e8f87f48864ac1b3fcef2bda18552277bb613fe79ead50e931e75f00'}
};
let task=fs.readFileSync(taskPath,'utf8'),index=fs.readFileSync(indexPath,'utf8');
const pair=expected[digest(task)];if(!pair||digest(index)!==pair.index) throw Error('unsupported engine sources');
function edit(which,old,replacement,count=1){
  let current=which==='task'?task:index;
  if(current.split(old).length!==count+1) throw Error('engine source mismatch');
  current=current.split(old).join(replacement);
  if(which==='task')task=current;else index=current;
}
edit('task',`        const die = (error) => {
            res.json({error});
            removeDirectory(srcPath);
        };`,`        const die = (error) => {
            // Preserve existing uploads for inspection and retry reconciliation.
            res.json({error});
        };`,2);
edit('task',`                fs.stat(srcPath, (err, stat) => {
                    if (err && err.code === 'ENOENT') fs.mkdir(srcPath, undefined, cb);
                    else cb(); // Dir already exists
                });`,`                // A retry must never overwrite a pre-existing upload body.
                fs.mkdir(srcPath, undefined, cb);`);
edit('task',`    handleCommit: (req, res, next) => {`,`    checkDestination: (req, res, next) => {
        const destPath = path.join(Directories.data, req.id);
        fs.lstat(destPath, err => {
            if (err && err.code === 'ENOENT') return next();
            res.json({error: 'Task destination exists or cannot be inspected'});
        });
    },

    handleCommit: (req, res, next) => {`);
edit('task',`            // Check if dest directory already exists
            cb => {
                if (req.files && req.files.length > 0) {
                    fs.stat(destPath, (err, stat) => {
                        if (err && err.code === 'ENOENT') cb();
                        else{
                            // Directory already exists, this could happen
                            // if a previous attempt at upload failed and the user
                            // used set-uuid to specify the same UUID over the previous run
                            // Try to remove it
                            removeDirectory(destPath, err => {
                                if (err) cb(new Error(\`Directory exists and we couldn't remove it.\`));
                                else cb();
                            });
                        } 
                    });
                } else {
                    cb();
                }
            },`,`            // Never remove a pre-existing task directory on a UUID collision.
            cb => fs.lstat(destPath, err => {
                if (err && err.code === 'ENOENT') return cb();
                cb(new Error('Task destination exists or cannot be inspected'));
            }),`);
edit('task',`                cb => {
                    const task = new Task(req.id, req.body.name, req.body.options,`,`                cb => fs.lstat(destPath, err => {
                    if (err && err.code === 'ENOENT') return cb();
                    cb(new Error('Task destination exists or cannot be inspected'));
                }),
                cb => {
                    const task = new Task(req.id, req.body.name, req.body.options,`);
edit('task',`                        if (err) {
                            // Cleanup
                            removeDirectory(srcPath);
                            removeDirectory(destPath);
                        } else TaskManager.singleton().processNextTask();`,`                        // On error the task reports failure; all source and
                        // destination files remain for operator reconciliation.
                        if (!err) TaskManager.singleton().processNextTask();`);
edit('index',"app.post('/task/new/init', authCheck, taskNew.assignUUID, formDataParser, taskNew.handleInit);","app.post('/task/new/init', authCheck, taskNew.assignUUID, taskNew.checkDestination, formDataParser, taskNew.handleInit);");
edit('index',"app.post('/task/new/commit/:uuid', authCheck, taskNew.getUUID, taskNew.handleCommit, taskNew.createTask);","app.post('/task/new/commit/:uuid', authCheck, taskNew.getUUID, taskNew.checkDestination, taskNew.handleCommit, taskNew.createTask);");
edit('index',"app.post('/task/new', authCheck, taskNew.assignUUID, taskNew.uploadImages,","app.post('/task/new', authCheck, taskNew.assignUUID, taskNew.checkDestination, taskNew.uploadImages,");
if(digest(task)!==pair.taskOut || digest(index)!==pair.indexOut) throw Error('patched engine sources differ');
fs.writeFileSync(taskPath,task);fs.writeFileSync(indexPath,index);
