// Build a Viewer database with the Viewer's own migrations (Viewer 821186f4),
// then optionally add one in-flight processing job through its real schema.
// Synthetic rows only; the database lives in a temporary folder.
'use strict';
const path = require('path');
const [viewerRoot, databasePath, mode] = process.argv.slice(2);
const { openDatabase } = require(path.join(viewerRoot, 'apps/viewer/server/database.js'));
const database = openDatabase(databasePath);
if (mode === 'inflight') {
  const t = new Date().toISOString();
  database.exec('PRAGMA foreign_keys=OFF');  // synthetic rows without the full task/model graph
  database.prepare("INSERT INTO processing_providers (id,provider_type,display_name,endpoint,created_at,updated_at) VALUES ('p1','nodeodm','Synthetic','http://192.0.2.10:3005',?,?)").run(t, t);
  database.prepare("INSERT INTO processing_attempts (id,task_id,attempt_number,provider_id,options_json,status,created_at,updated_at) VALUES ('a1','t1',1,'p1','{}','uploading',?,?)").run(t, t);
  database.prepare("INSERT INTO processing_jobs (id,attempt_id,job_type,status,available_at,created_at,updated_at) VALUES ('j1','a1','submit','pending','2999-01-01T00:00:00.000Z',?,?)").run(t, t);
}
const tables = database.prepare("SELECT COUNT(*) AS n FROM sqlite_master WHERE type='table'").get().n;
console.log(JSON.stringify({ tables, mode: mode || 'empty', journal: database.prepare('PRAGMA journal_mode').get().journal_mode }));
// Leave it open in WAL mode with uncheckpointed writes until the checker has run.
if (process.argv[5] === 'hold') setTimeout(() => database.close(), 20000); else database.close();
