'use strict';
// REVIEW ONLY: actual config + image's real minimist; argv contains tested flags.
const assert=require('assert').strict;
assert(['true','false'].includes(process.env.CONFIG_EXPECT));
assert.equal(require('/var/www/config').disableScheduledCleanup,process.env.CONFIG_EXPECT==='true');
console.log('actual config parser passed');
