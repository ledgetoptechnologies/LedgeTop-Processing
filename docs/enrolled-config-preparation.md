# Exclusive coordinator configuration preparation

`ConfigPreparation` prepares configuration artifacts for the existing enrolled
cutover plan. It does not start the coordinator, stop a worker, construct a
queue, change old permissions, overwrite current configs, mount processing state
or leases, or access NAS data. No helper was invoked on a live host while writing
this code.

Inputs are existing operator-owned private0600 files with the reviewed bootstrap
and final hashes. The existing startup-policy builder must reproduce the pinned
policy from those bytes and the reviewed schema/source inventory. Payload bytes
travel only over stdin, never argv, environment, images or logs.

The bounded helper uses the immutable candidate image, UID/GID10001, isolated
bytecode-disabled Python, no init, read-only root filesystem, network none,
one CPU/128 MiB/64 PIDs, dropped capabilities, no-new-privileges and no logging.
Its only mount is the exact existing configuration root. The original read-only
mount provides an independently observed directory identity and local-filesystem
check before creation. Source/package hashes are checked inside the read-only
image before any write. Existing modes and ownership remain unchanged.

The helper creates only `first-cutover-<operationId>` with exclusive mode0700 and
two exclusive no-follow mode0600 regular files. It writes, fsyncs, rereads and
hashes both files, fsyncs the operation/root directories, and verifies descriptor
closure. A failure retains partial new files and exits nonzero; it never retries
or repairs a partial directory. The controller never promotes a failed helper
merely because some bytes happen to match.

Creation and start each have durable control intents. A lost result is inspected
using the exact owned immutable helper and existing non-following config TAR
readback; it never causes another start automatically. Successful readback also
works through the original stopped container's retained read-only mount. Verified
cleanup removes only the known stopped helper, without force or volume removal,
and retains the service-owned files for the final recipe.

The writer's initial local Linux tests passed four actual synthetic-directory
cases plus three command/recipe cases. Subsequent source/package and cleanup
wiring changes are compile-checked but await their affected Linux suite and
actual Docker qualification. This is not installed/configuration activation
evidence. TrueNAS permissions and client data were not changed.
