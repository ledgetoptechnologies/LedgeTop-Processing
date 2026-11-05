"""Existing-key SSH transport for an exact read-only worker probe lifecycle.

No credentials are read into the bundle or argv. SSH host-key verification is
mandatory. The generic helper retains uncertain requests for inspection only.
This module does not restart, signal, update or create a worker service.
"""
import shlex

from .enrolled_coordinator_deployment import worker as validate_worker
from .enrolled_coordinator_transport import BoundedSubprocess, TrackedExec, require, digest, decode, canonical


class _ExistingSSH:
    def __init__(self, worker, binding, *, runner=None):
        self.worker,self.binding,self.runner=worker,binding,runner or BoundedSubprocess()

    def exec_python(self, identifier, source, *, timeout=30, output_limit=1048576, mutation=False):
        require(identifier==self.binding and type(source) is bytes,'exact_existing_worker_request_required')
        # OpenSSH's remote exec channel is a command string, not an argv API.
        # POSIX quoting preserves this fixed reviewed interpreter path as one
        # literal argument. Source/key/config bytes travel only over stdin.
        remote=shlex.join((self.worker['settings']['pythonExecutable'],'-I','-B','-'))
        argv=['/usr/bin/ssh','-o','BatchMode=yes','-o','ConnectTimeout=5','-o','StrictHostKeyChecking=yes',
              'bkoltz@'+self.worker['sshHost'],remote]
        raw=self.runner.run(argv,input_bytes=source,timeout=timeout,output_limit=output_limit,mutation=mutation)
        return decode(raw,output_limit)


class TrackedWorkerProbe(TrackedExec):
    """Host-bound envelope; never labels a worker host as a Docker container."""
    TARGET_FIELD='hostBindingSha256'
    PROTOCOL='ltds-cutover-ssh-exec-v1'

    def __init__(self, worker, *, authority_id, operation_id, operation_directory, runner=None):
        validate_worker(worker,authority_id)
        self.worker=decode(canonical(worker))
        self.host_binding=digest(self.worker)
        super().__init__(_ExistingSSH(self.worker,self.host_binding,runner=runner),operation_id,operation_directory,
                         owner_uid=self.worker['sshUid'])

    def __call__(self, source, *, request_id, timeout=30, output_limit=65536):
        return super().__call__(self.host_binding,source,request_id=request_id,timeout=timeout,output_limit=output_limit)

    def inspect_request(self, source, *, request_id, output_limit=65536, allow_failed=False):
        return super().inspect(self.host_binding,request_id,source,maximum=output_limit,allow_failed=allow_failed)
