"""Test-only owned-directory lifetime; never delete on uncertain worker exit.

No production authority/cleanup policy lives here. Paths must be new private
children of the explicitly provided persistent task artifact parent. Cleanup
uses original directory descriptors; no glob, generic rmtree or forced kill.
"""
import json
import os
from pathlib import Path
import stat
import tempfile


def require(value,reason):
    if not value:raise RuntimeError(reason)


def identity(value):return (value.st_dev,value.st_ino,value.st_uid)
def leaf_identity(value):return (value.st_dev,value.st_ino,value.st_uid,value.st_mode,value.st_nlink,
                                 value.st_size,value.st_mtime_ns,value.st_ctime_ns)


class RetainedQualificationArtifacts:
    def __init__(self,parent):
        require(os.name=='posix','linux_owned_fixture_artifacts_required')
        self.parent=Path(parent).absolute()
        require(self.parent.resolve()==self.parent and not any(p.is_symlink() for p in self.parent.parents),
                'original_fixture_artifact_parent_required')
        info=self.parent.lstat();self.parent_identity=identity(info)
        require(stat.S_ISDIR(info.st_mode) and info.st_uid==os.geteuid() and stat.S_IMODE(info.st_mode)==0o700,
                'private_persistent_fixture_artifact_parent_required')
        self.root=Path(tempfile.mkdtemp(prefix='factory-fixture-',dir=self.parent))
        self.root_identity=identity(self.root.lstat());self.children=None

    def _check_parent(self):
        info=self.parent.lstat()
        require(identity(info)==self.parent_identity and stat.S_ISDIR(info.st_mode)
                and stat.S_IMODE(info.st_mode)==0o700 and self.parent.resolve()==self.parent
                and not any(p.is_symlink() for p in self.parent.parents),
                'fixture_artifact_parent_changed')

    def _check(self):
        self._check_parent()
        value=self.root.lstat()
        require(identity(value)==self.root_identity and stat.S_ISDIR(value.st_mode)
                and stat.S_IMODE(value.st_mode)==0o700 and self.root.resolve()==self.root,
                'owned_fixture_artifact_directory_changed')

    def _record(self,suffix,value):
        # Parent identity remains independently bound even if the child was
        # renamed/replaced. Never write into an unverified replacement child.
        self._check_parent()
        directory=os.open(self.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
        try:
            require(identity(os.fstat(directory))==self.parent_identity,'fixture_artifact_parent_changed')
            name=self.root.name+'.'+suffix+'.json'
            descriptor=os.open(name,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW|os.O_CLOEXEC,0o600,dir_fd=directory)
            try:
                raw=json.dumps(dict(scope='local-fixture-artifact-lifetime',rootName=self.root.name,
                    rootIdentity=self.root_identity,**value),sort_keys=True,separators=(',',':')).encode()
                with os.fdopen(descriptor,'wb',closefd=False) as stream:stream.write(raw);stream.flush();os.fsync(descriptor)
            finally:os.close(descriptor)
            os.fsync(directory)
        finally:os.close(directory)

    def confirm_children_retired(self,instances):
        """Called only AFTER the actual factory context returned successfully."""
        from ops.qualify_enrolled_worker_preparation import process_identity
        self._check();observations=[]
        require(type(instances) is list and len(instances)==2,'complete_fixture_child_inventory_required')
        for instance in instances:
            child=instance.get('process')
            require(child is not None and child.poll()==0 and type(instance.get('startTime')) is str,
                    'actual_fixture_child_closure_required')
            try:current=process_identity(child.pid)
            except FileNotFoundError:current=None
            require(current!=instance['startTime'],'actual_fixture_child_still_present')
            observations.append(dict(pid=child.pid,startTime=instance['startTime'],exitCode=0,reaped=True))
        require(len({entry['pid'] for entry in observations})==2,'distinct_owned_fixture_children_required')
        self.children=observations

    def _preflight(self,directory,depth=0,counters=None,prefix=(),snapshot=None):
        if counters is None:counters=[0,0]
        if snapshot is None:snapshot={}
        require(depth<=16,'bounded_fixture_artifact_depth_required')
        for name in os.listdir(directory):
            info=os.stat(name,dir_fd=directory,follow_symlinks=False);counters[0]+=1
            snapshot[prefix+(name,)]=leaf_identity(info)
            require(counters[0]<=4096 and info.st_uid==os.geteuid(),'owned_bounded_fixture_artifact_tree_required')
            if stat.S_ISDIR(info.st_mode):
                child=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=directory)
                try:
                    require(identity(os.fstat(child))==identity(info),'fixture_artifact_child_changed')
                    self._preflight(child,depth+1,counters,prefix+(name,),snapshot)
                finally:os.close(child)
            else:
                require(stat.S_ISREG(info.st_mode) and info.st_nlink==1,'original_regular_fixture_artifact_required')
                counters[1]+=info.st_size;require(counters[1]<=64*1024**2,'bounded_fixture_artifact_bytes_required')
        return snapshot

    def _remove_children(self,directory,snapshot,prefix=()):
        names=os.listdir(directory)
        require(set(names)=={key[-1] for key in snapshot if key[:-1]==prefix},'fixture_artifact_inventory_changed')
        for name in names:
            self._check();info=os.stat(name,dir_fd=directory,follow_symlinks=False)
            require(leaf_identity(info)==snapshot[prefix+(name,)],'fixture_artifact_child_changed')
            if stat.S_ISDIR(info.st_mode):
                child=os.open(name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=directory)
                try:
                    require(identity(os.fstat(child))==identity(info),'fixture_artifact_child_changed')
                    self._remove_children(child,snapshot,prefix+(name,))
                finally:os.close(child)
                self._check();require(identity(os.stat(name,dir_fd=directory,follow_symlinks=False))==identity(info),
                                      'fixture_artifact_child_changed')
                os.rmdir(name,dir_fd=directory)
            else:
                require(stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_uid==os.geteuid(),
                        'original_regular_fixture_artifact_required')
                require(identity(os.stat(name,dir_fd=directory,follow_symlinks=False))==identity(info),
                        'fixture_artifact_child_changed')
                os.unlink(name,dir_fd=directory)

    def __enter__(self):self._check();return self

    def __exit__(self,error_type,error,traceback):
        if error_type is not None:
            self._record('retained',dict(retained=True,reason='fixture_or_shutdown_failed'))
            return False
        if self.children is None:
            self._record('retained',dict(retained=True,reason='child_closure_unconfirmed'))
            raise RuntimeError('actual_fixture_child_closure_required')
        try:
            self._check()
            parent=os.open(self.parent,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC)
            try:
                require(identity(os.fstat(parent))==self.parent_identity,'fixture_artifact_parent_changed')
                root=os.open(self.root.name,os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW|os.O_CLOEXEC,dir_fd=parent)
                try:
                    require(identity(os.fstat(root))==self.root_identity,'owned_fixture_artifact_directory_changed')
                    snapshot=self._preflight(root)
                    self._record('cleanup-started',dict(children=self.children,retained=False))
                    self._remove_children(root,snapshot);self._check()
                finally:os.close(root)
                os.rmdir(self.root.name,dir_fd=parent);os.fsync(parent)
            finally:os.close(parent)
        except BaseException:
            self._record('retained',dict(retained=True,reason='owned_cleanup_refused_or_uncertain'))
            raise
        self._record('cleanup-complete',dict(children=self.children,retained=False))
        return False
