"""Fixture-only trusted host recovery grant; not a production authority."""
import os, sqlite3
from ops.previous_init_witness import Held

class RecoveryGrant:
    def __init__(self,gate_journal,init_witness,wrapper_witness,*,fixture=False):
        if fixture is not True: raise Held('recovery grant not qualified')
        self.path=gate_journal; self.init=init_witness; self.wrapper=wrapper_witness
        self.owner_pid=os.getpid()
    def duplicate_dead_fds(self):
        if os.getpid()!=self.owner_pid: raise Held('original supervisor required')
        db=sqlite3.connect('file:'+str(self.path)+'?mode=ro',uri=True)
        try: row=db.execute('SELECT intent,wrapper_pid,wrapper_birth,init_pid,init_birth FROM gate WHERE id=1').fetchone()
        finally: db.close()
        if row is None or row[0]!='stop_requested': raise Held('durable stopped intent required')
        init_pid,init_parent,init_birth=self.init.binding()
        wrapper_pid,_,wrapper_birth=self.wrapper.binding()
        if (wrapper_pid,wrapper_birth,init_pid,init_birth,init_parent)!=(row[1],row[2],row[3],row[4],row[1]):
            raise Held('witness and durable owner identity mismatch')
        initfd=self.init.duplicate_after_exit()
        try: wrapperfd=self.wrapper.duplicate_after_exit()
        except BaseException:
            os.close(initfd); raise
        return initfd,wrapperfd
