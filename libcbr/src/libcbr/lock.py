'''
@author: briner
'''
import os
import logging
import pickle

my_logger=logging.getLogger('MyLogger')

class LockError(Exception):
    def __init__(self, dinfo, message):
        self.dlock_info=dinfo
        super(self, LockError).__init__(message)

class Lock(object):
    def __init__(self, path, dinfo={}):
        self.path=path
        self.is_deleted=False
        self.dinfo=dinfo
        if not os.path.isdir(self.dirname):
            os.mkdir(self.dirname)
        #
        # get the lock
        my_logger.debug('enter in "lock.this" for lock(%s)' % self.filename)
        if not os.path.isfile( self.path ):
            my_logger.debug('take the lock(%s), the easy way' % self.filename)
            # write the lock
            self._write()
            return
        #
        # Ouch, we got a lock 
        pid, unused_is_running, dinfo=self.get_pid_isrunning_dinfo()
        if pid==os.getpid():
            my_logger.debug('take the lock(%s) the funny way, because it was our lock' % self.filename)
        else:
            # check if this process is still running
            is_process_running=self.is_pid_running(pid)
            if is_process_running:
                msg='can not take the lock(%s), because it is already used in an other process(pid:%s)' % (self.filename,pid)
                my_logger.info( msg )
                raise LockError(dinfo, msg)
            else:
                my_logger.debug('take the lock, the hard way (a lock file, with a pid, without the relative process)')
                self._write()
    @property
    def filename(self):
        return os.path.basename(self.path)
    @property    
    def dirname(self):
        return os.path.dirname(self.path)
    def get_pid_isrunning_dinfo(self):
        try:
            fh=file(self.lockpath, 'r')
            pid=int(fh.readline().rstrip())
            dinfo=pickle.loads(fh.read())
            fh.close()
            is_running=self.is_pid_running(pid)
            return (pid, is_running, dinfo)
        except:
            return (None, None, None)
    get_pid_isrunning_dinfo=classmethod(get_pid_isrunning_dinfo)
    def is_pid_running(cls, pid):
        is_process_running=True
        try:
            os.kill(pid,0)
        except:
            is_process_running=False
        return is_process_running
    is_pid_running=classmethod(is_pid_running)
    def remove(self):
        if not self.is_deleted:
            os.remove(self.path)
            self.is_deleted=True
            my_logger.debug('unlocked lock(%s)' % self.filename)            
    def __del__(self):
        self.remove()
    def _write(self):
        fh=open(self.path,'w')
        fh.write('%d\n' % os.getpid() ) 
        fh.write('%s' % pickle.dumps(self.dinfo) )
        fh.close
        my_logger.debug('locked lock(%s)'%self.filename)
