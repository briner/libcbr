'''
Created on Aug 23, 2011

@author: briner
'''

import logging
import subprocess
#
import config
#from path import clean_it as path_clean_it
from os.path import commonprefix as ospath_commonprefix
from os.path import normpath as ospath_normpath

my_logger=logging.getLogger('MyLogger')

MOUNT_CMD=config.REMOTE_SSH+'/usr/sbin/mount -p'

_do_populate=True

class Mount(object):
    def __init__(self,  str_mount_record):
        '''str_mount _record comes from mount -p, the -p option signifies that it is formated as in /etc/vfstab
        from the vfstab mount the entries are
        device       device       mount      FS      fsck    mount      mount
        to mount     to fsck      point      type    pass    at boot    options'''
        if str_mount_record[-1]=='\n':
            str_mount_record=str_mount_record[:-1]
        (self.device_to_mount
        ,self.device_to_fsck
        ,self.mountpoint
        ,self.fs_type
        ,self.fsck_pass
        ,self.mount_at_boot
        ,self.mount_options)=str_mount_record.split(' ')
    def __str__(self):
        return 'mount(device:%s, mountpoint:%s, fs:%s)'%(self.device_to_mount,  self.mountpoint,  self.fs_type)

_lmount=[]
_do_populate_lmount=True
def get_lmount(under_path=None):
    if _do_populate_lmount:
        populate_lmount()
    lmount=_lmount[:]
    lret=[]
    if under_path:
        under_path=ospath_normpath(under_path)
        for mount in lmount:
            mountpoint=ospath_normpath(mount.mountpoint)
            if under_path==ospath_commonprefix([under_path,mountpoint]):
                lret.append(mount)
        return lret
    return lmount    

def populate_lmount():
    my_logger.debug('refresh mount.populate_lmount')
    global _do_populate_lmount
    global _lmount
    proc=subprocess.Popen(MOUNT_CMD, stdout=subprocess.PIPE, shell=True, cwd='/')
    lout=proc.stdout.readlines()
    retcode=proc.wait()
    if retcode != 0 :
        my_logger.error('the cmd (%s) did not succeed' % MOUNT_CMD  )
    _lmount=[]
    for out in lout:
        _lmount.append(Mount(out))
    _do_populate_lmount=False


def get_mount_by_mountpoint(mountpoint):
    for mount in get_lmount():
        if mount.mountpoint==mountpoint:
            return mount
    return None

def get_mount_by_device_to_mount(device_to_mount):   
    for mount in get_lmount():
        if mount.device_to_mount==device_to_mount:
            return mount
    return None

def umount_device(device_or_path_to_umount):
    global _do_populate
    _do_populate=True
    inst_cmd='umount %s' % device_or_path_to_umount
    proc=subprocess.Popen(inst_cmd, stderr=subprocess.PIPE, shell=True, cwd='/')
    retcode=proc.wait()
    if retcode != 0 :
        lerr=proc.stderr.readlines()
        my_logger.error('the cmd (%s) did not succeed' % inst_cmd  )
        for err in lerr:
            my_logger.error(' stderr: %s' % err.rstrip()  )
        raise Exception('umount %s error' % device_or_path_to_umount)
    
