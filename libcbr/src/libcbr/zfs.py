#!/usr/bin/python
# -*- coding: utf-8 -*-
'''
Created on Aug 23, 2011

@author: briner
'''

import subprocess
import logging
my_logger = logging.getLogger('MyLogger')
import unittest
import stackfunction
import os



#within pydinf
import mix
import mount as module_mount
import path
import notification 

LOCK_MAX_WAIT=300  # seconds = 5 min
LOCK_TIMEOUT=86400 # seconds = 24 hours

ZFS_SNAPSHOT_CMD="zfs snapshot %(isrecursive)s %(property_value)s %(zfsname_at_snapname)s" 
ZFSALLSNAP_CMD="/usr/local/bin/zfsallsnap snapshot -b -i -c %(zpoolname)s@%(snapname)s"
ZFSREMOVEALLSNAP_CMD="/usr/local/bin/zfsallsnap destroy %(zpoolname)s@%(snapname)s"
ZFSDESTROY_CMD="zfs destroy %s"
#
ZFS_LIST_CMD_LPROP_VALUE=['name', 'origin', 'ch.unige:created_by', 'ch.unige:no_snapshots', 'zoned'
                         ,'ch.unige.dolly:mountpoint', "ch.unige.dolly:zone"]
ZFS_LIST_CMD="zfs list -H -o %s -t filesystem" % ','.join(ZFS_LIST_CMD_LPROP_VALUE)
ZFS_SET_CMD="zfs set %s=%s %s"
#
ZPOOL_LIST_CMD='zpool list -H -o name'
#
SNAPSHOT_LIST_CMD_LPROP_VALUE=['name', 'ch.unige:expiration_datetime', 'ch.unige:created_by', 'ch.unige:no_snapshots'
                              ,'ch.unige.dolly:mountpoint', "ch.unige.dolly:zone", "ch.unige.dolly:do_not_keep"]
SNAPSHOT_LIST_CMD="zfs list -H -o %s -t snapshot" % ','.join(SNAPSHOT_LIST_CMD_LPROP_VALUE)
#
KEEP_SNAPSHOT=False


###############################################################################
###             C O D E

#
# verification on definition
# insure that lprop_value sur zfs list does ask for 'name', 'origin' values
missing_zfs=[property for prop in ['name','origin'] if prop not in  ZFS_LIST_CMD_LPROP_VALUE]
if missing_zfs:
    raise ValueError('ZFS_LIST_CMD_LPROP_VALUE must have these properties() included' % ','.join(missing_zfs))
# insure that lprop_value sur zfs list does ask for 'name', 'origin' values
missing_snap=[prop for prop in ['name'] if prop not in  SNAPSHOT_LIST_CMD_LPROP_VALUE]
if missing_snap:
    raise ValueError('ZNAPSHOT_LIST_CMD_LPROP_VALUE must have these properties() included' % ','.join(missing_snap))

#
# functions
def set_prop_value(prop, value, name):
    inst_cmd=ZFS_SET_CMD % (prop, value, name)
    my_logger.debug('zfs set cmd(%s):' % inst_cmd)
    proc=subprocess.Popen(inst_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd='/')
    lstdout=proc.stdout.readlines()
    lstderr=proc.stderr.readlines()
    proc.communicate()
    retcode=proc.wait()
    if retcode != 0:
        lmsg=['the cmd (%s) did not succeed' % inst_cmd]
        for line in lstdout:
            lmsg.append(' - stdout: %s' % line.rstrip())
        for line in lstderr:
            lmsg.append(' - stderr: %s' % line.rstrip())
        notification.notify.add(lmsg)
        for msg in lmsg:
            my_logger.error(msg)
        raise Exception( 'zfs set problem')
    
def destroy(name):
    inst_cmd=ZFSDESTROY_CMD % name
    my_logger.debug('zfs destroy cmd(%s):' % inst_cmd)
    proc=subprocess.Popen(inst_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd='/')
    lstdout=proc.stdout.readlines()
    lstderr=proc.stderr.readlines()
    proc.communicate()
    retcode=proc.wait()
    if retcode != 0:
        lmsg=['the cmd (%s) did not succeed' % inst_cmd]
        for line in lstdout:
            lmsg.append(' - stdout: %s' % line.rstrip())
        for line in lstderr:
            lmsg.append(' - stderr: %s' % line.rstrip())
        notification.notify.add(lmsg)
        for msg in lmsg:
            my_logger.error(msg)
        raise Exception( 'zfs destroy problem')
    

def clone_zfs(snapshotname, new_zfsname, doption={}):
    zfs=get_lzfs().by_name(snapshotname.split("@")[0])
    fun_unlock=stackfunction.stack_function.add(
        zfs.zpool.unlock_it,                                                      
        title="unlock file'semaphore of zpool(%s)" % zfs.zpool.name)
    zfs.zpool.lock_it()
    #
    property_value_str=' '.join("-o %s=%s" % (p,v) for p,v in  doption.iteritems())
    cmd_clone="/usr/sbin/zfs clone %s %s  %s" % (property_value_str
                                                   ,snapshotname, new_zfsname)
    #
    proc=subprocess.Popen(cmd_clone, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd='/')
    my_logger.debug('zfs clone cmd:'+cmd_clone)
    lstdout=proc.stdout.readlines()
    lstderr=proc.stderr.readlines()
    proc.communicate()
    retcode=proc.wait()
    if retcode != 0:
        lmsg=['the cmd (%s) did not succeed' % cmd_clone]
        for line in lstdout:
            lmsg.append(' - stdout: %s' % line.rstrip())
        for line in lstderr:
            lmsg.append(' - stderr: %s' % line.rstrip())
        notification.notify.add(lmsg)
        for msg in lmsg:
            my_logger.error(msg)
        raise Exception( 'clone_zfs')
    #
    fun_unlock()

######################################
#
#      class zpool, zfs, snapshot
#
######################################


class Zpool(object):
    zpoolname_locked=''
    def __init__(self,  name):
        self.name=name
        self.lzone=[]
        self._lzfs=None # ! this is a list
#        self.dsm_sys=None
    def __str__(self):
        return 'zpool(%s)' % (self.name)
    __repr__=__str__
    @property
    def uniq_value(self):
        """to comply with UniqList """
        return self.name
    @property
    def lzfs(self):
        if None != self._lzfs:
            return self._lzfs
        self._lzfs=[]
        for zfs in get_lzfs():
            if zfs.zpool == self:
                self._lzfs.append(zfs)
    def lock_it(self):
        zpool_lockname="/var/run/unige_zfs_%s.lock" % self.name        
        my_logger.debug('"lock_zpool" zpool(%s) with file(%s)' % (self.name, zpool_lockname))
        if self.__class__.zpoolname_locked:
            if self.name==self.__class__.zpoolname_locked:
                my_logger.debug('"lock_zpool" zpool(%s): was already done' % self.name)
                return
            else:
                msg='can not lock more than 1 zpool, zpool_locked(%s), zpool_to_lock(%s)' % (self.zpoolname_locked, self.name)
                raise Exception(msg)
        inst_cmd="/usr/bin/lockfile -l %s -r %s -1 %s" % (LOCK_TIMEOUT, LOCK_MAX_WAIT, zpool_lockname)
        proc=subprocess.Popen(inst_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd='/')
        proc.communicate()
        retcode=proc.wait()
        if retcode != 0:
            my_logger.error('the cmd (%s) did not succeed' % inst_cmd)
            raise Exception( 'lock zpool(%s) failed' % self.name)
        self.__class__.zpoolname_locked=self.name
        my_logger.debug('"lock_zpool" zpool(%s): done' % self.name)
    def unlock_it(self):
        zpool_lockname="/var/run/unige_zfs_%s.lock" % self.name        
        my_logger.debug('"unlock_zpool" zpool(%s) by removing file(%s)' % (self.__class__.zpoolname_locked, zpool_lockname))
        os.remove(zpool_lockname)
        my_logger.debug('"unlock_zpool" zpool(%s): done' % self.__class__.zpoolname_locked)
        self.__class__.zpoolname_locked=''

class ZfsError(Exception):pass
class ZfsErrorIncoherent(ZfsError):
    def __init__(self,msg):
        self.msg=msg
    def __str__(self):
        return repr(self.msg)

class Zfs(object):
    def __init__(self, lvalue):
        self._origin=None
        self.duser_prop_value={}
        for prop, value in zip(ZFS_LIST_CMD_LPROP_VALUE, lvalue):
            if prop.find(':') != -1 :
                self.duser_prop_value[prop]=value              
#                self.user_property_2_indented_class(prop, value)
            else:
                value=value if value != '-' else None
                prop="_"+prop if prop in ['origin'] else prop
                setattr(self,prop,value)
    @property
    def origin(self):
        if self._origin:
            return get_lsnapshot().by_name(self._origin)
        else:
            ret=None
        return ret
    @origin.setter
    def origin(self,value):
        self._origin=None if value == '-' else value 
    @property
    def uniq_value(self):
        """to comply with UniqList """
        return self.name
    def __str__(self):
        return 'zfs(%s)' % (self.name)
    __repr__=__str__
    def _get_zpool(self):
        zpoolname=self.name.split('/')[0]
        zpool=get_lzpool().by_name(zpoolname)
        if not zpool:
            msg='zfs(%s) has not zpool attached ' %self.name
            raise ZfsErrorIncoherent(msg)
        return zpool
    zpool=property(_get_zpool)
    def get_mountpoint_from_lmount(self):
        mount_entry=module_mount.get_mount_by_device_to_mount(self.name)
        if mount_entry:
            return mount_entry.mountpoint
        else:
            return None
    def unmount(self):
        msg='zfs umount zfs(%s)' % self.name
        my_logger.debug(msg)
        module_mount.umount_device(self.name)
    def _get_lsnapshot(self):
        return [snapshot for snapshot in get_lsnapshot() if snapshot.zfs==self]
    lsnapshot=property(_get_lsnapshot)
    def cmp_by_mountpoint_from_lmount(self, a,b):
        a_m=path.CPath( a.get_mountpoint_from_lmount() )
        b_m=path.CPath( b.get_mountpoint_from_lmount() )
        return path.CPath.__cmp__(a_m, b_m)
#        if not a_m:
#            return 1
#        if not b_m:
#            return -1
#        if (not a_m )and(not b_m ):
#            return 0
#        return mix.cmpAlphaNum(a_m, b_m)
    cmp_by_mountpoint_from_lmount=classmethod(cmp_by_mountpoint_from_lmount)
    def cmp_by_name(cls, a,b):
        return mix.cmpAlphaNum(a.name, b.name)
    cmp_by_name=classmethod(cmp_by_name)
    def do_snapshot(self, snapname, isrecursive=False, doption={}):
        zfsname_at_snapname=self.name+"@"+snapname
        isrecursive_str= '-r' if isrecursive else ''
        property_value_str=' '.join("-o %s=%s" % (p,v) for p,v in  doption.iteritems())
        inst_cmd=ZFS_SNAPSHOT_CMD % {'zfsname_at_snapname': zfsname_at_snapname
                               ,'isrecursive' : isrecursive_str
                               ,'property_value':property_value_str}
        #
        my_logger.info(inst_cmd)
        proc=subprocess.Popen(inst_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd='/')
        proc.communicate()
        retcode=proc.wait()
        if retcode != 0:
            my_logger.error('the cmd (%s) did not succeed' % inst_cmd)
            raise Exception( 'zfsallsnap problem')      


class Snapshot(object):
    def __init__(self, lvalue):
        self._zfsname=None
        self._snapname=None
        self.duser_prop_value={}
        for prop, value in zip(SNAPSHOT_LIST_CMD_LPROP_VALUE, lvalue):
            if prop.find(':') != -1 :
                self.duser_prop_value[prop]=value          
#                self.user_property_2_indented_class(prop, value)
            else:
                value=value if value != '-' else None
                prop="_"+prop if prop in ['name'] else prop
                setattr(self,prop,value)
    @property
    def uniq_value(self):
        """to comply with UniqList """
        return self.name
    @property
    def name(self):
        return self._name
    @property
    def zfsname(self):
        if self._zfsname==None:
            self._zfsname, self._snapname=self._name.split('@')
        return self._zfsname
    @property
    def snapname(self):
        if self._snapname==None:
            self._zfsname, self._snapname=self._name.split('@')
        return self._snapname
    @property
    def zfs(self):
        zfs=get_lzfs().by_name(self.zfsname)
        if zfs:
            return zfs
        else:
            print self.zfsname
            print '  '+'\n  '.join([zfs.name for zfs in get_lzfs()])
            raise Exception('snapshot(%s) has to be related of a zfs' % self.name)
    @property
    def lclone(self):
        ret=[zfs for zfs in get_lzfs() if zfs.origin==self]
        return ret
    def __cmp__(self, other):
        if isinstance(other, Snapshot):
            return mix.cmpAlphaNum(self.name, other.name)
        else:
            return -1
    def cmp_by_name(cls, a, b):
        return mix.cmpAlphaNum(a.name, b.name)
    cmp_by_name=classmethod(cmp_by_name)
#    def destroy(self):
#        destroy(self.name)
#        del self
    def __str__(self):
        return 'snap(%s)' % self.name

######################################
#
#          S N A P S H O T
#
######################################

class SnapshotList(list):
    def by_name(self, name):
        lret=[snap for snap in self if snap.name==name ]
        return lret[0] if lret else None
    def by_snapname(self, snapname):
        lret=SnapshotList([snap for snap in self if snap.snapname==snapname ])
        return lret if lret else None
    def by_zfsname(self, zfsname):
        lret=SnapshotList([snap for snap in self if snap.zfsname==zfsname ])
        return lret if lret else None
    def by_zfs(self, zfs):
        lret=SnapshotList([snap for snap in self if snap.zfsname==zfs.name ])
        return lret if lret else None

_lsnapshot=[]
_do_populate_lsnapshot=True
def get_lsnapshot():
    #        
    if _do_populate_lsnapshot==True:
        populate_lsnapshot()
    lret=SnapshotList(_lsnapshot[:])
    return lret

def populate_lsnapshot():
    my_logger.debug('refresh zfs.populate_lsnapshot')
    global _lsnapshot
    global _do_populate_lsnapshot
    #
    cmd=SNAPSHOT_LIST_CMD
    proc=subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, cwd='/')
    lout=proc.stdout.readlines()
    retcode=proc.wait()
    if retcode != 0 :
        my_logger.error('the cmd (%s) did not succeed' % cmd)
    lout=[out.rstrip() for out in lout]
    #
    _lsnapshot=mix.UniqList()
    for out in lout:
        snap=Snapshot(out.rstrip().split('\t'))
        _lsnapshot.append(snap)
    _do_populate_lsnapshot=False

######################################
#
#              Z F S
#
######################################

class ZfsList(list):
    def by_name(self, name):
        lret=[zfs for zfs in self if zfs.name==name]
        return lret[0] if lret else None
    def by_path(self, path):
        df_entry=mix.DiskFree.of(path)
        if not df_entry:
            return None
        mount_entry=module_mount.get_mount_by_mountpoint(df_entry.fs_mountpoint)
        if not mount_entry:
            return None
        zfs_holding_path=get_lzfs().by_name(mount_entry.device_to_mount)
        return zfs_holding_path
    def under_path(self, under_path):
        lmount=module_mount.get_lmount(under_path=under_path)
        lzfs=[]
        for mount in lmount:
            zfs=get_lzfs().by_name(mount.device_to_mount)
            if zfs:
                lzfs.append(zfs)
        return lzfs


_lzfs=[]
_do_populate_lzfs=True
def get_lzfs():
    #
    if _do_populate_lzfs:
        populate_lzfs()
    lret=ZfsList(_lzfs[:])
    return lret

def refresh_lzfs():
    global _do_populate_lzfs
    _do_populate_lzfs=True
    
def populate_lzfs():
    my_logger.debug('refresh zfs.populate_lzfs')
    global _lzfs
    global _do_populate_lzfs
    cmd=ZFS_LIST_CMD
    proc=subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, cwd='/')
    lout=proc.stdout.readlines()
    retcode=proc.wait()
    _lzfs=mix.UniqList()
    if retcode != 0 :
        my_logger.error('the cmd (%s) did not succeed' % cmd)
    _lzfs=ZfsList()
    for out in lout:
        zfs=Zfs(out.rstrip().split('\t') )
        _lzfs.append(zfs)
    _do_populate_lzfs=False

######################################
#
#              Z P O O L
#
######################################


_lzpool=[]
_do_populate_lzpool=True
class ZpoolList(list):
    def by_name(self, name):
        lret=[zpool for zpool in self if zpool.name==name]
        return lret[0] if lret else None

def get_lzpool():
    if _do_populate_lzpool:
        populate_lzpool()
    return ZpoolList(_lzpool[:])

def refresh_lzpool():
    global _do_populate_lzpool
    _do_populate_lzpool=True

def populate_lzpool():
    my_logger.debug('refresh zfs.populate_lzpool')
    global _lzpool
    global _do_populate_lzpool
    _lzpool=mix.UniqList()
    cmd=ZPOOL_LIST_CMD
    proc=subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, cwd='/')
    lout=proc.stdout.readlines()
    retcode=proc.wait()
    if retcode != 0 :
        my_logger.error('the cmd (%s) did not succeed' % cmd)
    for out in lout:
        zpool=Zpool(out.rstrip() ) 
        _lzpool.append( zpool )
    _do_populate_lzpool=False

#==============================================
# Test
#==============================================
class Test(unittest.TestCase):
    def test_populate_zpool(self):
        zpool=get_zpool_by_name('backup-test1_pool')
        self.assertTrue(bool(zpool), 'get_zpool_ba_name ok')
    def test_populate_zfs(self):
        zfs1=get_zfs_by_name('backup-test2_pool/backup-test2/usr')
        zfs2=get_zfs_by_name('backup-test2_pool')
        is_ok=bool(zfs1 and zfs2)
        self.assertTrue(is_ok, 'zfs ok')
    def test_get_zpool_from_zfs(self):
        zfs=get_zfs_by_name('backup-test2_pool/backup-test2/home')
        zpool=get_zpool_by_name('backup-test2_pool')
        self.assertTrue(zfs.zpool==zpool, 'can not find the corresponding zpool from a zfs ')
