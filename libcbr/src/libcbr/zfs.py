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

LOCK_MAX_WAIT=300  # seconds = 5 min
LOCK_TIMEOUT=86400 # seconds = 24 hours

ZFS_SNAPSHOT_CMD="zfs snapshot %(isrecursive)s %(property_value)s %(zfsname_at_snapname)s"
#ZFSALLSNAP_CMD="/usr/local/bin/zfsallsnap snapshot -b -i -c %(zpoolname)s@%(snapname)s"
#ZFSREMOVEALLSNAP_CMD="/usr/local/bin/zfsallsnap destroy %(zpoolname)s@%(snapname)s"
ZFSDESTROY_CMD="zfs destroy %s"
#
ZFS_SET_CMD="zfs set %s=%s %s"
#
ZPOOL_LIST_CMD='zpool list -H -o name'

ZFS_LIST_CMD_LPROP_VALUE=['name'
                         ,'type'
                         ,'origin'
                         ,'zoned'
                         ,'mountpoint'
                         ,'mounted'
                         ,'readonly'
                         ,'ch.unige:created_by'
                         ,'ch.unige:no_snapshots'
                         ,'ch.unige.dolly:mountpoint'
                         ,'ch.unige.dolly:zone'
                         ,'ch.unige:expiration_datetime'
                         ,'ch.unige.dolly:do_not_keep'
                         ,'ch.unige.dolly:unmount_datetime']
ZFS_LIST_CMD="zfs list -H -o %s -t filesystem,volume,snapshot" % ','.join(ZFS_LIST_CMD_LPROP_VALUE)
ZFS_LIST_CMD_4_ZPOOL="zfs list -H -o %s -t filesystem,volume,snapshot -r %%s" % ','.join(ZFS_LIST_CMD_LPROP_VALUE)

#
#VOLUME_LIST_CMD_LPROP_VALUE=["name", "ch.unige:created_by", 'ch.unige.dolly:mountpoint', "ch.unige.dolly:zone",
#                             "ch.unige.dolly:do_not_keep"]
#VOLUME_LIST_CMD="zfs list -H -o %s -t volume" % ','.join(VOLUME_LIST_CMD_LPROP_VALUE)
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


class ZfsError(Exception):pass
class ZfsErrorIncoherent(ZfsError):
    def __init__(self,msg):
        global lzfs_dump
        self.msg=msg
        self.lzfs_dump=lzfs_dump[:]
        my_logger.error("ZFS DUMP starting")
        for line in self.lzfs_dump:
            my_logger.error(line)
        my_logger.error("ZFS DUMP ended")

    def __str__(self):
        return repr(self.msg)
    def notify_it(self):
        my_logger.error('the cmd (%s) in function(%s) did not succeed' %
                        (self.function_name, self.inst_cmd) )
        for stdout_or_stdin, msg in self.lstdouterr:
            my_logger.error(" - %s : %s" % (stdout_or_stdin, msg))

class ZfsUnableToLockZpool(ZfsError):
    def __init__(self,zpoolname):
        my_logger.error("unable to get the lock for zpool(%s)"%zpoolname)


class ZfsCmdError(ZfsError):
    def __init__(self,function_name, inst_cmd, lstdouterr):
        self.function_name=function_name
        self.inst_cmd=inst_cmd
        self.lstdouterr=lstdouterr
    def __str__(self):
        lret=["libcbr.zfs.%s with inst_cmd(%s)" % (self.function_name, self.inst_cmd)]
        for stdouterr, msg in self.lstdouterr:
            lret.append(' - %s : %s' % (stdouterr, msg))
        return os.linesep.join(lret)
    def notify_it(self):
        my_logger.error('the cmd (%s) in function(%s) did not succeed' %
                        (self.function_name, self.inst_cmd) )
        for stdout_or_stdin, msg in self.lstdouterr:
            my_logger.error(" - %s : %s" % (stdout_or_stdin, msg))
#
# functions
def set_prop_value(prop, value, zfsname):
    if prop not in ZFS_LIST_CMD_LPROP_VALUE:
        raise ValueError('prop(%s) must be included in ZFS_LIST_CMD_LPROP_VALUE' % prop)
    my_logger.debug('started set_zfs_prop_value')
    inst_cmd=ZFS_SET_CMD % (prop, value, zfsname)
    my_logger.info('zfs set cmd(%s):' % inst_cmd)
    proc=subprocess.Popen(inst_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd='/')
    lstdout=proc.stdout.readlines()
    lstderr=proc.stderr.readlines()
    proc.communicate()
    retcode=proc.wait()
    if retcode != 0:
#        lmsg=['the cmd (%s) did not succeed' % inst_cmd]
        lstdouterr=[]
        for line in lstdout:
            lstdouterr.append( ('stdout', line.rstrip()))
        for line in lstderr:
            lstdouterr.append( ('stderr', line.rstrip()))
        error=ZfsCmdError("set_prop_value", inst_cmd, lstdouterr)
        error.notify_it()
        raise error
    my_logger.debug('ended set_zfs_prop_value')


def destroy(name):
    my_logger.debug('started zfs destroy')
    inst_cmd=ZFSDESTROY_CMD % name
    my_logger.info('zfs destroy cmd(%s):' % inst_cmd)
    proc=subprocess.Popen(inst_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd='/')
    lstdout=proc.stdout.readlines()
    lstderr=proc.stderr.readlines()
    proc.communicate()
    retcode=proc.wait()
    if retcode != 0:
        lstdouterr=[]
        for line in lstdout:
            lstdouterr.append( ('stdout', line.rstrip()) )
        for line in lstderr:
            lstdouterr.append( ('stderr', line.rstrip()) )
        error=ZfsCmdError("destroy", inst_cmd, lstdouterr)
        error.notify_it()
        raise error
    my_logger.debug('ended zfs destroy')

def clone_zfs(snapshotname, new_zfsname, doption={}):
    my_logger.info('started zfs clone_zfs')
    zfs=get_lzfs().by_name(snapshotname.split("@")[0])
    fun_unlock=stackfunction.stack_function.add(
        zfs.zpool.unlock_it,
        title="unlock file'semaphore of zpool(%s)" % zfs.zpool.name)
    zfs.zpool.lock_it()
    #
    property_value_str=' '.join("-o %s=%s" % (p,v) for p,v in  doption.iteritems())
    inst_cmd="/usr/sbin/zfs clone %s %s  %s" % (property_value_str
                                               ,snapshotname, new_zfsname)
    #
    my_logger.debug('zfs clone cmd(%s):' % inst_cmd)
    proc=subprocess.Popen(inst_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd='/')
    lstdout=proc.stdout.readlines()
    lstderr=proc.stderr.readlines()
    proc.communicate()
    retcode=proc.wait()
    if retcode != 0:
        lstdouterr=[]
        for line in lstdout:
            lstdouterr.append( ('stdout', line.rstrip()) )
        for line in lstderr:
            lstdouterr.append( ('stderr', line.rstrip()) )
        error=ZfsCmdError("clone_zfs", inst_cmd, lstdouterr)
        error.notify_it()
        raise error
    #
    fun_unlock()
    my_logger.debug('ended zfs clone_zfs')


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
            return self._lzfs[:]
        self._lzfs=[]
        for zfs in get_lzfs():
            if zfs.zpool == self:
                self._lzfs.append(zfs)
        return self._lzfs[:]
    def lock_it(self):
        my_logger.debug('started lock_zpool zpool(%s)' % self.name)
        zpool_lockname="/var/run/unige_zfs_%s.lock" % self.name
        my_logger.info('taking lock for zpool (%s) with file(%s)' % (self.name, zpool_lockname))
        if self.__class__.zpoolname_locked:
            if self.name==self.__class__.zpoolname_locked:
                my_logger.warning('"lock_zpool" zpool(%s): was already done' % self.name)
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
            raise ZfsUnableToLockZpool(self.name)
        my_logger.info("lock taken for zpool(%s). We are in a alone" % zpool_lockname)
        self.__class__.zpoolname_locked=self.name
        my_logger.debug('ended lock_zpool zpool(%s): done' % self.name)
    def unlock_it(self):
        zpool_lockname="/var/run/unige_zfs_%s.lock" % self.name
        my_logger.debug('started unlock_zpool zpool(%s) by removing file(%s)' % (self.__class__.zpoolname_locked, zpool_lockname))
        my_logger.info('releasing the lock(%s) on zpool. Hello everyone' % zpool_lockname)
        if os.path.isfile(zpool_lockname):
            os.remove(zpool_lockname)
        my_logger.debug('ended unlock_zpool zpool(%s): done' % self.__class__.zpoolname_locked)
        self.__class__.zpoolname_locked=''


class Zfs(object):
    def __init__(self, lvalue):
        self._origin=None
        self.duser_prop_value={}
        for prop, value in zip(ZFS_LIST_CMD_LPROP_VALUE, lvalue):
            value=value if value != '-' else None
            if prop.find(':') != -1 :
                self.duser_prop_value[prop]=value
            else:
                prop="_"+prop if prop in ['origin'] else prop
                setattr(self,prop,value)
    def get_is_mounted(self):
        if not self.mountpoint:
            return False
        if self.mountpoint == "legacy":
            return False
        if self.mounted != "yes":
            return False
        return True
    def is_under_path(self, under_path):
        if self.get_is_mounted():
            under_path=os.path.normpath(under_path)
            mountpoint=os.path.normpath(self.mountpoint)
            under_path=under_path+"/" if under_path[-1] != "/" else under_path
            mountpoint=os.path.normpath(self.mountpoint)+"/"
            if 0 == mountpoint.find(under_path):
                return True
        return False
    @property
    def origin(self):
        if self._origin:
            snapshot=get_lsnapshot().by_name(self._origin)
            if not snapshot:
                msg='can not found the origin snapshot object of zfs(%s), origin(%s)' %(self.name, self._origin)
                print '  '+'\n  '.join([snap.name for snap in get_lsnapshot()])
                raise ZfsErrorIncoherent(msg)
            return snapshot
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
    def set_property(self, prop, value):
        set_prop_value(prop, value, self.name)
    @property
    def zpool(self):
        zpoolname=self.name.split('/')[0]
        zpool=get_lzpool().by_name(zpoolname)
        if not zpool:
            msg='zfs(%s) has not zpool attached' %self.name
            raise ZfsErrorIncoherent(msg)
        return zpool
    def unmount(self):
        msg='zfs umount zfs(%s)' % self.name
        my_logger.debug(msg)
        module_mount.umount_device(self.name)
    @property
    def lsnapshot(self):
        return [snapshot for snapshot in get_lsnapshot() if snapshot.zfs==self]
    @classmethod
    def cmp_by_mountpoint(self, a,b):
        a_m=path.CPath( a.mountpoint )
        b_m=path.CPath( b.mountpoint )
        return path.CPath.__cmp__(a_m, b_m)
    @classmethod
    def cmp_by_name(cls, a,b):
        return mix.cmpAlphaNum(a.name, b.name)
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
        for prop, value in zip(ZFS_LIST_CMD_LPROP_VALUE, lvalue):
            if prop.find(':') != -1 :
                self.duser_prop_value[prop]=value
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
#           Z F S
#
######################################

class SnapshotList(list):
    def by_name(self, name):
        lret=[snap for snap in self if snap.name==name ]
        return lret[0] if lret else None
    def by_snapname(self, snapname):
        lret=SnapshotList([snap for snap in self if snap.snapname==snapname ])
        return lret
    def by_zfsname(self, zfsname):
        lret=SnapshotList([snap for snap in self if snap.zfsname==zfsname ])
        return lret
    def by_zfs(self, zfs):
        lret=SnapshotList([snap for snap in self if snap.zfsname==zfs.name ])
        return lret
    def by_zpool(self, zpool):
        lret=SnapshotList([snap for snap in self if snap.zfs.zpool.name==zpool.name ])
        return lret

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
        lret=SnapshotList([zfs for zfs in self if zfs.is_under_path(under_path)])
        return lret


_lzfs=[]
_lsnapshot=[]
_do_populate_lzfs=True

def refresh_lzfs():
    global _do_populate_lzfs
    _do_populate_lzfs=True

def get_lsnapshot():
    #
    if _do_populate_lzfs==True:
        populate_lzfs()
    lret=SnapshotList(_lsnapshot[:])
    return lret
def get_lzfs():
    #
    if _do_populate_lzfs:
        populate_lzfs()
    lret=ZfsList(_lzfs[:])
    return lret

lzfs_dump=[]
def populate_lzfs(for_zpool=None):
    my_logger.debug('refresh zfs.populate_lzfs')
    global _lsnapshot
    global _lzfs
    global _do_populate_lzfs
    global lzfs_dump
    if for_zpool:
        cmd=ZFS_LIST_CMD_4_ZPOOL % for_zpool.name
    else:
        cmd=ZFS_LIST_CMD
    proc=subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, cwd='/')
    lout=proc.stdout.readlines()
    retcode=proc.wait()
    _lzfs=mix.UniqList()
    _lsnapshot=mix.UniqList()
    if retcode != 0 :
        my_logger.error('the cmd (%s) did not succeed' % cmd)
    lzfs_dump=lout[:]
    for out in lout:
        lelem=out.rstrip().split('\t')
        fs_or_snap_or_volume=lelem[ZFS_LIST_CMD_LPROP_VALUE.index('type')]
        if fs_or_snap_or_volume in ['filesystem', 'volume']:
            zfs=Zfs(out.rstrip().split('\t') )
            _lzfs.append(zfs)
        elif fs_or_snap_or_volume == 'snapshot':
            snap=Snapshot(out.rstrip().split('\t'))
            _lsnapshot.append(snap)
        else:
            raise ZfsErrorIncoherent("unknown type(%s) for out(%s)"
                                     % (fs_or_snap_or_volume, out))
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
        zpool=get_lzpool().by_name('backup-test1_pool')
        self.assertTrue(bool(zpool), 'get_zpool_ba_name ok')
    def test_populate_zfs(self):
        zfs1=get_lzfs().by_name('backup-test2_pool/backup-test2/usr')
        zfs2=get_lzfs().by_name('backup-test2_pool')
        is_ok=bool(zfs1 and zfs2)
        self.assertTrue(is_ok, 'zfs ok')
    def test_get_zpool_from_zfs(self):
        zfs=get_lzfs().by_name('backup-test2_pool/backup-test2/home')
        zpool=get_lzpool().by_name('backup-test2_pool')
        self.assertTrue(zfs.zpool==zpool, 'can not find the corresponding zpool from a zfs ')
