'''
Created on Aug 23, 2011

@author: briner
'''

import os, sys
import subprocess
import logging
import select
import unittest


# within fwcbr
import mix
import log
import notification
import zfs as module_zfs
import path as mod_path


ZONEADM_LIST_CMD='zoneadm list -cp'
ZONENAME_CMD='zonename'
ZONECFG_EXPORT_CMD='zonecfg -z %s export'
ZLOGIN_CMD="zlogin %s %s"
FN_HOOK_BEFORE_SNAPSHOT_RELATIVE_2_ZONEPATH='etc/dolly/before_snapshot.hook' # attention stuff relative_2_zonepath must not start with a '/' at the begginning
FN_HOOK_AFTER_SNAPSHOT_RELATIVE_2_ZONEPATH='etc/dolly/after_snapshot.hook'  # attention stuff relative_2_zonepath must not start with a '/' at the begginning

my_logger=logging.getLogger('MyLogger')

def FILTER_IS_RUNNING(entry_4_ZONEADM_LIST_CMD):
    return 'running' == entry_4_ZONEADM_LIST_CMD.split(':')[2]


def check_relative_path():
    lvalue=[FN_HOOK_BEFORE_SNAPSHOT_RELATIVE_2_ZONEPATH
           ,FN_HOOK_AFTER_SNAPSHOT_RELATIVE_2_ZONEPATH]
    for value in lvalue:
        if value:
            if '/' == value[0]:
                print "attention a variable(%s) with a suffix(_RELATIVE_2_ZONEPATH) has a '/' at the begginning" % value
                sys.exit(1)

class ZoneFS(object):
    def __init__(self, fs_dir=None, fs_special=None, fs_type=None):
        self.dir =fs_dir
        self.special=fs_special
        self.type=fs_type
    def __repr__(self):
        return 'ZoneFS( type(%s) dir(%s) special(%s) )'  % (self.type, self.dir, self.special)

class ZoneError(Exception):
    pass

class ZoneErrorNotRunning(ZoneError):
    pass
        
class Zone(object):
    _LEN_OF_ZONE_LIST_ENTRY=10
    def __init__(self,  str_zone_list_entry_OR_zone_list_entry):
        if isinstance(str_zone_list_entry_OR_zone_list_entry, basestring):
            zone_list_entry=str_zone_list_entry_OR_zone_list_entry.split(':')
        else:
            zone_list_entry=str_zone_list_entry_OR_zone_list_entry
        # complement the list to 10 elements
        zone_list_entry=zone_list_entry+(self._LEN_OF_ZONE_LIST_ENTRY-len(zone_list_entry))*['']
        [self.zoneid \
        ,self.zonename \
        ,self.state \
        ,self.zonepath \
        ,self.uuid \
        ,self.brand \
        ,self.ip_type
        ,self.r_or_w
        ,self.file_mac_profile
        ,self.we_do_not_know_what_is_this_field]=zone_list_entry[:]
        self.is_local=self.zonename != 'global' 
        self._lrecipient=None   # this is a lazy list
        self._lfs_info=None # this is a lazy list
#        self._dsm_sys=None
        self._lfs=None
    def to_list(self):
        return [self.zoneid \
        ,self.zonename \
        ,self.state \
        ,self.zonepath \
        ,self.uuid \
        ,self.brand \
        ,self.ip_type
        ,self.r_or_w
        ,self.file_mac_profile]
    def _get_rootpath(self):
        if self.zonename=='global':
            return self.zonepath
        else:
            return os.path.join(self.zonepath, 'root')
    rootpath=property(_get_rootpath)
    def _get_uniq_value(self):
        """to comply with UniqList """
        return self.zonename
    uniq_value=property(_get_uniq_value)
    def __str__(self):
        return 'zone(%s)' % (self.zonename)
    __repr__=__str__
    def _get_is_running(self):
        return 'running' == self.state
    is_running=property(_get_is_running)
    def _get_lrecipient(self):
        if not self.is_running:
            msg='zone(%s) not in "running" state, can not _get_lrecipient (%s)' % self.zonename
            my_logger.warning(msg)
            return []
        if self._lrecipient:
            return self._lrecipient
        if [] == self._lrecipient:
            return []
        fn_etc_alias=os.path.join(self.zonepath, 'root/etc/aliases') # 
        if not os.path.isfile(fn_etc_alias):
            self._lrecipient=[]
            return self._lrecipient
        #
        # the good case
        fh_etc_alias=open(fn_etc_alias,'r')
        for line in fh_etc_alias.readlines() :
            if line.find('root:') == 0:
                self._lrecipient=[email.rstrip().lstrip() for email in line[len('root:'):].split(',') if -1 != email.find('@')]
        return self._lrecipient
    lrecipient=property(_get_lrecipient)
#    def _get_dsm_sys(self):
#        if self._dsm_sys:
#            return self._dsm_sys
#        if '/'==self.zonepath:
#            return None
#        fn=os.path.join(self.zonepath, 'root', FN_DSM_SYS_RELATIVE_2_ZONEPATH )
#        try:
#            self._dsm_sys=DsmSys(fn, within=self)
#        except DsmSysError:
#            return None
#        return self._dsm_sys
#    dsm_sys=property(_get_dsm_sys)
    def cmp_by_name(self, a,b):
        return mix.cmpAlphaNum(a.zonename, b.zonename)
    cmp_by_name=classmethod(cmp_by_name)
#    def erase_cache_4_zfs(self):
#        self._lfs=None
    def _get_lfs(self):
        if [] == self._lfs:
            return []
        if self._lfs:
            return self._lfs
        self.read_zone_config()
        return self._lfs
    lfs=property(_get_lfs)
    def read_zone_config(self):
        cmd=ZONECFG_EXPORT_CMD % self.zonename
        proc=subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, cwd='/')
        lout=proc.stdout.readlines()  
        retcode=proc.wait()
        if retcode != 0 : 
            my_logger.error('the cmd (%s) did not succeed' % cmd)
        lout=[out.rstrip() for out in lout]    
        lout_iter=iter(lout)
        self._lfs=[]
        for out in lout_iter:
            if 'add fs' != out:
                dparameter={}
                continue
            for out in lout_iter:
                if 'end' == out:
                    self._lfs.append(ZoneFS(**dparameter))
                    break
                key, value=out.split(' ')
                if 'set' == key:
                    key1,value1=value.split('=')
                    if key1 == 'dir':
                        dparameter['fs_dir']=value1
                    elif key1 == 'special':
                        dparameter['fs_special']=value1
                    if key1 == 'type':
                        dparameter['fs_type']=value1
# MOVE THIS PART SOMEWHERE IN DSM
#    def create_dsm_sys(self):
#        if '/'==self.zonepath:
#            raise Exception('can not create a dsm.sys for the global zone')
#        fn=os.path.join(self.zonepath,'root', FN_DSM_SYS_RELATIVE_2_ZONEPATH )
#        dirpath=os.path.dirname(fn)
#        if not os.path.isdir(dirpath):
#            msg='can not create dsm.sys on path(%s), dirpath(%s) does not exist already' % (fn,dirpath)
#            my_logger.info(msg)
#            return False
#        if os.path.isfile(fn):
#            msg='can not create dsm.sys on path(%s), path exist already' % fn
#            my_logger.info(msg)
#            return False
#        fh=open(fn, 'w')
#        fh.write(STR_NEW_DSM_SYS % {'servername':self.zonename
#                                   ,'dsm_sys': fn
#                                   ,'str_lrecipient': ','.join(self.lrecipient)})
#        fh.close()
#        dsm_sys=DsmSys(fn, within=self)
#        self._dsm_sys=dsm_sys
#        return True
    def zlogin(self,  cmd):
        cmd=ZLOGIN_CMD % (self.zonename,  cmd)
        my_logger.debug('enter in "Zone.zlogin"')
        if not self.is_running:
            msg='zone(%s) not in "running" state, can not execute the zlogin cmd (%s)' % (self.zonename,cmd)
            my_logger.warning(msg)
            return
        proc=subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, cwd='/')
        read_set=[proc.stdout, proc.stderr]
        lline_hook=[]
        send_email=False
        while read_set:
            rlist,unused_wlist,unused_xlist=select.select(read_set, [], [])
            if proc.stdout in rlist:
                stdout=proc.stdout.readline()
                if stdout == '':
                    read_set.remove(proc.stdout)
                else:
                    stdout=stdout.rstrip()
                    msg='hook (out): %s' % stdout
                    lline_hook.append(msg)
                    my_logger.debug(msg)
            if proc.stderr in rlist:
                stderr=proc.stderr.readline()
                if stderr == '':
                    read_set.remove(proc.stderr)
                else:
                    send_email=True
                    stderr=stderr.rstrip()
                    msg='hook (err): %s' % stderr
                    my_logger.error(msg)
                    lline_hook.append(log.getLogStr())
        if send_email:
            lbody_email=['zone(%s)' % self.zonename
                        ,' - hook cmd (%s)' % cmd
                        ]+ [' - %s' % line for line in lline_hook]
            notification.notify.add(lbody_email, lrecipient=self.lrecipient)
    def launch_hook_before_snapshot(self):
        my_logger.debug('enter in launch_hook_before_snapshot within the zone(%s)'%self.zonename)
        if not self.is_running:
            msg='zone(%s) not in "running" state, can not launch hook(before_snapshot)' % self.zonename
            my_logger.warning(msg)
            return
        fn_hook_global=os.path.join( self.zonepath, 'root', FN_HOOK_BEFORE_SNAPSHOT_RELATIVE_2_ZONEPATH )
        fn_hook_local=os.path.join( '/', FN_HOOK_BEFORE_SNAPSHOT_RELATIVE_2_ZONEPATH )
        if os.path.isfile(fn_hook_global):
            my_logger.info('execute hook(%s) before snapshot within the zone(%s)' % (fn_hook_local, self.zonename))
            self.zlogin(fn_hook_local)
        else:
            my_logger.info('no hook(%s) before snapshot for zone(%s)' % (fn_hook_local, self.zonename))
    def launch_hook_after_snapshot(self):
        my_logger.debug('enter in launch_hook_after_snapshot within the zone(%s)'%self.zonename)
        if not self.is_running:
            msg='zone(%s) not in "running" state, can not launch hook(after_snapshot)' % self.zonename
            my_logger.warning(msg)
            return
        fn_hook_global=os.path.join( self.zonepath, 'root', FN_HOOK_AFTER_SNAPSHOT_RELATIVE_2_ZONEPATH )
        fn_hook_local=os.path.join( '/', FN_HOOK_AFTER_SNAPSHOT_RELATIVE_2_ZONEPATH )
        if os.access(fn_hook_global, os.X_OK):
            my_logger.info('execute hook(%s) after snapshot for zone(%s)' % (fn_hook_local, self.zonename))
            self.zlogin(fn_hook_local)
        else:
            my_logger.info('no hook(%s) after snapshot for zone(%s)' % (fn_hook_local, self.zonename))
    def get_uniq(cls,name):
        lret=filter(lambda x: x.zonename==name, get_lzone())
        if len(lret) > 1:
            raise Exception ('in an UniqList, it could be only one element')
        if lret:
            return lret[0]
        else:
            return None
    get_uniq=classmethod(get_uniq)
    def _get_lfs_info(self):
        self._lfs_info
        if self._lfs_info!=None:
            return self._lfs_info
        lret=[]
        lret+=self.get_info_from_path_of_globalzone(self.zonepath)
        for fs in self.lfs:
            lret+=self.get_info_from_path_of_globalzone(os.path.join(self.rootpath, mod_path.lstrip_slash(fs.dir)))
        self._lfs_info=lret  
        return self._lfs_info
    lfs_info=property(_get_lfs_info)
    def get_info_from_path_of_globalzone(self,path):
        ''' scan zonecfg -z <zone> export and for each FS it gives
        reldir_zonepath : relative directory of the FS from the zonepath (eg. ./root/usr/local)
        is_visible : root mountpoint visible within the zone
        fs_inst : zfs instance or 'FSname' for other
        reldir_fs: path where the FS is mounted inside the zone (eg. /usr/local)
        zone: the zone instance
        '''
        lret=[]
        if os.path.isfile(path):
            filename=os.path.basename(path)
        else:
            filename=''
        zfs_holding_path=module_zfs.get_zfs_4_path(path)
        # check that this zfs is used by the zone
        zfs_holding_zonepath = module_zfs.get_zfs_4_path(self.zonepath)
        if zfs_holding_zonepath == zfs_holding_path:    
            reldir_fs=mod_path.lstrip_slash( os.path.normpath( path.replace(zfs_holding_zonepath.get_mountpoint_from_lmount(),'') ) )
            reldir_zonepath=mod_path.lstrip_slash( os.path.normpath( path.replace(self.zonepath,'') ) )
            ret={'reldir_zonepath':reldir_zonepath
                ,'is_visible': bool(-1 != path.find(self.rootpath) )
                ,'fs_inst': zfs_holding_zonepath
                ,'reldir_fs': reldir_fs
                ,'zone':self}
            lret.append(ret)
        for fs in self.lfs:
            if fs.type=='zfs':
                zfs=module_zfs.get_zfs_by_name(fs.special)
                if  zfs==zfs_holding_path:
                    reldir_fs=os.path.join(mod_path.lstrip_slash(  os.path.normpath(path.replace(zfs.get_mountpoint_from_lmount(),'') ) ), filename)
                    reldir_zonepath=os.path.join('root',mod_path.lstrip_slash(fs.dir), reldir_fs)
                    reldir_fs=reldir_fs.replace('/./','/')
                    reldir_zonepath=reldir_zonepath.replace('/./','/')
                    if reldir_fs == '.':
                        reldir_fs=''
                    if reldir_zonepath == '.':
                        reldir_zonepath=''
                    ret={'reldir_zonepath':reldir_zonepath
                         ,'is_visible': True
                         ,'fs_inst': zfs
                         ,'reldir_fs': reldir_fs
                         ,'zone':self}
                    lret.append(ret)
            else: # this is a lofs mount
                if fs.type=='lofs':
                    globalpath=os.path.join(self.rootpath, mod_path.lstrip_slash(fs.dir))
                    if 0 == path.find(globalpath):
                        reldir_fs=os.path.join(mod_path.lstrip_slash(  os.path.normpath(path.replace(globalpath,'') ) ), filename)
                        reldir_zonepath=os.path.normpath(os.path.join('root',mod_path.lstrip_slash(fs.special), reldir_fs))
                        #
                        reldir_fs=reldir_fs.replace('/./','/')
                        reldir_zonepath=reldir_zonepath.replace('/./','/')
                        if ( reldir_fs == '.' )or( reldir_fs == './' ):
                            reldir_fs=''
                        if ( reldir_zonepath == '.' )or( reldir_zonepath == './' ):
                            reldir_zonepath=''
                        ret={'reldir_zonepath': reldir_zonepath
                            ,'is_visible': True
                            ,'fs_inst': fs.special
                            ,'reldir_fs': reldir_fs
                            ,'zone':self}
                        lret.append(ret)
                else:
                    raise Exception("not zfs neither lofs! then what is it ? Exception raised in zone.get_info_from_path_of_globalzone")
        return lret


_ZONE_CLASS=Zone
def set_zone_class(class_definition):
    global _ZONE_CLASS 
    _ZONE_CLASS=class_definition

def get_zone_class():
    return _ZONE_CLASS

_do_populate_lzone=True
_lzone=[]
def get_lzone():
    if _do_populate_lzone:
        populate_lzone()
    return _lzone[:]

def populate_lzone(filter_fun=None):
    my_logger.debug('refresh zone.populate_lzone')
    global _lzone
    global _do_populate_lzone
    _lzone=mix.UniqList()
    cmd=ZONEADM_LIST_CMD
    proc=subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, cwd='/')
    lout=proc.stdout.readlines()
    retcode=proc.wait()
    if retcode != 0 :
        my_logger.error('the cmd (%s) did not succeed' % cmd)
    lout=[out.rstrip() for out in lout]
    lout=filter(filter_fun, lout)
    _lzone=[]
    for out in lout:
        zone=_ZONE_CLASS(out)
        _lzone.append(zone)
    _do_populate_lzone=False

def get_zone_by_name(name):
    zone=[zone for zone in get_lzone() if zone.zonename==name]
    if zone:
        return zone[0]
    else:
        return None

def get_lzone_by_path_of_globalzone(path):
    print ' - ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! ! !'
    print 'path', path
    lzone=[]
    for zone in get_lzone():
        print 'zonename', zone.zonename
        linfo=zone.get_info_from_path_of_globalzone(path)
        print 'linfo', linfo
        if linfo:
            lzone.append(zone)
    return lzone

_zonename=None
def get_zonename():
    global _zonename
    if _zonename:
        return _zonename
    cmd=ZONENAME_CMD
    proc=subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, cwd='/')
    lout=proc.stdout.readlines()
    retcode=proc.wait()
    if retcode != 0 :
        my_logger.error('the cmd (%s) did not succeed' % cmd)
    _zonename=lout[0].rstrip()
    return _zonename
        
    

#==============================================
# Test
#==============================================
class Test(unittest.TestCase):
    def test_populate_zone(self):
        zone=get_zone_by_name('backup-test2')
        self.assertTrue(bool(zone), 'get_zone_by_name ok')
    def test_get_lfs(self):
        zone=get_zone_by_name('backup-test2')
        fs=[fs for fs in zone.lfs if fs.dir=='/store/primary-idx'][0]
        test=(fs.type == 'zfs') and \
                 (fs.dir == '/store/primary-idx') and \
                 (fs.special == 'backup-test2_pool_b/backup-test2/primary-idx')
        self.assertTrue(test)

