#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
Created on Aug 23, 2011

@author: briner
'''
import os, stat, sys
ENV=os.environ.copy()
ENV['LC_ALL']='C'
import exceptions
import logging
import subprocess
import re
#
RE_DF=re.compile('(?P<fs_mountpoint>\S+)\s*\((?P<fs_name>\S+)\s*\):\s*(?P<no_blocks>\d+) blocks\s+(?P<no_files>\d+).*files')
DF_CMD='/usr/bin/df "%s"'
DEBUG=False
my_logger=logging.getLogger('MyLogger')

def full_readlink(path):
  if os.path.islink(path):
    return full_readlink(os.readlink(path))
  return path


def get_FQDN_hostname():
    import socket
    return socket.gethostname()

def get_hostname():
    fqdnHostname=get_FQDN_hostname()
    return fqdnHostname.split('.')[0]

class UniqList(list):
    """UniqList is a list of object, which ensure the unicity of object.uniq in it
    this means that the object must have a uniq_value attribute"""
    def __init__(self, *arg, **mydict):
        self._luniq_name=[]
        super(UniqList, self).__init__(*arg, **mydict)
    def append(self, newitem):
        uniq_value=newitem.uniq_value
        if uniq_value in self._luniq_name:
            raise Exception('can not append the item(%s) in self(%s), because the item(%s) is already in' %(newitem, self) )
        self._luniq_name.append(uniq_value)
        super(UniqList, self).append(newitem)

def isfifo(fn):
    try:
        unused_st = os.stat(fn)
    except os.error:
        return False
    return stat.S_ISFIFO(os.stat(fn).st_mode)


def getDF(path):
    #   The statvfs structure pointed to by buf includes the following members:
    #
    # ulong_t   f_bsize;          /* preferred   file system block size *
    # ulong_t   f_frsize;         /* fundamental filesystem block size (if supported) */
    # ulong_t   f_blocks;         /* total # of blocks on file  system in units of f_frsize */
    # ulong_t   f_bfree;          /* total # of free blocks */
    # ulong_t   f_bavail;         /* #  of free  blocks avail to non-superuser */
    # ulong_t   f_files;          /* total # of file nodes (inodes) */
    # ulong_t   f_ffree;          /* total # of free file nodes */
    # ulong_t   f_favail;         /* #  of inodes avail   to non-superuser */
    # ulong_t   f_fsid;           /* file  system id (dev for now) */
    # char f_basetype[FSTYPSZ];   /* target file system type name, null-terminated */
    # ulong_t   f_flag;           /* bit mask of flags */
    # ulong_t   f_namemax;        /* maximum file   name length */
    # char f_fstr[32];            /* file system specific string */
    # ulong_t   f_filler[16];     /* reserved for   future expansion */
    #
    #print os.statvfs.__doc__
    #statvfs(path) ->
    #(bsize, frsize, blocks, bfree, bavail, files, ffree, favail, flag, namemax)
    #   0       1       2      3      4       5      6       7      8     9
    #Perform a statvfs system call on the given path
    #
    statvfs=os.statvfs(path)
    ret={}
    ret['kbytes']=statvfs[1]*statvfs[2]/1024
    ret['avail']=statvfs[1]*statvfs[4]/1024
    ret['used']=ret['kbytes']-statvfs[1]*statvfs[3]/1024
    return ret


def cmpAlphaNum(str1,str2):
    if ( not str1 )or( not str2 ):
        return cmp(str1, str2)
    str1=str1.lower()
    str2=str2.lower()
    ReSplit='(\d+)'
    str1=re.split(ReSplit,str1)
    str2=re.split(ReSplit,str2)
    if( ''==str1[0] ):
        str1.remove('')
    if( ''==str1[len(str1)-1] ):
        str1.remove('')
    if( ''==str2[0] ):
        str2.remove('')
    if( ''==str2[len(str2)-1] ):
        str2.remove('')
    for i in range( min( len(str1),len(str2) ) ):
        try:
            tmp=int(str1[i])
            str1[i]=tmp
        except:ValueError
        try:
            tmp=int(str2[i])
            str2[i]=tmp
        except:ValueError
        if( str1[i]==str2[i] ):
            continue
        if (str1[i]>str2[i]):
            return 1
        else:
            return -1
    return cmp(len(str1),len(str2))

def send_email(sender, recipient, subject, body):
    if DEBUG:
        subject='DEBUG: to(%s), %s' % (recipient, subject)
        recipient=DEBUG
    from smtplib import SMTP
    from email.MIMEText import MIMEText
    from email.Header import Header
    from email.Utils import parseaddr, formataddr
    # Header class is smart enough to try US-ASCII, then the charset we
    # provide, then fall back to UTF-8.
    header_charset = 'ISO-8859-1'
    # We must choose the body charset manually
    for body_charset in 'UTF-8', 'ISO-8859-1', 'US-ASCII'  :
        try:
            body.encode(body_charset)
        except UnicodeError:
            pass
        else:
            break
    else:
        msg='cannot send email, no charset found'
        my_logger.error(msg)
        body=u'cannot send email, no charset found'
        print 'body charset',  body_charset
    # Split real name (which is optional) and email address parts
    sender_name, sender_addr = parseaddr(sender)
    recipient_name, recipient_addr = parseaddr(recipient)
    # We must always pass Unicode strings to Header, otherwise it will
    # use RFC 2047 encoding even on plain ASCII strings.
    sender_name = str(Header(unicode(sender_name), header_charset))
    recipient_name = str(Header(unicode(recipient_name), header_charset))
    # Make sure email addresses do not contain non-ASCII characters
    sender_addr = sender_addr.encode('ascii')
    recipient_addr = recipient_addr.encode('ascii')
    # Create the message ('plain' stands for Content-Type: text/plain)
    msg = MIMEText(body.encode(body_charset), 'plain', body_charset)
    msg['From'] = formataddr((sender_name, sender_addr))
    msg['To'] = formataddr((recipient_name, recipient_addr))
    msg['Subject'] = Header(unicode(subject), header_charset)
    # Send the message via SMTP to localhost:25
    smtp = SMTP("localhost")
    smtp.sendmail(sender, recipient, msg.as_string())
    smtp.quit()

def path_2_filesystem_name(path):
    cmd=DF_CMD % path
    proc=subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, cwd='/')
    lout=proc.stdout.readlines()
    retcode=proc.wait()
    if retcode != 0 :
        my_logger.error('the cmd (%s) did not succeed' % cmd)
    if len(lout) ==2:
        filesystem_name=lout[1].split(' ')[0]
        return filesystem_name
    return ''

def relpath(path, start):
    """Return a relative version of a path"""
    # This function is included in  python 2.6
    if not path:
        raise ValueError("no path specified")
    start_list = [x for x in os.path.abspath(start).split(os.sep) if x]
    path_list = [x for x in os.path.abspath(path).split(os.sep) if x]
    # Work out how much of the filepath is shared by start and path.
    i = len(os.path.commonprefix([start_list, path_list]))
    rel_list = [os.path.pardir] * (len(start_list)-i) + path_list[i:]
    if not rel_list:
        return os.path.curdir
    return os.path.join(*rel_list)


class DiskFree(object):
    def __init__(self, fs_mountpoint=None, fs_name=None, no_blocks=None, no_files=None):
        if ( fs_mountpoint == None )or( fs_name==None )or( no_blocks==None )or( no_files==None ):
            raise Exception('DiskFree not well initialized')
        self.fs_mountpoint=fs_mountpoint
        self.fs_name=fs_name
        self.no_blocks=no_blocks
        self.no_file=no_blocks
    @classmethod                
    def of(cls, path=None):
        '''if path is specified:
            - return a dict of fs_mountpoint, fs_name, nb_blocks, nb_files
        if path is not specified:
            - return a list of dict of fs_mountpoint, fs_name, nb_blocks, nb_files
        '''
        if path==None:
            lcmd=['/usr/bin/df -Fzfs -Z'
                    ,'/usr/bin/df -Flofs -Z'
                    ,'/usr/bin/df -Fufs -Z']
            raise Exception('not yet implemented')
        else:
            lcmd=['/usr/bin/df %s' % path]
        lmsg=[]
        lret=[]
        for cmd in lcmd:
            proc=subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, cwd='/', env=ENV )
            lout=proc.stdout.readlines()
            retcode=proc.wait()
            if (len(lout) !=1 )or( retcode != 0 ):
                lmsg.append('the cmd (%s) did not succeed' % cmd)
                continue
            for out in lout:
                match=RE_DF.match(out)
                if match:
                    matchdict=match.groupdict()
                    lret.append( cls( **matchdict ) )
            if lmsg:
                for msg in lmsg:
                    my_logger.error(msg)
                raise Exception(', '.join(lmsg))
#            RE_DF=re.compile('(?P<fs_mountpoint>\S+)\s*\((?P<fs_name>\S+)\s*\):\s*(?P<nb_blocks>\d+) blocks\s+(?P<nb_files>\d+).*files')
        if path:
            if 0 == len(lret):
                return None
            elif 1 == len(lret):
                return lret[0]
            else:
                raise Exception('Mix.execute_DF_on with path(%s) should return only one element. Instead we\'ve get(%s)' % (path, ', '.join(lret)))
        else:
            return lret
    
class _PathInfo(object):
    def _path_info(self, path):
        import zfs as module_zfs
        import zone as module_zone
        ret_dict={'path':path
                 ,'fs_inst':None
                 ,'fs_dir':None
                 ,'relpath':None}
        df_entry=DiskFree.of(path)
        ret_dict['fs_name']=df_entry['fs_name']
        #
        # fs_dir
        fs_inst=module_zfs.get_zfs_by_name(ret_dict['fs_name'])
        if fs_inst:
            ret_dict['fs_inst']=fs_inst
        else:
            pass
            #TODO
        ret_dict['fs_dir']=df_entry['fs_mountpoint']
        #
        # relpath : relative path from fs_dir
        ret_dict['relpath']=relpath(path, ret_dict['fs_dir'])
        #
        # lmap_zone_reldir
        ret_dict['lmap_zone_reldir']=[]
        if isinstance(fs_inst, module_zfs.Zfs):
            if not fs_inst.get_mountpoint_from_lmount():
                return ret_dict
            print '-------------------------------------------'
            print '-------------------------------------------'
            print path.upper(), ret_dict['fs_inst']
            for zone in module_zone.lzone:
                print ""
                lfs=zone.lfs
                print '---zone:',  zone.zonename
                print '---lfs:', lfs
                pretender_zone_fs_len=(None,None,0)
                fs_list = [x for x in os.path.abspath(fs_inst.get_mountpoint_from_lmount()).split(os.sep) if x]
                zonepath_list = [x for x in os.path.abspath(os.path.join(zone.zonepath,'root')).split(os.sep) if x]
                common_path= os.path.commonprefix([zonepath_list, fs_list])
                print '  +-----'
                print '  |',fs_inst.get_mountpoint_from_lmount(),' || ',zone.zonepath,' || ',os.sep.join(common_path)
                print '  +-----'
                if len(common_path) > pretender_zone_fs_len[2]:
                    rel_dir=''
                    print '+-+-----'
                    print '|0|', fs_inst.name,' || ',os.sep.join(common_path)
                    print '+-+-----'
                    pretender_zone_fs_len=(zone, fs_inst.dir , len(common_path), 1)
                print '  +-----'
                for fs in lfs:
                    print '  |', fs
                    print '  +-----'
                    print '  |-', '\n  |- '.join( [fs.special for fs in lfs] )
                    print '+-+-----'
                    print '|1|', fs_inst.name,' || ',fs.special
                    print '+-+-----'
                    if 'zfs'==fs.type:
                        if fs.special == fs_inst.name:
                            print '+-+-----'
                            print '|2|', fs
                            print '+-+-----'
                            ret_dict['lmap_zone_reldir'].append( (zone, fs_inst.dir, 2) )
                            continue     
                    elif 'lofs'==fs.type:
                        lofs_list = [x for x in os.path.abspath(fs.special).split(os.sep) if x]
                        common_path= os.path.commonprefix([lofs_list, fs_list])
                        if len(common_path) > pretender_zone_fs_len[2]:
                            print '+-+-----'
                            print '|3|', fs
                            print '+-+-----'
                            pretender_zone_fs_len=(zone, fs.dir, len(common_path), 3)
                    else:
                        raise Exception('this fs is not supported')
                if 0 < pretender_zone_fs_len[1]:
                    ret_dict['lmap_zone_reldir'].append( tuple(pretender_zone_fs_len[0:3]) )
            print ""
            print ret_dict['lmap_zone_reldir']
        else:
            raise Exception('path info for a non zfs FS is not supported')
        return ret_dict
    def path_info(self, path):
        return self._path_info(path)
        
_inst_path_info=_PathInfo()
path_info=_inst_path_info.path_info    


def get_size_of_list(l):
    return map(lambda x:len(x), l)

def ll_2_nicelist(ll):
    if list != type(ll):
        return 'this is not a list'
    if len(ll)==0:
        return ''
    for i in range(len(ll)):
        if list != type(ll[i]):
            return 'this list in not constitued of list'
        if 0 == i:
            refSizeList=get_size_of_list(ll[i])
            refLenList=len(ll[i])
        else:
            if refLenList != len(ll[i]):
                return 'error all the list should be the same lenght'
            sizeList=get_size_of_list(ll[i])
            for j in range(refLenList):
                if refSizeList[j]< sizeList[j]:
                    refSizeList[j] = sizeList[j]
    outputFormatLine=u' '.join( map(lambda x:"%-"+unicode(x)+'s', refSizeList) )
    lret=[outputFormatLine % tuple(l) for l in ll]
    return lret

def ll_2_nicestr(ll):
    l=ll_2_nicelist(ll)
    return '\n'.join(l)

#def ll_2_nicestr(ll):
#    if list != type(ll):
#        return 'this is not a list'
#    if len(ll)==0:
#        return ''
#    for i in range(len(ll)):
#        if list != type(ll[i]):
#            return 'this list in not constitued of list'
#        if 0 == i:
#            refSizeList=get_size_of_list(ll[i])
#            refLenList=len(ll[i])
#        else:
#            if refLenList != len(ll[i]):
#                return 'error all the list should be the same lenght'
#            sizeList=get_size_of_list(ll[i])
#            for j in range(refLenList):
#                if refSizeList[j]< sizeList[j]:
#                    refSizeList[j] = sizeList[j]
#    outputFormatLine=u' '.join( map(lambda x:"%-"+unicode(x)+'s', refSizeList) )
#    output='\n'.join( outputFormatLine % tuple(l) for l in ll)
#    return output

def ld_2_nicestr(ldentry,lkey=None):
    if 0== len(ldentry):
        return ''
    if None==lkey:
        lkey=ldentry[0].keys()
    dlen=get_len_ld(ldentry,lkey)
    setKey=set(lkey)
    setKeyDict=set(ldentry[0].keys())
    if not setKey.issubset( setKeyDict ):
        raise 'the list of key has to be included in the ldentry.keys'
    format_str=' '.join(map(lambda x:'%('+str(x)+')-'+str(dlen[x])+'s', lkey))
    return os.linesep.join(map(lambda x:format_str%x, ldentry))

def get_len_ld(listdict,lkey=None):
    dlen={}
    if None==lkey:
        lkey=listdict[0].keys()
    try:
        for tmpdict in listdict:
            for key in lkey:
                length=0
                if type( tmpdict[key] ) != type( None ):
                    length=len(unicode( tmpdict[key] ))
                if not dlen.has_key(key):
                    dlen[key]=length
                else:
                    if dlen[key] < length:
                        dlen[key] = length
    except:
        print '-------------------'
        print 'dict:',tmpdict,' key:',key,' tmpdict[key]:',dict[key]
        raise Exception
    return dlen

def remove_only_dir_recursively(path):
    sdirs=set()
    sfiles=set()
    for root, ldir, lfile in os.walk(path):
        lfile=[os.path.join(root, filename) for filename in lfile]
        ldir=[os.path.join(root, dirname) for dirname in ldir]
        sdirs=sdirs.union(set(ldir))
        sfiles=sfiles.union(set(lfile))
    lfile=list(sfiles)
    ldir=list(sdirs)
    if lfile:
        raise exceptions.OSError( '[Errno 17] File exists: %s' % ', '.join(lfile) )
    ldir.sort(cmpAlphaNum)
    ldir.reverse()
    for tmpdir in ldir:
        os.rmdir(tmpdir)
    os.rmdir(path)
    
        
    
    
    
