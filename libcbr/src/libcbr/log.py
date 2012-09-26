# 14.09.2011 cED re-work the code to use as a module of the fwcbr pacakge

import sys
import logging.handlers
import traceback

#
# This stuff allow to extract the output of the log as it is.
# It was mainly created to get some exact output of the log inside
# emails we send to administrator. 
class NullFilter2GetMsg(object):
    def filter(self, record):
        global last_record
        last_record=record
        return True
null_filter_2_get_msg=NullFilter2GetMsg()

def getLogStr():
    global last_record
    return '%s - %s - %s' %(last_record.asctime
                           ,last_record.levelname
                           ,last_record.msg)

#
# This is the default value when using
# my_logger=logging.getLogger('MyLogger')
# and it is primarly set as it is, to see
# errors when import fwcbr.
#
# Do not forget to execute fwcbr.log.enable_my_logger after importing fwcbr
my_logger=logging.getLogger('MyLogger')
my_logger.setLevel(logging.INFO)

        
    

class Logger(object):
    def __init__(self):
        self._isdebug=False
        self._is_stdout_enabled=False
        self.stdout_handler=logging.StreamHandler(sys.stdout)
        self._prefix=''
        self._prefix_actif=None
        self.file_handler=None
    @property
    def isdebug(self):
        return self._isdebug
    @isdebug.setter
    def isdebug(self, isdebug):
        if isdebug != self._isdebug:
            if isdebug:
                my_logger.setLevel(logging.DEBUG)
            else:
                my_logger.setLevel(logging.INFO)
            self._isdebug=isdebug
    @property
    def is_stdout_enabled(self):
        return self._is_stdout_enabled
    @is_stdout_enabled.setter
    def is_stdout_enabled(self, is_stdout_enabled):
        if is_stdout_enabled != self._is_stdout_enabled:
            if is_stdout_enabled:
                my_logger.addHandler(self.stdout_handler)
            else:
                my_logger.removeHandler(self.stdout_handler)
            self._is_stdout_enabled=is_stdout_enabled
    @property
    def prefix(self):
        return self._prefix
    @prefix.setter
    def prefix(self, prefix):
        if self._prefix_actif != prefix:
            self._prefix=prefix
            self._set_formatter()
    def _set_formatter(self):
        if not self.file_handler:
            return
        if self.prefix != self._prefix_actif:
            if self.prefix:
                file_handler_format="%%(asctime)s - %%(levelname)s - %s - %%(message)s" % self.prefix
            else:
                file_handler_format="%(asctime)s - %(levelname)s - %(process)d - %(message)s"
            self.file_handler.setFormatter(logging.Formatter(file_handler_format))
            self.prefix=self._prefix_actif
    def enable_it(self, is_stdout_enabled, isdebug, log_filename, prefix=''):
        '''enable_my_logger was created because we had problem when rotating files. We were wanting
        to rotate the file at the beginning of an execution and not in the middle. S
        to 
        Since the rotation happens in the beginning of the code,to be launch after we know that we 
        '''
        if prefix:
            self.prefix=prefix
        self.file_handler = logging.handlers.TimedRotatingFileHandler(log_filename, 'W0', 1, 5 )
        self.file_handler.setFormatter(' %(asctime)s - %(levelname)s - %(process)d - %(message)s')
        self._set_formatter()
        my_logger.addHandler(self.file_handler)
        my_logger.addFilter(null_filter_2_get_msg)
        self.isdebug=isdebug
        
        self.is_stdout_enabled=is_stdout_enabled
        lmsg=[]
        if self.is_stdout_enabled:
            lmsg.append('stdout log ON')
        if self.isdebug:
            lmsg.append('debug log ON')
        if lmsg:
            my_logger.info(', '.join(lmsg))

logger=Logger()