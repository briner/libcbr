'''
Created on Aug 23, 2011

@author: briner
'''

import sys
import logging
my_logger=logging.getLogger('MyLogger')

DEFAULT_GROUP='main'


stack_function=None

class FunctionContainer(object):
    """ FunctionContainer easy the way that the funtion of StackFunction works
         - by autoremoving it from StackFunction when run
    """
    def __init__(self, tfun, index, title, group, inst_stackfunction):
        self.title=title
        self.group=group
        self.stackfunction=inst_stackfunction
        self.fun=tfun[0]
        self.args=tfun[1]
        self.kw=tfun[2]
        self._index=index
    def __str__(self):
        return 'fun(%d) %s(%s,%s)' %(self._index, self.fun.func_name, self.args, self.kw)
    def __call__(self):
        my_logger.info('%s %s(%s, %s)' % (self.title, str(self.fun), str(self.args), str(self.kw)))
        self.fun(*self.args, **self.kw)
        self.stackfunction.delete(self._index)
    def delete(self):
        self.stackfunction.delete(self._index)
#    def _get_index(self):
#        return self._index
#    index=property(_get_index)
        
class StackFunction(object):
    """ StackFunction is mainly used in the case this soft caught a SIGTERM or CTRL+C.
        In that situation, the handler will launch self.terminate.
    """
    def __init__(self):
        self._index=0
        self.lmap_index_fun=[]
    def get_next_index(self):
        self._index+=1
        return self._index
    def add(self, fun_call, args=[], kw={}, title='<unknow function>', group=DEFAULT_GROUP):
        index=self.get_next_index()
        tfun=(fun_call, args, kw)
        fun=FunctionContainer( tfun, index, title, group, self)
        self.lmap_index_fun.append((index,fun))
        my_logger.info('added to stackfunction "%s" in group(%s) the function( %s(%s, %s) )' % 
                                       (title, group, str(fun), str(args), str(kw)) )
        return fun
    def _mpi_cmp(self, a, b):
        return cmp( a[0], b[0])
    def terminate_group(self, group=DEFAULT_GROUP):
        my_logger.info('terminate group(%s)' % group)
        #
        #
        self.lmap_index_fun.sort(self._mpi_cmp)
        lmap_index_fun=filter(lambda x: x[1].group==group, self.lmap_index_fun)
        for map_index_fun in lmap_index_fun:
            map_index_fun[1]()
        my_logger.info('terminate group(%s): done' % group)
    def terminate(self, signum, frame):
        # frame is not used in our code, but it required when doing a
        # signal.signal(signal.SIGINT, stack_function.terminate)
        my_logger.info('CTRL-C or SIGTERM catched')
        my_logger.info('stop the process and clean the system')
        self.lmap_index_fun.sort(self._mpi_cmp)
        self.lmap_index_fun.reverse()
        for map_index_fun in self.lmap_index_fun:
            map_index_fun[1]()
        my_logger.info('process stopped and system cleaned')
        sys.exit(1)
    def delete(self, index):
        self.lmap_index_fun=[mif for mif in self.lmap_index_fun if mif[0] != index]

stack_function=StackFunction()