'''
Created on Nov 21, 2011

@author: briner
'''

class Proxy(object):
    def __init__( self, subject ):
        self.__subject = subject
    def __getattr__( self, name ):
        return getattr( self.__subject, name )

class _RGB(object):
    def __init__( self, red, green, blue ):
        print '__init__ rgb'
        self.__red = red
        self.__green = green
        self.__blue = blue
    def Red( self ):
        return self.__red
    def Green( self ):
        return self.__green
    def Blue( self ):
        return self.__blue
    def uuid(self):
        print 'uuid rgb'
        return self.Blue()

_instances={}
class RGB(_RGB):
    def __new__(cls,*args, **kw):
        print args
        print kw
        print '__new__'
        subject=object.__new__(cls)
        subject_type=type(subject)
        subject_uuid=subject.uuid()
        key=(subject_type, subject_uuid)
        if key in _instances:
            proxy=_instances[key]
            proxy.__subject=subject
        else:
            proxy=object.__new__(cls)
        _instances[key]=proxy
        return proxy
    def __init__( self, *args,  ):
        print '__init__ proxy'
        self.__subject = subject
    def __getattr__( self, name ):
        return getattr( self.__subject, name )



class RGB(ProxyRegistry):pass

rgb = RGB( 100, 192, 240 )

q=RGB(rgb)

print id(q)

print '--------------------'

rgb = RGB( 84, 100, 240 )
w=ProxyRegistry(rgb)

print id(w)

print '--------------------'

rgb = RGB( 84, 100, 220 )
e=ProxyRegistry(rgb)

print id(e)

print '--------------------'

for key, value in _instances.iteritems():
    print key, value

print '--------------------'
print type(rgb)
print dir(rgb)

print '--------------------'
type