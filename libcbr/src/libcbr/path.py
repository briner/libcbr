import mix

class CPathError(Exception):
    '''Base class for exception for PATH'''
    pass
def lstrip_slash(path):
    if len(path)>0:
        if path[0]=='/':
            return path[1:]
    return path

def rstrip_slash(path):
    if len(path)>0:
        if path[-1]=='/':
            return path[:-1]
    return path

class CFormatError(CPathError):
    '''Exception raised when the format of the path is not coherent.
    Attributes:
      message -- explanation
    '''
    def __init__(self, message):
        self.args=[message]

class CPath(str):
    def __new__(cls,  path):
        #NICEIT: pass to super writting
        if not path[0]== '/':
            msg='the path >'+path+'< has to have a \'/\' at the beginning'
            raise CFormatError(msg)
        return str.__new__( cls, (path) )
    def __cmp__(self, other):
        if(str(self))==(str(other)):
            return 0
        else:
            lself=str(self).split('/')
            lother=str(other).split('/')
            if( ''==lself[0] ):
                lself.remove('')
            if( ''==lself[len(lself)-1] ):
                lself.remove('')
            if( ''==lother[0] ):
                lother.remove('')
            if( ''==lother[len(lother)-1] ):
                lother.remove('')
            for i in range( min( len(lself),len(lother) ) ):
                ret=mix.cmpAlphaNum(lself[i], lother[i])
                if  0 != ret:
                    return ret
            else:
                if len(lself)>len(lother):
                    #/obs/system /obs
                    return 1
                else:
                    return -1
    def lstrip_slash(self):
        return CPath(self)



import unittest
class CTestPath( unittest.TestCase ):
   def testEq(self):
      p1=CPath('/obs')
      p2=CPath('/obs')
      self.assertEqual(p1, p2)
   def testNeq(self):
      p1=CPath('/obs')
      p2=CPath('/obs/system2')
      self.assertNotEqual(p1, p2)
   def testSort(self):
      p1=CPath('/obs')
      p2=CPath('/obs/system')
      p3=CPath('/obs2')
      p4=CPath('/obs/system21')
      p5=CPath('/obs/system2')
      p6=CPath('/obs21')
      lpath=[p1, p2, p3, p4, p5, p6]
      lpath.sort()
      self.assertEqual(lpath, [p1, p2, p5, p4, p3, p6])

def suite():
   """Returns a suite with one instance of CTestPath for each
   method starting with the word test."""
   return unittest.makeSuite( CTestPath, 'test' )

if '__main__'== __name__:
   p1=CUrlPath('/obs')
   p2=CUrlPath('/obs2')
   p1==p2
