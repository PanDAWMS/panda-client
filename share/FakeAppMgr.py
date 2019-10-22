"""
replace AppMgr with a fake mgr to disable DLL loading

"""

# fake property
class fakeProperty(list):
    def __init__(self,name):
        self.name = name
        
    def __getattribute__(self,name):
        try:
            return object.__getattribute__(self,name)
        except Exception:
            setattr(self,name,fakeProperty(name))
            return object.__getattribute__(self,name)

    def __getstate__(self):
        return self.__dict__

    def __reduce_ex__(self,proto=0):
        if proto >= 2:
            proto = 1
        return super(fakeProperty,self).__reduce_ex__(proto)
                            
    def properties(self):
        prp = fakeProperty('properties')
        for attr in dir(self):
            prp.append(attr)
        return prp

    def get(self,name):
        return self.__getattribute__(name)

    def value(self):
        return self
    
        
# fake application manager    
class fakeAppMgr(fakeProperty):
    def __init__(self,origTheApp):
        self.origTheApp = origTheApp
        fakeProperty.__init__(self,'AppMgr')
        # streams
        try:
            self._streams = self.origTheApp._streams 
        except Exception:
            self._streams = []
        # for https://savannah.cern.ch/bugs/index.php?66675
        try:
            self.allConfigurables = self.origTheApp.allConfigurables
        except Exception:
            pass

    def service(self,name):
        return fakeProperty(name)

    def createSvc(self,name):
        return fakeProperty(name)        

    def algorithm(self,name):
        return fakeProperty(name)

    def setup(self,var):
        pass

    def exit(self):
        import sys
        sys.exit(0)
        
    def serviceMgr(self):
        try:
            return self.origTheApp.serviceMgr()
        except Exception:
            return self._serviceMgr

    def toolSvc(self):
        try:
            return self.origTheApp.toolSvc()
        except Exception:
            return self._toolSvc
        
    def addOutputStream(self,stream):
         self._streams += stream

    def initialize(self):
        include ('%s/etc/panda/share/ConfigExtractor.py' % os.environ['PANDA_SYS'])
        import sys
        sys.exit(0)
    
    def run(self):
        import sys
        sys.exit(0)


# replace AppMgr with the fake mgr to disable DLL loading
_theApp = theApp
theApp = fakeAppMgr(_theApp)

# for 13.X.0 or higher
try:
    import AthenaCommon.AppMgr
    AthenaCommon.AppMgr.theApp = theApp
except Exception:
    pass

# for boot strap
theApp.EventLoop = _theApp.EventLoop
theApp.OutStreamType = _theApp.OutStreamType
del _theApp
