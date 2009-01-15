import sys
import time
import feedparser

class RssFeedReader(object):

    # constructor
    def __init__(self,query,verbose=False):
        self.verbose  = verbose
        self.rss      = feedparser.parse(query)


    # accessor
    def __getattribute__(self,name):
        try:
            # return class attributes
            return object.__getattribute__(self,name)
        except:
            pass
        rss = object.__getattribute__(self,'rss')
        try:
            # return RSS attribute
            return rss.__getattr__(name)
        except:
            pass
        raise AttributeError, "'%s' object has no attribute '%s'" % \
              (self.__class__.__name__,name)


    # more functions coming once the page format is defined
