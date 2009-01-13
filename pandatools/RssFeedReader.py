import sys
import time
import feedparser

class RssFeedReader:

    # constructor
    def __init__(self,query,verbose=False):
        self.verbose  = verbose
        self.rss      = feedparser.parse(query)


    # accessor
    def __getattribute__(self,name):
        if name in self.rss.__dict__.keys():
            return self.rss.__getattribute__(name)
        raise AttributeError, "'%s' object has no attribute '%s'" % (__class__,name)


    # more functions coming once the page format is defined
