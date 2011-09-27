import urllib, re, string, os, time
from eventLookupClient import eventLookupClient

# client for countGuids Athenaeum service
# author:  Marcin.Nowak@cern.ch


class countGuidsClient(eventLookupClient):

   #serverURL = "http://j2eeps.cern.ch/test-Athenaeum/"
   serverURL = "http://j2eeps.cern.ch/atlas-project-Athenaeum/"
   #serverURL = "http://j2eeps.cern.ch/test-eventPicking/"
   servicePage = "CountGuids.jsp"
   getPage = "EventLookupGet.jsp"

   def __init__(self):
      eventLookupClient.__init__(self)
   
   def countGuids(self, datasetName, query='', tokens=''):
      """ contact the server and return GUIDs count
      tokens - token names
      """
      query_args = { 'key': self.key,
                     'worker': self.workerURL(),
                     'cert_proxy': self.certProxy,
                     'query': query,
                     'dataset': datasetName,
                     'tokens': tokens
                     }
      self.talkToServer(self.serverURL + self.servicePage, query_args)

      self.remoteFile = None
      for line in self.output:
         m = re.search("FILE=(.+)$", line)
         if m:
            return self.waitForFile( m.group(1) )

      return self.scanOutputForGuids()
   

   def scanOutputForGuids(self):
      """ Scan the server output looking for GUIDs
      return None in case of errors
      """
      self.countedGuids = []
      self.tokens = []
      stage = None
      tokpat = re.compile(r'([0-9A-F]{8}-([0-9A-F]{4}-){3}[0-9A-F]{12})')
      for line in self.output:
         if re.search(self.errorPattern, line, re.I):
            #print " -- Error line matched: " + line
            return None
         if stage == "readGuids":
            try:
               (count, guidline) = line.split(None,1)
               guids = guidline.split()
               if tokpat.match(guids[0]):
                  self.countedGuids.append( (count,guids,) )
                  continue
            except ValueError:
               pass
            # end of input, finish
            break
         if re.search("Event count per distinct GUIDs group:", line):
            stage = "readAttribs"
            continue
         if stage == "readAttribs":
            self.tokens = line.split()[1:]
            stage = "readGuids"
            continue

      return (self.tokens, self.countedGuids)
