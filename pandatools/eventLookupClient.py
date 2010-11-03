import urllib, re, string, os, time

# client for eventLookup Athenaeum service
# author:  Marcin.Nowak@cern.ch

class eventLookupClient:

   #serverURL = "http://j2eeps.cern.ch/test-Athenaeum/"
   serverURL = "http://j2eeps.cern.ch/atlas-project-Athenaeum/"
   #serverURL = "http://j2eeps.cern.ch/test-eventPicking/"
   lookupPage = "EventLookup.jsp"
   getPage = "EventLookupGet.jsp"
   key = "insider"
   workerHost = "atlddm10.cern.ch"
   #workerHost = "voatlas69.cern.ch"
   workerPort = '10004'
   connectionRefusedSleep = 20
   errorPattern = "(Exception)|(Error)|(Lookup cannot be run)|(invalid)|(NOT EXISTING)"

   
   def __init__(self):
      self.output = ""
      self.guids = {}
      self.guidsLine = ""
      self.certProxyFileName = None
      self.certProxy = ""
      self.debug = None
      self.remoteFile = None
      try:
         self.certProxyFileName = os.environ['X509_USER_PROXY']
      except KeyError:
         print 'You do not seem to have a certificate proxy! (do voms-proxy-init)'
         return
      proxy = open(self.certProxyFileName)
      try:
         for line in proxy:
            self.certProxy += line
      finally:
         proxy.close()
         
   def workerURL(self):
      return "http://" + self.workerHost + ":" + self.workerPort
 
   def doLookup(self, inputEvents, async=None, stream="", tokens="", extract=False):
      """ contact the server and return a list of GUIDs
      inputEvents  - list of run-event pairs
      async - request query procesing in a separate process, client will poll for results
      stream - stream
      tokens - token names
      """
      if inputEvents == []:
         return []

      runs_events = ""
      runs = set()
      sep = ""
      for run_ev in inputEvents:
         runs_events += sep + run_ev[0] + " " + run_ev[1]
         sep = "\n"
         runs.add(run_ev[0]);

      if async is None:
         if len(runs) > 50 or len(inputEvents) > 1000:
            async = True
      if async:
         async = "true"
      else:
         async = "false"

      query_args = { 'key': self.key,
                     'worker': self.workerURL(),
                     'runs_events': runs_events,
                     'cert_proxy': self.certProxy,
                     'async': async,
                     'stream': stream,
                     'tokens': tokens
                     }
      if extract:
         query_args['extract'] = "true"

      self.talkToServer(self.serverURL + self.lookupPage, query_args)

      self.remoteFile = None
      for line in self.output:
         m = re.search("FILE=(.+)$", line)
         if m:
            return self.waitForFile( m.group(1) )

      return self.scanOutputForGuids()
   

   def talkToServer(self, url, args):
      encoded_args = urllib.urlencode(args)
      if self.debug:
         print "Contacting URL: " + url
         print encoded_args

      for _try in range(1,6):
         response = urllib.urlopen(url, encoded_args)
         self.output = []
         retry = False
         for line in response:
            self.output.append(line)
            if re.search("Connection refused", line):
               retry = True
         if retry:
            if self.debug:
               print "Failed to connect to the server, try " + str(_try)
            time.sleep(self.connectionRefusedSleep)
         else:
            break
 

   def scanOutputForGuids(self):
      """ Scan the server output looking for a line with GUIDs
      return list of GUIDs if line found, put GUIDs in self.guids
      return None in case of errors
      """
      self.guids = {}
      self.tags = []
      self.tagAttributes = None
      stage = None
      tokpat = re.compile(r'[[]DB=(?P<FID>.*?)[]]')
      for line in self.output:
         if re.search(self.errorPattern, line, re.I):
            #print " -- Error line matched: " + line
            return None
         if stage == "readTags":
            if line[0:1] == ":":
               # break the line up into attributes, extract GUIDs
               values = []
               for attr in string.split(line[1:]):
                  tok = tokpat.match(attr)
                  if tok:
                     attr = tok.group('FID')
                     # self.guids - TODO - populate the guids dict
                  values.append(attr)
               self.tags.append( values )
               continue
            else:
               return (self.tagAttributes, self.tags)
         if re.match("\{.*\}$", line):
            guids = eval(line)
            if type(guids).__name__!='dict':
               return None
            self.guids = guids
            return guids
         if re.search("TAGs extracted:", line):
            stage = "readAttribs"
            continue
         if stage == "readAttribs":
            self.tagAttributes = string.split(line.strip(),",")
            stage = "readTags"
            continue
      return None


   def waitForFile(self, file):
      """ Wait for the server to do EventLookup and store results in file <file>
      Retrieve the file and scan for GUIDs - return them if found
      """
      query_args = { 'key': self.key,
                     'worker': self.workerURL(),
                     'file' : file,
                     'wait_time' : "45"
                     }
      self.remoteFile = file
      if self.debug:
         print "EventLookup waiting for server.  Remote file=" + file

      ready = False  
      while not ready:
         self.talkToServer(self.serverURL + self.getPage, query_args)
         ready = True
         for line in self.output:
            if re.match("NOT READY", line):
               if self.debug:
                  print "received NOT READY"
               time.sleep(1)
               ready = False

      return self.scanOutputForGuids()
 
