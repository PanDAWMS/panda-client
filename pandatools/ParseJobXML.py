#!/usr/bin/env python
import sys
try:
    from urllib import quote
except ImportError:
    from urllib.parse import quote
import xml.dom.minidom

class dom_job:
    """ infiles[inds]=[file1,file2...]
        outfiles = [file1,file2...]
        command  - script that will be executed on the grid
        prepend  - list of (option,value) prepended to output file name
        forward  - list of (option,value) forwarded to the grid job
    """
    def __init__(s,domjob=None,primaryds=None,defaultcmd=None,defaultout=[]):
        """ Loads <job></job> from xml file.
        If primaryds is set, makes sure it is present in job spec """
        s.infiles = {}
        s.outfiles = []
        s.command=defaultcmd
        s.prepend  = []
        s.forward = []
        if not domjob:
            return
        # script executed on the grid node for this job
        if len(domjob.getElementsByTagName('command'))>0:
            s.command = dom_parser.text(domjob.getElementsByTagName('command')[0])
        # input files
        for inds in domjob.getElementsByTagName('inds'):
            name = dom_parser.text(inds.getElementsByTagName('name')[0])
            files = inds.getElementsByTagName('file')
            if len(files)==0:
                continue
            s.infiles[name]=[]
            for file in files:
                s.infiles[name].append(dom_parser.text(file))
        if primaryds and primaryds not in s.infiles.keys():
            print('ERROR: primaryds=%s must be present in each job'%primaryds)
            sys.exit(0)
        # output files (also, drop duplicates within this job)
        outfiles = set(defaultout)
        [outfiles.add(dom_parser.text(v)) for v in domjob.getElementsByTagName('output')]
        s.outfiles = list(outfiles)
        # gearing options
        for o in domjob.getElementsByTagName('option'):
            name=o.attributes['name'].value
            value=dom_parser.text(o)
            prepend=dom_parser.true(o.attributes['prepend'].value)
            forward=dom_parser.true(o.attributes['forward'].value)
            if prepend:
                s.prepend.append((name,value))
            if forward:
                s.forward.append((name,value))
    def to_dom(s):
        """ Converts this job to a dom tree branch """
        x = xml.dom.minidom.Document()
        job = x.createElement('job')
        for inds in s.infiles.keys():
            job.appendChild(x.createElement('inds'))
            job.childNodes[-1].appendChild(x.createElement('name'))
            job.childNodes[-1].childNodes[-1].appendChild(x.createTextNode(inds))
            for file in s.infiles[inds]:
                job.childNodes[-1].appendChild(x.createElement('file'))
                job.childNodes[-1].childNodes[-1].appendChild(x.createTextNode(file))
        for outfile in s.outfiles:
            job.appendChild(x.createElement('output'))
            job.childNodes[-1].appendChild(x.createTextNode(outfile))
        if s.command:
            job.appendChild(x.createElement('command'))
            job.childNodes[-1].appendChild(x.createTextNode(s.command))
        for option in s.prepend + list(set(s.prepend+s.forward)-set(s.prepend)):
            job.appendChild(x.createElement('option'))
            job.childNodes[-1].setAttribute('name',str(option[0]))
            if option in s.forward:
                job.childNodes[-1].setAttribute('forward','true')
            else:
                job.childNodes[-1].setAttribute('forward','false')
            if option in s.prepend:
                job.childNodes[-1].setAttribute('prepend','true')
            else:
                job.childNodes[-1].setAttribute('prepend','false')
            job.childNodes[-1].appendChild(x.createTextNode(str(option[1])))
        return job
    def files_in_DS(s,DS):
        """ Returns a list of files used in a given job in a given dataset"""
        if DS in s.infiles:
            return s.infiles[DS]
        else:
            return []
    def forward_opts(s):
        """ passable string of forward options """
        return ' '.join( ['%s=%s'%(v[0],v[1]) for v in s.forward])
    def prepend_string(s):
        """ a tag string prepended to output files """
        return '_'.join( ['%s%s'%(v[0],v[1]) for v in s.prepend])
    def exec_string(s):
        """ exec string for prun.
        If user requested to run script run.sh (via <command>run.sh</command>), it will return
        opt1=value1 opt2=value2 opt3=value3 run.sh
        This way, all options will be set inside run.sh
        """
        return '%s %s'%(s.forward_opts(),s.command)
    def exec_string_enc(s):
        """ exec string for prun.
        If user requested to run script run.sh (via <command>run.sh</command>), it will return
        opt1=value1 opt2=value2 opt3=value3 run.sh
        This way, all options will be set inside run.sh
        """
        comStr = '%s %s'%(s.forward_opts(),s.command)
        return quote(comStr)
    def get_outmap_str(s,outMap):
        """ return mapping of original and new filenames 
        """
        newMap = {}
        for oldLFN in outMap:
            fileSpec = outMap[oldLFN]
            newMap[oldLFN] = str(fileSpec.lfn)
        return str(newMap)
    def outputs_list(s,prepend=False):
        """ python list with finalized output file names """
        if prepend and s.prepend_string():
            return [s.prepend_string()+'.'+o for o in s.outfiles]
        else:
            return [o for o in s.outfiles]
    def outputs(s,prepend=False):
        """ Comma-separated list of output files accepted by prun """
        return ','.join(s.outputs_list(prepend))

class dom_parser:
    def __init__(s,fname=None,xmlStr=None):
        """ creates a dom object out of a text file (if provided) """
        s.fname = fname
        s.dom = None
        s.title = None
        s.tag = None
        s.command = None
        s.outds = None
        s.inds = {}
        s.global_outfiles = []
        s.jobs = []
        s.primaryds = None
        if fname:
            s.dom = xml.dom.minidom.parse(fname)
            s.parse()
            s.check()
        if xmlStr is not None:
            s.dom = xml.dom.minidom.parseString(xmlStr)
            s.parse()
            s.check()

    @staticmethod
    def break_regex(v,N=100):
        """ breaks up a very long regex into a comma-separeted list of filters """
        _REGEXLIM=2**15-1000
        spl=v.split('|')
        res=[]
        for i in range(0,len(spl),N):
            piece = '|'.join(spl[i:i+N])
            assert len(piece)<_REGEXLIM,'Input dataset contains very long filenames. You must reduce parameter N in break_regex()'
            res.append( piece )
        return ','.join(res)
    @staticmethod
    def true(v):
        """ define True """
        return v in ('1', 'true', 'True', 'TRUE', 'yes', 'Yes', 'YES')
    @staticmethod
    def text(pnode):
        """ extracts the value stored in the node """
        rc = []
        for node in pnode.childNodes:
            if node.nodeType == node.TEXT_NODE:
                rc.append(str(node.data).strip())
        return ''.join(rc)
    def parse(s):
        """ loads submission configuration from an xml file """
        try:
            # general settings
            if len(s.dom.getElementsByTagName('title'))>0:
                s.title = dom_parser.text(s.dom.getElementsByTagName('title')[0])
            else:
                s.title = 'Default title'
            if len(s.dom.getElementsByTagName('tag'))>0:
                s.tag = dom_parser.text(s.dom.getElementsByTagName('tag')[0])
            else:
                s.tag = 'default_tag'
            s.command = None  # can be overridden in subjobs
            for elm in s.dom.getElementsByTagName('submission')[0].childNodes:
                if elm.nodeName != 'command':
                    continue
                s.command = dom_parser.text(elm)
                break
            s.global_outfiles = []    # subjobs can append *additional* outputs
            for elm in s.dom.getElementsByTagName('submission')[0].childNodes:
                if elm.nodeName != 'output':
                    continue
                s.global_outfiles.append(dom_parser.text(elm))
            s.outds = dom_parser.text(s.dom.getElementsByTagName('outds')[0])
            # declaration of all input datasets
            primarydss = []
            for elm in s.dom.getElementsByTagName('submission')[0].childNodes:
                if elm.nodeName != 'inds':
                    continue
                if 'primary' in elm.attributes.keys() and dom_parser.true(elm.attributes['primary'].value):
                    primary=True
                else:
                    primary=False
                stream=dom_parser.text(elm.getElementsByTagName('stream')[0])
                name=dom_parser.text(elm.getElementsByTagName('name')[0])
                s.inds[name]=stream
                if primary:
                    primarydss.append(name)
            # see if one of the input datasets was explicitly labeled as inDS
            if len(primarydss)==1:
                s.primaryds = primarydss[0]
            else:
                s.primaryds = None
            for job in s.dom.getElementsByTagName('job'):
                s.jobs.append(dom_job(job,primaryds=s.primaryds,defaultcmd=s.command,defaultout=s.global_outfiles))
        except Exception:
            print('ERROR: failed to parse',s.fname)
            raise
    def to_dom(s):
        """ Converts this submission to a dom tree branch """
        x = xml.dom.minidom.Document()
        submission = x.createElement('submission')
        if s.title:
            submission.appendChild(x.createElement('title'))
            submission.childNodes[-1].appendChild(x.createTextNode(s.title))
        if s.tag:
            submission.appendChild(x.createElement('tag'))
            submission.childNodes[-1].appendChild(x.createTextNode(s.tag))
        for name in s.inds:
            stream = s.inds[name]
            submission.appendChild(x.createElement('inds'))
            if name==s.primaryds:
                submission.childNodes[-1].setAttribute('primary','true')
            else:
                submission.childNodes[-1].setAttribute('primary','false')
            submission.childNodes[-1].appendChild(x.createElement('stream'))
            submission.childNodes[-1].childNodes[-1].appendChild(x.createTextNode(stream))
            submission.childNodes[-1].appendChild(x.createElement('name'))
            submission.childNodes[-1].childNodes[-1].appendChild(x.createTextNode(name))
        if s.command:
            submission.appendChild(x.createElement('command'))
            submission.childNodes[-1].appendChild(x.createTextNode(s.command))
        for outfile in s.global_outfiles:
            submission.appendChild(x.createElement('output'))
            submission.childNodes[-1].appendChild(x.createTextNode(outfile))
        submission.appendChild(x.createElement('outds'))
        submission.childNodes[-1].appendChild(x.createTextNode(s.outds))
        for job in s.jobs:
            submission.appendChild(job.to_dom())
        return submission
    def check(s):
        """ checks that all output files have unique qualifiers """
        quals=[]
        for j in s.jobs:
            quals+=j.outputs_list(True)
        if len(list(set(quals))) != len(quals):
            print('ERROR: found non-unique output file names across the jobs')
            print('(you likely need to review xml options with prepend=true)')
            sys.exit(0)
    def input_datasets(s):
        """ returns a list of all used input datasets """
        DSs = set()
        for j in s.jobs:
            for ds in j.infiles.keys():
                DSs.add(ds)
        return list(DSs)
    def outDS(s):
        """ output dataset """
        return s.outds
    def inDS(s):
        """ chooses a dataset we'll call inDS; others will become secondaryDS """
        # user manually labeled one of datasets as primary, so make it inDS:
        if s.primaryds:
            return s.primaryds
        # OR: choose inDS dataset randomly
        else:
            return s.input_datasets()[0]
    def secondaryDSs(s):
        """ returns all secondaryDSs. This excludes inDS, unless inDS is managed by prun"""
        return [d for d in s.input_datasets() if d!=s.inDS() ]
    def secondaryDSs_config(s,filter=True):
        """ returns secondaryDSs string in prun format:
        StreamName:nFilesPerJob:DatasetName[:MatchingPattern[:nSkipFiles]]
        nFilesPerJob is set to zero, so that it is updated later from actual file count.
        MatchingPattern is an OR-separated list of actual file names.
        """
        out = []
        DSs = s.secondaryDSs()
        for i,DS in enumerate(DSs):
            if DS in s.inds:
                stream=s.inds[DS]
            else:
                stream='IN%d'%(i+1,)
            # remove scope: since it conflicts with delimiter (:)
            DS = DS.split(':')[-1]
            if filter:
                out.append('%s:0:%s:%s'%(stream,DS,s.files_in_DS(DS,regex=True)))
            else:
                out.append('%s:0:%s'%(stream,DS))
        return ','.join(out)
    def writeInputToTxt(s):
        """ Prepares prun option --writeInputToTxt
        comma-separated list of STREAM:STREAM.files.dat
        """
        out = []
        DSs = s.secondaryDSs()
        for i,DS in enumerate(DSs):
            if DS in s.inds:
                stream=s.inds[DS]
            else:
                stream='IN%d'%(i+1,)
            out.append('%s:%s.files.dat'%(stream,stream))
        out.append('IN:IN.files.dat')
        return ','.join(out)
    def files_in_DS(s,DS,regex=False):
        """ Returns a list of all files from a given dataset
            that will be used in at least one job in this submission
            If regex==True, the list is converted to a regex string
        """
        assert DS in s.input_datasets(),'ERROR: dataset %s was not requested in the xml file'%DS
        files = []
        for j in s.jobs:
            if DS in j.infiles.keys():
                files+=j.infiles[DS]
        if regex:
            return '|'.join(sorted(list(set(files))))
        else:
            return sorted(list(set(files)))
    def nFiles_in_DS(s,DS):
        return len(s.files_in_DS(DS))
    def nJobs(s):
        return len(s.jobs)
    def dump(s,verbose=True):
        """ prints a summary of this submission """
        def P(key,value=''):
            if value=='':
                print(key)
            else:
                print((key+':').ljust(14),)
                print(value)
        P('XML FILE LOADED',s.fname)
        P('Title',s.title)
        P('Command',s.command)
        P('InDS',s.inDS())
        P('Output DS',s.outds)
        P('njobs',s.nJobs())
        if verbose:
            for i,job in enumerate(s.jobs):
                P('===============> JOB%d'%i)
                P('command',job.exec_string())
                P('outfiles',job.outputs())
                P('INPUTS:')
                j=0
                for dsname in job.infiles:
                    files = job.infiles[dsname]
                    P('  Dataset%d'%j,dsname)
                    for k,fname in enumerate(files):
                        P('     File%d'%k,fname)
                    j+=1

if __name__=="__main__":
    p = dom_parser('./job.xml')
    p.dump()
