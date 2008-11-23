import re
import os
import sys
import tempfile
import commands 

import PStep


class PSequence:

    # constructor
    def __init__(self,scriptName,fetFactory,sn):
        self.scriptName = scriptName
        self.steps      = {}
        self.sequence   = None
        self.pySequence = ''
        self.fetFactory = fetFactory
        self.sn         = sn

    
    # parse script
    def parse(self,verbose=False):
        # get step sections
        scFH = open(self.scriptName)
        stepName     = None
        sectionLines = ''
        seqSection   = False
        for line in scFH:
            # get step name
            match = re.search('^###\s*([a-zA-Z_0-9]+)',line)
            if match != None:
                # make Step instance
                if stepName != None and not seqSection:
                    self.steps[stepName] = PStep.PStep(stepName,sectionLines,
                                                       self.fetFactory,self.sn,verbose)
                stepName = match.group(1)
                # check duplication
                for name,step in self.steps.iteritems():
                    if stepName == step.name:
                        raise RuntimeError,'Step name : %s is duplicated' % stepName
                # terminate if Sequence section is found
                if re.search('Sequence',stepName,re.I) != None:
                    seqSection = True
                sectionLines = ''
                continue
            # remove empty lines
            if re.search('^\s*\n$',line) != None:
                continue
            # remove comment lines
            if line.startswith('#'):
                continue
            # append
            sectionLines += line
        # close
        scFH = open(self.scriptName)
        # set sequence
        self.sequence = sectionLines

        
    # convert script to python
    def convert(self,verbose=False):
        # parse script
        self.parse(verbose)
        # define steps
        for name in self.steps.keys():
            self.pySequence += "%s = self.steps['%s']\n" % (name,name)
        # append sequences
        for line in self.sequence:
            self.pySequence += line


    # main
    def run(self,verbose=False):
        # setup
        self.convert(verbose)
        # execute
        exec self.pySequence
        return True
            
