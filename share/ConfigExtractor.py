"""
extract configuration

"""

import re

_prompt = "ConfigExtractor > "
def _printConfig(str):
    print('%s%s' % (_prompt,str))


def _Service(str):
    tmpSvcNew = None
    tmpSvcOld = None
    # get new service
    try:
        svcMgr = theApp.serviceMgr()
        tmpSvcNew = getattr(svcMgr,str)
    except Exception:
        pass
    # get old service
    try:
        tmpSvcOld = Service(str)
    except Exception:
        pass
    # return old one for 12.0.6
    if tmpSvcOld is not None:
        return tmpSvcOld
    return tmpSvcNew

def _Algorithm(str):
    try:
        return Algorithm(str)
    except Exception:
        return None

    
######################
# input

EventSelector = _Service( "EventSelector" )
if hasattr(EventSelector,'InputCollections') and hasattr(EventSelector.InputCollections,'__len__') \
       and len(EventSelector.InputCollections):
    # POOL
    if hasattr(EventSelector,"CollectionType") and hasattr(EventSelector.CollectionType,'__len__') \
           and len(EventSelector.CollectionType) and EventSelector.CollectionType == "ExplicitROOT":
        # tag collection
        _printConfig('Input=COLL')
        # reference
        try:
            if EventSelector.RefName is not None:
                _printConfig('Input=COLLREF %s' % EventSelector.RefName)
        except Exception:
            pass
        # query
        try:
            if EventSelector.Query is not None:
                _printConfig('Input=COLLQUERY %s' % EventSelector.Query)
        except Exception:
            pass
    else:
        # normal POOL
        _printConfig('Input=POOL')
    # file list
    str = 'InputFiles '
    for file in EventSelector.InputCollections:
        str += '%s ' % file.split('/')[-1]
        _printConfig(str)
else:
    # ByteStream
    noInputFlag = True
    # both _Service and Service need to be checked due to Configurable
    compList = []
    try:
        compList.append(_Service( "ByteStreamInputSvc" ))
    except Exception:
        pass
    try:
        compList.append(Service( "ByteStreamInputSvc" ))
    except Exception:
        pass
    for ByteStreamInputSvc in compList:
        if (hasattr(ByteStreamInputSvc,'FullFileName') and hasattr(ByteStreamInputSvc.FullFileName,'__len__')
            and len(ByteStreamInputSvc.FullFileName)) or \
            (hasattr(ByteStreamInputSvc,'FilePrefix') and hasattr(ByteStreamInputSvc.FilePrefix,'__len__')
             and len(ByteStreamInputSvc.FilePrefix)):
            _printConfig('Input=BS')
            noInputFlag = False
            break
    if noInputFlag:
        try:
            eventSelector = _Service("EventSelector")
            if hasattr(eventSelector, 'Input') and hasattr(eventSelector.Input, '__len__') \
                    and eventSelector.Input:
                _printConfig('Input=BS')
                noInputFlag = False
        except Exception:
            pass
    if noInputFlag:
        _printConfig('No Input')


# back navigation
if hasattr(EventSelector,'BackNavigation') and EventSelector.BackNavigation == True:
    _printConfig('BackNavigation=ON')


# minimum bias
minBiasEventSelector = _Service( "minBiasEventSelector" )
if hasattr(minBiasEventSelector,'InputCollections') and hasattr(minBiasEventSelector.InputCollections,'__len__') \
       and len(minBiasEventSelector.InputCollections):
    _printConfig('Input=MINBIAS')


# cavern
cavernEventSelector = _Service( "cavernEventSelector" )
if hasattr(cavernEventSelector,'InputCollections') and hasattr(cavernEventSelector.InputCollections,'__len__') \
       and len(cavernEventSelector.InputCollections):
    _printConfig('Input=CAVERN')


# beam gas
BeamGasEventSelector = _Service( "BeamGasEventSelector" )
if hasattr(BeamGasEventSelector,'InputCollections') and hasattr(BeamGasEventSelector.InputCollections,'__len__') \
       and len(BeamGasEventSelector.InputCollections):
    _printConfig('Input=BEAMGAS')


# beam halo
BeamHaloEventSelector = _Service( "BeamHaloEventSelector" )
if hasattr(BeamHaloEventSelector,'InputCollections') and hasattr(BeamHaloEventSelector.InputCollections,'__len__') \
       and len(BeamHaloEventSelector.InputCollections):
    _printConfig('Input=BEAMHALO')


# condition files
CondProxyProvider = _Service( "CondProxyProvider" )
if hasattr(CondProxyProvider,'InputCollections') and hasattr(CondProxyProvider.InputCollections,'__len__') \
       and len(CondProxyProvider.InputCollections):
    condStr = ''
    for fName in CondProxyProvider.InputCollections:
        if not fName.startswith('LFN:'):
            condStr += "%s," % fName
    if condStr != '':
        retStr = "CondInput %s" % condStr
        retStr = retStr[:-1]    
        _printConfig(retStr)
    

######################
# configurable

_configs = []
seqList = []
try:
    # get all Configurable names
    from AthenaCommon.AlgSequence import AlgSequence
    tmpKeys = AlgSequence().allConfigurables.keys()
    # get AlgSequences
    seqList = [AlgSequence()]
    try:
        for key in tmpKeys:
            # check if it is available via AlgSequence
            if not hasattr(AlgSequence(),key.split('/')[-1]):
                continue
            # get full name
            tmpConf = getattr(AlgSequence(),key.split('/')[-1])
            if hasattr(tmpConf,'getFullName'):
                tmpFullName = tmpConf.getFullName()
                # append AthSequencer
                if tmpFullName.startswith('AthSequencer/'):
                    seqList.append(tmpConf)
    except Exception:
        pass
    # loop over all sequences
    for tmpAlgSequence in seqList:
        # loop over keys
        for key in tmpKeys:
            # check if it is available via AlgSequence
            if not hasattr(tmpAlgSequence,key.split('/')[-1]):
                continue
            # get fullname
            if key.find('/') == -1:
                if hasattr(tmpAlgSequence,key):
                    tmpAlg = getattr(tmpAlgSequence,key)
                    if hasattr(tmpAlg,'getFullName'):
                        _configs.append(getattr(tmpAlgSequence,key).getFullName())
                    elif hasattr(tmpAlg,'getName') and hasattr(tmpAlg,'getType'):
                        # ServiceHandle
                        _configs.append('%s/%s' % (tmpAlg.getType(),tmpAlg.getName()))
                    else:
                        # use short name if it doesn't have getFullName
                        _configs.append(key)
            else:
                _configs.append(key)
except Exception:
    pass


def _getConfig(key):
    if seqList == []: 
        from AthenaCommon.AlgSequence import AlgSequence
        return getattr(AlgSequence(),key.split('/')[-1])
    else:
        for tmpAlgSequence in seqList:
            if hasattr(tmpAlgSequence,key.split('/')[-1]):
                return getattr(tmpAlgSequence,key.split('/')[-1])            

    

######################
# output

# hist
HistogramPersistencySvc=_Service("HistogramPersistencySvc")
if hasattr(HistogramPersistencySvc,'OutputFile') and hasattr(HistogramPersistencySvc.OutputFile,'__len__') \
       and len(HistogramPersistencySvc.OutputFile):
    _printConfig('Output=HIST')
    _printConfig(' Name: %s' % HistogramPersistencySvc.OutputFile) 
    
# ntuple
NTupleSvc = _Service( "NTupleSvc" )
if hasattr(NTupleSvc,'Output') and hasattr(NTupleSvc.Output,'__len__') and len(NTupleSvc.Output):
    # look for streamname 
    for item in NTupleSvc.Output:
        match = re.search("(\S+)\s+DATAFILE",item)
        if match is not None:
            sName = item.split()[0]
            _printConfig('Output=NTUPLE %s' % sName)
            # extract name
            fmatch = re.search("DATAFILE=(\S+)\s",item)
            if fmatch is not None:
                fName = fmatch.group(1)
                fName = re.sub('[\"\']','',fName)
                fName = fName.split('/')[-1]
                _printConfig(' Name: %s'% fName)

streamOutputFiles = {}
ignoreMetaFiles = []

# RDO
foundStreamRD0 = False
key = "AthenaOutputStream/StreamRDO"
if key in _configs:
    StreamRDO = _getConfig( key )
else:
    StreamRDO = _Algorithm( key.split('/')[-1] )
if hasattr(StreamRDO,'OutputFile') and hasattr(StreamRDO.OutputFile,'__len__') and len(StreamRDO.OutputFile):
    streamOutputFiles[key.split('/')[-1]] = StreamRDO.OutputFile
    _printConfig('Output=RDO %s' % StreamRDO.OutputFile)
    _printConfig(' Name: %s'% StreamRDO.OutputFile)
    foundStreamRD0 = True
    ignoreMetaFiles.append(StreamRDO.OutputFile)
                
# ESD
foundStreamESD = False
key = "AthenaOutputStream/StreamESD"
if key in _configs:
    StreamESD = _getConfig( key )
else:
    StreamESD = _Algorithm( key.split('/')[-1] )
if hasattr(StreamESD,'OutputFile') and hasattr(StreamESD.OutputFile,'__len__') and len(StreamESD.OutputFile):
    streamOutputFiles[key.split('/')[-1]] = StreamESD.OutputFile
    _printConfig('Output=ESD %s' % StreamESD.OutputFile)
    _printConfig(' Name: %s'% StreamESD.OutputFile)
    foundStreamESD = True
    ignoreMetaFiles.append(StreamESD.OutputFile)

# AOD
foundStreamAOD = False
key = "AthenaOutputStream/StreamAOD"
if key in _configs:
    StreamAOD = _getConfig( key )
else:
    StreamAOD = _Algorithm( key.split('/')[-1] )
if hasattr(StreamAOD,'OutputFile') and hasattr(StreamAOD.OutputFile,'__len__') and len(StreamAOD.OutputFile):
    streamOutputFiles[key.split('/')[-1]] = StreamAOD.OutputFile
    _printConfig('Output=AOD %s' % StreamAOD.OutputFile)
    _printConfig(' Name: %s'% StreamAOD.OutputFile)
    foundStreamAOD = True
    ignoreMetaFiles.append(StreamAOD.OutputFile)

# TAG    
keys = ["AthenaOutputStream/StreamTAG","RegistrationStream/StreamTAG"]
foundKey = False
for key in keys:
    if key in _configs:
        StreamTAG = _getConfig( key )
        foundKey = True
        break
if not foundKey:
    StreamTAG = _Algorithm( key.split('/')[-1] )
if hasattr(StreamTAG,'OutputCollection') and hasattr(StreamTAG.OutputCollection,'__len__') and \
       len(StreamTAG.OutputCollection):
    _printConfig('Output=TAG')
    _printConfig(' Name: %s'% StreamTAG.OutputCollection)

# TAGCOM    
keys = ["AthenaOutputStream/StreamTAGCOM","RegistrationStream/StreamTAGCOM"]
foundKey = False
for key in keys:
    if key in _configs:
        StreamTAGX = _getConfig( key )
        foundKey = True
        break
if not foundKey:
    StreamTAGX = _Algorithm( key.split('/')[-1] )
if hasattr(StreamTAGX,'OutputCollection') and hasattr(StreamTAGX.OutputCollection,'__len__') and \
       len(StreamTAGX.OutputCollection):
    _printConfig('Output=TAGX %s %s' % (StreamTAGX.name(),StreamTAGX.OutputCollection))
    _printConfig(' Name: %s'% StreamTAGX.OutputCollection)

# AANT
aantStream = []
appStList = []
for alg in theApp.TopAlg+_configs:
    if alg.startswith("AANTupleStream" ):
        aName = alg.split('/')[-1]
        if alg in _configs:
            AANTupleStream = _getConfig(alg)
        else:
            AANTupleStream = Algorithm(aName)
        if hasattr(AANTupleStream.OutputName,'__len__') and len(AANTupleStream.OutputName):
            fName = AANTupleStream.OutputName
            # look for streamname 
            THistSvc = _Service( "THistSvc" )
            if hasattr(THistSvc.Output,'__len__') and len(THistSvc.Output):
                for item in THistSvc.Output:
                    if re.search(fName,item):
                        sName = item.split()[0]
                        # check stream name
                        if hasattr(AANTupleStream,'StreamName'):
                            if AANTupleStream.StreamName != sName:
                                continue
                        aantStream.append(sName)
                        tmpAantKey = (aName,sName,fName)
                        if tmpAantKey not in appStList:
                            _printConfig('Output=AANT %s %s %s' % (aName,sName,fName))
                            _printConfig(' Name: %s'% fName)
                            appStList.append(tmpAantKey)
                        break

# Stream1
key = "AthenaOutputStream/Stream1"
if key in _configs:
    Stream1 = _getConfig( key )
elif hasattr(theApp._streams,key.split('/')[-1]):
    Stream1 = getattr(theApp._streams,key.split('/')[-1])
else:
    Stream1 = _Algorithm( key.split('/')[-1] )
if hasattr(Stream1,'OutputFile') and hasattr(Stream1.OutputFile,'__len__') and len(Stream1.OutputFile):
    if (hasattr(Stream1,'Enable') and Stream1.Enable) or (not hasattr(Stream1,'Enable')):
        streamOutputFiles[key.split('/')[-1]] = Stream1.OutputFile        
        _printConfig('Output=STREAM1 %s' % Stream1.OutputFile)
        _printConfig(' Name: %s'% Stream1.OutputFile)
        ignoreMetaFiles.append(Stream1.OutputFile)

# Stream2
key = "AthenaOutputStream/Stream2"
if key in _configs:
    Stream2 = _getConfig( key )
elif hasattr(theApp._streams,key.split('/')[-1]):
    Stream2 = getattr(theApp._streams,key.split('/')[-1])
else:
    Stream2 = _Algorithm( key.split('/')[-1] )
if hasattr(Stream2,'OutputFile') and hasattr(Stream2.OutputFile,'__len__') and len(Stream2.OutputFile):
    if (hasattr(Stream2,'Enable') and Stream2.Enable) or (not hasattr(Stream2,'Enable')):    
        streamOutputFiles[key.split('/')[-1]] = Stream2.OutputFile
        _printConfig('Output=STREAM2 %s' % Stream2.OutputFile)
        _printConfig(' Name: %s'% Stream2.OutputFile)        
        ignoreMetaFiles.append(Stream2.OutputFile)
        

# General Stream
strGenFName = ''
strGenStream  = ''
strMetaStream = ''
ignoredStreamList = ['Stream1','Stream2','StreamBS','StreamBSFileOutput']
if foundStreamRD0:
    # for old releases where StreamRDO was an algorithm 
    ignoredStreamList += ['StreamRDO']
if foundStreamESD:
    # for streamESD defined as an algorithm 
    ignoredStreamList += ['StreamESD']
if foundStreamAOD:
    # for streamAOD defined as an algorithm     
    ignoredStreamList += ['StreamAOD']
    

desdStreams = {}
try:
    metaStreams = []
    for genStream in theApp._streams.getAllChildren()+AlgSequence().getAllChildren():
        # check name
        fullName = genStream.getFullName()
        if (fullName.split('/')[0] == 'AthenaOutputStream' or fullName.split('/')[0] == 'Athena::RootNtupleOutputStream') \
                and (not fullName.split('/')[-1] in ignoredStreamList):
            if hasattr(genStream,'OutputFile') and hasattr(genStream.OutputFile,'__len__') and len(genStream.OutputFile):
                if (hasattr(genStream,'Enable') and genStream.Enable) or (not hasattr(genStream,'Enable')):
                    # keep meta data
                    if genStream.OutputFile.startswith("ROOTTREE:") or \
                           (hasattr(genStream,'WriteOnFinalize') and genStream.WriteOnFinalize):
                        metaStreams.append(genStream)
                    elif fullName.split('/')[-1].startswith('StreamDESD'):
                        # ignore StreamDESD to treat it as multiple-streams later
                            continue
                    else:
                        strGenStream += '%s:%s,' % (fullName.split('/')[-1],genStream.OutputFile)
                        streamOutputFiles[fullName.split('/')[-1]] = genStream.OutputFile
                        strGenFName = genStream.OutputFile
                        ignoreMetaFiles.append(genStream.OutputFile)
    # associate meta stream
    for mStream in metaStreams:
        metaOutName = mStream.OutputFile.split(':')[-1]
        assStream = None
        # look for associated stream
        for stName in streamOutputFiles:
            stOut = streamOutputFiles[stName]
            if metaOutName == stOut:
                assStream = stName
                break
        # ignore meta stream since renaming is used instead of changing jobO
        if metaOutName in ignoreMetaFiles:
            continue
        # print meta stream
        if assStream is not None:
            _printConfig('Output=META %s %s' % (mStream.getFullName().split('/')[1],assStream))
            _printConfig(' Name: %s'% metaOutName)
except Exception:
    pass
if strGenStream != '':
    strGenStream = strGenStream[:-1]
    _printConfig('Output=STREAMG %s' % strGenStream)
    _printConfig(' Name: %s'% strGenFName)
if desdStreams != {}:
    for tmpStreamName in desdStreams:
        tmpOutFileName = desdStreams[tmpStreamName]
        _printConfig('Output=DESD %s' % tmpStreamName)
        _printConfig(' Name: %s'% tmpOutFileName)

# THIST
userDataSvcStream = {}
usedTHistStreams = []
THistSvc = _Service( "THistSvc" )
if hasattr(THistSvc.Output,'__len__') and len(THistSvc.Output):
    for item in THistSvc.Output:
        sName = item.split()[0]
        if sName not in aantStream:
            # extract name
            fmatch = re.search("DATAFILE=(\S+)\s",item)
            fName = None
            if fmatch is not None:
                fName = fmatch.group(1)
                fName = re.sub('[\"\']','',fName)
                fName = fName.split('/')[-1]
            # keep output of UserDataSvc
            if sName in ['userdataoutputstream'] or sName.startswith('userdataoutputstream'):
                userDataSvcStream[sName] = fName
                continue
            # skip if defined in StreamG
            if strGenFName != '' and fName == strGenFName:
                continue
            _printConfig('Output=THIST %s' % sName)
            if fmatch is not None:
                _printConfig(' Name: %s'% fName)

# ROOT outputs for interactive Athena
import ROOT
fList = ROOT.gROOT.GetListOfFiles()
for index in range(fList.GetSize()):
    if fList[index].GetOption() == 'CREATE':
        _printConfig('Output=IROOT %s' % fList[index].GetName())
        _printConfig(' Name: %s'% fList[index].GetName())

# BS
ByteStreamCnvSvc = _Service("ByteStreamCnvSvc")
if hasattr(ByteStreamCnvSvc,'ByteStreamOutputSvc') and \
       ByteStreamCnvSvc.ByteStreamOutputSvc=="ByteStreamEventStorageOutputSvc":
    _printConfig('Output=BS')
elif hasattr(ByteStreamCnvSvc,'ByteStreamOutputSvcList') and \
         'ByteStreamEventStorageOutputSvc' in ByteStreamCnvSvc.ByteStreamOutputSvcList:
    _printConfig('Output=BS')    

# selected BS
BSESOutputSvc = _Service("BSESOutputSvc")
if hasattr(BSESOutputSvc,'SimpleFileName'):
    _printConfig('Output=SelBS %s' % BSESOutputSvc.SimpleFileName)
    _printConfig(' Name: %s'% BSESOutputSvc.SimpleFileName)

# MultipleStream
try:
    from OutputStreamAthenaPool.MultipleStreamManager import MSMgr
    for tmpStream in MSMgr.StreamList:
        # avoid duplication
        if not tmpStream.Name in streamOutputFiles.keys():
            # remove prefix
            tmpFileBaseName = tmpStream.Stream.OutputFile.split(':')[-1]
            _printConfig('Output=MS %s %s' % (tmpStream.Name,tmpFileBaseName))
            _printConfig(' Name: %s'% tmpFileBaseName)
except Exception:
    pass

# UserDataSvc
if userDataSvcStream != {}:
    for userStName in userDataSvcStream:
        userFileName = userDataSvcStream[userStName]
        findStream = False
        # look for associated stream
        for stName in streamOutputFiles:
            stOut = streamOutputFiles[stName]
            if userFileName == stOut:
                _printConfig('Output=USERDATA %s' % stName)
                findStream = True
                break
        # use THIST if not found
        if not findStream:
            _printConfig('Output=THIST %s' % userStName)
            _printConfig(' Name: %s'% userFileName)
        
        

######################
# random number

AtRndmGenSvc = _Service( "AtRndmGenSvc" )
if hasattr(AtRndmGenSvc,'Seeds') and hasattr(AtRndmGenSvc.Seeds,'__len__') and len(AtRndmGenSvc.Seeds):
    # random seeds
    for item in AtRndmGenSvc.Seeds:
        _printConfig('RndmStream %s' % item.split()[0])
import types        
if hasattr(AtRndmGenSvc,'ReadFromFile') and isinstance(AtRndmGenSvc.ReadFromFile,types.BooleanType) and AtRndmGenSvc.ReadFromFile:
    # read from file
    rndFileName = "AtRndmGenSvc.out"
    if hasattr(AtRndmGenSvc.FileToRead,'__len__') and len(AtRndmGenSvc.FileToRead):
        rndFileName = AtRndmGenSvc.FileToRead
    _printConfig('RndmGenFile %s' % rndFileName)

# G4 random seed        
try:
    if hasattr(SimFlags,'SeedsG4'):
        _printConfig('G4RandomSeeds')
except Exception:
    pass
