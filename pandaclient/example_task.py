import uuid

inFileList = ['file1','file2','file3']

logDatasetName = 'panda.jeditest.log.{0}'.format(uuid.uuid4())

taskParamMap = {}

taskParamMap['nFilesPerJob'] = 1
taskParamMap['nFiles'] = len(inFileList)
#taskParamMap['nEventsPerInputFile']  = 10000
#taskParamMap['nEventsPerJob'] = 10000
#taskParamMap['nEvents'] = 25000
taskParamMap['noInput'] = True
taskParamMap['pfnList'] = inFileList
#taskParamMap['mergeOutput'] = True
taskParamMap['taskName'] = str(uuid.uuid4())
taskParamMap['userName'] = 'someone'
taskParamMap['vo'] = 'wlcg'
taskParamMap['taskPriority'] = 900
#taskParamMap['reqID'] = reqIdx
taskParamMap['architecture'] = 'power9'
taskParamMap['transUses'] = 'A'
taskParamMap['transHome'] = 'B'
taskParamMap['transPath'] = 'executable'
taskParamMap['processingType'] = 'step1'
taskParamMap['prodSourceLabel'] = 'test'
taskParamMap['taskType'] = 'test'
taskParamMap['workingGroup'] = 'groupA'
#taskParamMap['coreCount'] = 1
#taskParamMap['walltime'] = 1
taskParamMap['cloud'] = 'NA'
taskParamMap['site'] = 'TEST_PQ'
taskParamMap['log'] = {'dataset': logDatasetName,
                       'type':'template',
                       'param_type':'log',
                       'token':'local',
                       'destination':'local',
                       'value':'{0}.${{SN}}.log.tgz'.format(logDatasetName)}
outDatasetName = 'panda.jeditest.{0}'.format(uuid.uuid4())


taskParamMap['jobParameters'] = [
    {'type':'constant',
     'value':'-i "${IN/T}"',
     },
    {'type':'constant',
     'value': 'ecmEnergy=8000 runNumber=12345'
     },
    {'type':'template',
     'param_type':'output',
     'token':'local',     
     'destination':'local',
     'value':'outputEVNTFile={0}.${{SN}}.root'.format(outDatasetName),
     'dataset':outDatasetName,
     'offset':1000,
     },
    {'type':'constant',
     'value':'aaaa',
     },
    ]
