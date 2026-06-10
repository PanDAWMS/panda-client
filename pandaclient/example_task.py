import uuid

inFileList = ['file1','file2','file3']

logDatasetName = f'panda.jeditest.log.{uuid.uuid4()}'

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
                       'value':f'{logDatasetName}.${{SN}}.log.tgz'}
outDatasetName = f'panda.jeditest.{uuid.uuid4()}'


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
     'value':f'outputEVNTFile={outDatasetName}.${{SN}}.root',
     'dataset':outDatasetName,
     'offset':1000,
     },
    {'type':'constant',
     'value':'aaaa',
     },
    ]
