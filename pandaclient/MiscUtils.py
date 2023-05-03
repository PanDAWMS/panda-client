import re
import sys
import os
import json
import uuid
import traceback
import subprocess
try:
    import cPickle as pickle
except ImportError:
    import pickle
try:
    raw_input
except NameError:
    raw_input = input
try:
    unicode
except Exception:
    unicode = str


# wrapper for uuidgen
def wrappedUuidGen():
    return str(uuid.uuid4())


# make JEDI job parameter
def makeJediJobParam(lfn,dataset,paramType,padding=True,hidden=False,expand=False,
                     include='',exclude='',nFilesPerJob=None,offset=0,destination='',
                     token='',useNumFilesAsRatio=False,randomAtt=False,reusableAtt=False,
                     allowNoOutput=None, outDS=None, file_list=None):
    dictItem = {}
    if paramType == 'output':
        dictItem['type']       = 'template'
        dictItem['value']      = lfn
        dictItem['param_type'] = paramType
        dictItem['dataset']    = dataset
        dictItem['container']  = dataset
        if destination != '':
            dictItem['destination'] = destination
        if token != '':
            dictItem['token'] = token
        if not padding:
            dictItem['padding'] = padding
        if allowNoOutput is not None:
            for tmpPatt in allowNoOutput:
                if tmpPatt == '':
                    continue
                tmpPatt = '^.*'+tmpPatt+'$'
                if re.search(tmpPatt,lfn) is not None:
                    dictItem['allowNoOutput'] = True
                    break
    elif paramType == 'input':
        dictItem['type']       = 'template'
        dictItem['value']      = lfn
        dictItem['param_type'] = paramType
        dictItem['dataset']    = dataset
        if offset > 0:
            dictItem['offset'] = offset
        if include != '':
            dictItem['include'] = include
        if exclude != '':
            dictItem['exclude'] = exclude
        if expand:
            dictItem['expand'] = expand
        elif outDS:
            dictItem['consolidate'] = '.'.join(outDS.split('.')[:2]) + '.' + wrappedUuidGen() + '/'
        if nFilesPerJob not in [None,0]:
            dictItem['nFilesPerJob'] = nFilesPerJob
        if useNumFilesAsRatio and nFilesPerJob not in [None,0]:
            dictItem['ratio'] = nFilesPerJob
        if file_list:
            dictItem['files'] = file_list
    if hidden:
        dictItem['hidden'] = hidden
    if randomAtt:
        dictItem['random'] = True
    if reusableAtt:
        dictItem['reusable'] = True
    return [dictItem]


# get dataset name and num of files for a stream
def getDatasetNameAndNumFiles(streamDS,nFilesPerJob,streamName):
    if streamDS == "":
        # read from stdin
        print("\nThis job uses %s stream" % streamName)
        while True:
            streamDS = raw_input("Enter dataset name for {0}: ".format(streamName))
            streamDS = streamDS.strip()
            if streamDS != "":
                break
    # number of files per one signal
    if nFilesPerJob < 0:
        while True:
            tmpStr = raw_input("Enter the number of %s files per job : " % streamName)
            try:
                nFilesPerJob = int(tmpStr)
                break
            except Exception:
                pass
    # return
    return streamDS,nFilesPerJob


# convert UTF-8 to ASCII in json dumps
def unicodeConvert(input):
    if isinstance(input, dict):
        retMap = {}
        for tmpKey in input:
            tmpVal = input[tmpKey]
            retMap[unicodeConvert(tmpKey)] = unicodeConvert(tmpVal)
        return retMap
    elif isinstance(input, list):
        retList = []
        for tmpItem in input:
            retList.append(unicodeConvert(tmpItem))
        return retList
    elif isinstance(input, unicode):
        return input.encode('ascii', 'ignore').decode()
    return input


# decode json with ASCII
def decodeJSON(input_file):
    with open(input_file) as f:
        return json.load(f, object_hook=unicodeConvert)


# replacement for commands
def commands_get_status_output(com):
    data = ''
    try:
        # for python 2.6
        #data = subprocess.check_output(com, shell=True, universal_newlines=True, stderr=subprocess.STDOUT)
        p = subprocess.Popen(com, shell=True, universal_newlines=True, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT)
        data, unused_err = p.communicate()
        retcode = p.poll()
        if retcode:
            ex = subprocess.CalledProcessError(retcode, com)
            raise ex
        status = 0
    except subprocess.CalledProcessError as ex:
        # for python 2.6
        #data = ex.output
        status = ex.returncode
    if data[-1:] == '\n':
        data = data[:-1]
    return status, data


def commands_get_output(com):
    return commands_get_status_output(com)[1]


def commands_fail_on_non_zero_exit_status(
        com, error_status_on_failure,
        verbose_cmd=False, verbose_output=False,
        logger=None, error_log_msg=""):
    
    # print command if verbose
    if verbose_cmd:
        print(com)
    
    # execute command, get status code and message printed by the command 
    status, data = commands_get_status_output(com)
    
    # fail for non zero exit status 
    if status != 0:
        if not verbose_cmd:
            print(com)
        # print error message before failing
        print(data)
        # report error message if logger and log message have been provided 
        if logger and error_log_msg:
            logger.error(error_log_msg)
        
        if type(error_status_on_failure) == int:
            # use error status provided to the function
            sys.exit(error_status_on_failure)
        elif error_status_on_failure == "sameAsStatus":
            # use error status exit code returned 
            # by the execution of the command
            sys.exit(status)
        else: 
            # default exit status otherwise
            sys.exit(1)
    
    # print command output message if verbose
    if verbose_output and data:
        print(data)

    return status,data


# decorator to run with the original environment
def run_with_original_env(func):
    def new_func(*args, **kwargs):
        if 'LD_LIBRARY_PATH_ORIG' in os.environ and 'LD_LIBRARY_PATH' in os.environ:
            os.environ['LD_LIBRARY_PATH_RESERVE'] = os.environ['LD_LIBRARY_PATH']
            os.environ['LD_LIBRARY_PATH'] = os.environ['LD_LIBRARY_PATH_ORIG']
        if 'PYTHONPATH_ORIG' in os.environ:
            os.environ['PYTHONPATH_RESERVE'] = os.environ['PYTHONPATH']
            os.environ['PYTHONPATH'] = os.environ['PYTHONPATH_ORIG']
        if 'PYTHONHOME_ORIG' in os.environ and os.environ['PYTHONHOME_ORIG'] != '':
            if 'PYTHONHOME' in os.environ:
                os.environ['PYTHONHOME_RESERVE'] = os.environ['PYTHONHOME']
            os.environ['PYTHONHOME'] = os.environ['PYTHONHOME_ORIG']
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(str(e) + traceback.format_exc())
            raise e
        finally:
            if 'LD_LIBRARY_PATH_RESERVE' in os.environ:
                os.environ['LD_LIBRARY_PATH'] = os.environ['LD_LIBRARY_PATH_RESERVE']
            if 'PYTHONPATH_RESERVE' in os.environ:
                os.environ['PYTHONPATH'] = os.environ['PYTHONPATH_RESERVE']
            if 'PYTHONHOME_RESERVE' in os.environ:
                os.environ['PYTHONHOME'] = os.environ['PYTHONHOME_RESERVE']
    return new_func


# run commands with the original environment
@run_with_original_env
def commands_get_output_with_env(com):
    return commands_get_output(com)


@run_with_original_env
def commands_get_status_output_with_env(com):
    return commands_get_status_output(com)


# unpickle python2 pickle with python3
def pickle_loads(str_input):
    try:
        return pickle.loads(str_input)
    except Exception:
        try:
            return pickle.loads(str_input.encode('utf-8'), encoding='latin1')
        except Exception:
            return str_input


# parse secondary dataset option
def parse_secondary_datasets_opt(secondaryDSs):
    if secondaryDSs != '':
        # parse
        tmpMap = {}
        for tmpItem in secondaryDSs.split(','):
            if "#" in tmpItem:
                tmpItems = tmpItem.split('#')
            else:
                tmpItems = tmpItem.split(':')
            if 3 <= len(tmpItems) <= 6:
                tmpDsName = tmpItems[2]
                # change ^ to ,
                tmpDsName = tmpDsName.replace('^',',')
                # make map
                tmpMap[tmpDsName] = {'nFiles'     : int(tmpItems[1]),
                                     'streamName' : tmpItems[0],
                                     'pattern'    : '',
                                     'nSkip'      : 0,
                                     'files'      : []}
                # using filtering pattern
                if len(tmpItems) >= 4 and tmpItems[3]:
                    tmpMap[tmpItems[2]]['pattern'] = tmpItems[3]
                # nSkip
                if len(tmpItems) >= 5 and tmpItems[4]:
                    tmpMap[tmpItems[2]]['nSkip'] = int(tmpItems[4])
                # files
                if len(tmpItems) >= 6 and tmpItems[5]:
                    with open(tmpItems[5]) as f:
                        for l in f:
                            l = l.strip()
                            if l:
                                tmpMap[tmpItems[2]]['files'].append(l)
            else:
                errStr = "Wrong format %s in --secondaryDSs. Must be "\
                         "StreamName:nFilesPerJob:DatasetName[:Pattern[:nSkipFiles[:FileNameList]]]" \
                             % tmpItem
                return False, errStr
        # set
        secondaryDSs = tmpMap
    else:
        secondaryDSs = {}
    return True, secondaryDSs