import os
import re
import sys
import copy
import shutil
import atexit
import argparse
from pandaclient.Group_argparse import GroupArgParser
import random
import pickle
import json
from pandaclient.MiscUtils import parse_secondary_datasets_opt

try:
    unicode
except Exception:
    unicode = str

####################################################################

# error code
EC_Config    = 10
EC_CMT       = 20
EC_Extractor = 30
EC_Dataset   = 40
EC_Post      = 50
EC_Archive   = 60
EC_Split     = 70
EC_MyProxy   = 80
EC_Submit    = 90

# tweak sys.argv
sys.argv.pop(0)
sys.argv.insert(0, 'pathena')

usage = """pathena [options] <jobOption1.py> [<jobOption2.py> [...]]

  HowTo is available at https://panda-wms.readthedocs.io/en/latest/client/pathena.html"""

examples = """Examples:
  pathena --inDS=... --outDS=... jobO_1.py jobO_2.py
  pathena --inDS=... --outDS=... --trf "Reco_tf.py --inputAODFile %IN --outputDAODFile %OUT.pool.root ..."
  pathena --inOutDsJson=inout.json --trf "..."
"""

removedOpts = [  # list of deprecated options w.r.t version 0.4.9
  "--ara", 
  "--araOutFile", 
  "--ares", 
  "--blong", 
  "--burstSubmit", 
  "--cloud", 
  "--configJEM", 
  "--corCheck", 
  "--crossSite", 
  "--dbRunNumber", 
  "--disableRebrokerage", 
  "--enableJEM", 
  "--eventPickStagedDS", 
  "--individualOutDS", 
  "--libDS", 
  "--long", 
  "--mcData", 
  "--myproxy", 
  "--nCavPerJob", 
  "--nHighMinPerJob", 
  "--nLowMinPerJob", 
  "--nMinPerJob", 
  "--nSkipFiles", 
  "--noLock", 
  "--notUseTagLookup", 
  "--outputPath", 
  "--panda_cacheSrvURL", 
  "--panda_dbRelease", 
  "--panda_devidedByGUID", 
  "--panda_eventPickRunEvtDat", 
  "--panda_fullPathJobOs", 
  "--panda_inDS", 
  "--panda_inDSForEP", 
  "--panda_jobsetID", 
  "--panda_origFullExecString", 
  "--panda_parentJobsetID", 
  "--panda_runConfig", 
  "--panda_singleLine", 
  "--panda_srcName", 
  "--panda_srvURL", 
  "--panda_suppressMsg", 
  "--panda_tagParentFile", 
  "--panda_trf", 
  "--parentDS", 
  "--prestage", 
  "--provenanceID", 
  "--removeBurstLimit", 
  "--removeFileList", 
  "--removedDS", 
  "--seriesLabel", 
  "--skipScan", 
  "--splitWithNthFiledOfLFN", 
  "--tagQuery", 
  "--tagStreamRef", 
  "--transferredDS", 
  "--useAIDA", 
  "--useChirpServer", 
  "--useContElementBoundary", 
  "--useDirectIOSites", 
  "--useExperimental", 
  "--useGOForOutput", 
  "--useNthFieldForLFN", 
  "--useOldStyleOutput", 
  "--useShortLivedReplicas", 
  "--useSiteGroup", 
  "--useTagInTRF",
  "-l"
]

optP = GroupArgParser(usage=usage, conflict_handler="resolve")
optP.set_examples(examples)

# define option groups
group_print   = optP.add_group('print', 'info print')
group_pathena = optP.add_group('pathena', 'about pathena itself')
group_config  = optP.add_group('config', 'single configuration file to set multiple options')
group_input   = optP.add_group('input', 'input dataset(s)/files/format/seed')
group_output  = optP.add_group('output', 'output dataset/files')
group_job     = optP.add_group('job', 'job running control on grid')
group_build   = optP.add_group('build', 'build/compile the package and env setup')
group_submit  = optP.add_group('submit', 'job submission/site/retry')
group_evtFilter = optP.add_group('evtFilter', 'event filter such as good run and event pick')
group_expert  = optP.add_group('expert', 'for experts/developers only')

usage_containerJob="""Visit the following wiki page for examples:
  https://twiki.cern.ch/twiki/bin/view/PanDA/PandaRun#Run_user_containers_jobs

Please test the job interactively first prior to submitting to the grid.
Check the following on how to test container job interactively:
  https://twiki.cern.ch/twiki/bin/viewauth/AtlasComputing/SingularityInAtlas
"""
group_containerJob = optP.add_group('containerJob', "For container-based jobs", usage=usage_containerJob)

optP.add_helpGroup(addHelp='Some options such as --inOutDsJson may SPAN several groups')

# special options
group_pathena.add_argument('--version',action='store_const',const=True,dest='version',default=False,
                help='Displays version')
group_job.add_argument('--split', '--nJobs', metavar='nJobs', action='store', dest='split',  default=-1,
                type=int,    help='Number of sub-jobs to be generated.')

group_job.add_argument('--nFilesPerJob', action='store', dest='nFilesPerJob',  default=-1, type=int, help='Number of files on which each sub-job runs')
group_job.add_argument('--nEventsPerJob', action='store', dest='nEventsPerJob',  default=-1,
                type=int,    help='Number of events per subjob. This info is used mainly for job splitting. If you run on MC datasets, the total number of subjobs is nEventsPerFile*nFiles/nEventsPerJob. For data, the number of events for each file is retrieved from AMI and subjobs are created accordingly. Note that if you run transformations you need to explicitly specify maxEvents or something in --trf to set the number of events processed in each subjob. If you run normal jobOption files, evtMax and skipEvents in appMgr are automatically set on WN.')
action = group_job.add_argument('--nEventsPerFile', action='store', dest='nEventsPerFile',  default=0,
                type=int,    help='Number of events per file')
group_input.shareWithMe(action)
group_job.add_argument('--nGBPerJob',action='store',dest='nGBPerJob',default=-1, help='Instantiate one sub job per NGBPERJOB GB of input files. --nGBPerJob=MAX sets the size to the default maximum value')
group_job.add_argument('--nGBPerMergeJob',action='store',dest='nGBPerMergeJob',default=-1, help='Instantiate one merge job per NGBPERMERGEJOB GB of pre-merged files')
group_submit.add_argument('--site', action='store', dest='site',  default="AUTO",
                type=str,    help='Site name where jobs are sent. If omitted, jobs are automatically sent to sites where input is available. A comma-separated list of sites can be specified (e.g. siteA,siteB,siteC), so that best sites are chosen from the given site list. If AUTO is appended at the end of the list (e.g. siteA,siteB,siteC,AUTO), jobs are sent to any sites if input is not found in the previous sites')
group_build.add_argument('--athenaTag',action='store',dest='athenaTag',default='',type=str,
                help='Use differnet version of Athena on remote WN. By defualt the same version which you are locally using is set up on WN. e.g., --athenaTag=AtlasProduction,14.2.24.3')
group_input.add_argument('--inDS',  action='store', dest='inDS',  default='',
                type=str, help='Input dataset names. wildcard and/or comma can be used to concatenate multiple datasets')
group_input.add_argument('--notExpandInDS', action='store_const', const=True, dest='notExpandInDS',default=False,
                         help='Allow jobs to use files across dataset boundaries in input dataset container')
group_input.add_argument('--inDsTxt',action='store',dest='inDsTxt',default='',
                type=str, help='a text file which contains the list of datasets to run over. newlines are replaced by commas and the result is set to --inDS. lines starting with # are ignored')
action = group_input.add_argument('--inOutDsJson', action='store', dest='inOutDsJson', default='',
                  help="A json file to specify input and output datasets for bulk submission. It contains a json dump of [{'inDS': a comma-concatenated input dataset names, 'outDS': output dataset name}, ...]")
group_output.shareWithMe(action)
group_input.add_argument('--secondaryDSs', action='store', dest='secondaryDSs', default='',
                         help='A versatile option to specify arbitrary secondary inputs that takes a list of '
                              'secondary datasets. See PandaRun wiki page for detail')
group_input.add_argument('--notExpandSecDSs', action='store_const', const=True, dest='notExpandSecDSs', default=False,
                         help = 'Use files across dataset boundaries in secondary dataset containers')
group_input.add_argument('--minDS',  action='store', dest='minDS',  default='',
                type=str, help='Dataset name for minimum bias stream')
group_job.add_argument('--nMin',  action='store', dest='nMin',  default=-1,
                type=int, help='Number of minimum bias files per sub job')
group_input.add_argument('--notExpandMinDS', action='store_const', const=True, dest='notExpandMinDS',default=False,
                         help='Allow jobs to use files across dataset boundaries in minimum bias dataset container')
group_input.add_argument('--lowMinDS',  action='store', dest='lowMinDS',  default='',
                type=str, help='Dataset name for low pT minimum bias stream')
group_job.add_argument('--nLowMin',  action='store', dest='nLowMin',  default=-1,
                type=int, help='Number of low pT minimum bias files per job')
group_input.add_argument('--notExpandLowMinDS', action='store_const', const=True, dest='notExpandLowMinDS',default=False,
                         help='Allow jobs to use files across dataset boundaries in low minimum bias dataset container')
group_input.add_argument('--highMinDS',  action='store', dest='highMinDS',  default='',
                type=str, help='Dataset name for high pT minimum bias stream')
group_job.add_argument('--nHighMin',  action='store', dest='nHighMin',  default=-1,
                type=int, help='Number of high pT minimum bias files per job')
group_input.add_argument('--notExpandHighMinDS', action='store_const', const=True, dest='notExpandHighMinDS',default=False,
                         help='Allow jobs to use files across dataset boundaries in high minimum bias dataset container')
group_input.add_argument('--randomMin',action='store_const',const=True,dest='randomMin',default=False,
                help='randomize files in minimum bias dataset')
group_input.add_argument('--cavDS',  action='store', dest='cavDS',  default='',
                type=str, help='Dataset name for cavern stream')
group_job.add_argument('--nCav',  action='store', dest='nCav',  default=-1,
                type=int, help='Number of cavern files per job')
group_input.add_argument('--notExpandCavDS', action='store_const', const=True, dest='notExpandCavDS',default=False,
                         help='Allow jobs to use files across dataset boundaries in cavern dataset container')
group_input.add_argument('--randomCav',action='store_const',const=True,dest='randomCav',default=False,
                help='randomize files in cavern dataset')
group_evtFilter.add_argument('--goodRunListXML', action='store', dest='goodRunListXML', default='',
                type=str, help='Good Run List XML which will be converted to datasets by AMI')
group_evtFilter.add_argument('--goodRunListDataType', action='store', dest='goodRunDataType', default='',
                type=str, help='specify data type when converting Good Run List XML to datasets, e.g, AOD (default)')
group_evtFilter.add_argument('--goodRunListProdStep', action='store', dest='goodRunProdStep', default='',
                type=str, help='specify production step when converting Good Run List to datasets, e.g, merge (default)')
action = group_evtFilter.add_argument('--goodRunListDS', action='store', dest='goodRunListDS', default='',
                type=str, help='A comma-separated list of pattern strings. Datasets which are converted from Good Run List XML will be used when they match with one of the pattern strings. Either \ or "" is required when a wild-card is used. If this option is omitted all datasets will be used')
group_input.shareWithMe(action)
group_evtFilter.add_argument('--eventPickEvtList',action='store',dest='eventPickEvtList',default='',
                type=str, help='a file name which contains a list of runs/events for event picking')
group_evtFilter.add_argument('--eventPickDataType',action='store',dest='eventPickDataType',default='',
                type=str, help='type of data for event picking. one of AOD,ESD,RAW')
group_evtFilter.add_argument('--ei_api',action='store',dest='ei_api',default='',
                type=str, help='flag to signalise mc in event picking')
group_evtFilter.add_argument('--eventPickStreamName',action='store',dest='eventPickStreamName',default='',
                type=str, help='stream name for event picking. e.g., physics_CosmicCaloEM')
action = group_evtFilter.add_argument('--eventPickDS',action='store',dest='eventPickDS',default='',
                type=str, help='A comma-separated list of pattern strings. Datasets which are converted from the run/event list will be used when they match with one of the pattern strings. Either \ or "" is required when a wild-card is used. e.g., data\*')
group_input.shareWithMe(action)
group_evtFilter.add_argument('--eventPickAmiTag',action='store',dest='eventPickAmiTag',default='',
                type=str, help='AMI tag used to match TAG collections names. This option is required when you are interested in older data than the latest one. Either \ or "" is required when a wild-card is used. e.g., f2\*')
group_evtFilter.add_argument('--eventPickWithGUID',action='store_const',const=True,dest='eventPickWithGUID',default=False,
                help='Using GUIDs together with run and event numbers in eventPickEvtList to skip event lookup')
group_submit.add_argument('--sameSecRetry', action='store_const',const=False,dest='sameSecRetry',default=True,
                help="Use the same secondary input files when jobs are retried")
group_submit.add_argument('--express', action='store_const',const=True,dest='express',default=False,
                help="Send the job using express quota to have higher priority. The number of express subjobs in the queue and the total execution time used by express subjobs are limited (a few subjobs and several hours per day, respectively). This option is intended to be used for quick tests before bulk submission. Note that buildXYZ is not included in quota calculation. If this option is used when quota has already exceeded, the panda server will ignore the option so that subjobs have normal priorities. Also, if you submit 1 buildXYZ and N runXYZ subjobs when you only have quota of M (M < N),  only the first M runXYZ subjobs will have higher priorities")
group_print.add_argument('--debugMode', action='store_const',const=True,dest='debugMode',default=False,
                help="Send the job with the debug mode on. If this option is specified the subjob will send stdout to the panda monitor every 5 min. The number of debug subjobs per user is limited. When this option is used and the quota has already exceeded, the panda server supresses the option so that subjobs will run without the debug mode. If you submit multiple subjobs in a single job, only the first subjob will set the debug mode on. Note that you can turn the debug mode on/off by using pbook after jobs are submitted" )
group_output.add_argument('--addNthFieldOfInDSToLFN',action='store',dest='addNthFieldOfInDSToLFN',default='',type=str,
                help="A middle name is added to LFNs of output files when they are produced from one dataset in the input container or input dataset list. The middle name is extracted from the dataset name. E.g., if --addNthFieldOfInDSToLFN=2 and the dataset name is data10_7TeV.00160387.physics_Muon..., 00160387 is extracted and LFN is something like user.hoge.TASKID.00160387.blah. Concatenate multiple field numbers with commas if necessary, e.g., --addNthFieldOfInDSToLFN=2,6.")
group_output.add_argument('--addNthFieldOfInFileToLFN',action='store',dest='addNthFieldOfInFileToLFN',default='',type=str,
                help="A middle name is added to LFNs of output files similarly as --addNthFieldOfInDSToLFN, but strings are extracted from input file names")
group_job.add_argument('--useAMIEventLevelSplit',action='store_const',const=True,dest='useAMIEventLevelSplit',default=None,
                help="retrive the number of events per file from AMI to split the job using --nEventsPerJob")
group_output.add_argument('--appendStrToExtStream',action='store_const',const=True,dest='appendStrToExtStream',default=False,
                help='append the first part of filenames to extra stream names for --individualOutDS. E.g., if this option is used together with --individualOutDS, %%OUT.AOD.pool.root will be contained in an EXT0_AOD dataset instead of an EXT0 dataset')
group_output.add_argument('--mergeOutput', action='store_const', const=True, dest='mergeOutput', default=False,
                help="merge output files")
group_output.add_argument('--mergeLog', action='store_const', const=True, dest='mergeLog', default=False,
                help="merge log files. relevant only with --mergeOutput")
action = group_job.add_argument('--mergeScript',action='store',dest='mergeScript',default='',type=str,
                help='Specify user-defied script or execution string for output merging')
group_output.shareWithMe(action)
group_job.add_argument('--useCommonHalo', action='store_const', const=False, dest='useCommonHalo',  default=True,
                help="use an integrated DS for BeamHalo")
group_input.add_argument('--beamHaloDS',  action='store', dest='beamHaloDS',  default='',
                type=str, help='Dataset name for beam halo')
group_input.add_argument('--beamHaloADS',  action='store', dest='beamHaloADS',  default='',
                type=str, help='Dataset name for beam halo A-side')
group_input.add_argument('--beamHaloCDS',  action='store', dest='beamHaloCDS',  default='',
                type=str, help='Dataset name for beam halo C-side')
group_job.add_argument('--nBeamHalo',  action='store', dest='nBeamHalo',  default=-1,
                type=int, help='Number of beam halo files per sub job')
group_job.add_argument('--nBeamHaloA',  action='store', dest='nBeamHaloA',  default=-1,
                type=int, help='Number of beam halo files for A-side per sub job')
group_job.add_argument('--nBeamHaloC',  action='store', dest='nBeamHaloC',  default=-1,
                type=int, help='Number of beam halo files for C-side per sub job')
group_job.add_argument('--useCommonGas', action='store_const', const=False, dest='useCommonGas',  default=True,
                help="use an integrated DS for BeamGas")
group_input.add_argument('--beamGasDS',  action='store', dest='beamGasDS',  default='',
                type=str, help='Dataset name for beam gas')
group_input.add_argument('--beamGasHDS',  action='store', dest='beamGasHDS',  default='',
                type=str, help='Dataset name for beam gas Hydrogen')
group_input.add_argument('--beamGasCDS',  action='store', dest='beamGasCDS',  default='',
                type=str, help='Dataset name for beam gas Carbon')
group_input.add_argument('--beamGasODS',  action='store', dest='beamGasODS',  default='',
                type=str, help='Dataset name for beam gas Oxygen')
group_job.add_argument('--nBeamGas',  action='store', dest='nBeamGas',  default=-1,
                type=int, help='Number of beam gas files per sub job')
group_job.add_argument('--nBeamGasH',  action='store', dest='nBeamGasH',  default=-1,
                type=int, help='Number of beam gas files for Hydrogen per sub job')
group_job.add_argument('--nBeamGasC',  action='store', dest='nBeamGasC',  default=-1,
                type=int, help='Number of beam gas files for Carbon per sub job')
group_job.add_argument('--nBeamGasO',  action='store', dest='nBeamGasO',  default=-1,
                type=int, help='Number of beam gas files for Oxygen per sub job')
group_output.add_argument('--outDS', action='store', dest='outDS', default='',
                type=str, help='Name of an output dataset. OUTDS will contain all output files')
group_output.add_argument('--destSE',action='store', dest='destSE',default='',
                type=str, help='Destination strorage element')
group_input.add_argument('--nFiles', '--nfiles', action='store', dest='nfiles',  default=0,
                type=int,    help='Use an limited number of files in the input dataset')
group_print.add_argument('-v', action='store_const', const=True, dest='verbose',  default=False,
                help='Verbose')
group_submit.add_argument('--noEmail', action='store_const', const=True, dest='noEmail',  default=False,
                help='Suppress email notification')
group_pathena.add_argument('--update', action='store_const', const=True, dest='update',  default=False,
                help='Update panda-client to the latest version')
group_build.add_argument('--noBuild', action='store_const', const=True, dest='noBuild',  default=False,
                help='Skip buildJob')
group_submit.add_argument('--bulkSubmission', action='store_const', const=True, dest='bulkSubmission', default=False,
                help='Bulk submit tasks. When this option is used, --inOutDsJson is required while --inDS and --outDS are ignored. It is possible to use %%DATASET_IN and %%DATASET_OUT in --trf which are replaced with actual dataset names when tasks are submitted, and %%BULKSEQNUMBER which is replaced with a sequential number of tasks in the bulk submission')
group_build.add_argument('--noCompile', action='store_const',const=True,dest='noCompile',default=False,
                help='Just upload a tarball in the build step to avoid the tighter size limit imposed by --noBuild. The tarball contains binaries compiled on your local computer, so that compilation is skipped in the build step on remote WN')
action = group_output.add_argument('--noOutput', action='store_const', const=True, dest='noOutput',  default=False,
                help='Send job even if there is no output file')
group_submit.shareWithMe(action)
group_input.add_argument('--noRandom', action='store_const', const=True, dest='norandom',  default=False,
                help='Enter random seeds manually')
group_job.add_argument('--useAMIAutoConf',action='store_const',const=True,dest='useAMIAutoConf',default=False,
                help='Use AMI for AutoConfiguration')
group_submit.add_argument('--memory', action='store', dest='memory', default=-1, type=int,
                          help='Required memory size in MB per core. e.g., for 1GB per core --memory 1024')
group_submit.add_argument('--fixedRamCount', action='store_const', const=True, dest='fixedRamCount', default=False,
                          help='Use fixed memory size instead of estimated memory size')
group_submit.add_argument('--outDiskCount', action='store', dest='outDiskCount', default=None, type=int,
                          help="Expected output size in kB per 1 MB of input. The system automatically calculates this "
                               "value using successful jobs and the value contains a safety offset (100kB). "
                               "Use this option to disable it when jobs cannot have enough input files "
                               "due to the offset")
group_submit.add_argument('--nCore', action='store', dest='nCore', default=-1,
                type=int,    help='The number of CPU cores. Note that the system distinguishes only nCore=1 and nCore>1. This means that even if you set nCore=2 jobs can go to sites with nCore=8 and your application must use the 8 cores there. The number of available cores is defined in an environment variable, $ATHENA_PROC_NUMBER, on WNs. Your application must check the env variable when starting up to dynamically change the number of cores')
action = group_job.add_argument('--nThreads', action='store', dest='nThreads', default=-1,
                type=int,    help='The number of threads for AthenaMT. If this option is set to larger than 1, Athena is executed with --threads=$ATHENA_PROC_NUMBER at sites which have nCore>1. This means that even if you set nThreads=2 jobs can go to sites with nCore=8 and your application will use the 8 cores there')
group_submit.shareWithMe(action)
group_input.add_argument('--forceStaged', action='store_const', const=True, dest='forceStaged', default=False,
                help='Force files from primary DS to be staged to local disk, even if direct-access is possible')
group_input.add_argument('--avoidVP', action='store_const', const=True, dest='avoidVP', default=False,
                help='Not to use sites where virtual placement is enabled')
group_submit.add_argument('--maxCpuCount', action='store', dest='maxCpuCount', default=0, type=int,
                help=argparse.SUPPRESS)
group_expert.add_argument('--noLoopingCheck', action='store_const', const=True, dest='noLoopingCheck', default=False,
                help="Disable looping job check")
group_output.add_argument('--official', action='store_const', const=True, dest='official',  default=False,
                help='Produce official dataset')
action = group_job.add_argument('--unlimitNumOutputs', action='store_const', const=True, dest='unlimitNumOutputs',  default=False,
                help='Remove the limit on the number of outputs. Note that having too many outputs per job causes a severe load on the system. You may be banned if you carelessly use this option') 
group_output.shareWithMe(action)

group_output.add_argument('--descriptionInLFN',action='store',dest='descriptionInLFN',default='',
                help='LFN is user.nickname.jobsetID.something (e.g. user.harumaki.12345.AOD._00001.pool) by default. This option allows users to put a description string into LFN. i.e., user.nickname.jobsetID.description.something')
group_build.add_argument('--extFile', action='store', dest='extFile',  default='',
                help='pathena exports files with some special extensions (.C, .dat, .py .xml) in the current directory. If you want to add other files, specify their names, e.g., data1.root,data2.doc')
group_build.add_argument('--excludeFile',action='store',dest='excludeFile',default='',
                help='specify a comma-separated string to exclude files and/or directories when gathering files in local working area. Either \ or "" is required when a wildcard is used. e.g., doc,\*.C')
group_output.add_argument('--extOutFile', action='store', dest='extOutFile',  default='',
                help='A comma-separated list of extra output files which cannot be extracted automatically. Either \ or "" is required when a wildcard is used. e.g., output1.txt,output2.dat,JiveXML_\*.xml')
group_output.add_argument('--supStream', action='store', dest='supStream',  default='',
                help='suppress some output streams. Either \ or "" is required when a wildcard is used. e.g., ESD,TAG,GLOBAL,StreamDESD\* ')
group_build.add_argument('--gluePackages', action='store', dest='gluePackages',  default='',
                help='list of glue packages which pathena cannot find due to empty i686-slc4-gcc34-opt. e.g., External/AtlasHepMC,External/Lhapdf')
action = group_job.add_argument('--allowNoOutput',action='store',dest='allowNoOutput',default='',type=str,
                help='A comma-separated list of regexp patterns. Output files are allowed not to be produced if their filenames match with one of regexp patterns. Jobs go to finished even if they are not produced on WN')
group_output.shareWithMe(action)
group_submit.add_argument('--excludedSite', action='append', dest='excludedSite',  default=[],
                help="list of sites which are not used for site section, e.g., ANALY_ABC,ANALY_XYZ")
group_submit.add_argument('--noSubmit', action='store_const', const=True, dest='noSubmit',  default=False,
                help="Don't submit jobs")
group_submit.add_argument('--prodSourceLabel', action='store', dest='prodSourceLabel',  default='',
                help="set prodSourceLabel")
group_submit.add_argument('--processingType', action='store', dest='processingType',  default='pathena',
                help="set processingType")
group_submit.add_argument('--workingGroup', action='store', dest='workingGroup',  default=None,
                help="set workingGroup")
group_input.add_argument('--generalInput', action='store_const', const=True, dest='generalInput',  default=False,
                help='Read input files with general format except POOL,ROOT,ByteStream')
group_build.add_argument('--tmpDir', action='store', dest='tmpDir', default='',
                type=str, help='Temporary directory in which an archive file is created')
group_input.add_argument('--shipInput', action='store_const', const=True, dest='shipinput',  default=False,
                help='Ship input files to remote WNs')
group_submit.add_argument('--disableAutoRetry',action='store_const',const=True,dest='disableAutoRetry',default=False,
                help='disable automatic job retry on the server side')
group_input.add_argument('--fileList', action='store', dest='filelist', default='',
                type=str, help='List of files in the input dataset to be run')
group_build.add_argument('--dbRelease', action='store', dest='dbRelease', default='',
                type=str, help='DBRelease or CDRelease (DatasetName:FileName). e.g., ddo.000001.Atlas.Ideal.DBRelease.v050101:DBRelease-5.1.1.tar.gz. If --dbRelease=LATEST, the latest DBRelease is used')
group_build.add_argument('--addPoolFC', action='store', dest='addPoolFC',  default='',
                help="file names to be inserted into PoolFileCatalog.xml except input files. e.g., MyCalib1.root,MyGeom2.root")
group_input.add_argument('--inputFileList', action='store', dest='inputFileList', default='',
                type=str, help='name of file which contains a list of files to be run in the input dataset')
group_build.add_argument('--voms', action='store', dest='vomsRoles',  default=None, type=str,
                help="generate proxy with paticular roles. e.g., atlas:/atlas/ca/Role=production,atlas:/atlas/fr/Role=pilot")
group_job.add_argument('--useNextEvent', action='store_const', const=True, dest='useNextEvent',  default=False,
                help="Set this option if your jobO uses theApp.nextEvent(), e.g. for G4. Note that this option is not required when you run transformations using --trf")
group_job.add_argument('--trf', action='store', dest='trf',  default=False,
                help='run transformation, e.g. --trf "csc_atlfast_trf.py %%IN %%OUT.AOD.root %%OUT.ntuple.root -1 0"')
group_output.add_argument('--spaceToken', action='store', dest='spaceToken', default='',
                type=str, help='spacetoken for outputs. e.g., ATLASLOCALGROUPDISK')
group_input.add_argument('--notSkipMissing', action='store_const', const=True, dest='notSkipMissing',  default=False,
                help='If input files are not read from SE, they will be skipped by default. This option disables the functionality')
group_input.add_argument('--forceDirectIO', action='store_const', const=True, dest='forceDirectIO', default=False,
                help="Use directIO if directIO is available at the site ")
group_expert.add_argument('--expertOnly_skipScout', action='store_const',const=True,dest='skipScout',default=False,
                help=argparse.SUPPRESS)
group_job.add_argument('--respectSplitRule', action='store_const',const=True,dest='respectSplitRule',default=False,
                help="force scout jobs to follow split rules like nGBPerJob")
group_expert.add_argument('--devSrv', action='store_const', const=True, dest='devSrv',  default=False,
                help="Please don't use this option. Only for developers to use the dev panda server")
group_expert.add_argument('--intrSrv', action='store_const', const=True, dest='intrSrv',  default=False,
                help="Please don't use this option. Only for developers to use the intr panda server")
group_input.add_argument('--inputType', action='store', dest='inputType', default='',
                type=str, help='A regular expression pattern. Only files matching with the pattern in input dataset are used')
group_build.add_argument('--outTarBall', action='store', dest='outTarBall', default='',
                type=str, help='Save a gzipped tarball of local files which is the input to buildXYZ')
group_build.add_argument('--inTarBall', action='store', dest='inTarBall', default='',
                type=str, help='Use a gzipped tarball of local files as input to buildXYZ. Generall the tarball is created by using --outTarBall')
group_config.add_argument('--outRunConfig', action='store', dest='outRunConfig', default='',
                type=str, help='Save extracted config information to a local file')
group_config.add_argument('--inRunConfig', action='store', dest='inRunConfig', default='',
                type=str, help='Use a saved config information to skip config extraction')
group_input.add_argument('--pfnList', action='store', dest='pfnList', default='',
                type=str, help='Name of file which contains a list of input PFNs. Those files can be un-registered in DDM')
group_build.add_argument('--cmtConfig', action='store', dest='cmtConfig', default=None,
                type=str, help='CMTCONFIG=i686-slc5-gcc43-opt is used on remote worker-node by default even if you use another CMTCONFIG locally. This option allows you to use another CMTCONFIG remotely. e.g., --cmtConfig x86_64-slc5-gcc43-opt.')
group_output.add_argument('--allowTaskDuplication',action='store_const',const=True,dest='allowTaskDuplication',default=False,
                help="As a general rule each task has a unique outDS and history of file usage is recorded per task. This option allows multiple tasks to contribute to the same outDS. Typically useful to submit a new task with the outDS which was used by another broken task. Use this option very carefully at your own risk, since file duplication happens when the second task runs on the same input which the first task successfully processed")
group_input.add_argument('--skipFilesUsedBy', action='store', dest='skipFilesUsedBy', default='',
                type=str, help='A comma-separated list of TaskIDs. Files used by those tasks are skipped when running a new task')
group_submit.add_argument('--maxAttempt', action='store', dest='maxAttempt', default=-1,
                type=int, help='Maximum number of reattempts for each job (3 by default and not larger than 50)')
group_containerJob.add_argument('--containerImage', action='store', dest='containerImage', default='',
                type=str, help="Name of a container image")
group_containerJob.add_argument('--architecture', action='store', dest='architecture', default='',
                                help="Base OS platform, CPU, and/or GPU requirements. "
                                     "The format is @base_platform#CPU_spec&GPU_spec "
                                     "where base platform, CPU, or GPU spec can be omitted. "
                                     "If base platform is not specified it is automatically taken from "
                                     "$ALRB_USER_PLATFORM. "
                                     "CPU_spec = architecture<-vendor<-instruction set>>, "
                                     "GPU_spec = vendor<-model>. A wildcards can be used if there is no special "
                                     "requirement for the attribute. E.g., #x86_64-*-avx2&nvidia to ask for x86_64 "
                                     "CPU with avx2 support and nvidia GPU")
group_build.add_argument("-3", action="store_true", dest="python3", default=False,
                  help="Use python3")
group_input.add_argument('--respectLB', action='store_const', const=True, dest='respectLB', default=False,
                         help='To generate jobs repecting lumiblock boundaries')


# athena options
group_job.add_argument('-c',action='store',dest='singleLine',type=str,default='',metavar='COMMAND',
                help='One-liner, runs before any jobOs')
group_job.add_argument('-p',action='store',dest='preConfig',type=str,default='',metavar='BOOTSTRAP',
                help='location of bootstrap file')
group_job.add_argument('-s',action='store_const',const=True,dest='codeTrace',default=False,
                help='show printout of included files')

group_expert.add_argument('--queueData', action='store', dest='queueData', default='',
                type=str, help="Please don't use this option. Only for developers")
group_submit.add_argument('--useNewCode',action='store_const',const=True,dest='useNewCode',default=False,
                help='When task are resubmitted with the same outDS, the original souce code is used to re-run on failed/unprocessed files. This option uploads new source code so that jobs will run with new binaries')
group_config.add_argument('--loadJson', action='store', dest='loadJson',default=None,
                  help="Read command-line parameters from a json file which contains a dict of {parameter: value}. Arguemnts for Athena can be specified as {'atehna_args': [...,]}")
group_config.add_argument('--dumpJson', action='store', dest='dumpJson', default=None,
                  help='Dump all command-line parameters and submission result such as returnCode, returnOut, jediTaskID, and bulkSeqNumber if --bulkSubmission is used, to a json file')
group_config.add_argument('--parentTaskID', '--parentTaskID', action='store', dest='parentTaskID',  default=None,
                          type=int,
                          help='Set taskID of the paranet task to execute the task while the parent is still running')
group_submit.add_argument('--priority', action='store', dest='priority',  default=None, type=int,
                  help='Set priority of the task (1000 by default). The value must be between 900 and 1100. ' \
                       'Note that priorities of tasks are relevant only in ' \
                       "each user's share, i.e., your tasks cannot jump over other user's tasks " \
                       'even if you give higher priorities.')
group_submit.add_argument('--osMatching', action='store_const', const=True, dest='osMatching', default=False,
                  help='To let the brokerage choose sites which have the same OS as the local machine has.')
group_job.add_argument('--cpuTimePerEvent', action='store', dest='cpuTimePerEvent', default=-1, type=int,
                help='Expected HS06 seconds per event (~= 10 * the expected duration per event in seconds)')
group_job.add_argument('--fixedCpuTime', action='store_const', const=True, dest='fixedCpuTime', default=False,
                       help='Use fixed cpuTime instead of estimated cpuTime')
group_job.add_argument('--maxWalltime', action='store', dest='maxWalltime', default=0, type=int,
  help='Max walltime for each job in hours. Note that this option works only ' \
                     'when the nevents metadata of input files are available in rucio')

from pandaclient import MiscUtils
from pandaclient.MiscUtils import commands_get_output, commands_get_status_output, commands_get_status_output_with_env

# parse options
# check against the removed options first
for arg in sys.argv[1:]:
   optName = arg.split('=',1)[0]
   if optName in removedOpts:
      print("!!Warning!! option %s has been deprecated, pls dont use anymore\n" % optName)
      sys.argv.remove(arg)

# using parse_known_args for passing arguments with -
options, args = optP.parse_known_args()

if options.verbose:
    print(options)
    print(args)
    print('')
# load json
jsonExecStr = ''
if options.loadJson is not None:
    loadOpts = MiscUtils.decodeJSON(options.loadJson)
    for k in loadOpts:
        v = loadOpts[k]
        if isinstance(v, (str, unicode)):
            try:
                v = int(v)
            except Exception:
                pass
        origK = k
        if k == 'athena_args':
            args = v
            continue
        if not hasattr(options, k):
            print("ERROR: unknown parameter {0} in {1}".format(k, options.loadJson))
            sys.exit(1)
        else:
            setattr(options,k, v)
        if v is True:
            jsonExecStr += ' --{0}'.format(origK)
        else:
            if isinstance(v, (str, unicode)):
                jsonExecStr += " --{0}='{1}'".format(origK, v)
            else:
                jsonExecStr += " --{0}={1}".format(origK, v)
    if options.verbose:
        print("options after loading json")
        print(options)
        print('')

# display version
from pandaclient import PandaToolsPkgInfo
if options.version:
    print("Version: %s" % PandaToolsPkgInfo.release_version)
    sys.exit(0)

from pandaclient import Client
from pandaclient import PsubUtils
from pandaclient import AthenaUtils
from pandaclient import PLogger

# update panda-client
if options.update:
    res = PsubUtils.updatePackage(options.verbose)
    if res:
        sys.exit(0)
    else:
        sys.exit(1)

# full execution string
fullExecString = PsubUtils.convSysArgv()
fullExecString += jsonExecStr

# get logger
tmpLog = PLogger.getPandaLogger()

# use dev server
if options.devSrv:
    Client.useDevServer()

# use INTR server
if options.intrSrv:
    Client.useIntrServer()

# noCompile uses noBuild stuff
if options.noCompile:
    if options.noBuild:
        tmpLog.error("--noBuild and --noCompile cannot be used simultaneously")
        sys.exit(EC_Config)
    options.noBuild = True

# set noBuild for container
if options.containerImage != '':
    options.noBuild = True

# files to be deleted
delFilesOnExit = []

# save current dir
currentDir = os.path.realpath(os.getcwd())

# warning for PQ
PsubUtils.get_warning_for_pq(options.site, options.excludedSite, tmpLog)

# exclude sites
if options.excludedSite != []:
    options.excludedSite = PsubUtils.splitCommaConcatenatedItems(options.excludedSite)

# use certain sites
includedSite = None
if re.search(',',options.site) is not None:
    includedSite = PsubUtils.splitCommaConcatenatedItems([options.site])
    options.site = 'AUTO'

# site specified
siteSpecified = True
if options.site == 'AUTO':
    siteSpecified = False

# list of output files which can be skipped
options.allowNoOutput = options.allowNoOutput.split(',')

# use outputPath as outDS
if not options.outDS.endswith('/'):
    options.outDS = options.outDS + '/'

# read datasets from file
if options.inDsTxt != '':
    options.inDS = PsubUtils.readDsFromFile(options.inDsTxt)

# not expand inDS when setting parent
if options.parentTaskID:
    options.notExpandInDS = True

# bulk submission
ioList = []
if options.inOutDsJson != '':
    options.bulkSubmission = True
if options.bulkSubmission:
    if options.inOutDsJson == '':
        tmpLog.error("--inOutDsJson is missing")
        sys.exit(EC_Config)
    if options.eventPickEvtList != '':
        tmpLog.error("cannnot use --eventPickEvtList and --inOutDsJson at the same time")
        sys.exit(EC_Config)
    ioList = MiscUtils.decodeJSON(options.inOutDsJson)
    for ioItem in ioList:
        if not ioItem['outDS'].endswith('/'):
            ioItem['outDS'] += '/'
    options.inDS = ioList[0]['inDS']
    options.outDS = ioList[0]['outDS']
else:
    ioList = [{'inDS': options.inDS, 'outDS': options.outDS}]

# error
if options.outDS == '':
    tmpLog.error("no outDS is given\n pathena [--inDS input] --outDS output myJobO.py")
    sys.exit(EC_Config)
if options.split < -1 :
    tmpLog.error("Number of jobs should be a positive integer")
    sys.exit(EC_Config)
if options.pfnList != '':
    if options.inDS != '':
        tmpLog.error("--pfnList and --inDS cannot be used at the same time")
        sys.exit(EC_Config)
    if options.shipinput:
        tmpLog.error("--shipInput and --inDS cannot be used at the same time")
        sys.exit(EC_Config)
    if options.site == 'AUTO':
        tmpLog.error("--site must be specified when --pfnList is used")
        sys.exit(EC_Config)

# absolute path for PFN list
usePfnList = False
if options.pfnList != '':
    options.pfnList = os.path.realpath(options.pfnList)
    usePfnList = True

# split options are mutually exclusive
if (options.nFilesPerJob > 0 and options.nEventsPerJob > 0 and options.nGBPerJob != -1):
    tmpLog.error("split by files, split by events and split by file size can not be used simultaneously")
    sys.exit(EC_Config)

# split options are mutually exclusive
if (options.nEventsPerJob > 0 and options.nGBPerJob != -1):
    tmpLog.error("split by events and split by file size can not be used simultaneously")
    sys.exit(EC_Config)

# check nGBPerJob
if not options.nGBPerJob in [-1,'MAX']:
    # convert to int
    try:
        if options.nGBPerJob != 'MAX':
            options.nGBPerJob = int(options.nGBPerJob)
    except Exception:
        tmpLog.error("--nGBPerJob must be an integer or MAX")
        sys.exit(EC_Config)
    # check negative
    if options.nGBPerJob <= 0:
        tmpLog.error("--nGBPerJob must be positive")
        sys.exit(EC_Config)
    # incompatible parameters
    if options.nFilesPerJob > 0:
        tmpLog.error("--nFilesPerJob and --nGBPerJob must be used exclusively")
        sys.exit(EC_Config)

# trf parameter
if options.trf == False:
    orig_trfStr = ''
else:
    orig_trfStr = options.trf

# AMI event-level split
if options.useAMIEventLevelSplit is None:
    if options.inDS.startswith('data') or options.goodRunListXML != '':
        # use AMI for real data since the number of events per file is not uniform
        options.useAMIEventLevelSplit = True
    else:
        options.useAMIEventLevelSplit = False

# check DBRelease
if options.dbRelease != '' and (options.dbRelease.find(':') == -1 and options.dbRelease !='LATEST'):
    tmpLog.error("invalid argument for --dbRelease. Must be DatasetName:FileName or LATEST")
    sys.exit(EC_Config)

# Good Run List
if options.goodRunListXML != '' and options.inDS != '':
    tmpLog.error("cannnot use --goodRunListXML and --inDS at the same time")
    sys.exit(EC_Config)

# event picking
if options.eventPickEvtList != '' and options.inDS != '':
    tmpLog.error("cannnot use --eventPickEvtList and --inDS at the same time")
    sys.exit(EC_Config)

# param check for event picking
if options.eventPickEvtList != '':
    if options.eventPickDataType == '':
        tmpLog.error("--eventPickDataType must be specified")
        sys.exit(EC_Config)
    if options.trf != False:
        tmpLog.error("--eventPickEvtList doesn't work with --trf until official transformations support event picking")
        sys.exit(EC_Config)


# additional files
options.extFile = options.extFile.split(',')
try:
    options.extFile.remove('')
except Exception:
    pass
options.extOutFile = re.sub(' ','',options.extOutFile)
options.extOutFile = options.extOutFile.split(',')
try:
    options.extOutFile.remove('')
except Exception:
    pass

# user-specified merging script
if options.mergeScript != '':
    # enable merging
    options.mergeOutput = True
    # add it to extFile
    if not options.mergeScript in options.extFile:
        options.extFile.append(options.mergeScript)

# glue packages
options.gluePackages = options.gluePackages.split(',')
try:
    options.gluePackages.remove('')
except Exception:
    pass

# set excludeFile
AthenaUtils.setExcludeFile(options.excludeFile)

# mapping for extra stream names
if options.appendStrToExtStream:
    AthenaUtils.enableExtendedExtStreamName()

# file list
tmpList = options.filelist.split(',')
options.filelist = []
for tmpItem in tmpList:
    if tmpItem == '':
        continue
    # wild card
    tmpItem = tmpItem.replace('*','.*')
    # append
    options.filelist.append(tmpItem)
# read file list from file
if options.inputFileList != '':
    rFile = open(options.inputFileList)
    for line in rFile:
        line = re.sub('\n','',line)
        line = line.strip()
        if line != '':
            options.filelist.append(line)
    rFile.close()

# suppressed streams
options.supStream = options.supStream.upper().split(',')
try:
    options.supStream.remove('')
except Exception:
    pass

# split related
if options.split > 0:
    # set nFiles when nEventsPerJob and nEventsPerFile are set
    if options.nEventsPerJob > 0 and options.nEventsPerFile > 0:
        if options.nEventsPerJob >= options.nEventsPerFile:
            options.nfiles = options.nEventsPerJob / options.nEventsPerFile * options.split
        else:
            options.nfiles =  options.split / (options.nEventsPerFile / options.nEventsPerJob)
            if options.nfiles == 0:
                options.nfiles = 1

    # set nFiles when nFilesPerJob is set
    if options.nFilesPerJob > 0 and options.nfiles == 0:
        options.nfiles = options.nFilesPerJob * options.split

    # set nFiles per job when nFiles is set
    if options.nFilesPerJob < 0 and options.nfiles > 0:
        options.nFilesPerJob = options.nfiles / options.split
        if options.nFilesPerJob == 0:
            options.nFilesPerJob = 1

# check
if options.inDS != '' and options.split > 0 and options.nFilesPerJob < 0 and options.nfiles == 0 and options.nEventsPerJob < 0:
    tmpLog.error("--split requires --nFilesPerJob or --nFiles or --nEventsPerJob when --inDS is specified")
    sys.exit(EC_Config)

# remove whitespaces
if options.inDS != '':
    options.inDS = options.inDS.replace(' ', '')

# warning
if options.nFilesPerJob > 0 and options.nFilesPerJob < 5:
    tmpLog.warning("Very small --nFilesPerJob tends to generate so many short jobs which could send your task to exhausted state "
                   "after scouts are done, since short jobs are problematic for the grid. Please consider not to use the option.")

# check grid-proxy
PsubUtils.check_proxy(options.verbose, options.vomsRoles)

# get nickname
nickName = PsubUtils.getNickname(options.verbose)

if nickName == '':
    sys.exit(EC_Config)

# set Rucio accounting
PsubUtils.setRucioAccount(nickName,'pathena',True)

# convert in/outTarBall to full path
if options.inTarBall != '':
    options.inTarBall = os.path.abspath(os.path.expanduser(options.inTarBall))
if options.outTarBall != '':
    options.outTarBall = os.path.abspath(os.path.expanduser(options.outTarBall))

# convert n/outRunConfig to full path
if options.inRunConfig != '':
    options.inRunConfig = os.path.abspath(os.path.expanduser(options.inRunConfig))
if options.outRunConfig != '':
    options.outRunConfig = os.path.abspath(os.path.expanduser(options.outRunConfig))

# check maxCpuCount
if options.maxCpuCount > Client.maxCpuCountLimit:
    tmpLog.error("too large maxCpuCount. Must be less than %s" % Client.maxCpuCountLimit)
    sys.exit(EC_Config)

# create tmp dir
if options.tmpDir == '':
    tmpDir = '%s/%s' % (currentDir,MiscUtils.wrappedUuidGen())
else:
    tmpDir = '%s/%s' % (os.path.abspath(options.tmpDir),MiscUtils.wrappedUuidGen())
os.makedirs(tmpDir)

# set tmp dir in Client
Client.setGlobalTmpDir(tmpDir)

# exit action
def _onExit(dir, files, del_command):
    for tmpFile in files:
        del_command('rm -rf %s' % tmpFile)
    del_command('rm -rf %s' % dir)


atexit.register(_onExit, tmpDir, delFilesOnExit, commands_get_output)


# get Athena versions
if options.verbose or options.containerImage == '':
    cmt_verbose = True
else:
    cmt_verbose = False
stA,retA = AthenaUtils.getAthenaVer(cmt_verbose)
# failed
if not stA:
    if options.containerImage == '':
        sys.exit(EC_CMT)
    # disable Athena checks when using container image without Athena runtime env
    retA = {'workArea': os.getcwd(), 'athenaVer': '', 'groupArea': '', 'cacheVer':'', 'nightVer': '', 'cmtConfig': ''}

workArea  = retA['workArea']
athenaVer = 'Atlas-%s' % retA['athenaVer']
groupArea = retA['groupArea']
cacheVer  = retA['cacheVer']
nightVer  = retA['nightVer']

# overwrite with athenaTag
if options.athenaTag != '':
    athenaVer, cacheVer, nightVer = AthenaUtils.parse_athena_tag(options.athenaTag, options.verbose, tmpLog)

# set CMTCONFIG
options.cmtConfig = AthenaUtils.getCmtConfig(athenaVer,cacheVer,nightVer,options.cmtConfig,verbose=options.verbose)

# check CMTCONFIG
if not AthenaUtils.checkCmtConfig(retA['cmtConfig'],options.cmtConfig,options.noBuild):
    sys.exit(EC_CMT)

tmpLog.info('using CMTCONFIG=%s' % options.cmtConfig)

# get run directory
# remove special characters
sString=re.sub('[\+]','.',workArea)
runDir = re.sub('^%s' % sString, '', currentDir)
if runDir == currentDir and not AthenaUtils.useCMake() and options.containerImage == '':
    errMsg  = "You need to run pathena in a directory under %s. " % workArea
    errMsg += "If '%s' is a read-only directory, perhaps you did setup Athena without --testarea or the 'here' tag of asetup." % workArea
    tmpLog.error(errMsg)
    sys.exit(EC_Config)
elif runDir == '':
    runDir = '.'
elif runDir.startswith('/'):
    runDir = runDir[1:]
runDir = runDir+'/'

# event picking
if options.eventPickEvtList != '':
    epLockedBy = 'pathena'
    if not options.noSubmit:
        epStat,epOutput = Client.requestEventPicking(options.eventPickEvtList,
                                                     options.eventPickDataType,
                                                     options.eventPickStreamName,
                                                     options.eventPickDS,
                                                     options.eventPickAmiTag,
                                                     options.filelist,
                                                     '',
                                                     options.outDS,
                                                     epLockedBy,
                                                     fullExecString,
                                                     1,
                                                     options.eventPickWithGUID,
                                                     options.ei_api,
                                                     options.verbose)
        # set input dataset
        options.inDS = epOutput
    else:
        options.inDS = 'dummy'
    tmpLog.info('requested Event Picking service to stage input as %s' % options.inDS)
    # make run/event list file for event picking
    eventPickRunEvtDat = '%s/ep_%s.dat' % (currentDir,MiscUtils.wrappedUuidGen())
    evI = open(options.eventPickEvtList)
    evO = open(eventPickRunEvtDat,'w')
    evO.write(evI.read())
    # close
    evI.close()
    evO.close()
    # add to be deleted on exit
    delFilesOnExit.append(eventPickRunEvtDat)

# get job options
jobO = ''
if options.trf:
    # replace : to = for backward compatibility
    for optArg in ['DB','RNDM']:
        options.trf = re.sub('%'+optArg+':','%'+optArg+'=',options.trf)
    # use trf's parameters
    jobO = options.trf
else:
    # get jobOs from command-line
    if options.preConfig != '':
        jobO += '-p %s ' % options.preConfig
    if options.singleLine != '':
        options.singleLine = options.singleLine.replace('"','\'')
        jobO += '-c "%s" ' % options.singleLine
    for arg in args:
        jobO += ' %s' % arg
if jobO == "":
    tmpLog.error("no jobOptions is given\n   pathena [--inDS input] --outDS output myJobO.py")
    sys.exit(EC_Config)


if options.inRunConfig == '':
    # extract run configuration
    tmpLog.info('extracting run configuration')
    # run ConfigExtractor for normal jobO
    ret,runConfig = AthenaUtils.extractRunConfig(jobO,options.supStream,options.shipinput,
                                                 options.trf,verbose=options.verbose,
                                                 useAMI=options.useAMIAutoConf,inDS=options.inDS,
                                                 tmpDir=tmpDir)
    # save runconfig
    if options.outRunConfig != '':
        cFile = open(options.outRunConfig,'w')
        pickle.dump(runConfig,cFile)
        cFile.close()
else:
    # load from file
    ret = True
    tmpRunConfFile = open(options.inRunConfig)
    runConfig = pickle.load(tmpRunConfFile)
    tmpRunConfFile.close()
if not options.trf:
    # extractor failed
    if not ret:
        sys.exit(EC_Extractor)
    # shipped files
    if runConfig.other.inputFiles:
        for fileName in runConfig.other.inputFiles:
            # append .root for tag files
            if runConfig.other.inColl:
                match = re.search('\.root(\.\d+)*$',fileName)
                if match is None:
                    fileName = '%s.root' % fileName
            # check ship files in the current dir
            if not os.path.exists(fileName):
                tmpLog.error("%s needs exist in the current directory when --shipInput is used" % fileName)
                sys.exit(EC_Extractor)
            # append to extFile
            options.extFile.append(fileName)
            if not runConfig.input.shipFiles:
                runConfig.input['shipFiles'] = []
            if fileName not in runConfig.input['shipFiles']:
                runConfig.input['shipFiles'].append(fileName)
    # generator files
    if runConfig.other.rndmGenFile:
        # append to extFile
        for fileName in runConfig.other.rndmGenFile:
            options.extFile.append(fileName)
    # Condition file
    if runConfig.other.condInput:
        # append to extFile
        for fileName in runConfig.other.condInput:
            if options.addPoolFC == "":
                options.addPoolFC = fileName
            else:
                options.addPoolFC += ",%s" % fileName
    # set default ref name
    if not runConfig.input.collRefName:
        runConfig.input.collRefName = 'Token'
    # check dupication in extOutFile
    if runConfig.output.alloutputs != False:
        if options.verbose:
            tmpLog.debug("output files : %s" % str(runConfig.output.alloutputs))
        for tmpExtOutFile in tuple(options.extOutFile):
            if tmpExtOutFile in runConfig.output.alloutputs:
                options.extOutFile.remove(tmpExtOutFile)
else:
    # parse parameters for trf
    # AMI tag
    newJobO = ''
    for tmpString in jobO.split(';'):
        match = re.search(' AMI=',tmpString)
        if match is None:
            # use original command
            newJobO += (tmpString + ';')
        else:
            tmpLog.info('getting configration from AMI')
            # get configration using GetCommand.py
            com = 'GetCommand.py ' + re.sub('^[^ ]+ ','',tmpString.strip())
            if options.verbose:
                tmpLog.debug(com)
            amiSt,amiOut = commands_get_status_output_with_env(com)
            amiSt %= 255
            if amiSt != 0:
                tmpLog.error(amiOut)
                errSt =  'Failed to get configuration from AMI. '
                errSt += 'Using AMI=tag in --trf is disallowed since it may overload the AMI server. '
                errSt += 'Please use explicit configuration parameters in --trf'
                tmpLog.error(errSt)
                sys.exit(EC_Config)
            # get full command string
            fullCommand = ''
            for amiStr in amiOut.split('\n'):
                if amiStr != '' and not amiStr.startswith('#') and not amiStr.startswith('*'):
                    fullCommand = amiStr
            # failed to extract configration
            if fullCommand == '':
                tmpLog.error(amiOut)
                errSt =  "Failed to extract configuration from AMI's output"
                tmpLog.error(errSt)
                sys.exit(EC_Config)
            # replace
            newJobO += (fullCommand + ';')
    # remove redundant ;
    newJobO = newJobO[:-1]
    # replace
    if newJobO != '':
        jobO = newJobO
        if options.verbose:
            tmpLog.debug('new jobO : '+jobO)
    # output
    oneOut = False
    # replace ; for job sequence
    tmpString = re.sub(';',' ',jobO)
    # look for --outputDAODFile and --reductionConf
    match = re.search('--outputDAODFile[ =\"\']+([^ \"\',]+)',tmpString)
    outputDAODFile = None
    if match is not None:
        outputDAODFile = match.group(1)
        # remove %OUT
        outputDAODFile = re.sub(r'%OUT\.', '', outputDAODFile)
        match = re.search(r'(--reductionConf|--formats)[ =\"\']+([^ \"\']+)', tmpString)
        if match is not None:
            # remove %OUT from outputDAODFile
            jobO = jobO.replace('%OUT.'+outputDAODFile, outputDAODFile)
            # loop over all configs
            reductionConf = match.group(2)
            for reductionItem in reductionConf.split(','):
                reductionItem = reductionItem.strip()
                if reductionItem == '':
                    continue
                # make actual output names for derivation
                tmpOutName = 'DAOD_{0}.{1}'.format(reductionItem, outputDAODFile)
                if tmpOutName not in options.extOutFile:
                    options.extOutFile.append(tmpOutName)
                    oneOut = True
    # look for %OUT
    for tmpItem in tmpString.split():
        match = re.search('\%OUT\.([^ \"\',]+)',tmpItem)
        if match:
            # append basenames to extOutFile
            tmpOutName = match.group(1)
            # skip basename of derivation
            if outputDAODFile is not None and outputDAODFile == tmpOutName:
                continue
            if tmpOutName not in options.extOutFile:
                options.extOutFile.append(tmpOutName)
                oneOut = True
    # warning if no output
    if not oneOut:
        tmpLog.warning("argument of --trf doesn't contain any %OUT")
    # check for maxEvents and skipEvents
    if options.nEventsPerJob > 0 and options.nEventsPerJob < options.nEventsPerFile:
        if '%SKIPEVENTS' not in jobO:
            tmpLog.warning("Argument of --trf doesn't contain %SKIPEVENTS. All jobs with the same input file "
                           "may process the same events unless first events are skipped by using a trf parameter "
                           "like skipEvents or something")
        if 'maxEvents' not in jobO:
            tmpLog.warning("Argument of --trf doesn't contain maxEvents or something equivalent. Each job may process all events "
                           "in the input file. Note that --nEventsPerJob doesn't automatically append maxEvents "
                           "to the argument. Please ignore this message if you limit the number of events "
                           "in each job by using another trf parameter")

# no output jobs
tmpOutKeys = list(runConfig.output)
for tmpIgnorKey in ['outUserData','alloutputs']:
    try:
        tmpOutKeys.remove(tmpIgnorKey)
    except Exception:
        pass
if tmpOutKeys == [] and options.extOutFile == [] and not options.noOutput:
    errStr  = "No output stream was extracted from jobOs or --trf. "
    if not options.trf:
        errStr += "If your job defines an output without Athena framework "
        errStr += "(e.g., using ROOT.TFile.Open instead of THistSvc) "
        errStr += "please specify the output filename by using --extOutFile. "
        errStr += "Or if you define the output with a relatively new mechanism "
        errStr += "please report it to Savannah to update the automatic extractor. "
    errStr += "If you are sure that your job doesn't produce any output file "
    errStr += "(e.g., HelloWorldOptions.py) please use --noOutput. "
    tmpLog.error(errStr)
    sys.exit(EC_Extractor)

# set extOutFile to runConfig
if options.extOutFile != []:
    runConfig.output['extOutFile'] = options.extOutFile

# check ship files in the current dir
if not runConfig.input.shipFiles:
    runConfig.input.shipFiles = []
for file in runConfig.input.shipFiles:
    if not os.path.exists(file):
        tmpLog.error("%s needs exist in the current directory when using --shipInput" % file)
        sys.exit(EC_Extractor)

# get random number
runConfig.other['rndmNumbers'] = []
if not runConfig.other.rndmStream:
    runConfig.other.rndmStream = []
if len(runConfig.other.rndmStream) != 0:
    if options.norandom:
        print()
        print("Initial random seeds need to be defined.")
        print("Enter two numbers for each random stream.")
        print("  e.g., PYTHIA : 4789899 989240512")
        print('')
    for stream in runConfig.other.rndmStream:
        if options.norandom:
            # enter manually
            while True:
                randStr = input("%s : " % stream)
                num = randStr.split()
                if len(num) == 2:
                    break
                print(" Two numbers are needed")
            runConfig.other.rndmNumbers.append([int(num[0]),int(num[1])])
        else:
            # automatic
            runConfig.other.rndmNumbers.append([random.randint(1,5000000),random.randint(1,5000000)])
    if options.norandom:
        print('')
if runConfig.other.G4RandomSeeds == True:
    if options.norandom:
        print('')
        print("Initial G4 random seeds need to be defined.")
        print("Enter one positive number.")
        print('')
        # enter manually
        while True:
            num = input("SimFlags.SeedsG4=")
            try:
                num = int(num)
                if num > 0:
                    runConfig.other.G4RandomSeeds = num
                    break
            except Exception:
                pass
        print('')
    else:
        # automatic
        runConfig.other.G4RandomSeeds = random.randint(1,10000)
else:
    # set -1 to disable G4 Random Seeds
    runConfig.other.G4RandomSeeds = -1



#####################################################################
# input datasets

if options.inDS != '' or options.shipinput or options.pfnList != '':
    # minimum bias dataset
    if options.trf and jobO.find('%MININ') != -1:
        runConfig.input.inMinBias = True
    if runConfig.input.inMinBias:
        options.minDS,options.nMin = MiscUtils.getDatasetNameAndNumFiles(options.minDS,
                                                                         options.nMin,
                                                                         'Minimum-Bias')
    # low pT minimum bias dataset
    if options.trf and jobO.find('%LOMBIN') != -1:
        runConfig.input.inLoMinBias = True
    if runConfig.input.inLoMinBias:
        options.lowMinDS,options.nLowMin = MiscUtils.getDatasetNameAndNumFiles(options.lowMinDS,
                                                                               options.nLowMin,
                                                                               'Low pT Minimum-Bias')
    # high pT minimum bias dataset
    if options.trf and jobO.find('%HIMBIN') != -1:
        runConfig.input.inHiMinBias = True
    if runConfig.input.inHiMinBias:
        options.highMinDS,options.nHighMin = MiscUtils.getDatasetNameAndNumFiles(options.highMinDS,
                                                                                 options.nHighMin,
                                                                                 'High pT Minimum-Bias')
    # cavern dataset
    if options.trf and jobO.find('%CAVIN') != -1:
        runConfig.input.inCavern = True
    if runConfig.input.inCavern:
        options.cavDS,options.nCav = MiscUtils.getDatasetNameAndNumFiles(options.cavDS,
                                                                         options.nCav,
                                                                         'Cavern')
    # beam halo dataset
    if options.trf and jobO.find('%BHIN') != -1:
        runConfig.input.inBeamHalo = True
    if runConfig.input.inBeamHalo:
        # use common DS
        if options.useCommonHalo:
            options.beamHaloDS,options.nBeamHalo = MiscUtils.getDatasetNameAndNumFiles(options.beamHaloDS,
                                                                                       options.nBeamHalo,
                                                                                       'BeamHalo')
        else:
            # get DS for A-side
            options.beamHaloADS,options.nBeamHaloA = MiscUtils.getDatasetNameAndNumFiles(options.beamHaloADS,
                                                                                         options.nBeamHaloA,
                                                                                         'BeamHalo A-side')
            # get DS for C-side
            options.beamHaloCDS,options.nBeamHaloC = MiscUtils.getDatasetNameAndNumFiles(options.beamHaloCDS,
                                                                                         options.nBeamHaloC,
                                                                                         'BeamHalo C-side')
    # beam gas dataset
    if options.trf and jobO.find('%BGIN') != -1:
        runConfig.input.inBeamGas = True
    if runConfig.input.inBeamGas:
        # use common DS
        if options.useCommonGas:
            options.beamGasDS,options.nBeamGas = MiscUtils.getDatasetNameAndNumFiles(options.beamGasDS,
                                                                                     options.nBeamGas,
                                                                                     'BeamGas')
        else:
            # get DS for H
            options.beamGasHDS,options.nBeamGasH = MiscUtils.getDatasetNameAndNumFiles(options.beamGasHDS,
                                                                                       options.nBeamGasH,
                                                                                       'BeamGas Hydrogen')
            # get DS for C
            options.beamGasCDS,options.nBeamGasC = MiscUtils.getDatasetNameAndNumFiles(options.beamGasCDS,
                                                                                       options.nBeamGasC,
                                                                                       'BeamGas Carbon')
            # get DS for O
            options.beamGasODS,options.nBeamGasO = MiscUtils.getDatasetNameAndNumFiles(options.beamGasODS,
                                                                                       options.nBeamGasO,
                                                                                       'BeamGas Oxygen')

    # general secondaries
    tmpStat, tmpOut = parse_secondary_datasets_opt(options.secondaryDSs)
    if not tmpStat:
        tmpLog.error(tmpOut)
        sys.exit(EC_Config)
    else:
        options.secondaryDSs = tmpOut


#####################################################################
# archive sources and send it to HTTP-reachable location

if True:
    if options.inTarBall == '':
        # extract jobOs with full pathnames
        for tmpItem in jobO.split():
            if re.search('^/.*\.py$',tmpItem) is not None:
                # set random name to avoid overwriting
                tmpName = tmpItem.split('/')[-1]
                tmpName = '%s_%s' % (MiscUtils.wrappedUuidGen(),tmpName)
                # set
                AthenaUtils.fullPathJobOs[tmpItem] = tmpName

        # copy some athena specific files
        AthenaUtils.copyAthenaStuff(currentDir)

        # set extFile
        AthenaUtils.setExtFile(options.extFile)

        archiveName = ""
        if not (options.noBuild and not options.noCompile):
                # archive with cpack
            if AthenaUtils.useCMake():
                archiveName,archiveFullName = AthenaUtils.archiveWithCpack(True,tmpDir,options.verbose)
            # archive sources
            archiveName,archiveFullName = AthenaUtils.archiveSourceFiles(workArea,runDir,currentDir,tmpDir,
                                                                         options.verbose,options.gluePackages,
                                                                         archiveName=archiveName)
        else:
                # archive with cpack
            if AthenaUtils.useCMake():
                archiveName,archiveFullName = AthenaUtils.archiveWithCpack(False,tmpDir,options.verbose)
            # archive jobO
            archiveName,archiveFullName = AthenaUtils.archiveJobOFiles(workArea,runDir,currentDir,
                                                                       tmpDir,options.verbose,
                                                                       archiveName=archiveName)

        # archive InstallArea
        AthenaUtils.archiveInstallArea(workArea,groupArea,archiveName,archiveFullName,
                                       tmpDir,options.noBuild,options.verbose)
        # back to tmp dir
        os.chdir(tmpDir)
        # remove some athena specific files
        AthenaUtils.deleteAthenaStuff(currentDir)
        if not os.path.exists(archiveName):
            commands_get_status_output('tar -cf {0} -T /dev/null'.format(archiveName))
        # compress
        status,out = commands_get_status_output('gzip -f %s' % archiveName)
        if status != 0 or options.verbose:
            print(out)
        archiveName += '.gz'
        # check archive
        status,out = commands_get_status_output('ls -l %s' % archiveName)
        if status != 0:
            print(out)
            tmpLog.error("Failed to archive working area.\n        If you see 'Disk quota exceeded', try '--tmpDir /tmp'")
            sys.exit(EC_Archive)

        # check symlinks
        tmpLog.info("checking symbolic links")
        status,out = commands_get_status_output('tar tvfz %s' % archiveName)
        if status != 0:
            tmpLog.error("Failed to expand archive")
            sys.exit(EC_Archive)
        symlinks = []
        for line in out.split('\n'):
            items = line.split()
            if len(items) > 0 and items[0].startswith('l') and items[-1].startswith('/'):
                symlinks.append(line)
        if symlinks != []:
            tmpStr  = "Found some unresolved symlinks which may cause a problem\n"
            tmpStr += "     See, e.g., http://savannah.cern.ch/bugs/?43885\n"
            tmpStr += "   Please ignore if you believe they are harmless"
            tmpLog.warning(tmpStr)
            for symlink in symlinks:
                print("  %s" % symlink)
    else:
        # go to tmp dir
        os.chdir(tmpDir)
        # use a saved copy
        if not (options.noBuild and not options.noCompile):
            archiveName     = 'sources.%s.tar' % MiscUtils.wrappedUuidGen()
            archiveFullName = "%s/%s" % (tmpDir,archiveName)
        else:
            archiveName     = 'jobO.%s.tar' % MiscUtils.wrappedUuidGen()
            archiveFullName = "%s/%s" % (tmpDir,archiveName)
        # make copy to avoid name duplication
        shutil.copy(options.inTarBall,archiveFullName)

    # save
    if options.outTarBall != '':
        shutil.copy(archiveName,options.outTarBall)

    # put sources/jobO via HTTP POST
    if not options.noSubmit:
        tmpLog.info("uploading source/jobO files")
        status,out = Client.putFile(archiveName,options.verbose,useCacheSrv=True,reuseSandbox=True)
        if out.startswith('NewFileName:'):
            # found the same input sandbox to reuse
            archiveName = out.split(':')[-1]
        elif out != 'True':
            # failed
            print(out)
            tmpLog.error("Failed with %s" % status)
            sys.exit(EC_Post)
        # good run list
        if options.goodRunListXML != '':
            options.goodRunListXML = PsubUtils.uploadGzippedFile(options.goodRunListXML,currentDir,tmpLog,delFilesOnExit,
                                                                 options.noSubmit,options.verbose)




# special handling
specialHandling = ''
if options.express:
    specialHandling += 'express,'
if options.debugMode:
    specialHandling += 'debug,'
specialHandling = specialHandling[:-1]


####################################################################3
# submit jobs

# append tmpdir to import taskbuffer module
sys.path = [tmpDir]+sys.path

# make task
taskParamMap = {}
taskParamMap['taskName'] = options.outDS
if not options.allowTaskDuplication:
    taskParamMap['uniqueTaskName'] = True
taskParamMap['vo'] = 'atlas'
if options.containerImage == '':
    taskParamMap['architecture'] = AthenaUtils.getCmtConfigImg(athenaVer,cacheVer,nightVer,options.cmtConfig,
                                                               architecture=options.architecture)
else:
    taskParamMap['architecture'] = options.architecture
    taskParamMap['container_name'] = options.containerImage
taskParamMap['transUses'] = athenaVer
taskParamMap['transHome'] = 'AnalysisTransforms'+cacheVer+nightVer
taskParamMap['processingType'] = 'panda-client-{0}-jedi-athena'.format(PandaToolsPkgInfo.release_version)
if options.trf:
    taskParamMap['processingType'] += '-trf'
if options.eventPickEvtList != '':
    taskParamMap['processingType'] += '-evp'
    taskParamMap['waitInput'] = 1
if options.goodRunListXML != '':
    taskParamMap['processingType'] += '-grl'
if options.prodSourceLabel == '':
    taskParamMap['prodSourceLabel'] = 'user'
else:
    taskParamMap['prodSourceLabel'] = options.prodSourceLabel
if options.site != 'AUTO':
    taskParamMap['site'] = options.site
else:
    taskParamMap['site'] = None
taskParamMap['excludedSite'] = options.excludedSite
if includedSite is not None and includedSite != []:
    taskParamMap['includedSite'] = includedSite
else:
    taskParamMap['includedSite'] = None
if options.priority is not None:
    taskParamMap['currentPriority'] = options.priority
if options.nfiles > 0:
    taskParamMap['nFiles'] = options.nfiles
if options.nFilesPerJob > 0:
    taskParamMap['nFilesPerJob'] = options.nFilesPerJob
if not options.nGBPerJob in [-1,'MAX']:
    # don't set MAX since it is the defalt on the server side
    taskParamMap['nGBPerJob'] = options.nGBPerJob
if options.nEventsPerJob > 0:
    taskParamMap['nEventsPerJob'] = options.nEventsPerJob
    if options.nEventsPerFile <= 0:
        taskParamMap['useRealNumEvents'] = True
if options.nEventsPerFile > 0:
    taskParamMap['nEventsPerFile'] = options.nEventsPerFile
if options.split > 0 and options.nEventsPerJob > 0:
    taskParamMap['nEvents'] = options.split*options.nEventsPerJob
taskParamMap['cliParams'] = fullExecString
if options.noEmail:
    taskParamMap['noEmail'] = True
if options.skipScout:
    taskParamMap['skipScout'] = True
if options.respectSplitRule:
    taskParamMap['respectSplitRule'] = True
if options.disableAutoRetry:
    taskParamMap['disableAutoRetry'] = 1
if options.workingGroup is not None:
    taskParamMap['workingGroup'] = options.workingGroup
if options.official:
    taskParamMap['official'] = True
if options.useNewCode:
    taskParamMap['fixedSandbox'] = archiveName
if options.maxCpuCount > 0:
    taskParamMap['walltime'] = -options.maxCpuCount
if options.noLoopingCheck:
    taskParamMap['noLoopingCheck'] = True
if options.maxWalltime > 0:
    taskParamMap['maxWalltime'] = options.maxWalltime
if options.cpuTimePerEvent > 0:
    taskParamMap['cpuTime'] = options.cpuTimePerEvent
    taskParamMap['cpuTimeUnit'] = 'HS06sPerEvent'
if options.fixedCpuTime:
    taskParamMap['cpuTimeUnit'] = 'HS06sPerEventFixed'
if options.memory > 0:
    taskParamMap['ramCount'] = options.memory
    if options.fixedRamCount:
        taskParamMap['ramCountUnit'] = 'MBPerCoreFixed'
    else:
        taskParamMap['ramCountUnit'] = 'MBPerCore'
if options.outDiskCount is not None:
    taskParamMap['outDiskCount'] = options.outDiskCount
    taskParamMap['outDiskUnit'] = 'kBFixed'
if options.nCore > 1:
    taskParamMap['coreCount'] = options.nCore
elif options.nThreads > 1:
    taskParamMap['coreCount'] = options.nThreads
if options.skipFilesUsedBy != '':
    taskParamMap['skipFilesUsedBy'] = options.skipFilesUsedBy
taskParamMap['respectSplitRule'] = True
if options.respectLB:
    taskParamMap['respectLB'] = True
if options.maxAttempt > 0 and options.maxAttempt <= 50:
    taskParamMap['maxAttempt'] = options.maxAttempt
if options.debugMode:
    taskParamMap['debugMode'] = True
if options.osMatching:
    taskParamMap['osMatching'] = True
taskParamMap['osInfo'] = PsubUtils.get_os_information()
if options.parentTaskID:
    taskParamMap['noWaitParent'] = True
# source URL
matchURL = re.search("(http.*://[^/]+)/",Client.baseURLCSRVSSL)
if matchURL is not None:
    taskParamMap['sourceURL'] = matchURL.group(1)
# middle name
if options.addNthFieldOfInFileToLFN != '':
    taskParamMap['addNthFieldToLFN'] = options.addNthFieldOfInFileToLFN
    taskParamMap['useFileAsSourceLFN'] = True
elif options.addNthFieldOfInDSToLFN != '':
    taskParamMap['addNthFieldToLFN'] = options.addNthFieldOfInDSToLFN
# dataset name
logDatasetName = re.sub('/$','.log/',options.outDS)
# log
taskParamMap['log'] = {'dataset': logDatasetName,
                       'container': logDatasetName,
                       'type':'template',
                       'param_type':'log',
                       'value':'{0}.$JEDITASKID.${{SN}}.log.tgz'.format(logDatasetName[:-1])
                       }

if options.addNthFieldOfInFileToLFN != '':
    loglfn  = '{0}.{1}'.format(*logDatasetName.split('.')[:2])
    loglfn += '${MIDDLENAME}.$JEDITASKID._${SN}.log.tgz'
    taskParamMap['log']['value'] = loglfn
if options.spaceToken != '':
    taskParamMap['log']['token'] = options.spaceToken
if options.mergeOutput and options.mergeLog:
    # log merge
    mLogDatasetName = re.sub(r'\.log/', r'.merge_log/', logDatasetName)
    mLFN = re.sub(r'\.log\.tgz', r'.merge_log.tgz', taskParamMap['log']['value'])
    data = copy.deepcopy(taskParamMap['log'])
    data.update({'dataset': mLogDatasetName,
                 'container': mLogDatasetName,
                 'param_type': 'output',
                 'mergeOnly': True,
                 'value': mLFN})
    taskParamMap['log_merge'] = data

# make job parameters
taskParamMap['jobParameters'] = []

# build
if options.noBuild and not options.noCompile:
    taskParamMap['jobParameters'] += [
        {'type':'constant',
         'value': '-a {0}'.format(archiveName),
         },
        ]
else:
    taskParamMap['jobParameters'] += [
        {'type':'constant',
         'value': '-l ${LIB}',
         },
        ]
# pre execution string
pStr1 = ''
if runConfig.other.rndmStream != []:
    pStr1 = "AtRndmGenSvc=Service('AtRndmGenSvc');AtRndmGenSvc.Seeds=["
    for stream in runConfig.other.rndmStream:
        num = runConfig.other.rndmNumbers[runConfig.other.rndmStream.index(stream)]
        pStr1 += "'%s ${RNDMSEED} %s'," % (stream,num[1])
    pStr1 += "]"
    dictItem = {'type':'template',
                'param_type':'number',
                'value':'${RNDMSEED}',
                'hidden':True,
                'offset':runConfig.other.rndmStream[0][0],
                }
    taskParamMap['jobParameters'] += [dictItem]
# split by event option was invoked
pStr2 = ''
if options.nEventsPerJob > 0 and (not options.trf):
    # @ Number of events to be processed per job
    param1 = "theApp.EvtMax=${MAXEVENTS}"
    # @ possibly skip events in a file
    if runConfig.input.noInput:
        pStr2 = param1
    else:
        param2 = "EventSelector.SkipEvents=${SKIPEVENTS}"
        # @ Form a string to add to job parameters
        pStr2 = '%s;%s' % (param1,param2)
# set pre execution parameter
if pStr1 != '' or pStr2 != '':
    if pStr1 == '' or pStr2 == '':
        preStr = pStr1+pStr2
    else:
        preStr = "%s;%s" % (pStr1,pStr2)
    taskParamMap['jobParameters'] += [
        {'type':'constant',
         'value': '-f "',
         'padding':False,
         },
        ]
    taskParamMap['jobParameters'] += PsubUtils.convertParamStrToJediParam(preStr,{},'',
                                                                          False,False)
    taskParamMap['jobParameters'] += [
        {'type':'constant',
         'value': '"',
         },
        ]

# misc
param  = '--sourceURL ${SURL} '
param += '-r {0} '.format(runDir)
# addPoolFC
if options.addPoolFC != "":
    param += '--addPoolFC %s ' % options.addPoolFC
# disable to skip missing files
if options.notSkipMissing:
    param += '--notSkipMissing '
# given PFN
if options.pfnList != '':
    param += '--givenPFN '
# run TRF
if options.trf:
    param += '--trf '
# general input format
if options.generalInput:
    param += '--generalInput '
# use theApp.nextEvent
if options.useNextEvent:
    param += '--useNextEvent '
# use CMake
if AthenaUtils.useCMake() or options.containerImage != '':
    param += "--useCMake "
# AthenaMT
if options.nThreads > 1:
    param += "--useAthenaMT "
# use code tracer
if options.codeTrace:
    param += '--codeTrace '
# debug parameters
if options.queueData != '':
    param += "--overwriteQueuedata=%s " % options.queueData
# read BS
if runConfig.input.inBS:
    param += '-b '
# use back navigation
if runConfig.input.backNavi:
    param += '-e '
# ship input
if options.shipinput:
    param += '--shipInput '
# event picking
if options.eventPickEvtList != '':
    param += '--eventPickTxt=%s ' % eventPickRunEvtDat.split('/')[-1]
# assign
if param != '':
    taskParamMap['jobParameters'] += [
        {'type':'constant',
         'value': param,
         },
        ]

# input
inputMap = {}
if options.inDS != '':
    tmpDict = {'type':'template',
               'param_type':'input',
               'value':'-i "${IN/T}"',
               'dataset':options.inDS,
               'expand':True,
               'exclude':'\.log\.tgz(\.\d+)*$',
               }
    if options.notExpandInDS:
        del tmpDict['expand']
    if options.inputType != '':
        tmpDict['include'] = options.inputType
    if options.filelist != []:
        tmpDict['files'] = options.filelist
    taskParamMap['jobParameters'].append(tmpDict)
    taskParamMap['dsForIN'] = options.inDS
    inputMap['IN'] = options.inDS
elif options.pfnList != '':
    taskParamMap['pfnList'] = PsubUtils.getListPFN(options.pfnList)
    # use noInput mecahism
    taskParamMap['noInput'] = True
    if options.nfiles == 0:
        taskParamMap['nFiles'] = len(taskParamMap['pfnList'])
    taskParamMap['jobParameters'] += [
        {'type':'constant',
         'value':'-i "${IN/T}"',
         },
        ]
elif options.goodRunListXML != '':
    tmpDict = {'type':'template',
               'param_type':'input',
               'value':'-i "${IN/T}"',
               'dataset':'%%INDS%%',
               'expand':True,
               'exclude':'\.log\.tgz(\.\d+)*$',
               'files':'%%INLFNLIST%%',
               }
    taskParamMap['jobParameters'].append(tmpDict)
    taskParamMap['dsForIN'] = '%%INDS%%'
    inputMap['IN'] ='%%INDS%%'
else:
    # no input
    taskParamMap['noInput'] = True
    if options.nEventsPerJob > 0:
        taskParamMap['nEventsPerJob'] = options.nEventsPerJob
    else:
        taskParamMap['nEventsPerJob'] = 1
    if options.split > 0:
        taskParamMap['nEvents'] = options.split
    else:
        taskParamMap['nEvents'] = 1
    taskParamMap['nEvents'] *= taskParamMap['nEventsPerJob']
    taskParamMap['jobParameters'] += [
        {'type':'constant',
         'value': '-i "[]"',
         },
        ]

# extract DBR for --trf
dbrInTRF = False
if options.trf:
    tmpMatch = re.search('%DB=([^ \'\";]+)',jobO)
    if tmpMatch is not None:
        options.dbRelease = tmpMatch.group(1)
        dbrInTRF = True
# param for DBR
if options.dbRelease != '':
    dbrDS = options.dbRelease.split(':')[0]
    # change LATEST to DBR_LATEST
    if dbrDS == 'LATEST':
        dbrDS = 'DBR_LATEST'
    dictItem = {'type':'template',
                'param_type':'input',
                'value':'--dbrFile=${DBR}',
                'dataset':dbrDS,
                }
    taskParamMap['jobParameters'] += [dictItem]
    # no expansion
    if dbrInTRF:
        dictItem = {'type':'constant',
                    'value':'--noExpandDBR',
                    }
        taskParamMap['jobParameters'] += [dictItem]

# minimum bias
minBiasStream = ''
if options.minDS != '':
    if options.notExpandMinDS:
        expand_flag = False
    else:
        expand_flag = True
    dictItem = MiscUtils.makeJediJobParam('${MININ}',options.minDS,'input',hidden=True,
                                          expand=expand_flag,exclude='\.log\.tgz(\.\d+)*$',
                                          nFilesPerJob=options.nMin,useNumFilesAsRatio=True,
                                          randomAtt=options.randomMin)
    taskParamMap['jobParameters'] += dictItem
    inputMap['MININ'] = options.minDS
    minBiasStream += 'MININ,'
    if options.sameSecRetry:
        taskParamMap['reuseSecOnDemand'] = True
if options.lowMinDS != '':
    if options.notExpandLowMinDS:
        expand_flag = False
    else:
        expand_flag = True
    dictItem = MiscUtils.makeJediJobParam('${LOMBIN}',options.lowMinDS,'input',hidden=True,
                                          expand=expand_flag,exclude='\.log\.tgz(\.\d+)*$',
                                          nFilesPerJob=options.nLowMin,useNumFilesAsRatio=True,
                                          randomAtt=options.randomMin)
    taskParamMap['jobParameters'] += dictItem
    inputMap['LOMBIN'] = options.lowMinDS
    minBiasStream += 'LOMBIN,'
    if options.sameSecRetry:
        taskParamMap['reuseSecOnDemand'] = True
if options.highMinDS != '':
    if options.notExpandHighMinDS:
        expand_flag = False
    else:
        expand_flag = True
    dictItem = MiscUtils.makeJediJobParam('${HIMBIN}',options.highMinDS,'input',hidden=True,
                                          expand=expand_flag,exclude='\.log\.tgz(\.\d+)*$',
                                          nFilesPerJob=options.nHighMin,useNumFilesAsRatio=True,
                                          randomAtt=options.randomMin)
    taskParamMap['jobParameters'] += dictItem
    inputMap['HIMBIN'] = options.highMinDS
    minBiasStream += 'HIMBIN,'
    if options.sameSecRetry:
        taskParamMap['reuseSecOnDemand'] = True
minBiasStream = minBiasStream[:-1]
if minBiasStream != '':
    dictItem = {'type':'constant',
                'value':'-m "${{{0}/T}}"'.format(minBiasStream),
                }
    taskParamMap['jobParameters'] += [dictItem]


# cavern
if options.cavDS != '':
    if options.notExpandCavDS:
        expand_flag = False
    else:
        expand_flag = True
    dictItem = MiscUtils.makeJediJobParam('-n "${CAVIN/T}"',options.cavDS,'input',
                                          expand=expand_flag,exclude='\.log\.tgz(\.\d+)*$',
                                          nFilesPerJob=options.nCav,useNumFilesAsRatio=True,
                                          randomAtt=options.randomCav)
    taskParamMap['jobParameters'] += dictItem
    inputMap['CAVIN'] = options.cavDS
    if options.sameSecRetry:
        taskParamMap['reuseSecOnDemand'] = True

# beam halo
beamHaloStream = ''
if options.beamHaloDS != '':
    dictItem = MiscUtils.makeJediJobParam('${BHIN}',options.beamHaloDS,'input',hidden=True,
                                          expand=True,exclude='\.log\.tgz(\.\d+)*$',
                                          nFilesPerJob=options.nBeamHalo,useNumFilesAsRatio=True)
    taskParamMap['jobParameters'] += dictItem
    inputMap['BHIN'] = options.beamHaloDS
    beamHaloStream += 'BHIN,'
    if options.sameSecRetry:
        taskParamMap['reuseSecOnDemand'] = True
if options.beamHaloADS != '':
    dictItem = MiscUtils.makeJediJobParam('${BHAIN}',options.beamHaloADS,'input',hidden=True,
                                          expand=True,exclude='\.log\.tgz(\.\d+)*$',
                                          nFilesPerJob=options.nBeamHaloA,useNumFilesAsRatio=True)
    taskParamMap['jobParameters'] += dictItem
    inputMap['BHAIN'] = options.beamHaloADS
    beamHaloStream += 'BHAIN,'
    if options.sameSecRetry:
        taskParamMap['reuseSecOnDemand'] = True
if options.beamHaloCDS != '':
    dictItem = MiscUtils.makeJediJobParam('${BHCIN}',options.beamHaloCDS,'input',hidden=True,
                                          expand=True,exclude='\.log\.tgz(\.\d+)*$',
                                          nFilesPerJob=options.nBeamHaloC,useNumFilesAsRatio=True)
    taskParamMap['jobParameters'] += dictItem
    inputMap['BHCIN'] = options.beamHaloCDS
    beamHaloStream += 'BHCIN,'
    if options.sameSecRetry:
        taskParamMap['reuseSecOnDemand'] = True
beamHaloStream = beamHaloStream[:-1]
if beamHaloStream != '':
    dictItem = {'type':'constant',
                'value':'--beamHalo "${{{0}/T}}"'.format(beamHaloStream)
                }
    taskParamMap['jobParameters'] += [dictItem]


# beam gas
beamGasStream = ''
if options.beamGasDS != '':
    dictItem = MiscUtils.makeJediJobParam('${BGIN}',options.beamGasDS,'input',hidden=True,
                                          expand=True,exclude='\.log\.tgz(\.\d+)*$',
                                          nFilesPerJob=options.nBeamGas,useNumFilesAsRatio=True)
    taskParamMap['jobParameters'] += dictItem
    inputMap['BGIN'] = options.beamGasDS
    beamGasStream += 'BGIN,'
if options.beamGasHDS != '':
    dictItem = MiscUtils.makeJediJobParam('${BGHIN}',options.beamGasHDS,'input',hidden=True,
                                          expand=True,exclude='\.log\.tgz(\.\d+)*$',
                                          nFilesPerJob=options.nBeamGasH,useNumFilesAsRatio=True)
    taskParamMap['jobParameters'] += dictItem
    inputMap['BGHIN'] = options.beamGasHDS
    beamGasStream += 'BGHIN,'
if options.beamGasCDS != '':
    dictItem = MiscUtils.makeJediJobParam('${BGCIN}',options.beamGasCDS,'input',hidden=True,
                                          expand=True,exclude='\.log\.tgz(\.\d+)*$',
                                          nFilesPerJob=options.nBeamGasC,useNumFilesAsRatio=True)
    taskParamMap['jobParameters'] += dictItem
    inputMap['BGCIN'] = options.beamGasHDS
    beamGasStream += 'BGCIN,'
if options.beamGasODS != '':
    dictItem = MiscUtils.makeJediJobParam('${BGOIN}',options.beamGasODS,'input',hidden=True,
                                          expand=True,exclude='\.log\.tgz(\.\d+)*$',
                                          nFilesPerJob=options.nBeamGasO,useNumFilesAsRatio=True)
    taskParamMap['jobParameters'] += dictItem
    inputMap['BGOIN'] = options.beamGasODS
    beamGasStream += 'BGOIN,'
beamGasStream = beamGasStream[:-1]
if beamGasStream != '':
    dictItem = {'type':'constant',
                'value':'--beamGas "${{{0}/T}}"'.format(beamGasStream)
                }
    taskParamMap['jobParameters'] += [dictItem]

# general secondaries
if options.secondaryDSs:
    for tmpDsName in options.secondaryDSs:
        tmpMap = options.secondaryDSs[tmpDsName]
        # make template item
        streamName = tmpMap['streamName']
        if not options.notExpandSecDSs:
            expandFlag = True
        else:
            expandFlag = False
        dictItem = MiscUtils.makeJediJobParam('${' + streamName + '}', tmpDsName, 'input', hidden=True,
                                              expand=expandFlag, include=tmpMap['pattern'], offset=tmpMap['nSkip'],
                                              nFilesPerJob=tmpMap['nFiles'])
        taskParamMap['jobParameters'] += dictItem
        inputMap[streamName] = tmpDsName
    dictItem = {'type':'constant',
                'value':'-m "${{{0}/T}}"'.format(
                    ','.join([tmpMap['streamName'] for tmpMap in options.secondaryDSs.values()]))
                }
    taskParamMap['jobParameters'] += [dictItem]

# output
if options.addNthFieldOfInDSToLFN != '' or options.addNthFieldOfInFileToLFN != '':
    descriptionInLFN = '${MIDDLENAME}'
else:
    descriptionInLFN = ''
outMap,tmpParamList = AthenaUtils.convertConfToOutput(runConfig,options.extOutFile,options.outDS,
                                                      destination=options.destSE,spaceToken=options.spaceToken,
                                                      descriptionInLFN=descriptionInLFN,
                                                      allowNoOutput=options.allowNoOutput)
taskParamMap['jobParameters'] += [
    {'type':'constant',
     'value': '-o "%s" ' % outMap
     },
    ]
taskParamMap['jobParameters'] += tmpParamList


# jobO parameter
if not options.trf:
    tmpJobO = jobO
    # replace full-path jobOs
    for tmpFullName in AthenaUtils.fullPathJobOs:
        tmpLocalName = AthenaUtils.fullPathJobOs[tmpFullName]
        tmpJobO = re.sub(tmpFullName,tmpLocalName,tmpJobO)
    # modify one-liner for G4 random seeds
    if runConfig.other.G4RandomSeeds > 0:
        if options.singleLine != '':
            tmpJobO = re.sub('-c "%s" ' % options.singleLine,
                             '-c "%s;from G4AtlasApps.SimFlags import SimFlags;SimFlags.SeedsG4=${RNDMSEED}" ' \
                                 % options.singleLine,tmpJobO)
        else:
            tmpJobO = '-c "from G4AtlasApps.SimFlags import SimFlags;SimFlags.SeedsG4=${RNDMSEED}" ' + tmpJobO
        dictItem = {'type':'template',
                    'param_type':'number',
                    'value':'${RNDMSEED}',
                    'hidden':True,
                    'offset':runConfig.other.G4RandomSeeds,
                    }
        taskParamMap['jobParameters'] += [dictItem]
else:
    # replace parameters for TRF
    tmpJobO = jobO
    # output : basenames are in outMap['IROOT'] trough extOutFile
    tmpOutMap = []
    if 'IROOT' in outMap:
        for tmpName,tmpLFN in outMap['IROOT']:
            tmpJobO = tmpJobO.replace('%OUT.' + tmpName,tmpName)
    # replace DBR
    tmpJobO = re.sub('%DB=[^ \'\";]+','${DBR}',tmpJobO)
# set jobO parameter
taskParamMap['jobParameters'] += [
    {'type':'constant',
     'value': '-j "',
     'padding':False,
     },
    ]
if options.secondaryDSs:
    extra_in_list = [tmpMap['streamName'] for tmpMap in options.secondaryDSs.values()]
else:
    extra_in_list = []
taskParamMap['jobParameters'] += PsubUtils.convertParamStrToJediParam(
    tmpJobO, inputMap, options.outDS[:-1],
    True, False, usePfnList,
    extra_in_list=extra_in_list)
taskParamMap['jobParameters'] += [
    {'type':'constant',
     'value': '"',
     },
    ]

# use local IO for trf or BS
if options.forceStaged or ((options.trf or runConfig.input.inBS) and not options.forceDirectIO):
    taskParamMap['useLocalIO'] = 1

# use AMI to get the number of events per file
if options.useAMIEventLevelSplit == True:
    taskParamMap['getNumEventsInMetadata'] = True

# avoid VP
if options.avoidVP:
    taskParamMap['avoidVP'] = True

# build step
if options.noBuild and not options.noCompile:
    pass
else:
    jobParameters = '-i ${IN} -o ${OUT} --sourceURL ${SURL} '
    # no compile
    if options.noCompile:
        jobParameters += "--noCompile "
    # use CMake
    if AthenaUtils.useCMake() or options.containerImage != '':
        jobParameters += "--useCMake "
    # debug parameters
    if options.queueData != '':
        jobParameters += "--overwriteQueuedata=%s " % options.queueData
    # set task param
    taskParamMap['buildSpec'] = {
        'prodSourceLabel':'panda',
        'archiveName':archiveName,
        'jobParameters':jobParameters,
        }
    if options.prodSourceLabel != '':
         taskParamMap['buildSpec']['prodSourceLabel'] = options.prodSourceLabel

# preprocessing step

# good run list
if options.goodRunListXML != '':
    jobParameters = "--goodRunListXML {0} ".format(options.goodRunListXML)
    if options.goodRunDataType != '':
        jobParameters += "--goodRunListDataType {0} ".format(options.goodRunDataType)
    if options.goodRunProdStep != '':
        jobParameters += "--goodRunListProdStep {0} ".format(options.goodRunProdStep)
    if options.goodRunListDS != '':
        jobParameters += "--goodRunListDS {0} ".format(options.goodRunListDS)
    jobParameters += "--sourceURL ${SURL} "
    # set task param
    taskParamMap['preproSpec'] = {
        'prodSourceLabel':'panda',
        'jobParameters':jobParameters,
        }
    if options.prodSourceLabel != '':
         taskParamMap['preproSpec']['prodSourceLabel'] = options.prodSourceLabel

# merging
if options.mergeOutput:
    jobParameters = '-r {0} '.format(runDir)
    if options.mergeScript != '':
        jobParameters += '-j "{0}" '.format(options.mergeScript)
    if not options.noBuild:
        jobParameters += '-l ${LIB} '
    else:
        jobParameters += '-a {0} '.format(archiveName)
        jobParameters += "--sourceURL ${SURL} "
    jobParameters += "--useAthenaPackages "
    if AthenaUtils.useCMake() or options.containerImage != '':
        jobParameters += "--useCMake "
    jobParameters += '${TRN_OUTPUT:OUTPUT} '
    if options.mergeLog:
        jobParameters += '${TRN_LOG_MERGE:LOG_MERGE}'
    else:
        jobParameters += '${TRN_LOG:LOG}'
    taskParamMap['mergeSpec'] = {}
    taskParamMap['mergeSpec']['useLocalIO'] = 1
    taskParamMap['mergeSpec']['jobParameters'] = jobParameters
    taskParamMap['mergeOutput'] = True
    if options.nGBPerMergeJob > 0:
        taskParamMap['nGBPerMergeJob'] = options.nGBPerMergeJob



#####################################################################
# submission

exitCode = 0
dumpList = []

# submit task
for iSubmission, ioItem in enumerate(ioList):
    if options.verbose:
        print("== parameters ==")
        print("Site       : %s" % options.site)
        print("Athena     : %s" % athenaVer)
        if groupArea != '':
            print("Group Area : %s" % groupArea)
        if cacheVer != '':
            print("ProdCache  : %s" % cacheVer[1:])
        if nightVer != '':
            print("Nightly    : %s" % nightVer[1:])
        print("cmtConfig  : %s" % AthenaUtils.getCmtConfigImg(athenaVer,cacheVer,nightVer,options.cmtConfig))
        print("RunDir     : %s" % runDir)
        print("jobO       : %s" % jobO.lstrip())

    if len(ioList) == 1:
        newTaskParamMap = taskParamMap
    else:
        # replace input and output
        options.inDS = ioItem['inDS']
        options.outDS = ioItem['outDS']
        newTaskParamMap = PsubUtils.replaceInputOutput(taskParamMap, ioItem['inDS'],
                                                       ioItem['outDS'], iSubmission)
    taskID = None
    # check outDS format
    if not PsubUtils.checkOutDsName(options.outDS,options.official,nickName,
                                    options.mergeOutput, options.verbose):
        tmpLog.error("invalid output datasetname:%s" % options.outDS)
        sys.exit(EC_Config)
    # check task parameters
    exitCode, tmpStr = PsubUtils.checkTaskParam(newTaskParamMap, options.unlimitNumOutputs)
    if exitCode != 0 and len(ioList) == 1:
        sys.exit(exitCode)
    if options.noSubmit:
        if options.verbose:
            tmpLog.debug("==== taskParams ====")
            tmpKeys = list(newTaskParamMap)
            tmpKeys.sort()
            for tmpKey in tmpKeys:
                print('%s : %s' % (tmpKey, newTaskParamMap[tmpKey]))
    if not options.noSubmit and exitCode == 0:
        tmpLog.info("submit {0}".format(options.outDS))
        status,tmpOut = Client.insertTaskParams(newTaskParamMap, options.verbose, properErrorCode=True,
                                                parent_tid=options.parentTaskID)
        # result
        if status != 0:
            tmpStr = "task submission failed with {0}".format(status)
            tmpLog.error(tmpStr)
            exitCode = EC_Submit
        else:
            if tmpOut[0] in [0,3]:
                tmpStr = tmpOut[1]
                tmpLog.info(tmpStr)
                try:
                    m = re.search('jediTaskID=(\d+)', tmpStr)
                    taskID = int(m.group(1))
                except Exception:
                    pass
            else:
                tmpStr = "task submission failed. {0}".format(tmpOut[1])
                tmpLog.error(tmpStr)
                exitCode = EC_Submit
    dumpItem = copy.deepcopy(vars(options))
    dumpItem['returnCode'] = exitCode
    dumpItem['returnOut'] = tmpStr
    dumpItem['jediTaskID'] = taskID
    if len(ioList) > 1:
        dumpItem['bulkSeqNumber'] = iSubmission
    dumpList.append(dumpItem)

# go back to current dir
os.chdir(currentDir)
# dump
if options.dumpJson is not None:
    with open(options.dumpJson, 'w') as f:
        json.dump(dumpList, f)
# succeeded
sys.exit(0)
