"""Command-line arguments of pathena"""

import argparse

from pandaclient.CommonArgs import add_common_arguments
from pandaclient.Group_argparse import get_parser


def build_parser():
    """Build the command-line parser of pathena with the common and the pathena-specific arguments

    returns:
        GroupArgParser ready to parse the pathena command line
    """
    usage = """pathena [options] <jobOption1.py> [<jobOption2.py> [...]]

  HowTo is available at https://panda-wms.readthedocs.io/en/latest/client/pathena.html"""

    examples = """Examples:
  pathena --inDS=... --outDS=... jobO_1.py jobO_2.py
  pathena --inDS=... --outDS=... --trf "Reco_tf.py --inputAODFile %IN --outputDAODFile %OUT.pool.root ..."
  pathena --inOutDsJson=inout.json --trf "..."
  pathena --helpGroup JOB
"""

    optP = get_parser(usage=usage, conflict_handler="resolve")
    optP.set_examples(examples)

    # define option groups
    group_print = optP.add_group("print", "debug and verbose printout")
    group_pathena = optP.add_group("pathena", "pathena version")
    group_config = optP.add_group("config", "load/dump options, results and task parameters from/to files")
    group_input = optP.add_group("input", "input datasets/files and their selection, including pile-up inputs")
    group_output = optP.add_group("output", "output datasets, file names and merging")
    group_job = optP.add_group("job", "job splitting, per-job limits and the payload to run")
    group_build = optP.add_group("build", "packing of local files and setup of the remote build/runtime environment")
    group_submit = optP.add_group("submit", "site selection, resources, priority and retries")
    group_evtFilter = optP.add_group("evtFilter", "event selection by Good Run List or event picking")
    group_expert = optP.add_group("expert", "for experts and developers only")

    usage_containerJob = """Visit the following wiki page for examples:
  https://twiki.cern.ch/twiki/bin/view/PanDA/PandaRun#Run_user_containers_jobs

Please test the job interactively first prior to submitting to the grid.
Check the following on how to test container job interactively:
  https://twiki.cern.ch/twiki/bin/viewauth/AtlasComputing/SingularityInAtlas
"""
    group_containerJob = optP.add_group("containerJob", "running jobs in containers and hardware requirements", usage=usage_containerJob)

    add_common_arguments(optP)

    optP.add_helpGroup(addHelp="Some options such as --inOutDsJson may SPAN several groups")

    # special options
    group_pathena.add_argument(
        "--version",
        action="store_const",
        const=True,
        dest="version",
        default=False,
        help="Displays version",
    )
    group_build.add_argument(
        "--athenaTag",
        action="store",
        metavar="TAGS",
        dest="athenaTag",
        default="",
        type=str,
        help="Use different version of Athena on remote WN. By default the same version which you are locally using is set up on WN. E.g., --athenaTag=AtlasProduction,14.2.24.3",
    )
    group_input.add_argument(
        "--inDS",
        action="store",
        metavar="DATASET",
        dest="inDS",
        default="",
        type=str,
        help="Input dataset names. wildcard and/or comma can be used to concatenate multiple datasets",
    )
    group_input.add_argument(
        "--secondaryDSs",
        action="store",
        metavar="STREAM:N:DATASET[:...][,...]",
        dest="secondaryDSs",
        default="",
        help="A versatile option to specify arbitrary secondary inputs that takes a list of " "secondary datasets. See PandaRun wiki page for detail",
    )
    group_input.add_argument(
        "--minDS",
        action="store",
        metavar="DATASET",
        dest="minDS",
        default="",
        type=str,
        help="Dataset name for minimum bias stream",
    )
    group_job.add_argument(
        "--nMin",
        action="store",
        metavar="N",
        dest="nMin",
        default=-1,
        type=int,
        help="Number of minimum bias files per sub job",
    )
    group_input.add_argument(
        "--notExpandMinDS",
        action="store_const",
        const=True,
        dest="notExpandMinDS",
        default=False,
        help="Allow jobs to use files across dataset boundaries in minimum bias dataset container",
    )
    group_input.add_argument(
        "--lowMinDS",
        action="store",
        metavar="DATASET",
        dest="lowMinDS",
        default="",
        type=str,
        help="Dataset name for low pT minimum bias stream",
    )
    group_job.add_argument(
        "--nLowMin",
        action="store",
        metavar="N",
        dest="nLowMin",
        default=-1,
        type=int,
        help="Number of low pT minimum bias files per job",
    )
    group_input.add_argument(
        "--notExpandLowMinDS",
        action="store_const",
        const=True,
        dest="notExpandLowMinDS",
        default=False,
        help="Allow jobs to use files across dataset boundaries in low minimum bias dataset container",
    )
    group_input.add_argument(
        "--highMinDS",
        action="store",
        metavar="DATASET",
        dest="highMinDS",
        default="",
        type=str,
        help="Dataset name for high pT minimum bias stream",
    )
    group_job.add_argument(
        "--nHighMin",
        action="store",
        metavar="N",
        dest="nHighMin",
        default=-1,
        type=int,
        help="Number of high pT minimum bias files per job",
    )
    group_input.add_argument(
        "--notExpandHighMinDS",
        action="store_const",
        const=True,
        dest="notExpandHighMinDS",
        default=False,
        help="Allow jobs to use files across dataset boundaries in high minimum bias dataset container",
    )
    group_input.add_argument(
        "--randomMin",
        action="store_const",
        const=True,
        dest="randomMin",
        default=False,
        help="Randomize files in minimum bias dataset",
    )
    group_input.add_argument(
        "--cavDS",
        action="store",
        metavar="DATASET",
        dest="cavDS",
        default="",
        type=str,
        help="Dataset name for cavern stream",
    )
    group_job.add_argument(
        "--nCav",
        action="store",
        metavar="N",
        dest="nCav",
        default=-1,
        type=int,
        help="Number of cavern files per job",
    )
    group_input.add_argument(
        "--notExpandCavDS",
        action="store_const",
        const=True,
        dest="notExpandCavDS",
        default=False,
        help="Allow jobs to use files across dataset boundaries in cavern dataset container",
    )
    group_input.add_argument(
        "--randomCav",
        action="store_const",
        const=True,
        dest="randomCav",
        default=False,
        help="Randomize files in cavern dataset",
    )
    group_submit.add_argument(
        "--sameSecRetry",
        action="store_const",
        const=False,
        dest="sameSecRetry",
        default=True,
        help="Use the same secondary input files when jobs are retried",
    )
    group_job.add_argument(
        "--useAMIEventLevelSplit",
        action="store_const",
        const=True,
        dest="useAMIEventLevelSplit",
        default=None,
        help="Retrieve the number of events per file from AMI to split the job using --nEventsPerJob",
    )
    group_output.add_argument(
        "--appendStrToExtStream",
        action="store_const",
        const=True,
        dest="appendStrToExtStream",
        default=False,
        help="Append the first part of filenames to extra stream names when using --trf. E.g., if this option is used %%OUT.AOD.pool.root will be contained in an EXT0_AOD dataset instead of an EXT0 dataset",
    )
    group_output.add_argument(
        "--useFileFieldAsStream",
        action="store_const",
        const=True,
        dest="useFileFieldAsStream",
        default=False,
        help="Use the first part of filenames as output stream names when using --trf. E.g., if this option is used %%OUT.AOD.pool.root will be contained in an AOD dataset instead of an EXT0 dataset",
    )
    group_output.add_argument(
        "--outputStreamNames",
        nargs="+",
        metavar="STREAM",
        help="Use custom output stream names when using --trf, instead of the default EXT# suffixes; must specify one stream name for each job output (%%OUT instance, or --extOutFile entries)",
    )
    group_job.add_argument(
        "--useCommonHalo",
        action="store_const",
        const=False,
        dest="useCommonHalo",
        default=True,
        help="Use an integrated DS for BeamHalo",
    )
    group_input.add_argument(
        "--beamHaloDS",
        action="store",
        metavar="DATASET",
        dest="beamHaloDS",
        default="",
        type=str,
        help="Dataset name for beam halo",
    )
    group_input.add_argument(
        "--beamHaloADS",
        action="store",
        metavar="DATASET",
        dest="beamHaloADS",
        default="",
        type=str,
        help="Dataset name for beam halo A-side",
    )
    group_input.add_argument(
        "--beamHaloCDS",
        action="store",
        metavar="DATASET",
        dest="beamHaloCDS",
        default="",
        type=str,
        help="Dataset name for beam halo C-side",
    )
    group_job.add_argument(
        "--nBeamHalo",
        action="store",
        metavar="N",
        dest="nBeamHalo",
        default=-1,
        type=int,
        help="Number of beam halo files per sub job",
    )
    group_job.add_argument(
        "--nBeamHaloA",
        action="store",
        metavar="N",
        dest="nBeamHaloA",
        default=-1,
        type=int,
        help="Number of beam halo files for A-side per sub job",
    )
    group_job.add_argument(
        "--nBeamHaloC",
        action="store",
        metavar="N",
        dest="nBeamHaloC",
        default=-1,
        type=int,
        help="Number of beam halo files for C-side per sub job",
    )
    group_job.add_argument(
        "--useCommonGas",
        action="store_const",
        const=False,
        dest="useCommonGas",
        default=True,
        help="Use an integrated DS for BeamGas",
    )
    group_input.add_argument(
        "--beamGasDS",
        action="store",
        metavar="DATASET",
        dest="beamGasDS",
        default="",
        type=str,
        help="Dataset name for beam gas",
    )
    group_input.add_argument(
        "--beamGasHDS",
        action="store",
        metavar="DATASET",
        dest="beamGasHDS",
        default="",
        type=str,
        help="Dataset name for beam gas Hydrogen",
    )
    group_input.add_argument(
        "--beamGasCDS",
        action="store",
        metavar="DATASET",
        dest="beamGasCDS",
        default="",
        type=str,
        help="Dataset name for beam gas Carbon",
    )
    group_input.add_argument(
        "--beamGasODS",
        action="store",
        metavar="DATASET",
        dest="beamGasODS",
        default="",
        type=str,
        help="Dataset name for beam gas Oxygen",
    )
    group_job.add_argument(
        "--nBeamGas",
        action="store",
        metavar="N",
        dest="nBeamGas",
        default=-1,
        type=int,
        help="Number of beam gas files per sub job",
    )
    group_job.add_argument(
        "--nBeamGasH",
        action="store",
        metavar="N",
        dest="nBeamGasH",
        default=-1,
        type=int,
        help="Number of beam gas files for Hydrogen per sub job",
    )
    group_job.add_argument(
        "--nBeamGasC",
        action="store",
        metavar="N",
        dest="nBeamGasC",
        default=-1,
        type=int,
        help="Number of beam gas files for Carbon per sub job",
    )
    group_job.add_argument(
        "--nBeamGasO",
        action="store",
        metavar="N",
        dest="nBeamGasO",
        default=-1,
        type=int,
        help="Number of beam gas files for Oxygen per sub job",
    )
    group_print.add_argument(
        "-v",
        action="store_const",
        const=True,
        dest="verbose",
        default=False,
        help="Verbose",
    )
    group_build.add_argument(
        "--noBuild",
        action="store_const",
        const=True,
        dest="noBuild",
        default=False,
        help="Skip buildJob",
    )
    action = group_output.add_argument(
        "--noOutput",
        action="store_const",
        const=True,
        dest="noOutput",
        default=False,
        help="Send job even if there is no output file",
    )
    group_submit.shareWithMe(action)
    group_input.add_argument(
        "--noRandom",
        action="store_const",
        const=True,
        dest="norandom",
        default=False,
        help="Enter random seeds manually",
    )
    group_job.add_argument(
        "--useAMIAutoConf",
        action="store_const",
        const=True,
        dest="useAMIAutoConf",
        default=False,
        help="Use AMI for AutoConfiguration",
    )
    action = group_job.add_argument(
        "--nThreads",
        action="store",
        metavar="N",
        dest="nThreads",
        default=-1,
        type=int,
        choices=[1, 8],
        help="The number of threads for AthenaMT. If this option is set to 8, Athena is executed with --threads=$ATHENA_PROC_NUMBER at sites. The number of available threads is defined in an environment variable, $ATHENA_PROC_NUMBER, on WNs, or %%PROC_NUMBER in --trf",
    )
    group_submit.shareWithMe(action)
    group_build.add_argument(
        "--extFile",
        action="store",
        metavar="FILE[,FILE...]",
        dest="extFile",
        default="",
        help="Pathena exports files with some special extensions (.C, .dat, .py .xml) in the current directory. If you want to add other files, specify their names, e.g., data1.root,data2.doc",
    )
    group_output.add_argument(
        "--extOutFile",
        action="store",
        metavar="FILE[,FILE...]",
        dest="extOutFile",
        default="",
        help=r'A comma-separated list of extra output files which cannot be extracted automatically. Either \ or "" is required when a wildcard is used. E.g., output1.txt,output2.dat,JiveXML_\*.xml',
    )
    group_output.add_argument(
        "--supStream",
        action="store",
        metavar="STREAM[,STREAM...]",
        dest="supStream",
        default="",
        help=r'Suppress some output streams. Either \ or "" is required when a wildcard is used. E.g., ESD,TAG,GLOBAL,StreamDESD\*',
    )
    group_submit.add_argument(
        "--processingType",
        action="store",
        metavar="TYPE",
        dest="processingType",
        default="pathena",
        help="Set processingType",
    )
    group_input.add_argument(
        "--generalInput",
        action="store_const",
        const=True,
        dest="generalInput",
        default=False,
        help="Read input files with general format except POOL,ROOT,ByteStream",
    )
    group_input.add_argument(
        "--shipInput",
        action="store_const",
        const=True,
        dest="shipinput",
        default=False,
        help="Ship input files to remote WNs",
    )
    group_input.add_argument(
        "--fileList",
        action="store",
        dest="filelist",
        default="",
        type=str,
        help=argparse.SUPPRESS,
    )
    group_build.add_argument(
        "--dbRelease",
        action="store",
        metavar="DATASET:FILE",
        dest="dbRelease",
        default="",
        type=str,
        help="DBRelease or CDRelease (DatasetName:FileName). E.g., ddo.000001.Atlas.Ideal.DBRelease.v050101:DBRelease-5.1.1.tar.gz. If --dbRelease=LATEST, the latest DBRelease is used",
    )
    group_build.add_argument(
        "--addPoolFC",
        action="store",
        metavar="FILE[,FILE...]",
        dest="addPoolFC",
        default="",
        help="File names to be inserted into PoolFileCatalog.xml except input files. E.g., MyCalib1.root,MyGeom2.root",
    )
    group_job.add_argument(
        "--useNextEvent",
        action="store_const",
        const=True,
        dest="useNextEvent",
        default=False,
        help="Set this option if your jobO uses theApp.nextEvent(), e.g., for G4. Note that this option is not required when you run transformations using --trf",
    )
    group_job.add_argument(
        "--trf",
        action="store",
        metavar="CMD",
        dest="trf",
        default=False,
        help='Run transformation, e.g., --trf "csc_atlfast_trf.py %%IN %%OUT.AOD.root %%OUT.ntuple.root -1 0"',
    )
    group_job.add_argument(
        "--useArgJson",
        action="store_const",
        const=True,
        dest="useArgJson",
        default=None,
        help="Convert --key=value arguments in --trf into a JSON dict and pass it to the transformation via --argjson. This is useful to avoid the command line length limit on the shell",
    )
    group_input.add_argument(
        "--notSkipMissing",
        action="store_const",
        const=True,
        dest="notSkipMissing",
        default=False,
        help="If input files are not read from SE, they will be skipped by default. This option disables the functionality",
    )
    group_input.add_argument(
        "--forceDirectIO",
        action="store_const",
        const=True,
        dest="forceDirectIO",
        default=False,
        help="Use directIO if directIO is available at the site",
    )
    group_input.add_argument(
        "--inputType",
        action="store",
        metavar="PATTERN",
        dest="inputType",
        default="",
        type=str,
        help="A regular expression pattern. Only files matching with the pattern in input dataset are used",
    )
    group_config.add_argument(
        "--outRunConfig",
        action="store",
        metavar="FILE",
        dest="outRunConfig",
        default="",
        type=str,
        help="Save extracted config information to a local file",
    )
    group_config.add_argument(
        "--inRunConfig",
        action="store",
        metavar="FILE",
        dest="inRunConfig",
        default="",
        type=str,
        help="Use a saved config information to skip config extraction",
    )

    # athena options
    group_job.add_argument(
        "-c",
        action="store",
        dest="singleLine",
        type=str,
        default="",
        metavar="COMMAND",
        help="One-liner, runs before any jobOs",
    )
    group_job.add_argument(
        "-p",
        action="store",
        dest="preConfig",
        type=str,
        default="",
        metavar="BOOTSTRAP",
        help="Location of bootstrap file",
    )
    group_job.add_argument(
        "-s",
        action="store_const",
        const=True,
        dest="codeTrace",
        default=False,
        help="Show printout of included files",
    )
    group_job.add_argument(
        "--CA",
        action="store_const",
        const=True,
        dest="componentAccumulator",
        default=False,
        help=argparse.SUPPRESS,
    )

    group_config.add_argument(
        "--loadJson",
        action="store",
        metavar="FILE",
        dest="loadJson",
        default=None,
        help="Read command-line parameters from a json file which contains a dict of {parameter: value}. Arguments for Athena can be specified as {'athena_args': [...,]}",
    )

    return optP
