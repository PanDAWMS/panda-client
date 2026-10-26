"""Command-line arguments of prun"""

import argparse

from pandaclient.CommonArgs import add_common_arguments
from pandaclient.Group_argparse import get_parser


def build_parser():
    """Build the command-line parser of prun with the common and the prun-specific arguments

    returns:
        GroupArgParser ready to parse the prun command line
    """
    usage = """prun [options]

      HowTo is available at https://panda-wms.readthedocs.io/en/latest/client/prun.html"""

    examples = """Examples:
      prun --exec "echo %IN > input.txt; root.exe; root -b -q macrotest.C" --athenaTag=22.0.0 --inDS ...
      prun --exec "cpptest %IN" --bexec "make" --athenaTag=22.0.0 --inDS ...
      prun --loadJson prunConfig.json   # read all prun options from one json file
    """

    optP = get_parser(usage=usage, conflict_handler="resolve")
    optP.set_examples(examples)

    # command-line parameters
    group_print = optP.add_group("print", "info print")
    group_prun = optP.add_group("prun", "about prun itself")
    group_config = optP.add_group("config", "single configuration file to set multiple options")
    group_input = optP.add_group("input", "input dataset(s)/files/format")
    group_output = optP.add_group("output", "output dataset/files")
    group_job = optP.add_group("job", "job running control on grid")
    group_build = optP.add_group("build", "build/compile the package and env setup")
    group_submit = optP.add_group("submit", "job submission/site/retry")
    group_evtFilter = optP.add_group("evtFilter", "event filter such as good run and event pick")
    group_expert = optP.add_group("expert", "for experts/developers only")

    usage_containerJob = """Visit the following wiki page for examples:
      https://twiki.cern.ch/twiki/bin/view/PanDA/PandaRun#Run_user_containers_jobs

    Please test the job interactively first prior to submitting to the grid.
    Check the following on how to test container job interactively:
      https://twiki.cern.ch/twiki/bin/viewauth/AtlasComputing/SingularityInAtlas
    """
    group_containerJob = optP.add_group("containerJob", "For container-based jobs", usage=usage_containerJob)

    add_common_arguments(optP)

    optP.add_helpGroup(addHelp="Some options such as --inOutDsJson may SPAN several groups")

    group_prun.add_argument(
        "--version",
        action="store_const",
        const=True,
        dest="version",
        default=False,
        help="Displays version",
    )
    group_input.add_argument(
        "--inDS",
        action="store",
        dest="inDS",
        default="",
        help="Name of an input dataset or dataset container",
    )
    group_output.add_argument(
        "--outputs",
        action="store",
        dest="outputs",
        default="",
        help="Names of output files. Comma separated. e.g., --outputs out1.dat,out2.txt. You can specify a suffix for each output container like <datasetNameSuffix>:<outputFileName>. e.g., --outputs AAA:out1.dat,BBB:out2.txt. In this case output container names are outDS_AAA/ and outDS_BBB/ instead of outDS_out1.dat/ and outDS_out2.txt/",
    )
    group_output.add_argument(
        "--noSeparateLog",
        action="store_const",
        const=True,
        dest="noSeparateLog",
        default=False,
        help="Set this option when jobs don't produce log files",
    )

    group_build.add_argument(
        "--followLinks",
        action="store_const",
        const=True,
        dest="followLinks",
        default=False,
        help="Resolve symlinks to directories when building the input tarball",
    )

    # I do not know which group "--useHomeDir" should go?
    group_build.add_argument(
        "--useHomeDir",
        action="store_const",
        const=True,
        dest="useHomeDir",
        default=False,
        help="execute prun just under the HOME dir",
    )
    group_build.add_argument(
        "--noBuild",
        action="store_const",
        const=True,
        dest="noBuild",
        default=False,
        help="Skip buildGen",
    )
    group_input.add_argument(
        "--secondaryDSs",
        action="store",
        dest="secondaryDSs",
        default="",
        help="List of secondary datasets when the job requires multiple inputs. "
        "Comma-separated strings in the format of StreamName:nFiles:DatasetName[:Pattern[:nSkipFiles[:FileNameList]]]. "
        "StreamName is the stream name used in --exec to expand to actual filenames. "
        "nFiles is the number of files per job by default, while it means the ratio to the number of primary "
        "files when --useNumFilesInSecDSsAsRatio is set. DatasetName is the dataset name. "
        "Pattern is used to filter files in the dataset. nSkipFiles is the number of files to skip in the dataset. "
        "FileNameList is a file listing names of files to be used in the dataset. ",
    )
    group_input.add_argument(
        "--reusableSecondary",
        action="store",
        dest="reusableSecondary",
        default="",
        help="A comma-separated list of secondary streams which reuse files when all files are used",
    )
    group_input.add_argument(
        "--useNumFilesInSecDSsAsRatio",
        action="store_const",
        const=True,
        dest="useNumFilesInSecDSsAsRatio",
        default=False,
        help="Set the option when the nFiles field in --secondaryDSs means the ratio to the number of primary files",
    )
    group_input.add_argument(
        "--match",
        action="store",
        dest="match",
        default="",
        help="Use only files matching with given pattern",
    )
    group_input.add_argument(
        "--antiMatch",
        action="store",
        dest="antiMatch",
        default="",
        help="Skip files matching with given pattern",
    )
    group_input.add_argument(
        "--notSkipLog",
        action="store_const",
        const=True,
        dest="notSkipLog",
        default=False,
        help="Don't skip log files in input datasets (obsolete. use --useLogAsInput instead)",
    )

    group_build.add_argument(
        "--useRootCore",
        action="store_const",
        const=True,
        dest="useRootCore",
        default=False,
        help="Use RootCore. See PandaRun wiki page for detail",
    )
    group_build.add_argument(
        "--useAthenaPackages",
        action="store_const",
        const=True,
        dest="useAthenaPackages",
        default=False,
        help="Use Athena packages. See PandaRun wiki page for detail",
    )
    group_input.add_argument(
        "--nSkipFiles",
        action="store",
        dest="nSkipFiles",
        default=0,
        type=int,
        help="Skip N files in the input dataset",
    )
    group_job.add_argument(
        "--exec",
        action="store",
        dest="jobParams",
        default="",
        help='execution string. e.g., --exec "./myscript arg1 arg2"',
    )
    group_output.add_argument(
        "--execWithRealFileNames",
        action="store_const",
        const=True,
        dest="execWithRealFileNames",
        default=False,
        help="Run the execution string with real output filenames",
    )
    group_build.add_argument(
        "--maxFileSize",
        action="store",
        dest="maxFileSize",
        default=1024 * 1024,
        type=int,
        help="Maximum size of files to be sent to WNs (default 1024*1024B)",
    )
    group_build.add_argument(
        "--athenaTag",
        action="store",
        dest="athenaTag",
        default="",
        help="Tags to setup Athena on remote WNs, e.g., --athenaTag=AtlasProduction,14.2.24.3",
    )
    group_build.add_argument(
        "--rootVer",
        action="store",
        dest="rootVer",
        default="",
        help="Specify a ROOT version which is not included in Athena, e.g., --rootVer=5.28/00",
    )
    group_build.add_argument(
        "--workDir",
        action="store",
        dest="workDir",
        default=".",
        help="All files under WORKDIR will be transferred to WNs (default=./)",
    )
    group_build.add_argument(
        "--extFile",
        action="store",
        dest="extFile",
        default="",
        help="root or large files under WORKDIR are not sent to WNs by default. If you want to send some skipped files, specify their names, e.g., data.root,data.tgz",
    )
    group_input.add_argument(
        "--useLogAsInput",
        action="store_const",
        const=True,
        dest="useLogAsInput",
        default=False,
        help="log.tgz files in inDS are ignored by default. This option allows log files to be used as input",
    )
    group_submit.add_argument(
        "--processingType",
        action="store",
        dest="processingType",
        default="prun",
        help="set processingType",
    )
    group_build.add_argument("--vo", action="store", dest="vo", default=None, help="virtual organization name")
    group_expert.add_argument(
        "--msgDriven",
        action="store_const",
        const=True,
        dest="msgDriven",
        default=False,
        help=argparse.SUPPRESS,
    )
    group_expert.add_argument(
        "--persistentFile",
        action="store",
        dest="persistentFile",
        default="",
        help="Please don't use this option. Only for junction steps " "to keep persistent information in workflows",
    )
    group_build.add_argument(
        "--bexec",
        action="store",
        dest="bexec",
        default="",
        help='execution string for build stage. e.g., --bexec "make"',
    )
    group_job.add_argument(
        "--maxNFilesPerJob",
        action="store",
        dest="maxNFilesPerJob",
        default=200,
        type=int,
        help="The maximum number of files per job is 200 by default since too many input files result in a too long command-line argument on WN which crashes the job. This option relax the limit. In many cases it is better to use this option together with --writeInputToTxt",
    )
    group_input.add_argument(
        "--writeInputToTxt",
        action="store",
        dest="writeInputToTxt",
        default="",
        help="Write the input file list to a file so that your application gets the list from the file instead of stdin. The argument is a comma separated list of StreamName:FileName. e.g., IN:input1.txt,IN2:input2.txt",
    )
    group_build.add_argument(
        "--dbRelease",
        action="store",
        dest="dbRelease",
        default="",
        help="DBRelease or CDRelease (DatasetName:FileName). e.g., ddo.000001.Atlas.Ideal.DBRelease.v050101:DBRelease-5.1.1.tar.gz. If --dbRelease=LATEST, the latest DBRelease is used. Most likely the --useAthenaPackages or --athenaTag option is required to setup Athena runtime on WN",
    )
    group_build.add_argument(
        "--notExpandDBR",
        action="store_const",
        const=True,
        dest="notExpandDBR",
        default=False,
        help="By default, DBRelease.tar.gz is expanded on WN and gets deleted after changing environment variables accordingly. If you need tar.gz, use this option",
    )
    group_print.add_argument(
        "-v",
        "--verbose",
        action="store_const",
        const=True,
        dest="verbose",
        default=False,
        help="Verbose",
    )
    group_config.add_argument(
        "--loadJson",
        action="store",
        dest="loadJson",
        default=None,
        help="Read command-line parameters from a json file which contains a dict of {parameter: value}",
    )
    group_config.add_argument(
        "--dumpTaskParams",
        action="store",
        dest="dumpTaskParams",
        default=None,
        help="Dump task parameters to a json file",
    )
    group_config.add_argument(
        "--useSecrets",
        action="store_const",
        const=True,
        dest="useSecrets",
        default=False,
        help="Use secrets",
    )

    group_containerJob.add_argument(
        "--ctrCvmfs",
        action="store_const",
        const=True,
        dest="ctrCvmfs",
        default=False,
        help=argparse.SUPPRESS,
    )
    # help="Bind /cvmfs to the container, bool, default False")
    group_containerJob.add_argument(
        "--ctrNoX509",
        action="store_const",
        const=True,
        dest="ctrNoX509",
        default=False,
        help=argparse.SUPPRESS,
    )
    # help="Unset X509 environment in the container, bool, default False")
    group_containerJob.add_argument(
        "--ctrDatadir",
        action="store",
        dest="ctrDatadir",
        default="",
        help=argparse.SUPPRESS,
    )
    # help="Binds the job directory to datadir for I/O operations, string, default /ctrdata")
    group_containerJob.add_argument(
        "--ctrWorkdir",
        action="store",
        dest="ctrWorkdir",
        default="",
        help=argparse.SUPPRESS,
    )
    # help="chdir to workdir in the container, string, default /ctrdata")
    group_containerJob.add_argument(
        "--ctrDebug",
        action="store_const",
        const=True,
        dest="ctrDebug",
        default=False,
        help=argparse.SUPPRESS,
    )
    # help="Enable more verbose output from runcontainer, bool, default False")
    group_containerJob.add_argument(
        "--useSandbox",
        action="store_const",
        const=True,
        dest="useSandbox",
        default=False,
        help=argparse.SUPPRESS,
    )
    # help='To send files in the run directory to remote sites which are not sent out by default ' \
    #'when --containerImage is used')
    group_containerJob.add_argument(
        "--useCentralRegistry",
        action="store_const",
        const=True,
        dest="useCentralRegistry",
        default=False,
        help=argparse.SUPPRESS,
    )
    # help="Use the central container registry when --containerImage is used")
    group_containerJob.add_argument(
        "--notUseCentralRegistry",
        action="store_const",
        const=True,
        dest="notUseCentralRegistry",
        default=False,
        help=argparse.SUPPRESS,
    )
    # help="Not use the central container registry when --containerImage is used")
    group_containerJob.add_argument(
        "--alrb",
        action="store_const",
        const=True,
        dest="alrb",
        default=True,
        help="Use ALRB for container execution",
    )
    group_containerJob.add_argument(
        "--wrapExecInContainer",
        action="store_const",
        const=False,
        dest="directExecInContainer",
        default=True,
        help="Execute the --exec string through runGen in the container",
    )
    group_containerJob.add_argument(
        "--alrbArgs",
        action="store",
        dest="alrbArgs",
        default=None,
        help="Additional arguments for ALRB to run the container. "
        '"setupATLAS -c --help" shows available ALRB arguments. For example, '
        '--alrbArgs "--nocvmfs	--nohome" to skip mounting /cvmfs and $HOME. '
        "This option is mainly for experts who know how the system and the container "
        "communicates with each other and how additional ALRB arguments affect "
        "the consequence",
    )
    group_containerJob.add_argument(
        "--oldContMode",
        action="store_const",
        const=True,
        dest="oldContMode",
        default=False,
        help="Use runcontainer for container execution. Note that this option will be "
        "deleted near future. Try the new ARLB scheme as soon as possible and report "
        "if there is a problem",
    )

    return optP
