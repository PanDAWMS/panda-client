import atexit
import copy
import json
import os
import re
import shlex
import shutil
import sys

from pandaclient import (
    Client,
    MiscUtils,
    PandaToolsPkgInfo,
    PLogger,
    PrunScript,
    PsubUtils,
)
from pandaclient.Group_argparse import get_parser

try:
    from urllib import quote
except ImportError:
    from urllib.parse import quote

try:
    unicode
except Exception:
    unicode = str


# main
def main():
    # tweak sys.argv
    sys.argv.pop(0)
    sys.argv.insert(0, "pchain")

    usage = """pchain [options]
    """

    optP = get_parser(usage=usage, conflict_handler="resolve")

    group_output = optP.add_group("output", "output dataset/files")
    group_config = optP.add_group("config", "workflow configuration")
    group_submit = optP.add_group("submit", "job submission/site/retry")
    group_expert = optP.add_group("expert", "for experts/developers only")
    group_build = optP.add_group("build", "build/compile the package and env setup")
    group_check = optP.add_group("check", "check workflow description")

    optP.add_helpGroup()

    group_config.add_argument("--version", action="store_const", const=True, dest="version", default=False, help="Displays version")
    group_config.add_argument("-v", action="store_const", const=True, dest="verbose", default=False, help="Verbose")
    group_config.add_argument(
        "--dumpJson",
        action="store",
        dest="dumpJson",
        default=None,
        help="Dump all command-line parameters and submission result such as returnCode, returnOut, and requestID to a json file",
    )
    group_check.add_argument("--check", action="store_const", const=True, dest="checkOnly", default=False, help="Check workflow description locally")
    group_check.add_argument(
        "--debug", action="store_const", const=True, dest="debugCheck", default=False, help="verbose mode when checking workflow description locally"
    )

    group_output.add_argument("--cwl", action="store", dest="cwl", default=None, help="Name of the main CWL file to describe the workflow")
    group_output.add_argument("--yaml", action="store", dest="yaml", default=None, help="Name of the yaml file for workflow parameters")
    group_output.add_argument("--snakefile", action="store", dest="snakefile", default=None, help="Name of the main Snakefile to describe the workflow")
    group_output.add_argument(
        "--maxSizeInSandbox",
        action="store",
        dest="maxSizeInSandbox",
        default=1,
        type=int,
        help="Maximum size in MB of files in the workflow sandbox (default 1 MB)",
    )

    group_build.add_argument(
        "--useAthenaPackages",
        action="store_const",
        const=True,
        dest="useAthenaPackages",
        default=False,
        help="One or more tasks in the workflow uses locally-built Athena packages",
    )
    group_build.add_argument("--vo", action="store", dest="vo", default=None, help="virtual organization name")
    group_build.add_argument(
        "--extFile",
        action="store",
        dest="extFile",
        default="",
        help="root or large files under WORKDIR are not sent to WNs by default. "
        "If you want to send some skipped files, specify their names, "
        "e.g., data.root,big.tgz,*.o",
    )

    group_output.add_argument("--outDS", action="store", dest="outDS", default=None, help="Name of the dataset for output and log files")
    group_output.add_argument("--official", action="store_const", const=True, dest="official", default=False, help="Produce official dataset")

    group_submit.add_argument("--noSubmit", action="store_const", const=True, dest="noSubmit", default=False, help="Dry-run")
    group_submit.add_argument("-3", action="store_true", dest="python3", default=False, help="Use python3")
    group_submit.add_argument(
        "--voms",
        action="store",
        dest="vomsRoles",
        default=None,
        type=str,
        help="generate proxy with paticular roles. " "e.g., atlas:/atlas/ca/Role=production,atlas:/atlas/fr/Role=pilot",
    )
    group_submit.add_argument("--noEmail", action="store_const", const=True, dest="noEmail", default=False, help="Suppress email notification")
    group_submit.add_argument("--prodSourceLabel", action="store", dest="prodSourceLabel", default="", help="set prodSourceLabel")
    group_submit.add_argument("--workingGroup", action="store", dest="workingGroup", default=None, help="set workingGroup")
    group_submit.add_argument("--workflowName", action="store", dest="workflowName", default=None, help="set workflow name")

    group_expert.add_argument(
        "--intrSrv",
        action="store_const",
        const=True,
        dest="intrSrv",
        default=False,
        help="Please don't use this option. Only for developers to use the intr panda server",
    )
    group_expert.add_argument(
        "--relayHost", action="store", dest="relayHost", default=None, help="Please don't use this option. Only for developers to use the relay host"
    )

    # get logger
    tmpLog = PLogger.getPandaLogger()

    # show version
    if "--version" in sys.argv:
        print("Version: %s" % PandaToolsPkgInfo.release_version)
        sys.exit(0)

    # parse args
    options = optP.parse_args()

    # check
    if options.cwl:
        workflow_language = "cwl"
        workflow_file = options.cwl
        workflow_input = options.yaml
        args_to_check = ["yaml", "outDS"]
    elif options.snakefile:
        workflow_language = "snakemake"
        workflow_file = options.snakefile
        workflow_input = ""
        args_to_check = ["outDS"]
    else:
        tmpLog.error("argument --cwl or --snakefile is required")
        sys.exit(1)

    for arg_name in args_to_check:
        if not getattr(options, arg_name):
            tmpLog.error("argument --{0} is required".format(arg_name))
            sys.exit(1)

    # check grid-proxy
    PsubUtils.check_proxy(options.verbose, options.vomsRoles)

    # check output name
    nickName = PsubUtils.getNickname()
    if not PsubUtils.checkOutDsName(options.outDS, options.official, nickName, verbose=options.verbose):
        tmpStr = "invalid output dataset name: %s" % options.outDS
        tmpLog.error(tmpStr)
        sys.exit(1)

    # create tmp dir
    curDir = os.getcwd()
    tmpDir = os.path.join(curDir, MiscUtils.wrappedUuidGen())
    os.makedirs(tmpDir)

    # exit action
    def _onExit(dir, del_command):
        del_command("rm -rf %s" % dir)

    atexit.register(_onExit, tmpDir, MiscUtils.commands_get_output)

    # sandbox
    if options.verbose:
        tmpLog.debug("making sandbox")
    archiveName = "jobO.%s.tar.gz" % MiscUtils.wrappedUuidGen()
    archiveFullName = os.path.join(tmpDir, archiveName)
    find_opt = " -type f -size -{0}k".format(options.maxSizeInSandbox * 1024)
    tmpOut = MiscUtils.commands_get_output("find . {0} | tar cvfz {1} --files-from - ".format(find_opt, archiveFullName))

    if options.verbose:
        print(tmpOut + "\n")
        tmpLog.debug("checking sandbox")
        tmpOut = MiscUtils.commands_get_output("tar tvfz {0}".format(archiveFullName))
        print(tmpOut + "\n")

    if not options.noSubmit:
        tmpLog.info("uploading workflow sandbox")
        if options.vo:
            use_cache_srv = False
        else:
            use_cache_srv = True
        os.chdir(tmpDir)
        status, out = Client.putFile(archiveName, options.verbose, useCacheSrv=use_cache_srv, reuseSandbox=True)

        if out.startswith("NewFileName:"):
            # found the same input sandbox to reuse
            archiveName = out.split(":")[-1]
        elif out != "True":
            # failed
            print(out)
            tmpLog.error("Failed with %s" % status)
            sys.exit(1)
    os.chdir(curDir)
    try:
        shutil.rmtree(tmpDir)
    except Exception:
        pass

    # check if the workflow uses athena packages
    if not options.useAthenaPackages:
        with open(workflow_file, "r") as f:
            for line in f.readlines():
                if re.search(r"^\s*[^#]\s*opt_useAthenaPackages", line):
                    options.useAthenaPackages = True
                    break

    matchURL = re.search("(http.*://[^/]+)/", Client.baseURLCSRVSSL)
    sourceURL = matchURL.group(1)

    params = {
        "taskParams": {},
        "sourceURL": sourceURL,
        "sandbox": archiveName,
        "workflowSpecFile": workflow_file,
        "workflowInputFile": workflow_input,
        "language": workflow_language,
        "outDS": options.outDS,
        "base_platform": os.environ.get("ALRB_USER_PLATFORM", "centos7"),
    }
    if options.workflowName:
        params["workflow_name"] = options.workflowName

    # making task params with dummy exec
    task_type_args = {"container": "--containerImage __dummy_container__"}
    if options.useAthenaPackages:
        task_type_args["athena"] = "--useAthenaPackages"
    for task_type in task_type_args:
        os.chdir(curDir)
        prun_exec_str = "--exec __dummy_exec_str__ --outDS {0} {1}".format(options.outDS, task_type_args[task_type])
        if options.noSubmit:
            prun_exec_str += " --noSubmit"
        if options.verbose:
            prun_exec_str += " -v"
        if options.vo:
            prun_exec_str += " --vo {0}".format(options.vo)
        if options.prodSourceLabel:
            prun_exec_str += " --prodSourceLabel {0}".format(options.prodSourceLabel)
        if options.workingGroup:
            prun_exec_str += " --workingGroup {0}".format(options.workingGroup)
        if options.official:
            prun_exec_str += " --official"
        if options.extFile:
            prun_exec_str += " --extFile {0}".format(options.extFile)
        arg_dict = {"get_taskparams": True, "ext_args": shlex.split(prun_exec_str)}
        if options.checkOnly:
            arg_dict["dry_mode"] = True

        taskParamMap = PrunScript.main(**arg_dict)
        del taskParamMap["noInput"]
        del taskParamMap["nEvents"]
        del taskParamMap["nEventsPerJob"]

        params["taskParams"][task_type] = taskParamMap

    if options.noSubmit:
        if options.noSubmit:
            if options.verbose:
                tmpLog.debug("==== taskParams ====")
                tmpKeys = list(taskParamMap)
                tmpKeys.sort()
                for tmpKey in tmpKeys:
                    if tmpKey in ["taskParams"]:
                        continue
                    print("%s : %s" % (tmpKey, taskParamMap[tmpKey]))
        sys.exit(0)

    data = {"relay_host": options.relayHost, "verbose": options.verbose}
    if not options.checkOnly:
        action_type = "submit"
    else:
        action_type = "check"
        data["check"] = True

    # set to use INTR server just before taking action so that sandbox files go to the regular place
    if options.intrSrv:
        Client.useIntrServer()

    # action
    tmpLog.info("{0} workflow {1}".format(action_type, options.outDS))
    tmpStat, tmpOut = Client.send_workflow_request(params, **data)

    # result
    exit_code = 0
    request_id = None
    tmp_str = ""
    if tmpStat != 0:
        tmp_str = "workflow {0} failed with {1}".format(action_type, tmpStat)
        tmpLog.error(tmp_str)
        exit_code = 1
    else:
        if tmpOut[0]:
            stat_code = tmpOut[1]["status"]
            check_log = "messages from the server\n\n" + tmpOut[1]["log"]
            if options.checkOnly:
                tmpLog.info(check_log)
                if stat_code:
                    tmpLog.info("successfully verified workflow description")
                else:
                    tmpLog.error("workflow description is corrupted")
            else:
                if stat_code:
                    request_id = tmpOut[1]["request_id"]
                    tmp_str = "successfully submitted with request_id={0}".format(request_id)
                    tmpLog.info(tmp_str)
                else:
                    tmpLog.info(check_log)
                    tmp_str = "workflow submission failed with {0}".format(stat_code)
                    tmpLog.error(tmp_str)
                    exit_code = stat_code
        else:
            tmp_str = "workflow {0} failed. {1}".format(action_type, tmpOut[1])
            tmpLog.error(tmp_str)
            exit_code = 1

    # dump json
    if options.dumpJson:
        dump_item = copy.deepcopy(vars(options))
        dump_item["returnCode"] = exit_code
        dump_item["returnOut"] = tmp_str
        dump_item["requestID"] = request_id
        with open(options.dumpJson, "w") as f:
            json.dump(dump_item, f)

    return exit_code
