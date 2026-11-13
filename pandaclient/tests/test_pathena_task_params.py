"""Golden-file regression test for the task parameters built by pathena.

Runs PathenaScript.main() for a set of pathena command lines and compares the task parameters, the exit
code, the calls to the Athena-specific steps, and the printed output with data/pathena_task_params.json.

It doesn't touch the network, the grid, or Athena: every case uses --noSubmit, the proxy check and the
nickname are mocked, the run configuration extracted from the job options by Athena is replaced by the
one given in the case, the sandbox is not archived, and UUIDs, random seeds, server URLs, the release
version and the client OS are fixed.

Run with: python -m unittest pandaclient.tests.test_pathena_task_params

When a change to the task parameters is intended, regenerate the golden file and review its diff:
    python -m pandaclient.tests.test_pathena_task_params --regenerate
"""

import builtins
import contextlib
import copy
import io
import itertools
import json
import logging
import os
import random
import shutil
import sys
import tempfile
import unittest
from unittest import mock

# importing Client resolves the panda cache host with DNS unless this is set
os.environ.setdefault("PANDA_BEHIND_REAL_LB", "1")

from pandaclient import AthenaUtils, Client, MiscUtils, PandaToolsPkgInfo, PathenaScript, PsubUtils  # noqa: E402

GOLDEN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "pathena_task_params.json")

# environment variables read while building the task, unset so that the local setup doesn't change the results
ENV_TO_UNSET_PREFIXES = ("Atlas", "ALRB_", "CMAKE_PREFIX_PATH", "CMTCONFIG", "PANDA_EXEC_STRING", "PANDA_NICKNAME", "PANDA_PRODUCTION_ROLE", "RUCIO_")

BASE_ARGS = ["--outDS", "user.someone.test.v1", "--noSubmit"]
JOBO = ["myJobOptions.py"]
IN = ["--inDS", "mc23_13p6TeV.601229.PhPy8EG.DAOD_PHYS"]
DATA_IN = ["--inDS", "data18_13TeV.periodA.DAOD_PHYS"]
TRF = ["--trf", "Reco_tf.py --inputAODFile %IN --outputDAODFile %OUT.pool.root --formats PHYS --maxEvents 10"]

# run configuration extracted from the job options when the case doesn't give one
DEFAULT_RUN_CONFIG = {"input": {}, "other": {}, "output": {"outAOD": "AOD.pool.root", "alloutputs": ["AOD.pool.root"]}}
# run configuration of a job without output
NO_OUTPUT_RUN_CONFIG = {"input": {}, "other": {}, "output": {"alloutputs": []}}

# name -> pathena arguments, or a dict with
#   args: pathena arguments
#   run_config: run configuration returned by the mocked extractor, as {"input": ..., "other": ..., "output": ...}
#   extractor_fails: True to make the mocked extractor report a failure
#   env: extra environment variables
#   cwd: "work" to run pathena in the work area itself instead of its "sub" directory
#   work_area: Athena work area of the mocked Athena setup, the work dir by default
#   no_athena_env: True to mock a missing Athena setup
#   no_cmtconfig: True to run without the CMTCONFIG environment variable set by the Athena setup
#   inputs: answers to the questions pathena asks, e.g. the random seeds with --noRandom
#   ami: (status, output) of the AMI command run for AMI= in --trf
CASES = {
    "basic": IN + JOBO,
    "no_input": JOBO,
    "data_input_ami_split": DATA_IN + JOBO,
    "no_ami_split": DATA_IN + JOBO + ["--useAMIEventLevelSplit", "False"],
    "several_jobos": IN + ["-p", "pre.py", "a.py", "b.py", "-c", 'x="1"'],
    "no_jobo": IN,
    "unknown_arg": IN + JOBO + ["--noSuchOption"],
    "after_separator": IN + JOBO + ["--", "--anything"],
    "component_accumulator": IN + JOBO + ["--CA"],
    "version": ["--version"],
    "input_options": IN
    + JOBO
    + ["--nFilesPerJob", "6", "--inputType", "AOD", "--fileList", "a.root,b*.root", "--inputFileList", "files.txt", "--notExpandInDS"],
    "small_files_per_job": IN + JOBO + ["--nFilesPerJob", "2"],
    "split_without_unit": IN + JOBO + ["--nJobs", "4"],
    "n_jobs_negative": IN + JOBO + ["--nJobs=-5"],
    "events_per_job": IN + JOBO + ["--nEventsPerJob", "100"],
    "events_per_job_no_input": {
        "args": JOBO + ["--nEventsPerJob", "100", "--nJobs", "3"],
        "run_config": {"input": {"noInput": True}, "output": {"outHist": True}},
    },
    "outputs_various": {
        "args": IN + JOBO,
        "run_config": {
            "output": {
                "outNtuple": ["NT1", "NT2"],
                "outHist": True,
                "outESD": "ESD.pool.root",
                "outTAG": True,
                "outAANT": [["a1", "S1", "f1.root"], ["a2", "S2", "f1.root"]],
                "outTHIST": ["TH1"],
                "outIROOT": ["x.root"],
                "alloutputs": ["NT1.root", "x.root"],
            }
        },
    },
    "ext_out_file": IN + JOBO + ["--extOutFile", "out1.root, out2*.txt,AOD.pool.root"],
    "ext_out_file_stream_names": IN + JOBO + ["--extOutFile", "out1.root,out2.root", "--outputStreamNames", "AAA", "BBB"],
    "ext_out_file_stream_names_mismatch": IN + JOBO + ["--extOutFile", "out1.root,out2.root", "--outputStreamNames", "AAA"],
    "ext_stream_options": IN + JOBO + ["--extOutFile", "out1.root", "--appendStrToExtStream"],
    "file_field_as_stream": IN + JOBO + ["--extOutFile", "out1.root", "--useFileFieldAsStream"],
    "nth_field_in_lfn": IN + JOBO + ["--addNthFieldOfInDSToLFN", "2,3"],
    "allow_no_output": IN + JOBO + ["--extOutFile", "out1.root", "--allowNoOutput", "out1.root,nomatch.root"],
    "no_output_found": {"args": IN + JOBO, "run_config": NO_OUTPUT_RUN_CONFIG},
    "no_output_found_trf": {"args": IN + ["--trf", "run.sh %IN"], "run_config": NO_OUTPUT_RUN_CONFIG},
    "no_output_option": {"args": IN + JOBO + ["--noOutput"], "run_config": NO_OUTPUT_RUN_CONFIG},
    "extractor_fails": {"args": IN + JOBO, "extractor_fails": True},
    "ship_input": {
        "args": IN + JOBO + ["--shipInput"],
        "run_config": {"other": {"inputFiles": ["ship1.root", "ship2"], "inColl": True}, "output": {"outHist": True}},
    },
    "ship_input_missing": {
        "args": IN + JOBO + ["--shipInput"],
        "run_config": {"other": {"inputFiles": ["missing.root"]}, "output": {"outHist": True}},
    },
    "ship_files_missing": {"args": IN + JOBO, "run_config": {"input": {"shipFiles": ["missing.root"]}, "output": {"outHist": True}}},
    "generator_and_condition_files": {
        "args": IN + JOBO + ["--addPoolFC", "pfc.xml"],
        "run_config": {"other": {"rndmGenFile": ["gen.dat"], "condInput": ["cond1.root", "cond2.root"]}, "output": {"outHist": True}},
    },
    "condition_files_no_pool_fc": {
        "args": IN + JOBO,
        "run_config": {"other": {"condInput": ["cond1.root", "cond2.root"]}, "output": {"outHist": True}},
    },
    "random_streams": {"args": IN + JOBO, "run_config": {"other": {"rndmStream": ["PYTHIA", "PYTHIA_INIT"]}, "output": {"outHist": True}}},
    "random_streams_manual": {
        "args": IN + JOBO + ["--noRandom"],
        "run_config": {"other": {"rndmStream": ["PYTHIA"]}, "output": {"outHist": True}},
        "inputs": ["1", "11 22"],
    },
    "g4_seeds": {"args": IN + JOBO + ["-c", "x=1"], "run_config": {"other": {"G4RandomSeeds": True}, "output": {"outHist": True}}},
    "g4_seeds_no_one_liner": {"args": IN + JOBO, "run_config": {"other": {"G4RandomSeeds": True}, "output": {"outHist": True}}},
    "g4_seeds_manual": {
        "args": IN + JOBO + ["--noRandom"],
        "run_config": {"other": {"G4RandomSeeds": True}, "output": {"outHist": True}},
        "inputs": ["abc", "-3", "42"],
    },
    "byte_stream_back_navi": {"args": IN + JOBO, "run_config": {"input": {"inBS": True, "backNavi": True, "collRefName": "Ref"}, "output": {"outHist": True}}},
    "job_params_flags": IN
    + JOBO
    + [
        "--notSkipMissing",
        "--generalInput",
        "--useNextEvent",
        "-s",
        "--queueData",
        "q.json",
        "--nThreads",
        "8",
        "--maxCpuCount",
        "3600",
        "--workingGroup",
        "phys-higgs",
        "--supStream",
        "esd,aod",
        "--extFile",
        "x.txt,,y.txt",
    ],
    "multicore": IN + JOBO + ["--nCore", "8", "--nThreads", "8"],
    "trf_basic": IN + TRF,
    "trf_derivation": IN + ["--trf", "Reco_tf.py --inputAODFile=%IN --outputDAODFile=%OUT.pool.root --reductionConf=PHYS,PHYSLITE"],
    "trf_placeholders": IN + ["--trf", "run.sh %IN %OUT.a.root %RNDM:100 %DB:ddo.000001.Atlas.Ideal.DBRelease.v100:DBR.tar.gz --argJSON=args.json"],
    "trf_no_out": IN + ["--trf", "run.sh %IN"],
    "trf_events_per_job": IN + ["--trf", "run.sh %IN %OUT.a.root", "--nEventsPerJob", "10", "--nEventsPerFile", "100"],
    "trf_multicore_warning": IN + ["--trf", "run.sh %IN %OUT.a.root", "--nCore", "8"],
    "trf_multithread_warning": IN + ["--trf", "run.sh %IN %OUT.a.root", "--nThreads", "8"],
    "trf_ami": {
        "args": IN + ["--trf", "Reco_tf.py AMI=r1234 --inputAODFile %IN"],
        "ami": (0, "# comment\n* star\nReco_tf.py --inputAODFile %IN --outputESDFile %OUT.ESD.root\n"),
    },
    "trf_ami_fails": {"args": IN + ["--trf", "Reco_tf.py AMI=r1234"], "ami": (1, "error")},
    "trf_ami_no_command": {"args": IN + ["--trf", "Reco_tf.py AMI=r1234"], "ami": (0, "# only comments\n")},
    "trf_event_picking": ["--trf", "run.sh %IN %OUT.a.root", "--eventPickEvtList", "events.txt", "--eventPickDataType", "AOD"],
    "min_bias_cavern": IN
    + ["--trf", "sim.sh %IN %MININ %LOMBIN %HIMBIN %CAVIN %OUT.a.root"]
    + ["--minDS", "mc.minbias", "--nMin", "2", "--lowMinDS", "mc.low", "--nLowMin", "3", "--highMinDS", "mc.high", "--nHighMin", "4"]
    + ["--cavDS", "mc.cavern", "--nCav", "5", "--randomMin", "--randomCav", "--notExpandMinDS", "--notExpandCavDS"],
    "min_bias_from_run_config": {
        "args": IN + JOBO + ["--minDS", "mc.minbias", "--nMin", "2"],
        "run_config": {"input": {"inMinBias": True}, "output": {"outHist": True}},
    },
    "beam_halo_gas_common": IN
    + ["--trf", "sim.sh %IN %BHIN %BGIN %OUT.a.root", "--beamHaloDS", "mc.halo", "--nBeamHalo", "2", "--beamGasDS", "mc.gas", "--nBeamGas", "3"],
    "beam_halo_gas_separate": IN
    + ["--trf", "sim.sh %IN %BHIN %BGIN %OUT.a.root", "--useCommonHalo", "False", "--useCommonGas", "False"]
    + ["--beamHaloADS", "mc.haloA", "--nBeamHaloA", "1", "--beamHaloCDS", "mc.haloC", "--nBeamHaloC", "1"]
    + ["--beamGasHDS", "mc.gasH", "--nBeamGasH", "1", "--beamGasCDS", "mc.gasC", "--nBeamGasC", "1", "--beamGasODS", "mc.gasO", "--nBeamGasO", "1"],
    "same_sec_retry_off": IN + ["--trf", "sim.sh %IN %MININ %OUT.a.root", "--minDS", "mc.minbias", "--nMin", "2", "--sameSecRetry", "False"],
    "secondary": IN + JOBO + ["--secondaryDSs", "IN2:3:mc.ds2,IN3:1:mc.ds3:*.root:2", "--notExpandSecDSs"],
    "pfn_list": JOBO + ["--pfnList", "pfns.txt", "--site", "CERN-PROD"],
    "pfn_list_no_site": JOBO + ["--pfnList", "pfns.txt"],
    "good_run_list": JOBO + ["--goodRunListXML", "grl.xml", "--goodRunListDataType", "AOD", "--goodRunListProdStep", "merge"],
    "event_picking": JOBO + ["--eventPickEvtList", "events.txt", "--eventPickDataType", "AOD", "--eventPickStreamName", "physics_Main"],
    "dbrelease": IN + JOBO + ["--dbRelease", "ddo.000001.Atlas.Ideal.DBRelease.v100:DBRelease-100.tar.gz"],
    "no_build": IN + JOBO + ["--noBuild"],
    "no_compile": IN + JOBO + ["--noCompile"],
    "no_build_and_no_compile": IN + JOBO + ["--noBuild", "--noCompile"],
    "tarball_via_ddm": IN + JOBO + ["--noBuild", "--tarBallViaDDM", "user.someone.tarball:sources.tar.gz"],
    "merge": IN + JOBO + ["--mergeOutput", "--mergeScript", "hadd %OUT %IN", "--mergeSingleFile"],
    "merge_no_build": IN + JOBO + ["--mergeOutput", "--noBuild"],
    "cmake": {"args": IN + JOBO + ["--mergeOutput", "--gluePackages", "PkgA,PkgB"], "env": {"CMAKE_PREFIX_PATH": "/opt/cmake"}},
    "cmake_no_compile": {"args": IN + JOBO + ["--noCompile", "--mergeOutput"], "env": {"CMAKE_PREFIX_PATH": "/opt/cmake"}},
    "athena_tag": IN + JOBO + ["--athenaTag", "AthAnalysis,24.2.10", "--cmtConfig", "x86_64-el9-gcc13-opt"],
    "cmt_config_mismatch": IN + JOBO + ["--noBuild", "--cmtConfig", "x86_64-el9-gcc12-opt"],
    "container": IN + JOBO + ["--containerImage", "docker://atlas/athanalysis:24.2.10", "--architecture", "&nvidia"],
    "container_no_athena": {"args": IN + JOBO + ["--containerImage", "docker://atlas/athanalysis:24.2.10"], "no_athena_env": True},
    "no_athena": {"args": IN + JOBO, "no_athena_env": True},
    "no_cmtconfig": {"args": IN + JOBO, "no_cmtconfig": True},
    "outside_work_area": {"args": IN + JOBO, "work_area": "/somewhere/else"},
    "cwd_is_work_area": {"args": IN + JOBO + ["--mergeOutput"], "cwd": "work"},
    "transfer_types": IN + JOBO + ["--transferType", "direct"],
    "transfer_type_file": IN + JOBO + ["--transferType", "file"],
    "transfer_type_invalid": IN + JOBO + ["--transferType", "carrier-pigeon"],
    "direct_io_sites": IN + JOBO + ["--useDirectIOSites"],
    "force_staged_and_direct": {"args": IN + JOBO + ["--forceDirectIO"], "run_config": {"input": {"inBS": True}, "output": {"outHist": True}}},
    "force_staged": IN + JOBO + ["--forceStaged"],
    "too_large_cpu_count": IN + JOBO + ["--maxCpuCount", str(Client.maxCpuCountLimit + 1)],
    "out_ds_invalid": IN + JOBO + ["--outDS", "user.someone.bad;name"],
    "verbose": IN + JOBO + ["-v"],
    "verbose_trf": IN + TRF + ["-v"],
    "bulk": IN + JOBO + ["--inOutDsJson", "inout.json"],
    "in_tarball": IN + JOBO + ["--inTarBall", "sandbox.tar.gz", "--outTarBall", "saved.tar.gz"],
    "in_run_config": IN + JOBO + ["--inRunConfig", "run.cfg"],
    "out_run_config": IN + JOBO + ["--outRunConfig", "run.cfg"],
    "tmp_dir": IN + JOBO + ["--tmpDir", "tmp_parent"],
    "full_path_jobo": IN + ["/abs/path/myJobOptions.py", "rel.py"],
    "misc_task_options": IN + JOBO + ["--nGBPerJob", "10", "--memory", "4000", "--respectLB", "--noEmail", "--disableAutoRetry", "--maxAttempt", "5"],
}


def _uuid_generator():
    counter = itertools.count()
    return lambda: f"UUID{next(counter):04d}"


def _seed_generator():
    counter = itertools.count(1)
    return lambda low, high: low + next(counter)


def _athena_env(work_area):
    return {
        "workArea": work_area,
        "athenaVer": "24.0.10",
        "groupArea": "",
        "cacheVer": "_AthAnalysis,24.2.10",
        "nightVer": "",
        "cmtConfig": "x86_64-el9-gcc13-opt",
    }


def _make_run_config(spec):
    """Make the run configuration returned by AthenaUtils.extractRunConfig from {"input": ..., "other": ..., "output": ...}"""
    spec = copy.deepcopy(spec if spec is not None else DEFAULT_RUN_CONFIG)
    run_config = AthenaUtils.ConfigAttr()
    for key in ("input", "other", "output"):
        section = AthenaUtils.ConfigAttr()
        section.update(spec.get(key, {}))
        run_config[key] = section
    # the extractor always lists the outputs it found
    if "alloutputs" not in run_config["output"]:
        run_config["output"]["alloutputs"] = []
    return run_config


def _jsonable(value):
    """Convert mock call arguments into JSON-compatible data"""
    return json.loads(json.dumps(value, default=repr))


def _make_work_dir():
    # realpath since pathena resolves the current dir, e.g. /var -> /private/var on macOS
    work = os.path.realpath(tempfile.mkdtemp(prefix="pathena_test_"))
    sub_dir = os.path.join(work, "sub")
    os.makedirs(sub_dir)
    os.makedirs(os.path.join(sub_dir, "tmp_parent"))
    files = {
        "files.txt": "f1.root\n\n  f2.root  \n",
        "pfns.txt": "root://a/f1.root\nroot://a/f2.root\n",
        "events.txt": "358031 123456\n358031 123457\n",
        "grl.xml": "<LumiRangeCollection/>\n",
        "inout.json": json.dumps([{"inDS": "data.A", "outDS": "user.someone.A"}, {"inDS": "data.B", "outDS": "user.someone.B/"}]),
        "sandbox.tar.gz": "",
        "ship1.root": "",
        "ship2.root": "",
        "run.cfg": "",
    }
    for name, content in files.items():
        with open(os.path.join(sub_dir, name), "w") as f:
            f.write(content)
    return work, sub_dir


def run_case(case, extra_patches=()):
    """
    Run pathena for one case in a fresh work area and return the result as JSON-compatible data

    args:
        case: pathena arguments, or a dict as described for CASES
        extra_patches: more mock patchers to apply while pathena runs
    returns:
        dict with exit, task_params as passed to the task check, calls to the Athena steps, and output
    """
    if not isinstance(case, dict):
        case = {"args": case}
    work, sub_dir = _make_work_dir()
    args = BASE_ARGS + case["args"]

    environ = {key: value for key, value in os.environ.items() if not key.startswith(ENV_TO_UNSET_PREFIXES)}
    # set by the Athena setup
    if not case.get("no_cmtconfig"):
        environ["CMTCONFIG"] = "x86_64-el9-gcc13-opt"
    environ.update(case.get("env", {}))
    if case.get("no_athena_env"):
        athena_ver = (False, {})
    else:
        athena_ver = (True, _athena_env(case.get("work_area", work)))

    calls = []
    task_params = []

    def recorder(name, return_value=None):
        def record(*call_args, **call_kwargs):
            calls.append(_jsonable([name, list(call_args), call_kwargs]))
            return return_value(*call_args, **call_kwargs) if callable(return_value) else return_value

        return record

    def extract_run_config(*call_args, **call_kwargs):
        calls.append(_jsonable(["extractRunConfig", list(call_args), {key: value for key, value in call_kwargs.items() if key != "tmpDir"}]))
        return not case.get("extractor_fails"), _make_run_config(case.get("run_config"))

    real_check_task_param = PsubUtils.checkTaskParam

    def check_task_param(task_param_map, unlimit_num_outputs):
        task_params.append(copy.deepcopy(task_param_map))
        return real_check_task_param(task_param_map, unlimit_num_outputs)

    def archive(prefix):
        return lambda *a, **k: (k.get("archiveName") or f"{prefix}.UUID.tar", f"TMPDIR/{k.get('archiveName') or prefix + '.UUID.tar'}")

    exit_actions = []
    output = io.StringIO()
    # pathena logs through the panda-client logger, whose handler keeps the stdout of its creation time
    panda_logger = logging.getLogger("panda-client")
    saved_handlers = panda_logger.handlers[:]
    panda_logger.handlers = [logging.StreamHandler(output)]
    orig_cwd = os.getcwd()
    orig_sys_path = sys.path[:]
    result = {}
    try:
        os.chdir(work if case.get("cwd") == "work" else sub_dir)
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.dict(os.environ, environ, clear=True))
            stack.enter_context(mock.patch.object(sys, "argv", ["pathena"] + args))
            stack.enter_context(mock.patch.object(MiscUtils, "wrappedUuidGen", _uuid_generator()))
            stack.enter_context(mock.patch.object(random, "randint", _seed_generator()))
            stack.enter_context(mock.patch.object(builtins, "input", mock.MagicMock(side_effect=list(case.get("inputs", [])))))
            stack.enter_context(mock.patch.object(PandaToolsPkgInfo, "release_version", "0.0.0"))
            stack.enter_context(mock.patch.object(Client, "baseURLSSL", "https://pandaserver.test/server/panda"))
            stack.enter_context(mock.patch.object(Client, "baseURLCSRVSSL", "https://pandacache.test/server/panda"))
            stack.enter_context(mock.patch.object(Client, "requestEventPicking", recorder("requestEventPicking", (0, "user.someone.evtpick/"))))
            stack.enter_context(mock.patch.object(Client, "useDevServer", recorder("useDevServer")))
            stack.enter_context(mock.patch.object(Client, "useIntrServer", recorder("useIntrServer")))
            # grid and user
            stack.enter_context(mock.patch.object(PsubUtils, "check_proxy", recorder("check_proxy")))
            stack.enter_context(mock.patch.object(PsubUtils, "getNickname", recorder("getNickname", "someone")))
            stack.enter_context(mock.patch.object(PsubUtils, "checkTaskParam", check_task_param))
            # osInfo of the task, the platform of the client
            stack.enter_context(mock.patch.object(PsubUtils, "get_os_information", return_value="TestOS-1.0-x86_64"))
            # Athena setup and run configuration
            stack.enter_context(mock.patch.object(AthenaUtils, "getAthenaVer", return_value=athena_ver))
            stack.enter_context(mock.patch.object(AthenaUtils, "extractRunConfig", extract_run_config))
            ami = case.get("ami", (1, "AMI not mocked"))
            stack.enter_context(mock.patch.object(MiscUtils, "commands_get_status_output_with_env", recorder("GetCommand", ami)))
            # sandbox
            stack.enter_context(mock.patch.object(AthenaUtils, "copyAthenaStuff", recorder("copyAthenaStuff")))
            stack.enter_context(mock.patch.object(AthenaUtils, "deleteAthenaStuff", recorder("deleteAthenaStuff")))
            stack.enter_context(
                mock.patch.object(AthenaUtils, "archiveWithCpack", recorder("archiveWithCpack", lambda *a, **k: ("cpack.UUID.tar", "TMPDIR/cpack.UUID.tar")))
            )
            stack.enter_context(mock.patch.object(AthenaUtils, "archiveSourceFiles", recorder("archiveSourceFiles", archive("sources"))))
            stack.enter_context(mock.patch.object(AthenaUtils, "archiveJobOFiles", recorder("archiveJobOFiles", archive("jobO"))))
            stack.enter_context(mock.patch.object(AthenaUtils, "archiveInstallArea", recorder("archiveInstallArea")))
            stack.enter_context(mock.patch.object(PsubUtils, "finalize_sandbox", recorder("finalize_sandbox", lambda name, *a, **k: f"{name}.gz")))
            # module state modified by pathena
            for name in ("extFile", "user_set_ext_files", "excludeFile", "athenaStuff"):
                stack.enter_context(mock.patch.object(AthenaUtils, name, []))
            stack.enter_context(mock.patch.object(AthenaUtils, "fullPathJobOs", {}))
            stack.enter_context(mock.patch.object(PsubUtils, "limit_maxNumInputs", None, create=True))
            # the temporary files are removed by the test instead
            stack.enter_context(mock.patch("atexit.register", lambda *a, **k: exit_actions.append(a)))
            for patcher in extra_patches:
                stack.enter_context(patcher)
            stack.enter_context(contextlib.redirect_stdout(output))
            stack.enter_context(contextlib.redirect_stderr(output))
            try:
                PathenaScript.main()
                result["exit"] = "no exit"
            except SystemExit as exc:
                result["exit"] = exc.code
            except Exception as exc:
                result["exit"] = f"{type(exc).__name__}: {exc}"
    finally:
        os.chdir(orig_cwd)
        sys.path[:] = orig_sys_path
        panda_logger.handlers = saved_handlers
        shutil.rmtree(work, ignore_errors=True)

    result["task_params"] = task_params
    result["calls"] = calls
    result["exit_actions"] = len(exit_actions)
    # without the options printed by --verbose, which change with every new pathena option
    result["output"] = "".join(line for line in output.getvalue().splitlines(keepends=True) if not line.startswith("Namespace("))
    return json.loads(json.dumps(result, default=repr).replace(work, "WORK"))


def run_all_cases():
    return {name: run_case(case) for name, case in CASES.items()}


class TestPathenaTaskParams(unittest.TestCase):
    maxDiff = None

    @classmethod
    def setUpClass(cls):
        with open(GOLDEN_FILE) as f:
            cls.golden = json.load(f)

    def test_cases_match_golden_file(self):
        self.assertEqual(sorted(CASES), sorted(self.golden), "cases differ from the golden file: regenerate it")
        for name, case in CASES.items():
            with self.subTest(case=name):
                result = run_case(case)
                self.assertEqual(result, self.golden[name])
                # also the order of the keys, as sent to the server
                self.assertEqual(json.dumps(result), json.dumps(self.golden[name]))


if __name__ == "__main__":
    if "--regenerate" in sys.argv:
        results = run_all_cases()
        os.makedirs(os.path.dirname(GOLDEN_FILE), exist_ok=True)
        with open(GOLDEN_FILE, "w") as f:
            json.dump(results, f, indent=1)
            f.write("\n")
        print(f"wrote {GOLDEN_FILE}")
    else:
        unittest.main()
