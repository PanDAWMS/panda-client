"""Golden-file regression test for the task parameters built by prun.

Runs PrunScript.main(get_taskparams=True, dry_mode=True) for a set of prun command lines and compares the
task parameters, the exit code of the expected failures, and the printed output with
data/prun_task_params.json. This is the path used by pchain and by the workflow code in panda-server.
It doesn't touch the network or the grid: dry mode skips the proxy, nickname, and sandbox upload, the
Athena setup is mocked, and UUIDs, server URLs, the release version and the client OS are fixed.

Run with: python -m unittest pandaclient.tests.test_prun_task_params

When a change to the task parameters is intended, regenerate the golden file and review its diff:
    python -m pandaclient.tests.test_prun_task_params --regenerate
"""

import contextlib
import io
import itertools
import json
import logging
import os
import shutil
import sys
import tempfile
import unittest
from unittest import mock

# importing Client resolves the panda cache host with DNS unless this is set
os.environ.setdefault("PANDA_BEHIND_REAL_LB", "1")

from pandaclient import AthenaUtils, Client, MiscUtils, PandaToolsPkgInfo, PrunScript, PsubUtils  # noqa: E402

GOLDEN_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "prun_task_params.json")

# environment variables read while building the task, unset so that the local setup doesn't change the results
ENV_TO_UNSET_PREFIXES = ("Atlas", "ALRB_", "CMAKE_PREFIX_PATH", "CMTCONFIG", "PANDA_EXEC_STRING", "PANDA_NICKNAME", "PANDA_PRODUCTION_ROLE", "RUCIO_")

CMT_CONFIG = ["--cmtConfig", "x86_64-el9-gcc13-opt"]
BASE_ARGS = ["--exec", "run.sh %IN out.root", "--outDS", "user.someone.test.v1", "--noSubmit"]
IN = ["--inDS", "data18_13TeV.periodA.DAOD_PHYS"]

# name -> prun arguments, or a dict with
#   args: prun arguments
#   env: extra environment variables
#   cwd: "work" to run prun in the work dir itself instead of its "sub" directory
#   work_area: Athena work area of the mocked Athena setup, the work dir by default
#   no_athena_env: True to mock a missing Athena setup
CASES = {
    "no_input": [],
    "basic": IN + ["--outputs", "out.root"],
    "outputs_mixed": IN + ["--outputs", "AAA:out.root,BBB:regex|hist_.*\\.root,log*.txt,plain.txt", "--allowNoOutput", "plain.txt,nomatch.txt"],
    "outputs_nth_field": IN + ["--outputs", "out.root", "--addNthFieldOfInDSToLFN", "2,3"],
    "outputs_bad_slash": IN + ["--outputs", "a/b.root"],
    "outputs_dup_suffix": IN + ["--outputs", "X:a.root,X:b.root"],
    "outputs_short_outds": IN + ["--outDS", "user.someone/", "--outputs", "a.root"],
    "input_options": IN
    + ["--outputs", "o.root", "--match", "*.pool.root.*", "--antiMatch", "*.tmp", "--nSkipFiles", "3", "--nFilesPerJob", "2", "--inputFileList", "files.txt"],
    "input_notexpand_logs": IN + ["--outputs", "o.root", "--notExpandInDS", "--useLogAsInput", "--antiMatch", "bad*"],
    "secondary": IN
    + [
        "--outputs",
        "o.root",
        "--secondaryDSs",
        "IN2:3:mc.ds2,IN3:1:mc.ds3:*.root:2",
        "--reusableSecondary",
        "IN3",
        "--notExpandSecDSs",
        "--useNumFilesInSecDSsAsRatio",
    ],
    "secondary_no_primary": ["--secondaryDSs", "IN2:3:mc.ds2", "--outputs", "o.root"],
    "secondary_with_pattern_files": IN + ["--outputs", "o.root", "--secondaryDSs", "IN2:2:mc.ds2:*.pool.root:1,IN3:3:mc.ds3", "--useNumFilesInSecDSsAsRatio"],
    "pfn_list": ["--pfnList", "pfns.txt", "--outputs", "o.root", "--site", "CERN-PROD"],
    "pfn_list_no_site": ["--pfnList", "pfns.txt", "--outputs", "o.root"],
    "merge": IN + ["--outputs", "o.root", "--mergeOutput", "--mergeScript", "hadd %OUT %IN", "--mergeSingleFile"],
    "merge_nobuild": IN + ["--outputs", "o.root", "--mergeOutput", "--noBuild"],
    "nocompile": IN + ["--outputs", "o.root", "--noCompile", "--mergeOutput"],
    "build_options": IN
    + ["--outputs", "o.root", "--bexec", "make all && echo 'done'", "--rootVer", "6.28/04", "--cmtConfig", "x86_64-el9-gcc13-opt", "--queueData", "q.json"],
    "misc_job_params": IN
    + ["--outputs", "o.root", "--writeInputToTxt", "IN:in.txt,IN2:in2.txt", "--persistentFile", "state.tgz", "--execWithRealFileNames", "--forceStaged"],
    "transfer_direct": IN + ["--outputs", "o.root", "--transferType", "direct"],
    "direct_io_sites": IN + ["--outputs", "o.root", "--useDirectIOSites"],
    "tarball_via_ddm": IN + ["--outputs", "o.root", "--noBuild", "--tarBallViaDDM", "user.someone.tarball", "--mergeOutput"],
    "container_alrb": IN + ["--outputs", "o.root", "--containerImage", "docker://busybox", "--alrbArgs=--nohome"],
    "container_alrb_sandbox_merge": IN + ["--outputs", "o.root", "--containerImage", "docker://busybox", "--useSandbox", "--mergeOutput"],
    "container_old_mode": IN + ["--outputs", "o.root", "--containerImage", "docker://busybox", "--oldContMode", "--useCentralRegistry", "--mergeOutput"],
    "container_old_not_central": IN + ["--outputs", "o.root", "--containerImage", "docker://busybox", "--oldContMode", "--notUseCentralRegistry"],
    "container_old_both_registry": IN
    + ["--outputs", "o.root", "--containerImage", "docker://busybox", "--oldContMode", "--useCentralRegistry", "--notUseCentralRegistry"],
    "container_old_merge_not_central": IN
    + [
        "--outputs",
        "o.root",
        "--containerImage",
        "docker://busybox",
        "--oldContMode",
        "--notUseCentralRegistry",
        "--mergeOutput",
        "--ctrCvmfs",
        "--persistentFile",
        "s.tgz",
    ],
    "container_no_direct_exec": IN + ["--outputs", "o.root", "--containerImage", "docker://busybox", "--wrapExecInContainer"],
    "vo_and_misc": IN + ["--outputs", "o.root", "--vo", "wlcg", "--workingGroup", "phys-higgs.role:x", "--msgDriven", "--useSecrets", "--noSeparateLog"],
    "dbr_in_exec": IN + ["--outputs", "o.root", "--exec", "run.sh %DB:ddo.000001.Atlas.Ideal.DBRelease.v100:DBRelease-100.tar.gz %IN"],
    "dbrelease_opt": IN + ["--outputs", "o.root", "--dbRelease", "ddo.000001.Atlas.Ideal.DBRelease.v100:DBRelease-100.tar.gz"],
    "rndm": ["--exec", "gen.sh %RNDM:100", "--outputs", "o.root", "--nJobs", "5"],
    "no_exec": ["--exec", ""],
    "cwd_is_work_dir": {"args": IN + ["--outputs", "o.root", "--mergeOutput"], "cwd": "work"},
    "cmake_build_merge": {
        "args": IN + ["--outputs", "o.root", "--mergeOutput", "--rootVer", "6.30.02", "--useRootCore", "--bexec", "make"],
        "env": {"CMAKE_PREFIX_PATH": "/opt/cmake"},
    },
    "athena_tag": IN + ["--outputs", "o.root", "--athenaTag", "AnalysisBase,24.2.10"] + CMT_CONFIG,
    "athena_tag_verbose": IN + ["--outputs", "o.root", "--athenaTag", "AnalysisBase,24.2.10", "-v"] + CMT_CONFIG,
    "athena_packages": IN + ["--outputs", "o.root", "--useAthenaPackages"] + CMT_CONFIG,
    "athena_packages_verbose": IN + ["--outputs", "o.root", "--useAthenaPackages", "-v"] + CMT_CONFIG,
    "athena_packages_merge": IN + ["--outputs", "o.root", "--useAthenaPackages", "--mergeOutput", "--rootVer", "6.30.02"] + CMT_CONFIG,
    "athena_packages_no_cmtconfig": IN + ["--outputs", "o.root", "--useAthenaPackages"],
    "athena_cwd_is_work_area": {"args": IN + ["--outputs", "o.root", "--useAthenaPackages", "--mergeOutput"] + CMT_CONFIG, "cwd": "work"},
    "athena_cwd_outside_work_area": {"args": IN + ["--outputs", "o.root", "--useAthenaPackages"] + CMT_CONFIG, "work_area": "/somewhere/else"},
    "athena_no_env": {"args": IN + ["--outputs", "o.root", "--useAthenaPackages"] + CMT_CONFIG, "no_athena_env": True},
    "cmake_athena_packages": {
        "args": IN + ["--outputs", "o.root", "--useAthenaPackages", "--mergeOutput", "--noCompile"] + CMT_CONFIG,
        "env": {"CMAKE_PREFIX_PATH": "/opt/cmake"},
    },
}


def _uuid_generator():
    counter = itertools.count()
    return lambda: f"UUID{next(counter):04d}"


def _athena_env(work_area):
    return {
        "workArea": work_area,
        "athenaVer": "24.0.10",
        "groupArea": "",
        "cacheVer": "_AnalysisBase,24.2.10",
        "nightVer": "",
        "cmtConfig": "x86_64-el9-gcc13-opt",
    }


def run_case(case):
    """Run prun for one case in a fresh work dir and return the result as JSON-compatible data"""
    if not isinstance(case, dict):
        case = {"args": case}
    # realpath since prun resolves the work dir, e.g. /var -> /private/var on macOS
    work = os.path.realpath(tempfile.mkdtemp(prefix="prun_test_"))
    sub_dir = os.path.join(work, "sub")
    os.makedirs(sub_dir)
    with open(os.path.join(sub_dir, "files.txt"), "w") as f:
        f.write("f1.root\n\n  f2.root  \n")
    with open(os.path.join(sub_dir, "pfns.txt"), "w") as f:
        f.write("root://a/f1.root\nroot://a/f2.root\n")
    args = BASE_ARGS + case["args"] + ["--workDir", work]

    environ = {key: value for key, value in os.environ.items() if not key.startswith(ENV_TO_UNSET_PREFIXES)}
    environ.update(case.get("env", {}))
    if case.get("no_athena_env"):
        athena_ver = (False, {})
    else:
        athena_ver = (True, _athena_env(case.get("work_area", work)))

    output = io.StringIO()
    # main logs through the panda-client logger, whose handler keeps the stdout of its creation time
    panda_logger = logging.getLogger("panda-client")
    saved_handlers = panda_logger.handlers[:]
    panda_logger.handlers = [logging.StreamHandler(output)]
    orig_cwd = os.getcwd()
    try:
        os.chdir(work if case.get("cwd") == "work" else sub_dir)
        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.dict(os.environ, environ, clear=True))
            stack.enter_context(mock.patch.object(sys, "argv", ["prun"] + args))
            stack.enter_context(mock.patch.object(MiscUtils, "wrappedUuidGen", _uuid_generator()))
            stack.enter_context(mock.patch.object(PandaToolsPkgInfo, "release_version", "0.0.0"))
            stack.enter_context(mock.patch.object(Client, "baseURLSSL", "https://pandaserver.test/server/panda"))
            stack.enter_context(mock.patch.object(Client, "baseURLCSRVSSL", "https://pandacache.test/server/panda"))
            stack.enter_context(mock.patch.object(AthenaUtils, "getAthenaVer", return_value=athena_ver))
            # osInfo of the task, the platform of the client
            stack.enter_context(mock.patch.object(PsubUtils, "get_os_information", return_value="TestOS-1.0-x86_64"))
            # module state modified by prun
            for name in ("extFile", "user_set_ext_files", "excludeFile", "athenaStuff"):
                stack.enter_context(mock.patch.object(AthenaUtils, name, []))
            stack.enter_context(mock.patch.object(AthenaUtils, "fullPathJobOs", {}))
            # set by main
            stack.enter_context(mock.patch.object(PsubUtils, "limit_maxNumInputs", None, create=True))
            stack.enter_context(contextlib.redirect_stdout(output))
            stack.enter_context(contextlib.redirect_stderr(output))
            try:
                result = {"task_params": PrunScript.main(get_taskparams=True, dry_mode=True, ext_args=args)}
            except SystemExit as exc:
                result = {"exit": exc.code}
    finally:
        os.chdir(orig_cwd)
        panda_logger.handlers = saved_handlers
        shutil.rmtree(work, ignore_errors=True)

    # without the options printed by --verbose, which change with every new prun option
    result["output"] = "".join(line for line in output.getvalue().splitlines(keepends=True) if not line.startswith("Namespace("))
    return json.loads(json.dumps(result).replace(work, "WORK"))


def run_all_cases():
    return {name: run_case(case) for name, case in CASES.items()}


class TestPrunTaskParams(unittest.TestCase):
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
