"""Behaviour tests for prun steps that the golden-file test doesn't reach in dry mode.

Covers the confirmation before sending the home directory, the event picking request, and the
temporary directory with its cleanup at exit. Uses the same setup as test_prun_task_params, with the
grid and server calls mocked.

Run with: python -m unittest pandaclient.tests.test_prun_main
"""

import os
import shutil
import tempfile
import unittest
from unittest import mock

from pandaclient import Client, MiscUtils, PrunScript, PsubUtils
from pandaclient.tests import test_prun_task_params as golden_test
from pandaclient.tests.test_prun_task_params import IN, run_case

# --maxCpuCount above Client.maxCpuCountLimit: prun exits right after the home dir check, so the tests
# can tell whether prun went past the check
TOO_LARGE_CPU_COUNT = ["--maxCpuCount", str(Client.maxCpuCountLimit + 1)]
TOO_LARGE_CPU_COUNT_MESSAGE = "too large maxCpuCount"


class TestHomeDirConfirmation(unittest.TestCase):
    """prun asks before sending all the files under $HOME"""

    def run_in_home_dir(self, answers, extra_args=()):
        input_mock = mock.MagicMock(side_effect=answers)
        case = {"args": IN + ["--outputs", "o.root"] + TOO_LARGE_CPU_COUNT + list(extra_args), "cwd": "work", "home_is_work_dir": True}
        patches = [mock.patch("builtins.input", input_mock), mock.patch.object(PsubUtils, "check_proxy")]
        return run_case(case, dry_mode=False, extra_patches=patches), input_mock

    def test_no_stops_prun(self):
        result, input_mock = self.run_in_home_dir(["N"])
        self.assertEqual(result.get("exit"), MiscUtils.EC_Config)
        self.assertNotIn(TOO_LARGE_CPU_COUNT_MESSAGE, result["output"])
        self.assertIn("is executed just under the HOME directory", result["output"])
        input_mock.assert_called_once_with("y/N: ")

    def test_yes_continues(self):
        result, input_mock = self.run_in_home_dir(["y"])
        self.assertIn(TOO_LARGE_CPU_COUNT_MESSAGE, result["output"])
        input_mock.assert_called_once_with("y/N: ")

    def test_asks_again_until_y_or_N(self):
        # only exactly y or N are accepted, with surrounding spaces stripped
        result, input_mock = self.run_in_home_dir(["", "n", "yes", "Y", "  N  "])
        self.assertEqual(input_mock.call_count, 5)
        self.assertNotIn(TOO_LARGE_CPU_COUNT_MESSAGE, result["output"])

    def test_no_question_with_use_home_dir(self):
        result, input_mock = self.run_in_home_dir([], extra_args=["--useHomeDir"])
        input_mock.assert_not_called()
        self.assertIn(TOO_LARGE_CPU_COUNT_MESSAGE, result["output"])

    def test_no_question_with_use_athena_packages(self):
        # the files under the work area are sent, not those under the work dir
        result, input_mock = self.run_in_home_dir([], extra_args=["--useAthenaPackages", "--cmtConfig", "x86_64-el9-gcc13-opt"])
        input_mock.assert_not_called()
        self.assertIn(TOO_LARGE_CPU_COUNT_MESSAGE, result["output"])

    def test_no_question_in_dry_mode(self):
        input_mock = mock.MagicMock(side_effect=[])
        case = {"args": IN + ["--outputs", "o.root"] + TOO_LARGE_CPU_COUNT, "cwd": "work", "home_is_work_dir": True}
        result = run_case(case, dry_mode=True, extra_patches=[mock.patch("builtins.input", input_mock)])
        input_mock.assert_not_called()
        self.assertIn(TOO_LARGE_CPU_COUNT_MESSAGE, result["output"])


class TestEventPicking(unittest.TestCase):
    """--eventPickEvtList asks the event picking service for a dataset and uses it as input"""

    ARGS = ["--eventPickEvtList", "events.txt", "--eventPickDataType", "AOD", "--eventPickStreamName", "physics_Main", "--outputs", "o.root"]

    def test_requested_dataset_is_the_input(self):
        request_mock = mock.MagicMock(return_value=(0, "user.someone.evtpick.ds/"))
        # without the --noSubmit of the other cases
        base_args = [arg for arg in golden_test.BASE_ARGS if arg != "--noSubmit"]
        with mock.patch.object(golden_test, "BASE_ARGS", base_args):
            result = run_case({"args": self.ARGS}, extra_patches=[mock.patch.object(Client, "requestEventPicking", request_mock)])

        self.assertEqual(result["task_params"]["dsForIN"], "user.someone.evtpick.ds/")
        request_mock.assert_called_once()
        args = request_mock.call_args.args
        self.assertEqual(args[0], "events.txt")
        self.assertEqual(args[1:5], ("AOD", "physics_Main", "", ""))
        # outDS with the trailing / added by prun, and prun as the requester
        self.assertEqual(args[7:9], ("user.someone.test.v1/", "prun"))
        self.assertTrue(args[9].startswith("prun "), args[9])

    def test_no_request_with_no_submit(self):
        request_mock = mock.MagicMock()
        result = run_case({"args": self.ARGS}, extra_patches=[mock.patch.object(Client, "requestEventPicking", request_mock)])
        request_mock.assert_not_called()
        self.assertEqual(result["task_params"]["dsForIN"], "dummy")


class TestExtFile(unittest.TestCase):
    """--extFile patterns are comma-separated, with * as wildcard, converted to regular expressions"""

    def ext_file_seen_by_sandbox_options(self, extra_args):
        seen = []
        real_process_sandbox_options = PrunScript.process_sandbox_options

        def recording_process_sandbox_options(options):
            seen.append(list(options.extFile))
            return real_process_sandbox_options(options)

        run_case(
            {"args": IN + ["--outputs", "o.root"] + extra_args},
            extra_patches=[mock.patch.object(PrunScript, "process_sandbox_options", recording_process_sandbox_options)],
        )
        self.assertEqual(len(seen), 1)
        return seen[0]

    def test_patterns(self):
        self.assertEqual(self.ext_file_seen_by_sandbox_options(["--extFile", "data/*.root,config.json,*"]), ["data/.*.root", "config.json", ".*"])

    def test_no_patterns(self):
        self.assertEqual(self.ext_file_seen_by_sandbox_options([]), [])


class TestTmpDir(unittest.TestCase):
    """prun makes a temporary directory, removed with the other temporary files at exit"""

    def test_tmp_dir_and_cleanup(self):
        parent = tempfile.mkdtemp(prefix="prun_tmp_dir_test_")
        register_mock = mock.MagicMock()
        makedirs_calls = []
        real_makedirs = os.makedirs

        def recording_makedirs(path, *args, **kwargs):
            makedirs_calls.append(path)
            return real_makedirs(path, *args, **kwargs)

        try:
            run_case(
                {"args": IN + ["--outputs", "o.root", "--tmpDir", parent]},
                extra_patches=[
                    mock.patch.object(PrunScript.atexit, "register", register_mock),
                    mock.patch.object(PrunScript.os, "makedirs", recording_makedirs),
                ],
            )
            # <--tmpDir>/<uuid>, the first UUID of the run
            tmp_dir = os.path.join(parent, "UUID0000")
            self.assertIn(tmp_dir, makedirs_calls)
            register_mock.assert_called_once()
            cleanup, registered_tmp_dir, _, del_command = register_mock.call_args.args
            self.assertEqual(registered_tmp_dir, tmp_dir)
            self.assertIs(del_command, MiscUtils.commands_get_output)

            # the cleanup removes the files to delete and the tmp dir
            real_makedirs(tmp_dir)
            extra_file = os.path.join(parent, "to_delete.txt")
            open(extra_file, "w").close()
            cleanup(tmp_dir, [extra_file], del_command)
            self.assertFalse(os.path.exists(tmp_dir))
            self.assertFalse(os.path.exists(extra_file))
        finally:
            shutil.rmtree(parent, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
