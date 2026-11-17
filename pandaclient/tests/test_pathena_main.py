"""Behaviour tests for pathena steps that the golden-file test doesn't reach with --noSubmit.

Covers the event picking request and the submission, and the temporary directory with its cleanup at exit.
Uses the same setup as test_pathena_task_params, with the grid and server calls mocked.

Run with: python -m unittest pandaclient.tests.test_pathena_main
"""

import os
import shutil
import tempfile
import unittest
from unittest import mock

from pandaclient import Client, MiscUtils, PsubUtils
from pandaclient.tests import test_pathena_task_params as golden_test
from pandaclient.tests.test_pathena_task_params import IN, JOBO, run_case

# without the --noSubmit of the other cases
BASE_ARGS_WITH_SUBMISSION = [arg for arg in golden_test.BASE_ARGS if arg != "--noSubmit"]


def submission_patches(submit_mock):
    """Patches for the sandbox upload and the task submission"""
    return [
        mock.patch.object(Client, "putFile", return_value=(0, "True")),
        mock.patch.object(PsubUtils, "submit_task", submit_mock),
    ]


class TestEventPicking(unittest.TestCase):
    """--eventPickEvtList asks the event picking service for a dataset and uses it as input"""

    ARGS = JOBO + ["--eventPickEvtList", "events.txt", "--eventPickDataType", "AOD", "--eventPickStreamName", "physics_Main"]

    def test_requested_dataset_is_the_input(self):
        request_mock = mock.MagicMock(return_value=(0, "user.someone.evtpick.ds/"))
        submit_mock = mock.MagicMock(return_value=(0, "submitted", 123))
        with mock.patch.object(golden_test, "BASE_ARGS", BASE_ARGS_WITH_SUBMISSION):
            result = run_case(
                {"args": self.ARGS},
                extra_patches=[mock.patch.object(Client, "requestEventPicking", request_mock)] + submission_patches(submit_mock),
            )

        self.assertEqual(result["exit"], 0)
        request_mock.assert_called_once()
        args = request_mock.call_args.args
        self.assertEqual(args[0], "events.txt")
        self.assertEqual(args[1:5], ("AOD", "physics_Main", "", ""))
        # outDS with the trailing / added by pathena, and pathena as the requester
        self.assertEqual(args[7:9], ("user.someone.test.v1/", "pathena"))
        self.assertTrue(args[9].startswith("pathena "), args[9])
        # the submitted task reads the picked events, and the job gets the run/event list
        submit_mock.assert_called_once()
        task_params = submit_mock.call_args.args[0]
        self.assertEqual(task_params["dsForIN"], "user.someone.evtpick.ds/")
        runtime_args = [param["value"] for param in task_params["jobParameters"] if param.get("value", "").startswith("--sourceURL")]
        self.assertEqual(len(runtime_args), 1)
        self.assertIn("--eventPickTxt=ep_", runtime_args[0])

    def test_no_request_with_no_submit(self):
        request_mock = mock.MagicMock()
        result = run_case({"args": self.ARGS}, extra_patches=[mock.patch.object(Client, "requestEventPicking", request_mock)])
        request_mock.assert_not_called()
        self.assertEqual(result["task_params"][0]["dsForIN"], "dummy")


class TestSubmission(unittest.TestCase):
    """pathena submits one task per input and output dataset pair, and exits with 0 when all were tried"""

    def test_bulk_submission(self):
        submit_mock = mock.MagicMock(side_effect=[(1, "failed", None), (0, "submitted", 124)])
        with mock.patch.object(golden_test, "BASE_ARGS", BASE_ARGS_WITH_SUBMISSION):
            result = run_case({"args": IN + JOBO + ["--inOutDsJson", "inout.json"]}, extra_patches=submission_patches(submit_mock))
        self.assertEqual(submit_mock.call_count, 2)
        self.assertEqual([call.args[0]["dsForIN"] for call in submit_mock.call_args_list], ["data.A", "data.B"])
        # the failure of one of the tasks doesn't change the exit code
        self.assertEqual(result["exit"], 0)


class TestTmpDir(unittest.TestCase):
    """pathena makes a temporary directory, removed with the other temporary files at exit"""

    def test_tmp_dir_and_cleanup(self):
        parent = tempfile.mkdtemp(prefix="pathena_tmp_dir_test_")
        register_mock = mock.MagicMock()
        makedirs_calls = []
        real_makedirs = os.makedirs

        def recording_makedirs(path, *args, **kwargs):
            makedirs_calls.append(path)
            return real_makedirs(path, *args, **kwargs)

        try:
            run_case(
                {"args": IN + JOBO + ["--tmpDir", parent]},
                extra_patches=[mock.patch("atexit.register", register_mock), mock.patch("os.makedirs", recording_makedirs)],
            )
            # <--tmpDir>/<uuid>, the first UUID of the run
            tmp_dir = os.path.join(parent, "UUID0000")
            self.assertIn(tmp_dir, makedirs_calls)
            register_mock.assert_called_once()
            cleanup, registered_tmp_dir, _, del_command = register_mock.call_args.args
            self.assertEqual(registered_tmp_dir, tmp_dir)
            self.assertIs(del_command, MiscUtils.commands_get_output)

            # the cleanup removes the files to delete and the tmp dir
            os.makedirs(tmp_dir, exist_ok=True)
            extra_file = os.path.join(parent, "to_delete.txt")
            open(extra_file, "w").close()
            cleanup(tmp_dir, [extra_file], del_command)
            self.assertFalse(os.path.exists(tmp_dir))
            self.assertFalse(os.path.exists(extra_file))
        finally:
            shutil.rmtree(parent, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
