"""Tests for the option checks of pbook retry.

The retry options are checked against their declarations in PBookScript.retry: by Click on the CLI,
and by PBookScript itself for values from a plain Python call (REPL) or from newOpts. The core is
mocked, so nothing is sent to the server.

Run with: python -m unittest pandaclient.tests.test_pbook_retry
"""

import contextlib
import io
import unittest
from unittest import mock

from typer.testing import CliRunner

from pandaclient import PBookScript


class RetryTestBase(unittest.TestCase):
    def setUp(self):
        self.core = mock.MagicMock()
        patcher = mock.patch.object(PBookScript, "_ensure_init", return_value=self.core)
        patcher.start()
        self.addCleanup(patcher.stop)

    def call_retry(self, *args, **kwargs):
        """PBookScript.retry called as from the REPL, returning what it reported on stderr."""
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            PBookScript.retry(*args, **kwargs)
        return stderr.getvalue()

    def assert_rejected(self, *args, message=None, **kwargs):
        stderr = self.call_retry(*args, **kwargs)
        self.assertIn("Nothing was retried.", stderr)
        if message is not None:
            self.assertIn(message, stderr)
        self.core.retry.assert_not_called()

    def assert_sent(self, expected_opts, *args, **kwargs):
        stderr = self.call_retry(*args, **kwargs)
        self.assertEqual(stderr, "")
        self.core.retry.assert_called_once_with(123, newOpts=expected_opts)


class TestDeclarations(unittest.TestCase):
    def test_task_params_are_declared(self):
        params = PBookScript._cli_params("retry")
        for name in PBookScript._RETRY_TASK_PARAMS:
            self.assertIn(name, params)

    def test_new_opts_keys(self):
        """the newOpts keys derived from the declarations: Python names plus option aliases"""
        keys = PBookScript._retry_new_opts_keys()
        self.assertEqual(
            set(keys),
            {
                "site",
                "excludedSite",
                "includedSite",
                "nFilesPerJob",
                "nMaxFilesPerJob",
                "maxNFilesPerJob",
                "nGBPerJob",
                "nFiles",
                "nEvents",
                "loopingCheck",
                "memory",
                "ramCount",
                "avoidVP",
                "ignoreMissingInDS",
                "forceStaged",
                "maxCore",
            },
        )
        self.assertEqual(keys["maxNFilesPerJob"], "nMaxFilesPerJob")
        self.assertEqual(keys["ramCount"], "memory")


class TestPythonCall(RetryTestBase):
    def test_n_gb_per_job_rejected(self):
        for value in (1.5, 0, -1, True, "10"):
            with self.subTest(value=value):
                self.assert_rejected(123, nGBPerJob=value, message="nGBPerJob")

    def test_n_gb_per_job_range_message(self):
        self.assert_rejected(123, nGBPerJob=0, message="Invalid value for 'nGBPerJob': 0 is not in the range x>=1.")

    def test_n_gb_per_job_accepted(self):
        self.assert_sent({"nGBPerJob": 10}, 123, nGBPerJob=10)

    def test_other_types_rejected(self):
        self.assert_rejected(123, memory="4000", message="'memory' must be an integer, got '4000'.")
        self.assert_rejected(123, loopingCheck=1, message="'loopingCheck' must be True or False, got 1.")
        self.assert_rejected(123, excludedSite=["siteA", "siteB"], message="'excludedSite' must be a string")

    def test_translated_to_task_params(self):
        self.assert_sent({"ramCount": 4000, "maxCoreCount": 4, "loopingCheck": False}, 123, memory=4000, maxCore=4, loopingCheck=False)

    def test_no_options(self):
        self.assert_sent(None, 123)


class TestNewOpts(RetryTestBase):
    def test_n_gb_per_job_rejected(self):
        for value in (0.5, "10", None, 0):
            with self.subTest(value=value):
                self.assert_rejected(123, newOpts={"nGBPerJob": value}, message="nGBPerJob")

    def test_alias_named_in_message(self):
        self.assert_rejected(123, newOpts={"ramCount": "4000"}, message="'ramCount' must be an integer")

    def test_unknown_key_rejected(self):
        self.assert_rejected(123, newOpts={"foo": 1}, message="invalid option(s) in newOpts: foo")

    def test_conflicting_aliases_rejected(self):
        self.assert_rejected(123, newOpts={"memory": 1, "ramCount": 2}, message="newOpts sets memory and ramCount to different values")

    def test_not_a_dict_rejected(self):
        self.assert_rejected(123, newOpts="{'nGBPerJob': 10}", message="newOpts must be a dict")

    def test_accepted(self):
        cases = (
            ({"nGBPerJob": 10}, {"nGBPerJob": 10}),
            ({"ramCount": 4000}, {"ramCount": 4000}),
            ({"memory": 4000, "ramCount": 4000}, {"ramCount": 4000}),
            ({"maxNFilesPerJob": 5}, {"nMaxFilesPerJob": 5}),
            ({"maxCore": 4}, {"maxCoreCount": 4}),
            ({"excludedSite": "siteA,siteB"}, {"excludedSite": "siteA,siteB"}),
        )
        for new_opts, expected in cases:
            with self.subTest(new_opts=new_opts):
                self.core.reset_mock()
                self.assert_sent(expected, 123, newOpts=new_opts)

    def test_overrides_individual_options(self):
        self.assert_sent({"nGBPerJob": 10}, 123, nGBPerJob=0, newOpts={"nGBPerJob": 10})


class TestCli(RetryTestBase):
    def invoke(self, *args):
        return CliRunner().invoke(PBookScript.app, ["retry", "123", *args])

    def test_n_gb_per_job_rejected(self):
        for value in ("1.5", "0", "-1"):
            with self.subTest(value=value):
                result = self.invoke("--nGBPerJob", value)
                self.assertNotEqual(result.exit_code, 0)
                self.core.retry.assert_not_called()

    def test_n_gb_per_job_accepted(self):
        result = self.invoke("--nGBPerJob", "10", "--ramCount", "4000")
        self.assertEqual(result.exit_code, 0, result.output)
        self.core.retry.assert_called_once_with(123, newOpts={"nGBPerJob": 10, "ramCount": 4000})


if __name__ == "__main__":
    unittest.main()
