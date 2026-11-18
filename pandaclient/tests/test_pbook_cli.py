"""Command-line compatibility tests for pbook's batch mode.

`pbook <command> [args] [--options] [legacy key=value]` must keep accepting the same command
lines with the same meaning, whichever library parses them. Each case runs PBookScript.main()
in-process with the PanDA core replaced by a recorder, and checks which core calls were made
with which values, the exit code, and where the output went. Nothing contacts a server.

What is deliberately not checked: the layout of --help and of error messages, and whether
shortened option names (--lim for --limit) are accepted.

Run with: python -m unittest pandaclient.tests.test_pbook_cli
"""

import contextlib
import io
import os
import tempfile
import threading
import types
import unittest
from unittest import mock

from pandaclient import Client, PandaToolsPkgInfo, PBookCore, PBookScript

# what the recorder core hands back, so the 'all' and workflow code paths have something to iterate over
ACTIVE_TASKS = [11, 12]
FINISHED_TASKS = [21, 22]
WORKFLOW_OUTPUT = "workflow-output"

SHOW_DEFAULTS = {"limit": 1000, "days": 14, "format": "standard"}

USAGE_ERROR = 2


def C(name, *args, **kwargs):
    """An expected core call."""
    return name, args, kwargs


class _Method:
    def __init__(self, core, name):
        self._core = core
        self._name = name

    def __call__(self, *args, **kwargs):
        return self._core._record(self._name, args, kwargs)

    def original_func(self, *args, **kwargs):
        # PBookCore's decorated methods expose the undecorated function, which pbook calls for 'all'
        # with the core as its first argument
        return self._core._record(self._name + ".original_func", args[1:], kwargs)


class RecorderCore:
    """Stands in for PBookCore: records every method call."""

    def __init__(self):
        self.calls = []
        self._lock = threading.Lock()  # pbook runs per-task calls in a thread pool

    def _record(self, name, args, kwargs):
        with self._lock:
            self.calls.append((name, args, kwargs))
        if name == "get_active_tasks":
            return [types.SimpleNamespace(jeditaskid=t) for t in ACTIVE_TASKS]
        if name == "show" and kwargs.get("format") == "json":
            return [{"jediTaskID": t} for t in FINISHED_TASKS]
        if name == "execute_workflow_command":
            return 0, [[0, WORKFLOW_OUTPUT]]
        return None

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return _Method(self, name)


def _sorted_calls(calls):
    # calls made from the thread pool arrive in any order
    return sorted(calls, key=repr)


def run_pbook(args, env=None, func_return_value=True):
    core = RecorderCore()
    init_calls = []
    ctx_state = {}
    stdout, stderr = io.StringIO(), io.StringIO()

    def fake_ensure_init(sanity_check=False):
        init_calls.append(sanity_check)
        return core

    full_env = {"COLUMNS": "200", "PANDA_CONFIG_ROOT": _config_root}
    full_env.update(env or {})
    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(PBookScript, "_ensure_init", fake_ensure_init))
        stack.enter_context(mock.patch.object(PBookScript, "_get_core", lambda: core))
        stack.enter_context(mock.patch.object(PBookScript, "_ctx_state", ctx_state))
        stack.enter_context(mock.patch.object(PBookScript, "_fork_child_pid", None))
        # interactive / -c mode forks; run the child's side in-process
        stack.enter_context(mock.patch.object(PBookScript.os, "fork", return_value=0))
        dev = stack.enter_context(mock.patch.object(Client, "useDevServer"))
        intr = stack.enter_context(mock.patch.object(Client, "useIntrServer"))
        stack.enter_context(mock.patch.object(PBookCore, "func_return_value", func_return_value))
        # no case may end up in the interactive prompt, which would wait on stdin
        stack.enter_context(mock.patch("builtins.input", side_effect=AssertionError("pbook started the interactive prompt")))
        stack.enter_context(mock.patch("sys.argv", ["pbook"] + list(args)))
        stack.enter_context(mock.patch.dict(os.environ, full_env))
        stack.enter_context(contextlib.redirect_stdout(stdout))
        stack.enter_context(contextlib.redirect_stderr(stderr))
        try:
            PBookScript.main()
            exit_code = 0
        except SystemExit as exc:
            exit_code = 0 if exc.code is None else exc.code if isinstance(exc.code, int) else 1
    return types.SimpleNamespace(
        exit=exit_code,
        out=stdout.getvalue(),
        err=stderr.getvalue(),
        calls=_sorted_calls(core.calls),
        init_calls=init_calls,
        verbose=ctx_state.get("verbose"),
        dev_server=dev.called,
        intr_server=intr.called,
    )


_config_root = None


def setUpModule():
    global _config_root
    _config_root = tempfile.mkdtemp(prefix="pbook-cli-test-")


class PbookTestCase(unittest.TestCase):
    # prepended to the args of check_calls / check_usage_errors cases
    command = None

    def _argv(self, args):
        return ([self.command] if self.command else []) + list(args)

    def check_calls(self, cases):
        """cases: (args, expected calls); each must exit 0 without writing to stderr."""
        for args, expected in cases:
            args = self._argv(args)
            with self.subTest(args=args):
                result = run_pbook(args)
                self.assertEqual(result.exit, 0, result.out + result.err)
                self.assertEqual(result.err, "")
                self.assertEqual(result.calls, _sorted_calls(expected))

    def check_usage_errors(self, cases):
        """cases: args rejected by the parser: exit 2, message on stderr, nothing sent to the core."""
        for args in cases:
            args = self._argv(args)
            with self.subTest(args=args):
                result = run_pbook(args)
                self.assertEqual(result.exit, USAGE_ERROR, result.out + result.err)
                self.assertNotEqual(result.err, "")
                self.assertEqual(result.calls, [])

    def check_refused(self, args, exit_code, message):
        """Rejected by pbook's own checks rather than the parser."""
        result = run_pbook(args)
        self.assertEqual(result.exit, exit_code, result.out + result.err)
        self.assertIn(message, result.err)
        self.assertEqual(result.calls, [])


class TestShow(PbookTestCase):
    command = "show"

    def test_accepted(self):
        long_ = dict(SHOW_DEFAULTS, format="long")
        all_filters = dict(username="joe", limit=1000, taskname="t*", days=3, jeditaskid=5, reqid=6, status="done", superstatus="x", format="standard")
        self.check_calls(
            [
                ([], [C("show", **SHOW_DEFAULTS)]),
                (["123"], [C("show", 123, **SHOW_DEFAULTS)]),
                (["run"], [C("show", "run", **SHOW_DEFAULTS)]),
                (["fin", "--days=7", "--limit=100"], [C("show", "fin", limit=100, days=7, format="standard")]),
                (["123", "--limit=1"], [C("show", 123, limit=1, days=14, format="standard")]),
                (["123", "--limit", "1"], [C("show", 123, limit=1, days=14, format="standard")]),
                (["123", "limit=1"], [C("show", 123, limit=1, days=14, format="standard")]),
                (["limit=1", "123"], [C("show", 123, limit=1, days=14, format="standard")]),
                (["--limit=1", "123"], [C("show", 123, limit=1, days=14, format="standard")]),
                (["--format=long"], [C("show", **long_)]),
                (["--format", "long"], [C("show", **long_)]),
                (["format=long"], [C("show", **long_)]),
                (["--days=-1"], [C("show", limit=1000, days=-1, format="standard")]),
                (["--days", "-1"], [C("show", limit=1000, days=-1, format="standard")]),
                (
                    ["--username=joe", "--taskname=t*", "--days=3", "--jeditaskid=5", "--reqid=6", "--status=done", "--superstatus=x"],
                    [C("show", **all_filters)],
                ),
                (
                    ["username=joe", "taskname=t*", "days=3", "jeditaskid=5", "reqid=6", "status=done", "superstatus=x"],
                    [C("show", **all_filters)],
                ),
                # a positional that merely contains '=' is not a legacy option
                (["foo=bar"], [C("show", "foo=bar", **SHOW_DEFAULTS)]),
            ]
        )

    def test_usage_errors(self):
        self.check_usage_errors(
            [
                ["--format=xml"],
                ["format=xml"],
                ["--limit=abc"],
                ["--jeditaskid=x"],
                ["123", "456"],
                ["--nosuch=1"],
                ["--limit"],
            ]
        )


class TestShowl(PbookTestCase):
    command = "showl"

    def test_accepted(self):
        long_ = dict(SHOW_DEFAULTS, format="long")
        self.check_calls(
            [
                ([], [C("show", **long_)]),
                (["123", "--status=done"], [C("show", 123, status="done", **long_)]),
                (["123", "status=done"], [C("show", 123, status="done", **long_)]),
            ]
        )

    def test_usage_errors(self):
        self.check_usage_errors([["--format=json"]])


class TestKill(PbookTestCase):
    command = "kill"

    def test_accepted(self):
        self.check_calls(
            [
                (["123"], [C("kill", 123)]),
                (["1,2,3"], [C("kill", 1), C("kill", 2), C("kill", 3)]),
                (["all"], [C("get_active_tasks")] + [C("kill", t) for t in ACTIVE_TASKS]),
            ]
        )

    def test_usage_errors(self):
        self.check_usage_errors([[]])

    def test_invalid_id(self):
        self.check_refused(["kill", "abc"], 1, "invalid task ID")


class TestFinish(PbookTestCase):
    command = "finish"

    def test_accepted(self):
        cases = [
            (["123"], [C("finish", 123, soft=False)]),
            (["123", "--soft"], [C("finish", 123, soft=True)]),
            (["--soft", "123"], [C("finish", 123, soft=True)]),
            (["1,2", "--soft"], [C("finish", 1, soft=True), C("finish", 2, soft=True)]),
            (["all", "--soft"], [C("get_active_tasks")] + [C("finish.original_func", t, soft=True) for t in ACTIVE_TASKS]),
        ]
        for value in ("True", "true", "yes", "1"):
            cases.append((["123", f"soft={value}"], [C("finish", 123, soft=True)]))
        for value in ("False", "false", "no", "0"):
            cases.append((["123", f"soft={value}"], [C("finish", 123, soft=False)]))
        self.check_calls(cases)

    def test_usage_errors(self):
        self.check_usage_errors([[], ["123", "--soft=yes"]])

    def test_legacy_flag_needs_bool(self):
        self.check_refused(["finish", "123", "soft=maybe"], 1, "is a flag and expects True/False")


def R(task_id, opts=None):
    return C("retry", task_id, newOpts=opts)


class TestRetry(PbookTestCase):
    command = "retry"

    def test_accepted(self):
        cases = [
            (["123"], [R(123)]),
            (["123", "--excludedSite=a,b"], [R(123, {"excludedSite": "a,b"})]),
            (["123", "--excludedSite", "a,b"], [R(123, {"excludedSite": "a,b"})]),
            (["123", "excludedSite=a,b"], [R(123, {"excludedSite": "a,b"})]),
            (["123", "--nGBPerJob=1"], [R(123, {"nGBPerJob": 1})]),
            (["123", "nGBPerJob=10"], [R(123, {"nGBPerJob": 10})]),
            (["123", "--maxCore=4"], [R(123, {"maxCoreCount": 4})]),
            (
                ["123", "--site=S", "--includedSite=x,y", "--nFilesPerJob=2", "--nFiles=3", "--nEvents=4"],
                [R(123, {"site": "S", "includedSite": "x,y", "nFilesPerJob": 2, "nFiles": 3, "nEvents": 4})],
            ),
            (["1,2", "--site=S"], [R(1, {"site": "S"}), R(2, {"site": "S"})]),
            (
                ["all"],
                [C("show", status="finished", days=14, limit=1000, format="json")] + [C("retry.original_func", t, newOpts=None) for t in FINISHED_TASKS],
            ),
            (
                ["all", "--days=30", "--limit=2000"],
                [C("show", status="finished", days=30, limit=2000, format="json")] + [C("retry.original_func", t, newOpts=None) for t in FINISHED_TASKS],
            ),
            (
                ["all", "days=30", "limit=2000"],
                [C("show", status="finished", days=30, limit=2000, format="json")] + [C("retry.original_func", t, newOpts=None) for t in FINISHED_TASKS],
            ),
            (
                ["all", "--excludedSite=a"],
                [C("show", status="finished", days=14, limit=1000, format="json")]
                + [C("retry.original_func", t, newOpts={"excludedSite": "a"}) for t in FINISHED_TASKS],
            ),
        ]
        # every spelling of an aliased option, dashed and legacy
        for spelling in ("memory", "ramCount"):
            cases.append((["123", f"--{spelling}=2000"], [R(123, {"ramCount": 2000})]))
            cases.append((["123", f"{spelling}=2000"], [R(123, {"ramCount": 2000})]))
        for spelling in ("nMaxFilesPerJob", "maxNFilesPerJob"):
            cases.append((["123", f"--{spelling}=3"], [R(123, {"nMaxFilesPerJob": 3})]))
            cases.append((["123", f"{spelling}=3"], [R(123, {"nMaxFilesPerJob": 3})]))
        # on / off / not given
        for name in ("loopingCheck", "avoidVP", "ignoreMissingInDS", "forceStaged"):
            cases.append((["123", f"--{name}"], [R(123, {name: True})]))
            cases.append((["123", f"--no-{name}"], [R(123, {name: False})]))
            cases.append((["123", f"{name}=True"], [R(123, {name: True})]))
            # with typer, the legacy False was dropped and the option silently not sent
            cases.append((["123", f"{name}=False"], [R(123, {name: False})]))
        self.check_calls(cases)

    def test_usage_errors(self):
        self.check_usage_errors(
            [
                [],
                ["123", "--memory=abc"],
                ["123", "--nGBPerJob=x"],
                ["123", "--nGBPerJob=1.5"],
                ["123", "--nGBPerJob=0"],
                ["123", "nGBPerJob=0"],
                ["123", "--loopingCheck=1"],
                ["123", "--days=x"],
            ]
        )

    def test_days_only_with_all(self):
        self.check_refused(["retry", "123", "--days=3"], 0, "only valid when task_ids is 'all'")

    def test_new_opts_not_from_shell(self):
        self.check_refused(["retry", "123", "--new-opts=x"], 0, "newOpts must be a dict")


class TestDebug(PbookTestCase):
    command = "debug"

    def test_accepted(self):
        cases = []
        for value in ("True", "true", "TRUE", "t", "yes", "y", "on", "1"):
            cases.append((["1234", value], [C("debug", 1234, True)]))
        for value in ("False", "false", "f", "no", "n", "off", "0"):
            cases.append((["1234", value], [C("debug", 1234, False)]))
        self.check_calls(cases)

    def test_usage_errors(self):
        self.check_usage_errors([["1234", "maybe"], ["x", "True"], ["1234"], []])


class TestOtherCommands(PbookTestCase):
    def test_accepted(self):
        self.check_calls(
            [
                (["get_user_job_metadata", "123", "out.json"], [C("getUserJobMetadata", 123, "out.json")]),
                (["reload_input", "123"], [C("reload_input", 123)]),
                (["recover_lost_files", "123"], [C("recover_lost_files", 123, False)]),
                (["recover_lost_files", "123", "--test_mode"], [C("recover_lost_files", 123, True)]),
                (["recover_lost_files", "123", "test_mode=True"], [C("recover_lost_files", 123, True)]),
                (["show_native_workflow", "136"], [C("show_native_workflow", 136, format="standard", show_data=False)]),
                (["show_native_workflow", "136", "--format=json", "--show_data"], [C("show_native_workflow", 136, format="json", show_data=True)]),
                (["show_native_workflow", "136", "format=long", "show_data=True"], [C("show_native_workflow", 136, format="long", show_data=True)]),
                (["set_secret", "k", "v"], [C("set_secret", "k", "v", False)]),
                (["set_secret", "k", "/p", "--is_file"], [C("set_secret", "k", "/p", True)]),
                (["set_secret", "k", "/p", "is_file=True"], [C("set_secret", "k", "/p", True)]),
                (["delete_secret", "k"], [C("set_secret", "k", None)]),
                (["delete_all_secrets"], [C("set_secret", None, None)]),
                (["list_secrets"], [C("list_secrets", False)]),
                (["list_secrets", "--full"], [C("list_secrets", True)]),
                (["list_secrets", "full=True"], [C("list_secrets", True)]),
                (["generate_credential"], [C("generate_credential")]),
            ]
        )

    def test_workflow_commands(self):
        for command, action in (
            ("show_workflow", "get_status"),
            ("kill_workflow", "abort"),
            ("retry_workflow", "retry"),
            ("finish_workflow", "finish"),
            ("pause_workflow", "suspend"),
            ("resume_workflow", "resume"),
        ):
            with self.subTest(command=command):
                result = run_pbook([command, "456"])
                self.assertEqual(result.exit, 0, result.err)
                self.assertEqual(result.calls, [C("execute_workflow_command", action, 456)])
                self.assertIn(WORKFLOW_OUTPUT, result.out)

    def test_usage_errors(self):
        self.check_usage_errors(
            [
                ["get_user_job_metadata", "123"],
                ["reload_input", "x"],
                ["show_workflow"],
                ["show_native_workflow", "136", "--format=xml"],
                ["set_secret", "k"],
                ["delete_all_secrets", "extra"],
                ["list_secrets", "--full=1"],
            ]
        )


class TestHelpCommand(PbookTestCase):
    def test_overview(self):
        result = run_pbook(["help"])
        self.assertEqual(result.exit, 0)
        for text in ("Tasks", "Workflows", "show_native_workflow", "generate_credential"):
            self.assertIn(text, result.out)

    def test_one_command(self):
        result = run_pbook(["help", "show"])
        self.assertEqual(result.exit, 0)
        self.assertIn("Print task records", result.out)
        self.assertIn("superstatus", result.out)

    def test_unknown_command(self):
        result = run_pbook(["help", "nosuch"])
        self.assertEqual(result.exit, 0)
        self.assertIn("Unknown command", result.out)


class TestGlobalOptions(PbookTestCase):
    def test_version(self):
        result = run_pbook(["--version"])
        self.assertEqual(result.exit, 0)
        self.assertIn(f"Version: {PandaToolsPkgInfo.release_version}", result.out)
        self.assertEqual(result.calls, [])

    def test_verbose(self):
        self.assertTrue(run_pbook(["-v", "show"]).verbose)
        self.assertFalse(run_pbook(["show"]).verbose)

    def test_servers(self):
        result = run_pbook(["--devSrv", "show"])
        self.assertTrue(result.dev_server)
        self.assertFalse(result.intr_server)
        self.assertEqual(result.calls, [C("show", **SHOW_DEFAULTS)])
        result = run_pbook(["--intrSrv", "show"])
        self.assertTrue(result.intr_server)
        self.assertFalse(result.dev_server)

    def test_hidden_options_accepted(self):
        for args in (["-3", "show"], ["--prompt_with_newline", "show"]):
            with self.subTest(args=args):
                result = run_pbook(args)
                self.assertEqual(result.exit, 0, result.err)
                self.assertEqual(result.calls, [C("show", **SHOW_DEFAULTS)])

    def test_usage_errors(self):
        # global options go before the command
        self.check_usage_errors([["nosuch"], ["--bogus"], ["show", "-v"], ["-c"]])

    def test_help_lists_options(self):
        for args, expected in (
            (["-h"], ["-c", "--version", "show", "retry", "generate_credential"]),
            (["--help"], ["-c", "--version", "show", "retry", "generate_credential"]),
            (["show", "-h"], ["--limit", "--format", "--superstatus", "task_id"]),
            (["show", "--help"], ["--limit", "--format", "--superstatus", "task_id"]),
            (["retry", "--help"], ["--excludedSite", "--ramCount", "--maxNFilesPerJob", "--no-loopingCheck", "--new-opts"]),
            (["debug", "--help"], ["panda_id", "mode_on"]),
        ):
            with self.subTest(args=args):
                result = run_pbook(args)
                self.assertEqual(result.exit, 0, result.err)
                self.assertEqual(result.calls, [])
                for text in expected:
                    self.assertIn(text, result.out)

    def test_hidden_options_not_in_help(self):
        result = run_pbook(["--help"])
        for text in ("--devSrv", "--intrSrv", "--prompt_with_newline"):
            self.assertNotIn(text, result.out)


class TestCodeSnippet(PbookTestCase):
    def test_runs_snippet(self):
        result = run_pbook(["-c", "show(123)"])
        self.assertEqual(result.exit, 0, result.err)
        self.assertEqual(result.calls, [C("show", 123, **SHOW_DEFAULTS)])
        # initialized once up front with the sanity check, then again (a no-op) by the command
        self.assertEqual(result.init_calls, [True, False])

    def test_exit_code_from_result(self):
        self.assertEqual(run_pbook(["-c", "show(123)"], func_return_value=False).exit, 1)

    def test_python_values(self):
        result = run_pbook(["-c", "kill([1, 2])"])
        self.assertEqual(result.calls, [C("kill", 1), C("kill", 2)])

    def test_with_verbose(self):
        result = run_pbook(["-v", "-c", "show()"])
        self.assertEqual(result.exit, 0, result.err)
        self.assertTrue(result.verbose)
        self.assertEqual(result.calls, [C("show", **SHOW_DEFAULTS)])


class TestShellCompletion(unittest.TestCase):
    """The protocol templates/pbook_completion.sh speaks: COMP_WORDS/COMP_CWORD in, one candidate per line out."""

    def complete(self, line):
        words = line.split(" ")
        # joined the way pbook_completion.sh does it: "${COMP_WORDS[*]}" with IFS set to a newline
        env = {"_PBOOK_COMPLETE": "complete_bash", "COMP_WORDS": "\n".join(words), "COMP_CWORD": str(len(words) - 1)}
        result = run_pbook([], env=env)
        self.assertEqual(result.exit, 0, result.err)
        self.assertEqual(result.calls, [])
        return set(result.out.split())

    def test_candidates(self):
        for line, expected in (
            ("pbook sh", {"show", "showl", "show_workflow", "show_native_workflow"}),
            ("pbook lis", {"list_secrets"}),
            ("pbook show --f", {"--format"}),
            ("pbook show --format ", {"standard", "long", "json", "plain"}),
            ("pbook show_native_workflow 1 --format j", {"json"}),
            ("pbook retry 1 --no-l", {"--no-loopingCheck"}),
            ("pbook retry 1 --ram", {"--ramCount"}),
            ("pbook retry 1 --excl", {"--excludedSite"}),
        ):
            with self.subTest(line=line):
                self.assertEqual(self.complete(line), expected)

    def test_all_commands(self):
        names = self.complete("pbook ")
        for name in ("help", "show", "retry", "set_secret", "generate_credential"):
            self.assertIn(name, names)


if __name__ == "__main__":
    unittest.main()
