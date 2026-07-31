"""
pbook CLI — PanDA task bookkeeper with typer-based shell autocompletion.
"""

from __future__ import annotations

import atexit
import code
import os
import re
import readline
import rlcompleter
import signal
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from inspect import signature
from typing import Optional

import typer
from rich import box
from rich.console import Console
from rich.table import Table

from pandaclient import Client, PandaToolsPkgInfo
from pandaclient.MiscUtils import commands_get_output

# ─── Runtime state ────────────────────────────────────────────────────────────
_tmp_dir: Optional[str] = None
_history_file: Optional[str] = None
_fork_child_pid: Optional[int] = None
_setup_done: bool = False
_ctx_state: dict = {}

app = typer.Typer(
    name="pbook",
    help="PanDA task bookkeeper. Run without arguments for interactive mode.",
    invoke_without_command=True,
    no_args_is_help=False,
)

# ─── Utilities ────────────────────────────────────────────────────────────────


def _parallel(func, items):
    # Parallel execution in a thread pool of 8 threads, for example when the user wants to act on a list of task IDs
    with ThreadPoolExecutor(8) as pool:
        return list(pool.map(func, items))


def _parse_ids(raw: str):
    """'all' → str, '42' → int, '1,2,3' → [int,...]."""
    if raw == "all":
        return "all"
    parts = raw.split(",")
    try:
        ids = [int(p) for p in parts]
        return ids[0] if len(ids) == 1 else ids
    except ValueError:
        typer.echo(f"Error: invalid task ID(s): {raw}", err=True)
        raise typer.Exit(1)


def _setup() -> None:
    global _tmp_dir, _history_file, _setup_done
    if _setup_done:
        return
    _setup_done = True

    readline.parse_and_bind("tab: complete")
    readline.parse_and_bind("set show-all-if-ambiguous On")

    if "CMTSITE" not in os.environ:
        os.environ["CMTSITE"] = ""

    pconf_dir = os.path.expanduser(os.environ.get("PANDA_CONFIG_ROOT", "~/.panda"))
    os.makedirs(pconf_dir, exist_ok=True)

    _history_file = os.path.join(pconf_dir, ".history")
    if os.path.exists(_history_file):
        try:
            readline.read_history_file(_history_file)
        except Exception:
            pass
    readline.set_history_length(1024)

    _tmp_dir = tempfile.mkdtemp()

    for path in sys.path:
        real = path or "."
        if (
            os.path.exists(real)
            and os.path.isdir(real)
            and "pandaclient" in os.listdir(real)
            and os.path.exists(os.path.join(real, "pandaclient", "__init__.py"))
        ):
            link = os.path.join(_tmp_dir, "taskbuffer")
            if not os.path.exists(link):
                os.symlink(os.path.join(real, "pandaclient"), link)
            break
    if _tmp_dir not in sys.path:
        sys.path.insert(0, _tmp_dir)

    atexit.register(_cleanup)


def _cleanup() -> None:
    if _fork_child_pid == 0 and _history_file:
        readline.write_history_file(_history_file)

    if _tmp_dir:
        commands_get_output(f"rm -rf {_tmp_dir}")


def _make_core(verbose: bool = False):
    from pandaclient import PBookCore

    return PBookCore.PBookCore(verbose=verbose)


def _get_core():
    _setup()
    return _make_core(_ctx_state.get("verbose", False))


def _catch_sig(sig, frame):
    _cleanup()
    # Hard kill all processes in the group
    commands_get_output(f"kill -9 -- -{os.getpgrp()}")


# ─── Interactive REPL namespace ───────────────────────────────────────────────

# ─── REPL kwarg completer ─────────────────────────────────────────────────────

_FUNC_KWARGS: dict[str, list[str]] = {
    "show": ["username", "limit", "taskname", "days", "jeditaskid", "reqid", "status", "superstatus", "format"],
    "showl": ["username", "limit", "taskname", "days", "jeditaskid", "reqid", "status", "superstatus"],
    "kill": [],
    "finish": ["soft"],
    "retry": [
        "newOpts",
        "days",
        "limit",
        "site",
        "excludedSite",
        "includedSite",
        "nFilesPerJob",
        "nMaxFilesPerJob",
        "nGBPerJob",
        "nFiles",
        "nEvents",
        "loopingCheck",
        "memory",
        "avoidVP",
        "ignoreMissingInDS",
        "forceStaged",
        "maxCore",
    ],
    "debug": ["modeOn"],
    "get_user_job_metadata": [],
    "recover_lost_files": ["test_mode"],
    "set_secret": ["is_file"],
    "list_secrets": ["full"],
}

_KWARG_VALUES: dict[str, dict[str, list[str]]] = {
    "show": {
        "format": ["standard", "long", "json", "plain"],
    },
    "finish": {
        "soft": ["True", "False"],
    },
    "debug": {
        "modeOn": ["True", "False"],
    },
    "recover_lost_files": {
        "test_mode": ["True", "False"],
    },
    "list_secrets": {
        "full": ["True", "False"],
    },
    "set_secret": {
        "is_file": ["True", "False"],
    },
    "retry": {
        "loopingCheck": ["True", "False"],
        "avoidVP": ["True", "False"],
        "ignoreMissingInDS": ["True", "False"],
        "forceStaged": ["True", "False"],
    },
}


def _run_repl(ns: dict, banner: str) -> None:
    """Manual REPL using InteractiveConsole.push() so we own readline setup entirely."""
    completer = _PBookCompleter(ns)
    readline.set_completer(completer.complete)
    readline.parse_and_bind("tab: complete")
    readline.parse_and_bind("set show-all-if-ambiguous On")

    console = code.InteractiveConsole(ns)
    print(banner)

    more = False
    while True:
        prompt = "... " if more else ">>> "
        try:
            readline.set_completer(completer.complete)
            line = input(prompt)
        except EOFError:
            print()
            break
        except KeyboardInterrupt:
            print("\nKeyboardInterrupt")
            console.resetbuffer()
            more = False
            continue
        more = console.push(line)


class _PBookCompleter:
    """Readline completer: kwarg names and values when inside a call, names otherwise."""

    def __init__(self, ns: dict) -> None:
        self._base = rlcompleter.Completer(ns)
        self._matches: list[str] = []

    def complete(self, text: str, state: int) -> Optional[str]:
        if state == 0:
            self._matches = self._compute(text)
        return self._matches[state] if state < len(self._matches) else None

    def _compute(self, text: str) -> list[str]:
        line = readline.get_line_buffer()

        # Kwarg value completion(tier 1, highest priority): last token is  kwarg=  or  kwarg='partial
        # >>> show(format='j│
        # Now the line matches both m_func (show() and m_val (line 234's regex: kwarg="format", quote="'", partial="j").
        # It looks up _KWARG_VALUES["show"]["format"] (line 165) = ["standard", "long", "json", "plain"], filters to those starting with j → json.
        # This branch is checked before tier 2, so once you're past the =, you get value suggestions instead of more kwarg names.
        m_val = re.search(r"\b(\w+)\s*=\s*(['\"]?)(\w*)$", line)
        m_func = re.match(r"(\w+)\s*\(", line)
        if m_val and m_func:
            kwarg, quote, partial = m_val.group(1), m_val.group(2), m_val.group(3)
            func_name = m_func.group(1)
            values = _KWARG_VALUES.get(func_name, {}).get(kwarg, [])
            hits = [v for v in values if v.startswith(partial)]
            if hits:
                return hits

        # Kwarg name completion: cursor is inside an open call
        # >>> show(│
        # readline.get_line_buffer() returns "show(". The regex on line 245, (\w+)\s*\([^)]*$, matches with func_name = "show" and nothing after the (.
        # So it looks up _FUNC_KWARGS["show"] (line 133) and lists username, limit, taskname, days, jeditaskid, reqid, status, superstatus, format —
        # rlcompleter is never consulted here.

        # >>> show(form│
        # Same regex still matches (func_name = "show", and [^)]* swallows form), but we filter _FUNC_KWARGS["show"] down to names starting with
        # form → just format.
        m = re.search(r"(\w+)\s*\([^)]*$", line)
        if m:
            hits = [k for k in _FUNC_KWARGS.get(m.group(1), []) if k.startswith(text)]
            if hits:
                return hits

        #  Plain name completion (tier 3, the rlcompleter fallback): standard name completion, stripping trailing '(' rlcompleter adds to callables
        # >>> sho│
        # Not inside any (...), so tiers 1 and 2 don't match. Falls through to rlcompleter (line 257-260), which scans ns for names starting with
        # sho → matches show, showl. Since these are callables, rlcompleter.complete() would normally return "show("/"showl("; the .rstrip("()").rstrip("(")
        # on line 259 strips that back to plain show, showl.
        if not text:
            # rlcompleter.complete() special-cases blank text by calling readline.insert_text()
            # itself, which re-enters readline from inside this callback and confuses the active
            # Tab press; list the namespace directly instead of delegating to it here
            return sorted(k for k in self._base.namespace if not k.startswith("_"))
        results, i = [], 0
        while (c := self._base.complete(text, i)) is not None:
            results.append(c.rstrip("()").rstrip("("))
            i += 1
        return results


_RETRY_ALLOWED_OPTS = [
    "site",
    "excludedSite",
    "includedSite",
    "nFilesPerJob",
    "nMaxFilesPerJob",
    "nGBPerJob",
    "nFiles",
    "nEvents",
    "loopingCheck",
    "maxNFilesPerJob",
    "memory",
    "ramCount",
    "avoidVP",
    "ignoreMissingInDS",
    "forceStaged",
    "maxCore",
]


def _build_namespace(core) -> dict:
    _console = Console()

    def help(command=None):
        """Show available commands, or detailed help for a specific command."""
        if command is not None:
            name = command if isinstance(command, str) else command.__name__
            func = ns.get(name, command if callable(command) else None)
            if func is None:
                _console.print(f"[red]Unknown command:[/red] {name}")
                return
            sig = str(signature(func)).replace("(", f"[bold cyan]{name}[/bold cyan](", 1)
            _console.print(f"\n[bold]{sig}[/bold]")
            doc = (func.__doc__ or "No description.").strip()
            _console.print(f"\n{doc}\n")
            return

        # No argument - summary table
        table = Table(box=box.SIMPLE, show_header=True, header_style="bold magenta")
        table.add_column("Command", style="bold cyan", no_wrap=True)
        table.add_column("Signature", style="dim", no_wrap=True)
        table.add_column("Description")

        _GROUPS = [
            ("Tasks", ["show", "showl", "kill", "finish", "retry", "debug"]),
            ("Files & input", ["get_user_job_metadata", "recover_lost_files", "reload_input"]),
            ("Workflows", ["show_workflow", "kill_workflow", "retry_workflow", "finish_workflow", "pause_workflow", "resume_workflow"]),
            ("Secrets", ["set_secret", "list_secrets", "delete_secret", "delete_all_secrets"]),
            ("Auth", ["generate_credential"]),
        ]
        for group, names in _GROUPS:
            table.add_section()
            table.add_row(f"[bold white]{group}[/bold white]", "", "")
            for name in names:
                func = ns.get(name)
                if func is None:
                    continue
                sig = str(signature(func))
                doc = (func.__doc__ or "").strip().splitlines()[0]
                table.add_row(f"  {name}", sig, doc)

        _console.print(table)
        _console.print("Usage: [bold]help(show)[/bold]  or  [bold]pbook show --help[/bold]\n")

    def show(taskID=None, *, username=None, limit=1000, taskname=None, days=14, jeditaskid=None, reqid=None, status=None, superstatus=None, format="standard"):
        """Print task records.

        taskID: jediTaskID / reqID / 'run' (active) / 'fin' (terminated) / omit for all.
        format: standard | long | json | plain
        """
        kwargs = {
            k: v
            for k, v in dict(
                username=username,
                limit=limit,
                taskname=taskname,
                days=days,
                jeditaskid=jeditaskid,
                reqid=reqid,
                status=status,
                superstatus=superstatus,
                format=format,
            ).items()
            if v is not None
        }
        kwargs.setdefault("limit", limit)
        kwargs.setdefault("days", days)
        kwargs["format"] = format
        return core.show(taskID, **kwargs) if taskID is not None else core.show(**kwargs)

    def showl(taskID=None, *, username=None, limit=1000, taskname=None, days=14, jeditaskid=None, reqid=None, status=None, superstatus=None):
        """Print task records in long format (shortcut for show(..., format='long'))."""
        return show(
            taskID,
            username=username,
            limit=limit,
            taskname=taskname,
            days=days,
            jeditaskid=jeditaskid,
            reqid=reqid,
            status=status,
            superstatus=superstatus,
            format="long",
        )

    def kill(taskIDs):
        """Kill tasks. taskIDs: int, list of ints, or 'all' for all active tasks."""
        if taskIDs == "all":
            return _parallel(lambda t: core.kill(t.jeditaskid), core.get_active_tasks())
        elif isinstance(taskIDs, (list, tuple)):
            return _parallel(core.kill, taskIDs)
        elif isinstance(taskIDs, int):
            return [core.kill(taskIDs)]

        print("Error: Invalid argument")
        return None

    def finish(taskIDs, soft=False):
        """Finish tasks. taskIDs: int, [int,...], or 'all' for all active tasks. soft=True waits for running jobs."""
        if taskIDs == "all":
            return _parallel(
                lambda t: core.finish.original_func(core, t.jeditaskid, soft=soft),
                core.get_active_tasks(),
            )
        elif isinstance(taskIDs, (list, tuple)):
            return _parallel(lambda tid: core.finish(tid, soft=soft), taskIDs)
        elif isinstance(taskIDs, int):
            return [core.finish(taskIDs, soft=soft)]

        print("Error: Invalid argument")
        return None

    def retry(taskIDs, newOpts=None, days=14, limit=1000, **kwargs):
        """Retry failed/cancelled tasks.

        taskIDs (required): int, list of ints, or 'all'.
        Allowed kwargs: site, excludedSite, includedSite, nFilesPerJob, nMaxFilesPerJob,
          nGBPerJob, nFiles, nEvents, loopingCheck, memory, avoidVP,
          ignoreMissingInDS, forceStaged, maxCore.
        Example: retry('all', loopingCheck=True)
        """
        if newOpts is None:
            newOpts = dict(kwargs)
        for key in list(newOpts):
            if key == "memory":
                newOpts["ramCount"] = newOpts.pop(key)
            elif key == "maxCore":
                newOpts["maxCoreCount"] = newOpts.pop(key)
            elif key not in _RETRY_ALLOWED_OPTS:
                print(f'Error: Unknown option "{key}"')
                return None
        opts = newOpts or None
        if isinstance(taskIDs, (list, tuple)):
            return _parallel(lambda tid: core.retry(tid, newOpts=opts), taskIDs)
        elif isinstance(taskIDs, int):
            return [core.retry(taskIDs, newOpts=opts)]
        elif taskIDs == "all":
            data = core.show(status="finished", days=days, limit=limit, format="json")
            return _parallel(lambda d: core.retry.original_func(core, d["jediTaskID"], newOpts=opts), data)

        print("Error: Invalid argument")
        return None

    def debug(PandaID, modeOn):
        """Toggle debug mode for a subjob. modeOn: True/False."""
        core.debug(PandaID, modeOn)

    def get_user_job_metadata(taskID, outputFileName):
        """Write user metadata of successful jobs to a JSON file."""
        core.getUserJobMetadata(taskID, outputFileName)

    def reload_input(task_id):
        """Reload input dataset and retry with new contents."""
        core.reload_input(task_id)

    def recover_lost_files(taskID, test_mode=False):
        """Request recovery of lost files from a task."""
        core.recover_lost_files(taskID, test_mode)

    def show_workflow(request_id):
        """Show workflow status."""
        _, output = core.execute_workflow_command("get_status", request_id)
        if output:
            print(output)

    def kill_workflow(request_id):
        """Kill a workflow."""
        _, output = core.execute_workflow_command("abort", request_id)
        if output:
            print(output[0][-1])

    def retry_workflow(request_id):
        """Retry a workflow."""
        _, output = core.execute_workflow_command("retry", request_id)
        if output:
            print(output[0][-1])

    def finish_workflow(request_id):
        """Finish a workflow."""
        _, output = core.execute_workflow_command("finish", request_id)
        if output:
            print(output[0][-1])

    def pause_workflow(request_id):
        """Pause a workflow."""
        _, output = core.execute_workflow_command("suspend", request_id)
        if output:
            print(output[0][-1])

    def resume_workflow(request_id):
        """Resume a workflow."""
        _, output = core.execute_workflow_command("resume", request_id)
        if output:
            print(output[0][-1])

    def set_secret(key, value, is_file=False):
        """Set a secret key-value pair. is_file=True to upload a file."""
        core.set_secret(key, value, is_file)

    def delete_secret(key):
        """Delete a secret."""
        core.set_secret(key, None)

    def delete_all_secrets():
        """Delete all secrets."""
        core.set_secret(None, None)

    def list_secrets(full=False):
        """List secrets. full=True to show full values."""
        core.list_secrets(full)

    def generate_credential():
        """Generate a new proxy or token."""
        core.generate_credential()

    # Generate the namespace with the local functions and exclude any imported functions or variables
    ns = {k: v for k, v in locals().items() if callable(v) and getattr(v, "__module__", None) == __name__}
    return ns


# ─── Top-level callback ───────────────────────────────────────────────────────


@app.callback(invoke_without_command=True)
def _main(
    ctx: typer.Context,
    verbose: bool = typer.Option(False, "-v", help="Verbose"),
    command_string: Optional[str] = typer.Option(None, "-c", help="Execute a Python code snippet"),
    version: bool = typer.Option(False, "--version", is_eager=True, help="Display version"),
    dev_srv: bool = typer.Option(False, "--devSrv", hidden=True),
    intr_srv: bool = typer.Option(False, "--intrSrv", hidden=True),
    prompt_with_newline: bool = typer.Option(False, "--prompt_with_newline", hidden=True),
) -> None:
    """PanDA task bookkeeper. Run without arguments for interactive mode."""
    if version:
        typer.echo(f"Version: {PandaToolsPkgInfo.release_version}")
        raise typer.Exit()

    # Set up the development or integration server
    if dev_srv:
        Client.useDevServer()
    if intr_srv:
        Client.useIntrServer()

    _ctx_state.update({"verbose": verbose})

    if ctx.invoked_subcommand is not None:
        return

    # Interactive or snippet mode
    _setup()
    global _fork_child_pid
    _fork_child_pid = os.fork()

    # Fork failed
    if _fork_child_pid == -1:
        typer.echo("ERROR: Failed to fork", err=True)
        raise typer.Exit(1)

    # Child process
    if _fork_child_pid == 0:
        if verbose:
            typer.echo(str(ctx.params))
        if prompt_with_newline:
            sys.ps1 = ">>> \n"
        core = _make_core(verbose)
        ns = _build_namespace(core)

        # The user wants to execute a Python code snippet instead of entering the REPL
        if command_string:
            core.init()
            exec(command_string, {}, ns)  # noqa: S102
            from pandaclient import PBookCore as _PBC

            raise typer.Exit(0 if _PBC.func_return_value else 1)
        core.init()
        _run_repl(ns, banner=f"\nStart pBook {PandaToolsPkgInfo.release_version}")

    # Parent process
    else:
        signal.signal(signal.SIGINT, _catch_sig)
        signal.signal(signal.SIGHUP, _catch_sig)
        signal.signal(signal.SIGTERM, _catch_sig)
        pid, status = os.wait()
        if os.WIFSIGNALED(status):
            raise typer.Exit(-os.WTERMSIG(status))
        elif os.WIFEXITED(status):
            raise typer.Exit(os.WEXITSTATUS(status))
        raise typer.Exit(0)


# ─── Subcommands ──────────────────────────────────────────────────────────────


@app.command()
def show(
    task_id: Optional[str] = typer.Argument(None, help="jediTaskID, reqID, 'run' (active only), or 'fin' (terminated only)"),
    username: Optional[str] = typer.Option(None, "--username", help="Filter by username"),
    limit: int = typer.Option(1000, "--limit", help="Maximum number of records"),
    taskname: Optional[str] = typer.Option(None, "--taskname", help="Filter by task name"),
    days: int = typer.Option(14, "--days", help="Look back N days (capped at 90 without a task ID)"),
    jeditaskid: Optional[int] = typer.Option(None, "--jeditaskid", help="Filter by jediTaskID"),
    reqid: Optional[int] = typer.Option(None, "--reqid", help="Filter by reqID"),
    status: Optional[str] = typer.Option(None, "--status", help="Filter by task status"),
    superstatus: Optional[str] = typer.Option(None, "--superstatus", help="Filter by super-status"),
    output_format: str = typer.Option("standard", "--format", help="Output format: standard|long|json|plain"),
) -> None:
    """Print task records."""
    core = _get_core()
    core.init(sanity_check=False)
    kwargs: dict = {"limit": limit, "days": days, "format": output_format}
    for k, v in [
        ("username", username),
        ("taskname", taskname),
        ("jeditaskid", jeditaskid),
        ("reqid", reqid),
        ("status", status),
        ("superstatus", superstatus),
    ]:
        if v is not None:
            kwargs[k] = v
    if task_id is not None:
        try:
            first_arg = int(task_id)
        except ValueError:
            first_arg = task_id
        core.show(first_arg, **kwargs)
    else:
        core.show(**kwargs)


@app.command()
def showl(
    task_id: Optional[str] = typer.Argument(None, help="jediTaskID, reqID, 'run', or 'fin'"),
    username: Optional[str] = typer.Option(None, "--username"),
    limit: int = typer.Option(1000, "--limit"),
    taskname: Optional[str] = typer.Option(None, "--taskname"),
    days: int = typer.Option(14, "--days"),
    jeditaskid: Optional[int] = typer.Option(None, "--jeditaskid"),
    reqid: Optional[int] = typer.Option(None, "--reqid"),
    status: Optional[str] = typer.Option(None, "--status"),
    superstatus: Optional[str] = typer.Option(None, "--superstatus"),
) -> None:
    """Print task records in long format (shortcut for show --format long)."""
    core = _get_core()
    core.init(sanity_check=False)
    kwargs: dict = {"limit": limit, "days": days, "format": "long"}
    for k, v in [
        ("username", username),
        ("taskname", taskname),
        ("jeditaskid", jeditaskid),
        ("reqid", reqid),
        ("status", status),
        ("superstatus", superstatus),
    ]:
        if v is not None:
            kwargs[k] = v
    if task_id is not None:
        try:
            first_arg = int(task_id)
        except ValueError:
            first_arg = task_id
        core.show(first_arg, **kwargs)
    else:
        core.show(**kwargs)


@app.command()
def kill(
    task_ids: str = typer.Argument(..., help="Task ID, comma-separated IDs, or 'all'"),
) -> None:
    """Kill tasks."""
    core = _get_core()
    core.init(sanity_check=False)
    ids = _parse_ids(task_ids)
    if ids == "all":
        _parallel(lambda t: core.kill(t.jeditaskid), core.get_active_tasks())
    elif isinstance(ids, list):
        _parallel(core.kill, ids)
    else:
        core.kill(ids)


@app.command()
def finish(
    task_ids: str = typer.Argument(..., help="Task ID, comma-separated IDs, or 'all'"),
    soft: bool = typer.Option(False, "--soft", help="Wait for running jobs to finish instead of killing them"),
) -> None:
    """Finish tasks."""
    core = _get_core()
    core.init(sanity_check=False)
    ids = _parse_ids(task_ids)
    if ids == "all":
        _parallel(lambda t: core.finish.original_func(core, t.jeditaskid, soft=soft), core.get_active_tasks())
    elif isinstance(ids, list):
        _parallel(lambda tid: core.finish(tid, soft=soft), ids)
    else:
        core.finish(ids, soft=soft)


@app.command()
def retry(
    task_ids: str = typer.Argument(..., help="Task ID, comma-separated IDs, or 'all'"),
    days: int = typer.Option(14, "--days", help="Look-back window when task_ids='all'"),
    limit: int = typer.Option(1000, "--limit", help="Max tasks to retry when task_ids='all'"),
    site: Optional[str] = typer.Option(None, "--site"),
    excluded_site: Optional[str] = typer.Option(None, "--excludedSite"),
    included_site: Optional[str] = typer.Option(None, "--includedSite"),
    n_files_per_job: Optional[int] = typer.Option(None, "--nFilesPerJob"),
    n_max_files_per_job: Optional[int] = typer.Option(None, "--nMaxFilesPerJob"),
    n_gb_per_job: Optional[float] = typer.Option(None, "--nGBPerJob"),
    n_files: Optional[int] = typer.Option(None, "--nFiles"),
    n_events: Optional[int] = typer.Option(None, "--nEvents"),
    looping_check: Optional[bool] = typer.Option(None, "--loopingCheck"),
    memory: Optional[int] = typer.Option(None, "--memory"),
    avoid_vp: Optional[bool] = typer.Option(None, "--avoidVP"),
    ignore_missing_in_ds: Optional[bool] = typer.Option(None, "--ignoreMissingInDS"),
    force_staged: Optional[bool] = typer.Option(None, "--forceStaged"),
    max_core: Optional[int] = typer.Option(None, "--maxCore"),
) -> None:
    """Retry failed/cancelled tasks."""
    core = _get_core()
    core.init(sanity_check=False)
    new_opts = {
        k: v
        for k, v in {
            "site": site,
            "excludedSite": excluded_site,
            "includedSite": included_site,
            "nFilesPerJob": n_files_per_job,
            "nMaxFilesPerJob": n_max_files_per_job,
            "nGBPerJob": n_gb_per_job,
            "nFiles": n_files,
            "nEvents": n_events,
            "loopingCheck": looping_check,
            "ramCount": memory,
            "avoidVP": avoid_vp,
            "ignoreMissingInDS": ignore_missing_in_ds,
            "forceStaged": force_staged,
            "maxCoreCount": max_core,
        }.items()
        if v is not None
    }
    opts = new_opts or None
    ids = _parse_ids(task_ids)
    if isinstance(ids, list):
        _parallel(lambda tid: core.retry(tid, newOpts=opts), ids)
    elif isinstance(ids, int):
        core.retry(ids, newOpts=opts)
    else:
        data = core.show(status="finished", days=days, limit=limit, format="json")
        _parallel(lambda d: core.retry.original_func(core, d["jediTaskID"], newOpts=opts), data)


@app.command()
def debug(
    panda_id: int = typer.Argument(..., help="PanDA subjob ID"),
    mode_on: bool = typer.Argument(..., help="True to enable, False to disable"),
) -> None:
    """Toggle debug mode for a subjob."""
    core = _get_core()
    core.init(sanity_check=False)
    core.debug(panda_id, mode_on)


@app.command(name="get-user-job-metadata")
def get_user_job_metadata(
    task_id: int = typer.Argument(..., help="Task ID"),
    output_file: str = typer.Argument(..., help="Output JSON file path"),
) -> None:
    """Write user metadata of successful jobs to a JSON file."""
    core = _get_core()
    core.init(sanity_check=False)
    core.getUserJobMetadata(task_id, output_file)


@app.command(name="reload-input")
def reload_input(
    task_id: int = typer.Argument(..., help="Task ID"),
) -> None:
    """Reload input dataset and retry the task with new contents."""
    core = _get_core()
    core.init(sanity_check=False)
    core.reload_input(task_id)


@app.command(name="recover-lost-files")
def recover_lost_files(
    task_id: int = typer.Argument(..., help="Task ID"),
    test_mode: bool = typer.Option(False, "--test-mode", help="Dry-run mode"),
) -> None:
    """Request recovery of lost files from a task."""
    core = _get_core()
    core.init(sanity_check=False)
    core.recover_lost_files(task_id, test_mode)


@app.command(name="show-workflow")
def show_workflow(
    request_id: int = typer.Argument(..., help="Workflow request ID"),
) -> None:
    """Show workflow status."""
    core = _get_core()
    core.init(sanity_check=False)
    _, output = core.execute_workflow_command("get_status", request_id)
    if output:
        print(output)


@app.command(name="kill-workflow")
def kill_workflow(
    request_id: int = typer.Argument(..., help="Workflow request ID"),
) -> None:
    """Kill a workflow."""
    core = _get_core()
    core.init(sanity_check=False)
    _, output = core.execute_workflow_command("abort", request_id)
    if output:
        print(output[0][-1])


@app.command(name="retry-workflow")
def retry_workflow(
    request_id: int = typer.Argument(..., help="Workflow request ID"),
) -> None:
    """Retry a workflow."""
    core = _get_core()
    core.init(sanity_check=False)
    _, output = core.execute_workflow_command("retry", request_id)
    if output:
        print(output[0][-1])


@app.command(name="finish-workflow")
def finish_workflow(
    request_id: int = typer.Argument(..., help="Workflow request ID"),
) -> None:
    """Finish a workflow."""
    core = _get_core()
    core.init(sanity_check=False)
    _, output = core.execute_workflow_command("finish", request_id)
    if output:
        print(output[0][-1])


@app.command(name="pause-workflow")
def pause_workflow(
    request_id: int = typer.Argument(..., help="Workflow request ID"),
) -> None:
    """Pause a workflow."""
    core = _get_core()
    core.init(sanity_check=False)
    _, output = core.execute_workflow_command("suspend", request_id)
    if output:
        print(output[0][-1])


@app.command(name="resume-workflow")
def resume_workflow(
    request_id: int = typer.Argument(..., help="Workflow request ID"),
) -> None:
    """Resume a workflow."""
    core = _get_core()
    core.init(sanity_check=False)
    _, output = core.execute_workflow_command("resume", request_id)
    if output:
        print(output[0][-1])


@app.command(name="set-secret")
def set_secret(
    key: str = typer.Argument(..., help="Secret key"),
    value: str = typer.Argument(..., help="Secret value or file path"),
    is_file: bool = typer.Option(False, "--is-file", help="Treat value as a file path to upload"),
) -> None:
    """Set a secret key-value pair."""
    core = _get_core()
    core.init(sanity_check=False)
    core.set_secret(key, value, is_file)


@app.command(name="list-secrets")
def list_secrets(
    full: bool = typer.Option(False, "--full", help="Show full secret values"),
) -> None:
    """List secrets."""
    core = _get_core()
    core.init(sanity_check=False)
    core.list_secrets(full)


@app.command(name="delete-secret")
def delete_secret(
    key: str = typer.Argument(..., help="Secret key to delete"),
) -> None:
    """Delete a secret."""
    core = _get_core()
    core.init(sanity_check=False)
    core.set_secret(key, None)


@app.command(name="delete-all-secrets")
def delete_all_secrets() -> None:
    """Delete all secrets."""
    core = _get_core()
    core.init(sanity_check=False)
    core.set_secret(None, None)


@app.command(name="generate-credential")
def generate_credential() -> None:
    """Generate a new proxy or token."""
    core = _get_core()
    core.generate_credential()


# ─── Entry point ──────────────────────────────────────────────────────────────


def main() -> None:
    sys.argv[0] = "pbook"
    app()
