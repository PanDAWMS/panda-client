"""
Do NOT import this module in your code.
Import PBookCore instead.

pbook CLI — PanDA task bookkeeper.

Each command below is defined exactly once, as a Typer command using the
Annotated[...] parameter style. Because the Typer/Click metadata lives in the
annotation rather than in the default value, these functions remain ordinary
callables with ordinary defaults - the same function is used to build the
`pbook <command> --flag ...` CLI (with real shell completion) *and* is placed
directly into the interactive REPL namespace (`>>> command(...)`).
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
from inspect import Parameter, signature
from typing import (
    Annotated,
    Literal,
    Optional,
    Union,
    get_args,
    get_origin,
    get_type_hints,
)

import typer
from rich import box
from rich.console import Console
from rich.markup import escape as _esc
from rich.table import Table

from pandaclient import Client, PandaToolsPkgInfo
from pandaclient.MiscUtils import commands_get_output

# ─── Runtime state ────────────────────────────────────────────────────────────
_tmp_dir: Optional[str] = None
_history_file: Optional[str] = None
_fork_child_pid: Optional[int] = None
_setup_done: bool = False
_ctx_state: dict = {}
_core = None
_core_inited: bool = False

help_text = """
PanDA task bookkeeper. Run without arguments for interactive mode.

$ pbook [options] # interactive mode
$ pbook [options] command [args] [kwargs] # batch mode

The same command can be executed in interactive mode:

$ pbook
>>> command(*args, **kwargs)

or in batch mode:

$ pbook command arg1 arg2 ... argN --kwarg1=value1 --kwarg2=value2 ... --kwargN=valueN
$ pbook command arg1 arg2 ... argN kwarg1=value1 kwarg2=value2 ... kwargN=valueN
Please note that the latter option is kept for backward compatibility, but we plan to drop it in the future. 

E.g.

$ pbook
>>> show(123, format='long')

is equivalent to

$ pbook show 123 --format='long'
$ pbook show 123 format='long'

If arg or value is a list in interactive mode, it is represented as a comma-separate list in batch mode. E.g.
to kill three tasks in interactive mode:

$ pbook
>>> kill([123, 456, 789])

or in batch mode:

$ pbook kill 123,456,789

To see the list of commands and help of each command,

$ pbook
>>> help()
>>> help(command_name)

or

$ pbook --help
$ pbook command_name --help
"""

app = typer.Typer(
    name="pbook",
    help=help_text,
    invoke_without_command=True,
    no_args_is_help=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)

# ─── Utilities ────────────────────────────────────────────────────────────────


def _parallel(func, items):
    with ThreadPoolExecutor(8) as pool:
        return list(pool.map(func, items))


def _parse_ids(raw):
    """'all' -> 'all'; '42' -> 42; '1,2,3' -> [1,2,3]; anything not a string passes through as-is."""
    if not isinstance(raw, str):
        return raw
    if raw == "all":
        return "all"
    parts = raw.split(",")
    try:
        ids = [int(p) for p in parts]
        return ids[0] if len(ids) == 1 else ids
    except ValueError:
        typer.echo(f"Error: invalid task ID(s): {raw}", err=True)
        raise typer.Exit(1)


_INVALID = object()


def _require_bool(name: str, value):
    """Report non-bool values for a bool parameter, without raising.

    Click enforces this on the CLI path (a flag is present or absent, never a stray
    string) - unreachable from real CLI dispatch. Commands are also called directly
    from the REPL though, where a plain Python call - e.g. finish(123, soft='gfd') -
    bypasses that check entirely and would otherwise silently treat any truthy string
    as on. Raising here would just dump a traceback into the interactive session, so
    report the problem and let the caller return early instead: `if soft is _INVALID:
    return`.
    """
    if not isinstance(value, bool):
        typer.echo(f"Error: '{name}' must be True or False, got {value!r}", err=True)
        return _INVALID
    return value


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
    """Return the (memoized) core for this process, uninitialized."""
    global _core
    _setup()
    if _core is None:
        _core = _make_core(_ctx_state.get("verbose", False))
    return _core


def _ensure_init(sanity_check: bool = False):
    """Return the core, running PBookCore.init() exactly once per process.

    The REPL calls this once upfront with sanity_check=True; every command
    function then calls it again with the default before touching the core,
    which is a no-op once already initialized - so commands stay correct
    whether they run once (batch mode) or repeatedly (REPL session).
    """
    global _core_inited
    core = _get_core()
    if not _core_inited:
        core.init(sanity_check=sanity_check)
        _core_inited = True
    return core


def _catch_sig(sig, frame):
    _cleanup()
    # Hard kill all processes in the group
    commands_get_output(f"kill -9 -- -{os.getpgrp()}")


# ─── REPL namespace & completion ──────────────────────────────────────────────


def _build_namespace() -> dict:
    """The REPL namespace: every registered Typer command, keyed by its real Python name."""
    return {info.callback.__name__: info.callback for info in app.registered_commands}


def _kwarg_names(func) -> list:
    """All parameter names a function accepts - candidates for `name=` completion."""
    return list(signature(func).parameters)


def _kwarg_choices(func, name: str) -> list:
    """Value choices for a parameter, derived from its type hint: Literal[...] members or True/False for bool.

    Returned bare (unquoted) - readline's own quote-matching auto-closes an opening quote
    the user already typed, so we don't need to (and shouldn't try to) add one ourselves.
    """
    try:
        hints = get_type_hints(func, include_extras=True)
    except Exception:
        return []
    ann = hints.get(name)
    if ann is None:
        return []
    while hasattr(ann, "__metadata__"):
        ann = ann.__origin__
    if get_origin(ann) is Union:
        non_none = [a for a in get_args(ann) if a is not type(None)]
        if len(non_none) == 1:
            ann = non_none[0]
    if get_origin(ann) is Literal:
        return [str(v) for v in get_args(ann)]
    if ann is bool:
        return ["True", "False"]
    return []


class _PBookCompleter:
    """Readline completer: kwarg names and values when inside a call, names otherwise."""

    def __init__(self, ns: dict) -> None:
        self._ns = ns
        self._base = rlcompleter.Completer(ns)
        self._matches: list = []

    def complete(self, text: str, state: int) -> Optional[str]:
        if state == 0:
            self._matches = self._compute(text)
        return self._matches[state] if state < len(self._matches) else None

    def _compute(self, text: str) -> list:
        line = readline.get_line_buffer()

        # Kwarg value completion (tier 1): last token is  kwarg=  or  kwarg='partial
        # Return bare values - readline's own quote-matching auto-closes an opening quote
        # the user already typed, so we deliberately don't add quotes ourselves here.
        m_val = re.search(r"\b(\w+)\s*=\s*(['\"]?)(\w*)$", line)
        m_func = re.match(r"(\w+)\s*\(", line)
        if m_val and m_func:
            kwarg, partial = m_val.group(1), m_val.group(3)
            func = self._ns.get(m_func.group(1))
            if func is not None:
                # We're unambiguously past a `kwarg=` - this is a value position, not a
                # name position, even if this particular kwarg has no enumerable choices
                # (e.g. limit: int). Return here regardless, so an empty result doesn't
                # fall through to tier 2's kwarg-name completion.
                return [v for v in _kwarg_choices(func, kwarg) if v.startswith(partial)]

        # Kwarg name completion (tier 2): cursor is inside an open call
        m = re.search(r"(\w+)\s*\([^)]*$", line)
        if m:
            func = self._ns.get(m.group(1))
            if func is not None:
                hits = [k for k in _kwarg_names(func) if k.startswith(text)]
                if hits:
                    return hits

        # Plain name completion (tier 3, rlcompleter fallback)
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
    python3: bool = typer.Option(False, "-3", hidden=True),
) -> None:
    """PanDA task bookkeeper. Run without arguments for interactive mode."""
    if version:
        typer.echo(f"Version: {PandaToolsPkgInfo.release_version}")
        raise typer.Exit()

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

    if _fork_child_pid == -1:
        typer.echo("ERROR: Failed to fork", err=True)
        raise typer.Exit(1)

    if _fork_child_pid == 0:
        if verbose:
            typer.echo(str(ctx.params))
        if prompt_with_newline:
            sys.ps1 = ">>> \n"
        _ensure_init(sanity_check=True)
        ns = _build_namespace()

        if command_string:
            exec(command_string, {}, ns)  # noqa: S102
            from pandaclient import PBookCore as _PBC

            raise typer.Exit(0 if _PBC.func_return_value else 1)
        _run_repl(ns, banner=f"\nStart pBook {PandaToolsPkgInfo.release_version}")

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


# ─── Commands ──────────────────────────────────────────────────────────────────

_HELP_GROUPS = [
    ("Tasks", ["show", "showl", "kill", "finish", "retry", "debug"]),
    ("Files & input", ["get_user_job_metadata", "recover_lost_files", "reload_input"]),
    ("Workflows", ["show_workflow", "kill_workflow", "retry_workflow", "finish_workflow", "pause_workflow", "resume_workflow"]),
    ("Secrets", ["set_secret", "list_secrets", "delete_secret", "delete_all_secrets"]),
    ("Auth", ["generate_credential"]),
]


def _type_name(ann) -> str:
    """Render a resolved type annotation as a short, human-readable name (Optional[str], Literal[...], etc.)."""
    if ann is None or ann is type(None):
        return ""
    origin = get_origin(ann)
    if origin is Union:
        args = get_args(ann)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1 and len(args) == 2:
            return f"Optional[{_type_name(non_none[0])}]"
        return " | ".join(_type_name(a) for a in args)
    if origin is Literal:
        return "Literal[" + ", ".join(repr(v) for v in get_args(ann)) + "]"
    if origin is not None:
        args = get_args(ann)
        origin_name = getattr(origin, "__name__", str(origin))
        return f"{origin_name}[{', '.join(_type_name(a) for a in args)}]" if args else origin_name
    return getattr(ann, "__name__", str(ann))


def _format_signature(func) -> str:
    """A clean '(param: Type = default, ...)' string, stripping the Typer/Annotated plumbing."""
    try:
        hints = get_type_hints(func, include_extras=True)
    except Exception:
        hints = {}
    parts = []
    for pname, p in signature(func).parameters.items():
        ann = hints.get(pname)
        while hasattr(ann, "__metadata__"):
            ann = ann.__origin__
        piece = pname
        type_str = _type_name(ann)
        if type_str:
            piece += f": {type_str}"
        if p.default is not Parameter.empty:
            piece += f" = {p.default!r}"
        parts.append(piece)
    return f"({', '.join(parts)})"


@app.command()
def help(
    command: Annotated[Optional[str], typer.Argument(help="Command name for detailed help")] = None,
) -> None:
    """Show available commands, or detailed help for a specific command."""
    ns = _build_namespace()
    console = Console()

    if command is not None:
        name = command if isinstance(command, str) else command.__name__
        func = ns.get(name, command if callable(command) else None)
        if func is None:
            console.print(f"[red]Unknown command:[/red] {_esc(name)}")
            return
        console.print(f"\n[bold cyan]{name}[/bold cyan][bold]{_esc(_format_signature(func))}[/bold]")
        doc = (func.__doc__ or "No description.").strip()
        console.print(f"\n{_esc(doc)}\n")
        return

    table = Table(box=box.SIMPLE, show_header=True, header_style="bold magenta")
    table.add_column("Command", style="bold cyan", no_wrap=True)
    table.add_column("Description")
    table.add_column("Signature")

    for group, names in _HELP_GROUPS:
        table.add_section()
        table.add_row(f"[bold white]{group}[/bold white]", "", "")
        for name in names:
            func = ns.get(name)
            if func is None:
                continue
            doc = (func.__doc__ or "").strip().splitlines()[0] if func.__doc__ else ""
            sig = console.highlighter(_format_signature(func))
            table.add_row(f"  {name}", _esc(doc), sig)

    console.print(table)
    console.print("Usage: [bold]help(show)[/bold]  or  [bold]pbook show --help[/bold]\n")


@app.command()
def show(
    task_id: Annotated[Optional[str], typer.Argument(help="jediTaskID, reqID, 'run' (active only), or 'fin' (terminated only)")] = None,
    username: Annotated[Optional[str], typer.Option(help="Filter by username. By default, the name from the voms/token is used.")] = None,
    limit: Annotated[int, typer.Option(help="Maximum number of records")] = 1000,
    taskname: Annotated[Optional[str], typer.Option(help="Filter by task name")] = None,
    days: Annotated[int, typer.Option(help="Look back N days (capped at 90 without a task ID)")] = 14,
    jeditaskid: Annotated[Optional[int], typer.Option(help="Filter by jediTaskID")] = None,
    reqid: Annotated[Optional[int], typer.Option(help="Filter by reqID")] = None,
    status: Annotated[Optional[str], typer.Option(help="Filter by task status")] = None,
    superstatus: Annotated[Optional[str], typer.Option(help="Filter by super-status")] = None,
    format: Annotated[Literal["standard", "long", "json", "plain"], typer.Option("--format", help="Output format")] = "standard",
) -> None:
    """Print task records.

    The first non-keyword argument (task_id) can be a jediTaskID or reqID, or 'run' (show active tasks
    only), or 'fin' (show terminated tasks only), or can be omitted. Records are fetched
    directly from the PanDA server, so they are always up to date. Note that days is capped
    at 90 days unless a jediTaskID or reqID is specified, in which case tasks of any age are
    returned. See the default filter conditions in the annotations.

    examples:
      >>> show()
      >>> show(123)
      >>> show(12345678, format='long')
      >>> show(taskname='my_task_name')
      >>> show('run')
      >>> show('fin', days=7, limit=100)
      >>> show(format='json')

      $ pbook show --format=long --status=done --limit=100
    """
    core = _ensure_init()
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
        ).items()
        if v is not None
    }
    kwargs["format"] = format
    if task_id is not None:
        try:
            first_arg = int(task_id)
        except (TypeError, ValueError):
            first_arg = task_id
        return core.show(first_arg, **kwargs)
    return core.show(**kwargs)


@app.command()
def showl(
    task_id: Annotated[Optional[str], typer.Argument(help="jediTaskID, reqID, 'run', or 'fin'")] = None,
    username: Annotated[Optional[str], typer.Option(help="Filter by username")] = None,
    limit: Annotated[int, typer.Option(help="Maximum number of records")] = 1000,
    taskname: Annotated[Optional[str], typer.Option(help="Filter by task name")] = None,
    days: Annotated[int, typer.Option(help="Look back N days (capped at 90 without a task ID)")] = 14,
    jeditaskid: Annotated[Optional[int], typer.Option(help="Filter by jediTaskID")] = None,
    reqid: Annotated[Optional[int], typer.Option(help="Filter by reqID")] = None,
    status: Annotated[Optional[str], typer.Option(help="Filter by task status")] = None,
    superstatus: Annotated[Optional[str], typer.Option(help="Filter by super-status")] = None,
) -> None:
    """Print task records in long format (shortcut for show --format='long').

    examples:
      >>> showl()
      >>> showl(123)
      >>> showl(12345678)
      >>> showl(taskname='my_task_name')

      $ pbook showl --status=done --limit=100
    """
    return show(
        task_id,
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


@app.command()
def kill(
    task_ids: Annotated[str, typer.Argument(help="Task ID, comma-separated IDs, or 'all'")],
) -> None:
    """Kill tasks.

    Kill all subJobs in task_ids (ID or a list of IDs, can be either jediTaskID or reqID).
    If 'all', kill all active tasks of the user.

    example:
      >>> kill(123)
      >>> kill([123, 345, 567])
      >>> kill('all')

      $ pbook kill 123
      $ pbook kill 123,345,567
      $ pbook kill all
    """
    core = _ensure_init()
    ids = _parse_ids(task_ids)
    if ids == "all":
        return _parallel(lambda t: core.kill(t.jeditaskid), core.get_active_tasks())
    elif isinstance(ids, list):
        return _parallel(core.kill, ids)
    return core.kill(ids)


@app.command()
def finish(
    task_ids: Annotated[str, typer.Argument(help="Task ID, comma-separated IDs, or 'all'")],
    soft: Annotated[bool, typer.Option("--soft", help="Wait for running jobs to finish instead of killing them")] = False,
) -> None:
    """Finish tasks.

    Finish all subJobs in task_ids (ID or a list of IDs, can be either jediTaskID or reqID).
    If task_ids is 'all', finish all active tasks of the user. If soft is False (default),
    all running jobs are killed and the task finishes immediately. If soft is True, new jobs
    are not generated and the task finishes once all running jobs finish.

    example:
      >>> finish(123)
      >>> finish(234, soft=True)
      >>> finish([123, 345, 567])
      >>> finish('all')

      $ pbook finish 123,345,567 --soft
    """
    soft = _require_bool("soft", soft)
    if soft is _INVALID:
        return
    core = _ensure_init()
    ids = _parse_ids(task_ids)
    if ids == "all":
        return _parallel(lambda t: core.finish.original_func(core, t.jeditaskid, soft=soft), core.get_active_tasks())
    elif isinstance(ids, list):
        return _parallel(lambda tid: core.finish(tid, soft=soft), ids)
    return core.finish(ids, soft=soft)


@app.command()
def retry(
    task_ids: Annotated[str, typer.Argument(help="Task ID, comma-separated IDs, or 'all'")],
    days: Annotated[int, typer.Option("--days", help="Look-back window when task_ids='all'")] = 14,
    limit: Annotated[int, typer.Option("--limit", help="Max tasks to retry when task_ids='all'")] = 1000,
    site: Annotated[Optional[str], typer.Option("--site", help="Run the task on a particular PanDA queue")] = None,
    excludedSite: Annotated[Optional[str], typer.Option("--excludedSite", help="Comma separated list of PanDA queues to exclude, e.g. 'siteA,siteB'")] = None,
    includedSite: Annotated[Optional[str], typer.Option("--includedSite", help="Comma separated list of PanDA queues to include, e.g. 'siteA,siteB'")] = None,
    nFilesPerJob: Annotated[Optional[int], typer.Option("--nFilesPerJob", help="Number of files on which each sub-job runs (default 50)")] = None,
    nMaxFilesPerJob: Annotated[
        Optional[int],
        typer.Option(
            "--nMaxFilesPerJob",
            "--maxNFilesPerJob",
            help="Maximum number of input files to be processed by a single job in the task.",
        ),
    ] = None,
    nGBPerJob: Annotated[
        Optional[float],
        typer.Option("--nGBPerJob", help="Maximum input size in GB to be processed by a single job in the task.."),
    ] = None,
    nFiles: Annotated[Optional[int], typer.Option("--nFiles", help="Total number of input files to be processed by the task.")] = None,
    nEvents: Annotated[
        Optional[int],
        typer.Option("--nEvents", help="Total number of events to be processed by the task."),
    ] = None,
    loopingCheck: Annotated[
        Optional[bool],
        typer.Option("--loopingCheck", help="Enable (True) or disable (False) the automatic check that kills jobs suspected of being stuck in a loop"),
    ] = None,
    memory: Annotated[Optional[int], typer.Option("--memory", "--ramCount", help="Required memory size in MB per core")] = None,
    avoidVP: Annotated[Optional[bool], typer.Option("--avoidVP", help="Avoid PanDA queues which use Virtual Placement")] = None,
    ignoreMissingInDS: Annotated[
        Optional[bool], typer.Option("--ignoreMissingInDS", help="Ignore missing input datasets which were deleted after the task is submitted.")
    ] = None,
    forceStaged: Annotated[
        Optional[bool],
        typer.Option("--forceStaged", help="Force files from the primary dataset to be staged to local disk instead of using direct access"),
    ] = None,
    maxCore: Annotated[Optional[int], typer.Option("--maxCore", help="Maximum number of CPU cores that a single job is allowed to utilize")] = None,
    newOpts: Annotated[Optional[str], typer.Option("--new-opts", hidden=True)] = None,
) -> None:
    """Retry failed/canceled tasks.

    Retry failed/canceled subJobs in task_ids (ID or a list of IDs, can be either jediTaskID
    or reqID). You can specify options (site, excludedSite,...) to overwrite task parameters for new attempts.
    If input files were used or are being used by other jobs for the same output dataset container, those
    files are skipped to avoid job duplication when retrying failed subjobs.

    If task_ids is 'all', it retries 1000 tasks at most that have finished for the last 14
    days. It is possible to retry more tasks by setting the days and limit options. If
    named arguments are specified, they are applied to all retried tasks.

    In interactive mode, newOpts can be passed with a raw dict of task-retry options and it overrides
    all of the individual options above. newOpts is not supported from the shell CLI.

    example:
      >>> retry(123)
      >>> retry([123, 345, 567])
      >>> retry(789, newOpts={'excludedSite': 'siteA,siteB'})
      >>> retry(789, excludedSite='siteA,siteB')
      >>> retry('all')
      >>> retry('all', days=30, limit=2000)
      >>> retry('all', newOpts={'excludedSite': 'siteA,siteB'})

      $ pbook retry 123
      $ pbook retry 123,345,567 --excludedSite=siteA,siteB
      $ pbook retry all --days=30 --limit=2000

    """
    core = _ensure_init()
    if newOpts is not None:
        opts = newOpts
    else:
        new_opts = {
            k: v
            for k, v in {
                "site": site,
                "excludedSite": excludedSite,
                "includedSite": includedSite,
                "nFilesPerJob": nFilesPerJob,
                "nMaxFilesPerJob": nMaxFilesPerJob,
                "nGBPerJob": nGBPerJob,
                "nFiles": nFiles,
                "nEvents": nEvents,
                "loopingCheck": loopingCheck,
                "ramCount": memory,
                "avoidVP": avoidVP,
                "ignoreMissingInDS": ignoreMissingInDS,
                "forceStaged": forceStaged,
                "maxCoreCount": maxCore,
            }.items()
            if v is not None
        }
        opts = new_opts or None
    ids = _parse_ids(task_ids)
    if isinstance(ids, list):
        return _parallel(lambda tid: core.retry(tid, newOpts=opts), ids)
    elif ids == "all":
        data = core.show(status="finished", days=days, limit=limit, format="json")
        return _parallel(lambda d: core.retry.original_func(core, d["jediTaskID"], newOpts=opts), data)
    return core.retry(ids, newOpts=opts)


@app.command()
def debug(
    panda_id: Annotated[int, typer.Argument(help="PanDA subjob ID")],
    mode_on: Annotated[bool, typer.Argument(help="True to enable, False to disable")],
) -> None:
    """Toggle debug mode for a subjob.

    mode_on is True/False to enable/disable the debug mode. Note that the maximum number of
    debug subjobs is limited. If you already hit the limit you need to disable the debug mode
    for a subjob before debugging another subjob.

    example:
      >>> debug(1234, True)

      $ pbook debug 1234 True
    """
    mode_on = _require_bool("mode_on", mode_on)
    if mode_on is _INVALID:
        return
    core = _ensure_init()
    core.debug(panda_id, mode_on)


@app.command(name="get_user_job_metadata")
def get_user_job_metadata(
    task_id: Annotated[int, typer.Argument(help="Task ID")],
    output_file: Annotated[str, typer.Argument(help="Output JSON file path")],
) -> None:
    """Write user metadata of successful jobs to a JSON file.

    Get user metadata of successful jobs in a task and write them locally to a JSON file.

    example:
      >>> get_user_job_metadata(123, 'output.json')

      $ pbook get_user_job_metadata 123 output.json
    """
    core = _ensure_init()
    core.getUserJobMetadata(task_id, output_file)


@app.command(name="reload_input")
def reload_input(
    task_id: Annotated[int, typer.Argument(help="Task ID")],
) -> None:
    """Reload input dataset and retry the task with new contents.

    This is useful when input dataset contents are changed after the task is submitted.

    example:
      >>> reload_input(123)

      $ pbook reload_input 123
    """
    core = _ensure_init()
    core.reload_input(task_id)


@app.command(name="recover_lost_files")
def recover_lost_files(
    task_id: Annotated[int, typer.Argument(help="Task ID")],
    test_mode: Annotated[bool, typer.Option("--test_mode", help="Dry-run mode")] = False,
) -> None:
    """Request recovery of lost files from a task.

    Send a request to recover lost files produced by a task. Use test_mode for testing.

    example:
      >>> recover_lost_files(123)
      >>> recover_lost_files(123, test_mode=True)

      $ pbook recover_lost_files 123
      $ pbook recover_lost_files 123 --test_mode
    """
    test_mode = _require_bool("test_mode", test_mode)
    if test_mode is _INVALID:
        return
    core = _ensure_init()
    core.recover_lost_files(task_id, test_mode)


@app.command(name="show_workflow")
def show_workflow(
    request_id: Annotated[int, typer.Argument(help="Workflow request ID")],
) -> None:
    """Show workflow status.

    Send a request to show the status of a workflow.

    example:
      >>> show_workflow(456)

      $ pbook show_workflow 456
    """
    core = _ensure_init()
    _, output = core.execute_workflow_command("get_status", request_id)
    if output:
        print(output)


@app.command(name="kill_workflow")
def kill_workflow(
    request_id: Annotated[int, typer.Argument(help="Workflow request ID")],
) -> None:
    """Kill a workflow.

    Send a request to kill a workflow.

    example:
      >>> kill_workflow(456)

      $ pbook kill_workflow 456
    """
    core = _ensure_init()
    _, output = core.execute_workflow_command("abort", request_id)
    if output:
        print(output[0][-1])


@app.command(name="retry_workflow")
def retry_workflow(
    request_id: Annotated[int, typer.Argument(help="Workflow request ID")],
) -> None:
    """Retry a workflow.

    Send a request to retry a workflow.

    example:
      >>> retry_workflow(456)

      $ pbook retry_workflow 456
    """
    core = _ensure_init()
    _, output = core.execute_workflow_command("retry", request_id)
    if output:
        print(output[0][-1])


@app.command(name="finish_workflow")
def finish_workflow(
    request_id: Annotated[int, typer.Argument(help="Workflow request ID")],
) -> None:
    """Finish a workflow.

    Send a request to finish a workflow.

    example:
      >>> finish_workflow(456)

      $ pbook finish_workflow 456
    """
    core = _ensure_init()
    _, output = core.execute_workflow_command("finish", request_id)
    if output:
        print(output[0][-1])


@app.command(name="pause_workflow")
def pause_workflow(
    request_id: Annotated[int, typer.Argument(help="Workflow request ID")],
) -> None:
    """Pause a workflow.

    Send a request to pause a workflow.

    example:
      >>> pause_workflow(456)

      $ pbook pause_workflow 456
    """
    core = _ensure_init()
    _, output = core.execute_workflow_command("suspend", request_id)
    if output:
        print(output[0][-1])


@app.command(name="resume_workflow")
def resume_workflow(
    request_id: Annotated[int, typer.Argument(help="Workflow request ID")],
) -> None:
    """Resume a workflow.

    Send a request to resume a workflow.

    example:
      >>> resume_workflow(456)

      $ pbook resume_workflow 456
    """
    core = _ensure_init()
    _, output = core.execute_workflow_command("resume", request_id)
    if output:
        print(output[0][-1])


@app.command(name="set_secret")
def set_secret(
    key: Annotated[str, typer.Argument(help="Secret key")],
    value: Annotated[str, typer.Argument(help="Secret value or file path")],
    is_file: Annotated[bool, typer.Option("--is_file", help="Treat value as a file path to upload")] = False,
) -> None:
    """Set a secret key-value pair.

    Define a pair of secret key-value strings. The value can be a file path to upload a
    secret file when is_file=True.

    example:
      >>> set_secret('mykey', 'myvalue')
      >>> set_secret('mykey', '/path/to/file', is_file=True)

      $ pbook set_secret mykey myvalue
      $ pbook set_secret mykey /path/to/file --is_file
    """
    is_file = _require_bool("is_file", is_file)
    if is_file is _INVALID:
        return
    core = _ensure_init()
    core.set_secret(key, value, is_file)


@app.command(name="delete_secret")
def delete_secret(
    key: Annotated[str, typer.Argument(help="Secret key to delete")],
) -> None:
    """Delete a secret.

    example:
      >>> delete_secret('mykey')

      $ pbook delete_secret mykey
    """
    core = _ensure_init()
    core.set_secret(key, None)


@app.command(name="delete_all_secrets")
def delete_all_secrets() -> None:
    """Delete all secrets.

    example:
      >>> delete_all_secrets

      $ pbook delete_all_secrets
    """
    core = _ensure_init()
    core.set_secret(None, None)


@app.command(name="list_secrets")
def list_secrets(
    full: Annotated[bool, typer.Option("--full", help="Show full secret values")] = False,
) -> None:
    """List secrets.

    Value strings are truncated by default. full=True to see entire strings.

    example:
      >>> list_secrets()
      >>> list_secrets(full=True)

      $ pbook list_secrets
      $ pbook list_secrets --full
    """
    full = _require_bool("full", full)
    if full is _INVALID:
        return
    core = _ensure_init()
    core.list_secrets(full)


@app.command(name="generate_credential")
def generate_credential() -> None:
    """Generate a new proxy or token."""
    core = _get_core()
    core.generate_credential()


# ─── Entry point ──────────────────────────────────────────────────────────────

# Global options that consume the following argv token as their own value, so the
# subcommand-token scan below can skip over both.
_GLOBAL_VALUE_OPTS = {"-c"}


def _rewrite_legacy_kwargs(argv: list) -> list:
    """Rewrite legacy bare `key=value` batch args into `--key=value`.

    The pre-Typer pbook batch mode accepted `pbook show format=long`; Click only
    recognizes `--format=long`. Find the subcommand, look up its real option names via
    introspection (never a separately maintained list), and rewrite any later bare
    `key=value` token whose key matches one of them. Anything else - already-dashed
    flags, positional args that happen to contain "=" - passes through untouched.
    """
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok in _GLOBAL_VALUE_OPTS:
            i += 2
            continue
        if tok.startswith("-"):
            i += 1
            continue
        break
    if i >= len(argv):
        return argv

    sub_cmd = typer.main.get_command(app).commands.get(argv[i])
    if sub_cmd is None:
        return argv

    option_flags = {}
    flag_only = set()
    for param in sub_cmd.params:
        flags = [o for o in getattr(param, "opts", []) if o.startswith("--")]
        if flags:
            primary = flags[0]
            # Register every alias (e.g. --ramCount for --memory), not just the primary
            # name, so the legacy bare `key=value` syntax recognizes them too.
            for flag in flags:
                option_flags[flag.lstrip("-")] = primary
            if getattr(param, "is_flag", False):
                flag_only.add(param.name)

    rewritten = argv[: i + 1]
    for tok in argv[i + 1 :]:
        key, sep, value = tok.partition("=")
        if sep and not tok.startswith("-") and key in option_flags:
            flag = option_flags[key]
            if key in flag_only:
                # Click flag-style options (e.g. --soft) take no value at all; the legacy
                # syntax passed an explicit True/False, so translate that into presence
                # (truthy) or absence (falsy - same as the option's own default) instead.
                normalized = value.strip().lower()
                if normalized in ("true", "1", "yes"):
                    rewritten.append(flag)
                elif normalized not in ("false", "0", "no"):
                    typer.echo(
                        f"Error: '{key}' is a flag and expects True/False (got '{value}' in '{tok}')",
                        err=True,
                    )
                    sys.exit(1)
                continue
            rewritten.append(f"{flag}={value}")
        else:
            rewritten.append(tok)
    return rewritten


def main() -> None:
    sys.argv[0] = "pbook"
    sys.argv[1:] = _rewrite_legacy_kwargs(sys.argv[1:])
    app()
