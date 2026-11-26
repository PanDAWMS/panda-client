"""
Do NOT import this module in your code.
Import PBookCore instead.

pbook CLI — PanDA task bookkeeper.

Each command below is defined exactly once, as a plain function registered with
@command() and using the Annotated[...] parameter style. Because the command-line
metadata (Opt/Arg) lives in the annotation rather than in the default value, these
functions remain ordinary callables with ordinary defaults - the same function is
used to build the `pbook <command> --flag ...` CLI (argparse, plus bash completion)
*and* is placed directly into the interactive REPL namespace (`>>> command(...)`).
"""

# No `from __future__ import annotations` here: it turns the annotations into strings that
# get_type_hints resolves, and on Python <= 3.10 that wraps an option defaulting to None in an
# extra Optional around its Annotated[..., Opt(...)]. _cli_info and _union_members cope with
# that, but there is no reason to make every annotation go through it.

import argparse
import atexit
import code
import functools
import inspect
import os
import re
import readline
import rlcompleter
import shlex
import signal
import sys
import tempfile
import types
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

If arg or value is a list in interactive mode, it is represented as a comma-separated list in batch mode. E.g.
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

# ─── Command registry ─────────────────────────────────────────────────────────


class Opt:
    """Command-line metadata for an option, kept in Annotated[...] so the command stays a plain function.

    flags are the option names; extra names are aliases (Opt("--memory", "--ramCount")). A single
    "--name/--no-name" makes an on/off switch whose value is None when neither is given. Without
    flags, the option is --<parameter name> with underscores turned into dashes. min and max bound
    a number (inclusive); they are checked on the command line and, through _check_declared_value,
    for values passed from Python.
    """

    def __init__(
        self,
        *flags: str,
        help: Optional[str] = None,
        hidden: bool = False,
        min: Optional[float] = None,
        max: Optional[float] = None,
    ) -> None:
        self.flags = flags
        self.help = help
        self.hidden = hidden
        self.min = min
        self.max = max


class Arg:
    """Command-line metadata for a positional argument, kept in Annotated[...] like Opt."""

    def __init__(self, help: Optional[str] = None) -> None:
        self.help = help
        self.hidden = False


# command name -> function, in definition order (the order `pbook --help` lists them in)
_COMMANDS: dict = {}


def command(name: Optional[str] = None):
    """Register a function as a pbook command. The function itself is returned unchanged."""

    def register(func):
        _COMMANDS[name or func.__name__] = func
        return func

    return register


# ─── Utilities ────────────────────────────────────────────────────────────────


def _parallel(func, items):
    """
    Parallel execution of func with items in a pool of 8 threads.
    """
    with ThreadPoolExecutor(8) as pool:
        return list(pool.map(func, items))


class _CommandFailed(Exception):
    """A command stopped on an error it has already reported.

    Batch mode and -c exit with status 1 on it; the interactive console just goes on to the next
    prompt. Commands raise this rather than calling sys.exit(), which would end the whole console
    session: InteractiveConsole deliberately lets SystemExit through.
    """


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
        print(f"Error: invalid task ID(s): {raw}", file=sys.stderr)
        raise _CommandFailed


_INVALID = object()


def _require_bool(name: str, value):
    """Report non-bool values for a bool parameter, without raising.

    The parser enforces this on the CLI path (a flag is present or absent, never a stray
    string) - unreachable from real CLI dispatch. Commands are also called directly
    from the REPL though, where a plain Python call - e.g. finish(123, soft='gfd') -
    bypasses that check entirely and would otherwise silently treat any truthy string
    as on. Raising here would just dump a traceback into the interactive session, so
    report the problem and let the caller return early instead: `if soft is _INVALID:
    return`.
    """
    if not isinstance(value, bool):
        print(f"Error: '{name}' must be True or False, got {value!r}", file=sys.stderr)
        return _INVALID
    return value


@functools.lru_cache(maxsize=None)
def _cli_params(command_name: str) -> dict:
    """The command-line description of a command's parameters (_ParamSpec), keyed by Python parameter name.

    They carry what the declaration says about each parameter - its type, its range (min/max),
    its choices and its option names including aliases - so values that don't come through the
    CLI can be checked against the same declaration.
    """
    return {spec.name: spec for spec in _param_specs(_COMMANDS[command_name])}


def _declared_type(func, name: str):
    """The type a parameter of func is declared with, without Annotated[...] and Optional[...]; None if unknown."""
    try:
        hints = get_type_hints(func, include_extras=True)
    except Exception:
        return None
    ann = hints.get(name)
    if ann is None:
        return None
    # On Python <= 3.10 a parameter defaulting to None comes back as Optional[Annotated[Optional[X], ...]]:
    # _union_members flattens that to the same members as on newer Pythons
    ann = _strip_annotated(ann)
    if _is_union(ann):
        non_none = [a for a in _union_members(ann) if a is not type(None)]
        if len(non_none) == 1:
            ann = non_none[0]
    return ann


# Checks for the Python type a value passed from Python must have, per declared type, with how to
# name the type in an error message. bool is excluded from int and float explicitly since it is a
# subclass of int.
_DECLARED_PYTHON_TYPES = {
    bool: (lambda v: isinstance(v, bool), "True or False"),
    int: (lambda v: isinstance(v, int) and not isinstance(v, bool), "an integer"),
    float: (lambda v: isinstance(v, (int, float)) and not isinstance(v, bool), "a number"),
    str: (lambda v: isinstance(v, str), "a string"),
}


def _check_declared_value(func, param, name: str, value) -> Optional[str]:
    """Error message when a value passed from Python doesn't fit the declaration of a parameter of func, else None.

    param is its _ParamSpec (see _cli_params). The command-line parser checks values coming through
    the CLI, but a plain Python call from the REPL, or a value inside a dict like retry's newOpts,
    bypasses it. The parser's conversion can't simply be reused for these: it starts from strings
    (and would turn 'yes' into True for a flag), so the Python type is checked strictly here
    against the type hint, then the range and choices the declaration gives.
    name is how the caller referred to the parameter, used in the message.
    """
    check = _DECLARED_PYTHON_TYPES.get(_declared_type(func, param.name))
    if check is not None:
        is_valid, description = check
        if not is_valid(value):
            return f"'{name}' must be {description}, got {value!r}."
    error = param.value_error(value)
    if error:
        return f"Invalid value for '{name}': {error}"
    return None


def _setup() -> None:
    global _tmp_dir, _history_file, _setup_done
    if _setup_done:
        return
    _setup_done = True

    readline.parse_and_bind("tab: complete")
    readline.parse_and_bind("set show-all-if-ambiguous On")

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
    """The REPL namespace: every registered command, keyed by its real Python name."""
    return {func.__name__: func for func in _COMMANDS.values()}


def _kwarg_names(func) -> list:
    """All parameter names a function accepts - candidates for `name=` completion."""
    return list(signature(func).parameters)


def _kwarg_choices(func, name: str) -> list:
    """Value choices for a parameter, derived from its type hint: Literal[...] members or True/False for bool.

    Returned bare (unquoted) - readline's own quote-matching auto-closes an opening quote
    the user already typed, so we don't need to (and shouldn't try to) add one ourselves.
    """
    ann = _declared_type(func, name)
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


class _Console(code.InteractiveConsole):
    """InteractiveConsole that stays quiet about _CommandFailed: the command already reported its error."""

    def runcode(self, code) -> None:
        # same as InteractiveInterpreter.runcode (self.locals is the dict passed in), plus the _CommandFailed case
        try:
            exec(code, self.locals)  # noqa: S102  # pyright: ignore[reportArgumentType]
        except SystemExit:
            raise
        except _CommandFailed:
            pass
        except BaseException:
            self.showtraceback()


def _run_repl(ns: dict, banner: str) -> None:
    """Manual REPL using InteractiveConsole.push() so we own readline setup entirely."""
    completer = _PBookCompleter(ns)
    readline.set_completer(completer.complete)
    readline.parse_and_bind("tab: complete")
    readline.parse_and_bind("set show-all-if-ambiguous On")

    console = _Console(ns)
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


# ─── Top-level options ────────────────────────────────────────────────────────

# (flags, dest, help); help None = hidden. All are on/off switches except -c, which takes a value.
_GLOBAL_OPTIONS = [
    (("-v",), "verbose", "Verbose"),
    (("-c",), "command_string", "Execute a Python code snippet"),
    (("--version",), "version", "Display version"),
    (("--devSrv",), "dev_srv", None),
    (("--intrSrv",), "intr_srv", None),
    (("--prompt_with_newline",), "prompt_with_newline", None),
    (("-3",), "python3", None),
]


def _main(args: argparse.Namespace) -> None:
    """Run what the parsed command line asks for: a command (batch mode), a -c snippet, or the REPL."""
    if args.version:
        print(f"Version: {PandaToolsPkgInfo.release_version}")
        sys.exit(0)

    if args.dev_srv:
        Client.useDevServer()
    if args.intr_srv:
        Client.useIntrServer()

    verbose = args.verbose
    _ctx_state.update({"verbose": verbose})

    func = getattr(args, "_func", None)
    if func is not None:
        try:
            func(**{name: getattr(args, name) for name in signature(func).parameters})
        except _CommandFailed:
            sys.exit(1)
        return

    command_string = args.command_string
    prompt_with_newline = args.prompt_with_newline

    # Interactive or snippet mode
    _setup()
    global _fork_child_pid
    _fork_child_pid = os.fork()

    if _fork_child_pid == -1:
        print("ERROR: Failed to fork", file=sys.stderr)
        sys.exit(1)

    if _fork_child_pid == 0:
        if verbose:
            print(str({dest: getattr(args, dest) for _, dest, _ in _GLOBAL_OPTIONS}))
        if prompt_with_newline:
            sys.ps1 = ">>> \n"
        _ensure_init(sanity_check=True)
        ns = _build_namespace()

        if command_string:
            try:
                exec(command_string, {}, ns)  # noqa: S102
            except _CommandFailed:
                sys.exit(1)
            from pandaclient import PBookCore as _PBC

            sys.exit(0 if _PBC.func_return_value else 1)
        _run_repl(ns, banner=f"\nStart pBook {PandaToolsPkgInfo.release_version}")

    else:
        signal.signal(signal.SIGINT, _catch_sig)
        signal.signal(signal.SIGHUP, _catch_sig)
        signal.signal(signal.SIGTERM, _catch_sig)
        pid, status = os.wait()
        if os.WIFSIGNALED(status):
            sys.exit(-os.WTERMSIG(status))
        if os.WIFEXITED(status):
            sys.exit(os.WEXITSTATUS(status))
        sys.exit(0)


# ─── Commands ──────────────────────────────────────────────────────────────────

_HELP_GROUPS = [
    ("Tasks", ["show", "showl", "kill", "finish", "retry", "debug"]),
    ("Files & input", ["get_user_job_metadata", "recover_lost_files", "reload_input"]),
    ("Workflows", ["show_workflow", "show_native_workflow", "kill_workflow", "retry_workflow", "finish_workflow", "pause_workflow", "resume_workflow"]),
    ("Secrets", ["set_secret", "list_secrets", "delete_secret", "delete_all_secrets"]),
    ("Auth", ["generate_credential"]),
]


# `X | None` is a types.UnionType rather than a typing.Union on Python 3.10-3.13
_UnionType = getattr(types, "UnionType", None)


def _strip_annotated(ann):
    """Annotated[X, ...] -> X, the Typer metadata removed."""
    while get_origin(ann) is Annotated:
        ann = get_args(ann)[0]
    return ann


def _is_union(ann) -> bool:
    origin = get_origin(ann)
    return origin is Union or (_UnionType is not None and origin is _UnionType)


def _union_members(ann) -> list:
    """The members of a union, flattened and deduplicated, with Annotated stripped from each.

    On Python <= 3.10 get_type_hints wraps a parameter defaulting to None in an extra
    Optional, outside the Annotated: `site: Annotated[Optional[str], Opt()] = None`
    comes back as Optional[Annotated[Optional[str], OptionInfo]]. Flattening turns that into
    the same (str, NoneType) as on newer Pythons.
    """
    members = []
    for arg in get_args(ann):
        arg = _strip_annotated(arg)
        for member in _union_members(arg) if _is_union(arg) else [arg]:
            if member not in members:
                members.append(member)
    return members


def _type_name(ann) -> str:
    """Render a resolved type annotation as a short, human-readable name (Optional[str], Literal[...], etc.)."""
    ann = _strip_annotated(ann)
    if ann is None or ann is type(None):
        return ""
    origin = get_origin(ann)
    if _is_union(ann):
        args = _union_members(ann)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1 and len(args) == 2:
            return f"Optional[{_type_name(non_none[0])}]"
        return " | ".join(_type_name(a) for a in args)
    if origin is Literal:
        return f"Literal[{', '.join(repr(v) for v in get_args(ann))}]"
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
        piece = pname
        type_str = _type_name(hints.get(pname))
        if type_str:
            piece += f": {type_str}"
        if p.default is not Parameter.empty:
            piece += f" = {p.default!r}"
        parts.append(piece)
    return f"({', '.join(parts)})"


def _cli_info(ann):
    """The Opt/Arg metadata in an annotation, or None.

    It sits in Annotated[..., Opt(...)], which Python <= 3.10 nests inside an Optional
    (see _union_members), so unions are searched too.
    """
    if get_origin(ann) is Annotated:
        for meta in ann.__metadata__:
            if isinstance(meta, (Opt, Arg)):
                return meta
        return _cli_info(get_args(ann)[0])
    if _is_union(ann):
        for arg in get_args(ann):
            info = _cli_info(arg)
            if info is not None:
                return info
    return None


def _param_help(func) -> list:
    """(name, help) for each parameter with a help text, as `pbook <command> --help` shows them.

    Hidden parameters are left out, as they are from --help.
    """
    try:
        hints = get_type_hints(func, include_extras=True)
    except Exception:
        return []
    params = []
    for pname in signature(func).parameters:
        info = _cli_info(hints.get(pname))
        if info is not None and info.help and not getattr(info, "hidden", False):
            params.append((pname, info.help))
    return params


@command()
def help(
    command: Annotated[Optional[str], Arg(help="Command name for detailed help")] = None,
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
        params = _param_help(func)
        if params:
            console.print("[bold]Parameters:[/bold]")
            param_table = Table(box=None, show_header=False, padding=(0, 2, 0, 2))
            param_table.add_column(style="bold cyan", no_wrap=True)
            param_table.add_column()
            for pname, phelp in params:
                param_table.add_row(_esc(pname), _esc(phelp))
            console.print(param_table)
            console.print()
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


@command()
def show(
    task_id: Annotated[Optional[str], Arg(help="jediTaskID, reqID, 'run' (active only), or 'fin' (terminated only)")] = None,
    username: Annotated[Optional[str], Opt(help="Filter by username. By default, the name from the voms/token is used.")] = None,
    limit: Annotated[int, Opt(help="Maximum number of records")] = 1000,
    taskname: Annotated[Optional[str], Opt(help="Filter by task name")] = None,
    days: Annotated[int, Opt(help="Look back N days (capped at 90 without a task ID)")] = 14,
    jeditaskid: Annotated[Optional[int], Opt(help="Filter by jediTaskID")] = None,
    reqid: Annotated[Optional[int], Opt(help="Filter by reqID")] = None,
    status: Annotated[Optional[str], Opt(help="Filter by task status")] = None,
    superstatus: Annotated[Optional[str], Opt(help="Filter by super-status")] = None,
    format: Annotated[Literal["standard", "long", "json", "plain"], Opt("--format", help="Output format")] = "standard",
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


@command()
def showl(
    task_id: Annotated[Optional[str], Arg(help="jediTaskID, reqID, 'run', or 'fin'")] = None,
    username: Annotated[Optional[str], Opt(help="Filter by username")] = None,
    limit: Annotated[int, Opt(help="Maximum number of records")] = 1000,
    taskname: Annotated[Optional[str], Opt(help="Filter by task name")] = None,
    days: Annotated[int, Opt(help="Look back N days (capped at 90 without a task ID)")] = 14,
    jeditaskid: Annotated[Optional[int], Opt(help="Filter by jediTaskID")] = None,
    reqid: Annotated[Optional[int], Opt(help="Filter by reqID")] = None,
    status: Annotated[Optional[str], Opt(help="Filter by task status")] = None,
    superstatus: Annotated[Optional[str], Opt(help="Filter by super-status")] = None,
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


@command()
def kill(
    task_ids: Annotated[str, Arg(help="Task ID, comma-separated IDs, or 'all'")],
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
    if isinstance(ids, list):
        return _parallel(core.kill, ids)
    return core.kill(ids)


@command()
def finish(
    task_ids: Annotated[str, Arg(help="Task ID, comma-separated IDs, or 'all'")],
    soft: Annotated[bool, Opt("--soft", help="Wait for running jobs to finish instead of killing them")] = False,
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
    if isinstance(ids, list):
        return _parallel(lambda tid: core.finish(tid, soft=soft), ids)
    return core.finish(ids, soft=soft)


# The retry() parameters sent to the server as task parameters, mapped to the task parameter name
# the server expects for each. Everything else about them - type, range and option names - comes
# from their declarations in retry(). The server silently ignores task parameters a retry cannot
# change, so newOpts keys that don't name one of these are refused before the request is sent.
_RETRY_TASK_PARAMS = {
    "site": "site",
    "excludedSite": "excludedSite",
    "includedSite": "includedSite",
    "nFilesPerJob": "nFilesPerJob",
    "nMaxFilesPerJob": "nMaxFilesPerJob",
    "nGBPerJob": "nGBPerJob",
    "nFiles": "nFiles",
    "nEvents": "nEvents",
    "loopingCheck": "loopingCheck",
    "memory": "ramCount",
    "avoidVP": "avoidVP",
    "ignoreMissingInDS": "ignoreMissingInDS",
    "forceStaged": "forceStaged",
    "maxCore": "maxCoreCount",
}


def _retry_new_opts_keys() -> dict:
    """The keys newOpts accepts, mapped to the retry() parameter each one sets.

    A key is the Python name of a parameter in _RETRY_TASK_PARAMS or one of its declared option
    names without the leading dashes, so aliases like --maxNFilesPerJob or --ramCount are accepted
    as well. The negative --no-... forms of flags are not.
    """
    params = _cli_params("retry")
    keys = {}
    for name in _RETRY_TASK_PARAMS:
        keys[name] = name
        for flag in params[name].flags:
            keys[flag.lstrip("-")] = name
    return keys


def _report_invalid_retry_value(param: str, name: str, value) -> bool:
    """Report a value that doesn't fit the declaration of the retry() parameter param; True if reported.

    Values from the CLI were already checked by the parser. The same declarations apply to values from a
    plain Python call or from newOpts, which bypass it. name is how the caller referred to param.
    """
    error = _check_declared_value(retry, _cli_params("retry")[param], name, value)
    if error:
        print(f"Error: {error} Nothing was retried.", file=sys.stderr)
        return True
    return False


def _translate_retry_new_opts(new_opts):
    """newOpts as {retry() parameter: value}, or None (with the error reported) when it is not valid."""
    if not isinstance(new_opts, dict):
        print(
            f"Error: newOpts must be a dict of retry options, e.g. {{'excludedSite': 'siteA,siteB'}}, got {new_opts!r}. "
            "newOpts is only supported in interactive mode. Nothing was retried.",
            file=sys.stderr,
        )
        return None
    keys = _retry_new_opts_keys()
    invalid = [key for key in new_opts if key not in keys]
    if invalid:
        print(
            f"Error: invalid option(s) in newOpts: {', '.join(map(str, invalid))}. Nothing was retried. Valid options: {', '.join(keys)}.",
            file=sys.stderr,
        )
        return None
    translated = {}
    for key, value in new_opts.items():
        param = keys[key]
        if _report_invalid_retry_value(param, key, value):
            return None
        if param in translated and translated[param] != value:
            aliases = [k for k in new_opts if keys[k] == param]
            print(f"Error: newOpts sets {' and '.join(aliases)} to different values. Nothing was retried.", file=sys.stderr)
            return None
        translated[param] = value
    return translated


@command()
def retry(
    task_ids: Annotated[str, Arg(help="Task ID, comma-separated IDs, or 'all'")],
    days: Annotated[Optional[int], Opt("--days", help="Look-back window in days when task_ids='all' (default 14)")] = None,
    limit: Annotated[Optional[int], Opt("--limit", help="Max tasks to retry when task_ids='all' (default 1000)")] = None,
    site: Annotated[Optional[str], Opt("--site", help="Run the task on a particular PanDA queue")] = None,
    excludedSite: Annotated[Optional[str], Opt("--excludedSite", help="Comma separated list of PanDA queues to exclude, e.g. 'siteA,siteB'")] = None,
    includedSite: Annotated[Optional[str], Opt("--includedSite", help="Comma separated list of PanDA queues to include, e.g. 'siteA,siteB'")] = None,
    nFilesPerJob: Annotated[Optional[int], Opt("--nFilesPerJob", help="Number of files on which each sub-job runs (default 50)")] = None,
    nMaxFilesPerJob: Annotated[
        Optional[int],
        Opt(
            "--nMaxFilesPerJob",
            "--maxNFilesPerJob",
            help="Maximum number of input files to be processed by a single job in the task.",
        ),
    ] = None,
    nGBPerJob: Annotated[
        Optional[int],
        Opt("--nGBPerJob", min=1, help="Maximum input size in GB to be processed by a single job in the task."),
    ] = None,
    nFiles: Annotated[Optional[int], Opt("--nFiles", help="Total number of input files to be processed by the task.")] = None,
    nEvents: Annotated[
        Optional[int],
        Opt("--nEvents", help="Total number of events to be processed by the task."),
    ] = None,
    memory: Annotated[Optional[int], Opt("--memory", "--ramCount", help="Required memory size in MB per core")] = None,
    maxCore: Annotated[Optional[int], Opt("--maxCore", help="Maximum number of CPU cores that a single job is allowed to utilize")] = None,
    loopingCheck: Annotated[
        Optional[bool],
        Opt(
            "--loopingCheck/--no-loopingCheck",
            help="Enable/disable the automatic check that kills jobs suspected of being stuck in a loop",
        ),
    ] = None,
    avoidVP: Annotated[Optional[bool], Opt("--avoidVP/--no-avoidVP", help="Avoid PanDA queues which use Virtual Placement")] = None,
    ignoreMissingInDS: Annotated[
        Optional[bool],
        Opt(
            "--ignoreMissingInDS/--no-ignoreMissingInDS",
            help="Ignore missing input datasets which were deleted after the task is submitted.",
        ),
    ] = None,
    forceStaged: Annotated[
        Optional[bool],
        Opt(
            "--forceStaged/--no-forceStaged",
            help="Force files from the primary dataset to be staged to local disk instead of using direct access",
        ),
    ] = None,
    newOpts: Annotated[
        Optional[str],
        Opt(
            "--new-opts",
            help="Interactive mode only: raw dict of task-retry options, e.g. {'excludedSite': 'siteA,siteB'}, "
            "overriding the individual options. Not supported from the shell CLI.",
        ),
    ] = None,
) -> None:
    """Retry failed/canceled tasks.

    Retry failed/canceled subJobs in task_ids (ID or a list of IDs, can be either jediTaskID
    or reqID). You can specify options (site, excludedSite,...) to overwrite task parameters for new attempts.
    If input files were used or are being used by other jobs for the same output dataset container, those
    files are skipped to avoid job duplication when retrying failed subjobs.

    If a task is still running, scouting or pending, only the input files that have used up their
    attempts get 3 more attempts, and the task keeps running. Files that still have attempts left
    are not changed, so retrying a running task several times doesn't keep raising their limits.

    If task_ids is 'all', it retries 1000 tasks at most that have finished for the last 14
    days. It is possible to retry more tasks by setting the days and limit options, which are
    only valid with 'all'. If named arguments are specified, they are applied to all retried tasks.

    In interactive mode, newOpts can be passed with a raw dict of task-retry options and it overrides
    all of the individual options above. newOpts is not supported from the shell CLI. Its keys are
    the names of the individual options (site, excludedSite, ..., memory or ramCount, maxCore); any
    other key is reported as an error and nothing is retried. Its values must have the same types and
    ranges as the individual options, e.g. a positive integer for nGBPerJob.

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
    # must stay the first statement: the parameters as passed (REPL) or parsed (CLI), nothing else
    args = dict(locals())
    ids = _parse_ids(task_ids)
    for name, value in (("days", days), ("limit", limit)):
        if value is None:
            continue
        if isinstance(value, dict):
            print(f"Error: '{name}' got a dict; pass retry options by name, e.g. newOpts={value!r}. Nothing was retried.", file=sys.stderr)
            return
        if isinstance(value, bool) or not isinstance(value, int):
            print(f"Error: '{name}' must be an integer, got {value!r}. Nothing was retried.", file=sys.stderr)
            return
        if ids != "all":
            print(f"Error: '{name}' is only valid when task_ids is 'all'. Nothing was retried.", file=sys.stderr)
            return
    days = 14 if days is None else days
    limit = 1000 if limit is None else limit
    if newOpts is not None:
        values = _translate_retry_new_opts(newOpts)
        if values is None:
            return
    else:
        values = {name: args[name] for name in _RETRY_TASK_PARAMS if args[name] is not None}
        for name, value in values.items():
            if _report_invalid_retry_value(name, name, value):
                return
    opts = {_RETRY_TASK_PARAMS[name]: value for name, value in values.items()} or None
    core = _ensure_init()
    if isinstance(ids, list):
        return _parallel(lambda tid: core.retry(tid, newOpts=opts), ids)
    if ids == "all":
        data = core.show(status="finished", days=days, limit=limit, format="json")
        return _parallel(lambda d: core.retry.original_func(core, d["jediTaskID"], newOpts=opts), data)
    return core.retry(ids, newOpts=opts)


@command()
def debug(
    panda_id: Annotated[int, Arg(help="PanDA subjob ID")],
    mode_on: Annotated[bool, Arg(help="True to enable, False to disable")],
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


@command(name="get_user_job_metadata")
def get_user_job_metadata(
    task_id: Annotated[int, Arg(help="Task ID")],
    output_file: Annotated[str, Arg(help="Output JSON file path")],
) -> None:
    """Write user metadata of successful jobs to a JSON file.

    Get user metadata of successful jobs in a task and write them locally to a JSON file.

    example:
      >>> get_user_job_metadata(123, 'output.json')

      $ pbook get_user_job_metadata 123 output.json
    """
    core = _ensure_init()
    core.getUserJobMetadata(task_id, output_file)


@command(name="reload_input")
def reload_input(
    task_id: Annotated[int, Arg(help="Task ID")],
) -> None:
    """Reload input dataset and retry the task with new contents.

    This is useful when input dataset contents are changed after the task is submitted.

    example:
      >>> reload_input(123)

      $ pbook reload_input 123
    """
    core = _ensure_init()
    core.reload_input(task_id)


@command(name="recover_lost_files")
def recover_lost_files(
    task_id: Annotated[int, Arg(help="Task ID")],
    test_mode: Annotated[bool, Opt("--test_mode", help="Dry-run mode")] = False,
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


@command(name="show_workflow")
def show_workflow(
    request_id: Annotated[int, Arg(help="Workflow request ID")],
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


@command(name="show_native_workflow")
def show_native_workflow(
    workflow_id: Annotated[int, Arg(help="Native workflow ID")],
    format: Annotated[Literal["standard", "long", "json", "plain"], Opt("--format", help="Output format")] = "standard",
    show_data: Annotated[bool, Opt("--show_data", help="Also show the data passing between the steps")] = False,
) -> None:
    """Show a PanDA native workflow.

    Print the workflow with its steps and the data passing between them. Each step shows which
    steps feed it, the task it runs and how far that task has got, as a percentage of the input
    files finished. A step whose target is not a task, or which has not been submitted yet, has no
    progress to report. The data passing between the steps is counted but not listed unless
    show_data is given, since the dataset names are long. Note that this takes a native workflow
    ID, unlike show_workflow, which takes an iDDS request ID.

    example:
      >>> show_native_workflow(136)
      >>> show_native_workflow(136, format='long')
      >>> show_native_workflow(136, show_data=True)

      $ pbook show_native_workflow 136 --format=json
      $ pbook show_native_workflow 136 --show_data
    """
    show_data = _require_bool("show_data", show_data)
    if show_data is _INVALID:
        return
    core = _ensure_init()
    core.show_native_workflow(workflow_id, format=format, show_data=show_data)


@command(name="kill_workflow")
def kill_workflow(
    request_id: Annotated[int, Arg(help="Workflow request ID")],
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


@command(name="retry_workflow")
def retry_workflow(
    request_id: Annotated[int, Arg(help="Workflow request ID")],
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


@command(name="finish_workflow")
def finish_workflow(
    request_id: Annotated[int, Arg(help="Workflow request ID")],
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


@command(name="pause_workflow")
def pause_workflow(
    request_id: Annotated[int, Arg(help="Workflow request ID")],
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


@command(name="resume_workflow")
def resume_workflow(
    request_id: Annotated[int, Arg(help="Workflow request ID")],
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


@command(name="set_secret")
def set_secret(
    key: Annotated[str, Arg(help="Secret key")],
    value: Annotated[str, Arg(help="Secret value or file path")],
    is_file: Annotated[bool, Opt("--is_file", help="Treat value as a file path to upload")] = False,
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


@command(name="delete_secret")
def delete_secret(
    key: Annotated[str, Arg(help="Secret key to delete")],
) -> None:
    """Delete a secret.

    example:
      >>> delete_secret('mykey')

      $ pbook delete_secret mykey
    """
    core = _ensure_init()
    core.set_secret(key, None)


@command(name="delete_all_secrets")
def delete_all_secrets() -> None:
    """Delete all secrets.

    example:
      >>> delete_all_secrets()

      $ pbook delete_all_secrets
    """
    core = _ensure_init()
    core.set_secret(None, None)


@command(name="list_secrets")
def list_secrets(
    full: Annotated[bool, Opt("--full", help="Show complete secret values instead of truncating them")] = False,
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


@command(name="generate_credential")
def generate_credential() -> None:
    """Generate a new proxy or token."""
    core = _get_core()
    core.generate_credential()


# ─── Command line ─────────────────────────────────────────────────────────────

# The strings click/typer accepted for a bool positional (pbook debug 1234 yes), kept so the same
# command lines keep working; matched after strip() and lower().
_BOOL_STATES = {
    "1": True,
    "0": False,
    "yes": True,
    "no": False,
    "true": True,
    "false": False,
    "on": True,
    "off": False,
    "t": True,
    "f": False,
    "y": True,
    "n": False,
    "": False,
}


def _parse_bool(value: str) -> bool:
    try:
        return _BOOL_STATES[value.strip().lower()]
    except KeyError:
        raise argparse.ArgumentTypeError(f"{value!r} is not a valid boolean")


class _ParamSpec:
    """How one command parameter appears on the command line, derived from its signature and Opt/Arg."""

    def __init__(self, name: str, param: Parameter, ann) -> None:
        info = _cli_info(ann)
        self.name = name
        self.is_option = isinstance(info, Opt)
        self.help = info.help if info is not None else None
        self.hidden = info.hidden if info is not None else False
        self.required = param.default is Parameter.empty
        self.default = None if self.required else param.default

        base = _strip_annotated(ann)
        if _is_union(base):
            non_none = [a for a in _union_members(base) if a is not type(None)]
            base = non_none[0] if len(non_none) == 1 else str
        self.choices = [str(v) for v in get_args(base)] if get_origin(base) is Literal else None
        self.is_bool = base is bool
        if self.choices is not None:
            self.convert = str
        elif self.is_bool:
            self.convert = _parse_bool
        else:
            self.convert = base if base in (int, float) else str

        self.min = getattr(info, "min", None)
        self.max = getattr(info, "max", None)
        if self.min is not None or self.max is not None:
            to_number = self.convert

            def convert(value):
                number = to_number(value)
                error = self.value_error(number)
                if error:
                    raise argparse.ArgumentTypeError(error)
                return number

            # argparse names the type in "invalid int value: 'x'"
            convert.__name__ = to_number.__name__
            self.convert = convert

        # option names; "--x/--no-x" is an on/off switch, None when neither is given
        self.flags, self.off_flags = [], []
        for flag in (info.flags if self.is_option else ()) or ([f"--{name.replace('_', '-')}"] if self.is_option else []):
            on, _, off = flag.partition("/")
            self.flags.append(on)
            if off:
                self.off_flags.append(off)
        # an option that is only present or absent (--soft), as opposed to one taking a value
        self.is_switch = self.is_option and self.is_bool

    def range_text(self) -> Optional[str]:
        """The allowed range as Click wrote it (x>=1, 1<=x<=5, x<=5), or None if unbounded."""
        if self.min is not None and self.max is not None:
            return f"{self.min}<=x<={self.max}"
        if self.min is not None:
            return f"x>={self.min}"
        if self.max is not None:
            return f"x<={self.max}"
        return None

    def value_error(self, value) -> Optional[str]:
        """Why an already-typed value is outside the declared range or choices, in Click's wording; None if it fits."""
        if self.choices is not None and str(value) not in self.choices:
            return f"{value!r} is not one of {', '.join(repr(c) for c in self.choices)}."
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            return None  # not a number: the type check reports it, there is no range to compare against
        if (self.min is not None and value < self.min) or (self.max is not None and value > self.max):
            return f"{value} is not in the range {self.range_text()}."
        return None


def _param_specs(func) -> list:
    try:
        hints = get_type_hints(func, include_extras=True)
    except Exception:
        hints = {}
    return [_ParamSpec(name, param, hints.get(name)) for name, param in signature(func).parameters.items()]


def _help_with_default(spec: _ParamSpec) -> str:
    if spec.hidden:
        return argparse.SUPPRESS
    text = spec.help or ""
    if spec.range_text():
        text += f" [{spec.range_text()}]"
    if spec.default is not None and not spec.is_switch:
        text += f" [default: {spec.default}]"
    return text


def _add_param(parser: argparse.ArgumentParser, spec: _ParamSpec) -> None:
    if not spec.is_option:
        kwargs = dict(type=spec.convert, help=_help_with_default(spec), metavar=spec.name)
        if spec.choices is not None:
            kwargs["choices"] = spec.choices
        if not spec.required:
            kwargs.update(nargs="?", default=spec.default)
        parser.add_argument(spec.name, **kwargs)
    elif spec.off_flags:
        parser.add_argument(*spec.flags, dest=spec.name, action="store_const", const=True, default=spec.default, help=_help_with_default(spec))
        off_help = argparse.SUPPRESS if spec.hidden else f"Opposite of {spec.flags[0]}"
        parser.add_argument(*spec.off_flags, dest=spec.name, action="store_const", const=False, help=off_help)
    elif spec.is_switch:
        parser.add_argument(*spec.flags, dest=spec.name, action="store_true", default=spec.default, help=_help_with_default(spec))
    else:
        kwargs = dict(dest=spec.name, type=spec.convert, default=spec.default, help=_help_with_default(spec))
        if spec.choices is not None:
            kwargs["choices"] = spec.choices
        else:
            kwargs["metavar"] = f"<{spec.convert.__name__}>"
        parser.add_argument(*spec.flags, **kwargs)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="pbook", description=help_text, formatter_class=argparse.RawDescriptionHelpFormatter)
    for flags, dest, help_ in _GLOBAL_OPTIONS:
        if dest == "command_string":
            parser.add_argument(*flags, dest=dest, metavar="<str>", help=help_)
        else:
            parser.add_argument(*flags, dest=dest, action="store_true", help=help_ if help_ is not None else argparse.SUPPRESS)
    subparsers = parser.add_subparsers(title="commands", metavar="command")
    for name, func in _COMMANDS.items():
        doc = inspect.cleandoc(func.__doc__ or "")
        sub = subparsers.add_parser(
            name,
            help=doc.splitlines()[0] if doc else "",
            description=doc,
            formatter_class=argparse.RawDescriptionHelpFormatter,
        )
        for spec in _param_specs(func):
            _add_param(sub, spec)
        sub.set_defaults(_func=func)
    return parser


# Global options that consume the following argv token as their own value, so the
# command-token scan below can skip over both.
_GLOBAL_VALUE_OPTS = {flags[0] for flags, dest, _ in _GLOBAL_OPTIONS if dest == "command_string"}


def _find_command(argv: list) -> int:
    """Index of the command name in argv (after the global options), or len(argv) if there is none."""
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
    return min(i, len(argv))


def _rewrite_legacy_kwargs(argv: list) -> list:
    """Rewrite legacy bare `key=value` batch args into `--key=value`.

    The old pbook batch mode accepted `pbook show format=long`; the parser only
    recognizes `--format=long`. Find the command, look up its real option names via
    introspection (never a separately maintained list), and rewrite any later bare
    `key=value` token whose key matches one of them. Anything else - already-dashed
    flags, positional args that happen to contain "=" - passes through untouched.
    """
    i = _find_command(argv)
    if i >= len(argv) or argv[i] not in _COMMANDS:
        return argv

    # key -> spec, for every name of every option, aliases included (ramCount as well as memory)
    options = {}
    for spec in _param_specs(_COMMANDS[argv[i]]):
        for flag in spec.flags:
            options[flag.lstrip("-")] = spec

    rewritten = argv[: i + 1]
    for tok in argv[i + 1 :]:
        key, sep, value = tok.partition("=")
        if not (sep and not tok.startswith("-") and key in options):
            rewritten.append(tok)
            continue
        spec = options[key]
        if not spec.is_switch:
            rewritten.append(f"{spec.flags[0]}={value}")
            continue
        # Switches (--soft, --loopingCheck/--no-loopingCheck) take no value; the legacy syntax
        # passed an explicit True/False, so translate that into the on or off option. A switch
        # without an off option is off by default, so False just leaves it out.
        normalized = value.strip().lower()
        if normalized in ("true", "1", "yes"):
            rewritten.append(spec.flags[0])
        elif normalized in ("false", "0", "no"):
            if spec.off_flags:
                rewritten.append(spec.off_flags[0])
        else:
            print(f"Error: '{key}' is a flag and expects True/False (got '{value}' in '{tok}')", file=sys.stderr)
            sys.exit(1)
    return rewritten


def _attach_option_values(argv: list) -> list:
    """Attach the value to each option that takes one: `--taskname -analysis` -> `--taskname=-analysis`.

    Click took the word after such an option as its value whatever it looked like. argparse takes a
    word starting with '-' for another option instead, and then reports the value as missing. With
    the value attached, argparse uses it as it is, so `pbook show --taskname -analysis` keeps working.
    Covers the command's options (aliases and unambiguous abbreviations included) and -c. Nothing
    after `--` is touched.
    """
    i = _find_command(argv)
    attached = []
    j = 0
    while j < i:
        tok = argv[j]
        if tok in _GLOBAL_VALUE_OPTS and j + 1 < len(argv):
            # a short option: -c-1 is -c with the value -1 (-c=-1 would be the value "=-1")
            value = argv[j + 1]
            attached.extend([tok + value] if value.startswith("-") else [tok, value])
            j += 2
            continue
        attached.append(tok)
        j += 1
    if i >= len(argv) or argv[i] not in _COMMANDS:
        return attached + argv[i:]

    specs = [spec for spec in _param_specs(_COMMANDS[argv[i]]) if spec.is_option]
    takes_value = {flag for spec in specs if not spec.is_switch for flag in spec.flags}
    all_flags = {flag for spec in specs for flag in spec.flags + spec.off_flags} | {"--help"}
    attached.append(argv[i])
    k = i + 1
    while k < len(argv):
        tok = argv[k]
        if tok == "--":
            attached.extend(argv[k:])
            break
        if tok.startswith("--") and "=" not in tok and k + 1 < len(argv):
            # the option argparse will resolve tok to: an exact name, or the only one it abbreviates
            matches = [tok] if tok in all_flags else [flag for flag in all_flags if flag.startswith(tok)]
            if len(matches) == 1 and matches[0] in takes_value:
                attached.append(f"{tok}={argv[k + 1]}")
                k += 2
                continue
        attached.append(tok)
        k += 1
    return attached


# ─── Bash completion ──────────────────────────────────────────────────────────

# templates/pbook_completion.sh runs `_PBOOK_COMPLETE=complete_bash pbook` with the line being
# completed in COMP_WORDS (words joined by newlines) and the index of the word under the cursor in
# COMP_CWORD, and offers whatever comes back, one candidate per line. Completions installed with
# typer's --install-completion use the same protocol.
_COMPLETE_VAR = "_PBOOK_COMPLETE"


def _completions(words: list, cword: int) -> list:
    args = words[1:cword]
    incomplete = words[cword] if cword < len(words) else ""

    i = _find_command(args)
    if i >= len(args):
        # still before the command: global options or the command name
        if incomplete.startswith("-"):
            names = [f for flags, _, help_ in _GLOBAL_OPTIONS if help_ is not None for f in flags] + ["-h", "--help"]
        else:
            names = list(_COMMANDS)
        return [n for n in names if n.startswith(incomplete)]

    func = _COMMANDS.get(args[i])
    if func is None:
        return []
    specs = [s for s in _param_specs(func) if s.is_option and not s.hidden]

    def values_of(flag):
        for spec in specs:
            if flag in spec.flags and not spec.is_switch:
                return spec.choices or []
        return None

    # the value of an option: `--format j` or `--format=j`
    if "=" in incomplete and incomplete.startswith("--"):
        flag, _, partial = incomplete.partition("=")
        return [v for v in values_of(flag) or [] if v.startswith(partial)]
    if args[i + 1 :] and values_of(args[-1]) is not None:
        return [v for v in values_of(args[-1]) if v.startswith(incomplete)]

    if incomplete.startswith("-"):
        names = [f for spec in specs for f in spec.flags + spec.off_flags] + ["-h", "--help"]
        return [n for n in names if n.startswith(incomplete)]
    return []


def _split_words(line: str) -> list:
    """Split COMP_WORDS shell-style. pbook_completion.sh joins the words with newlines (it sets
    IFS to a newline), so any whitespace separates them. An unclosed quote is kept as the last word."""
    lexer = shlex.shlex(line, posix=True)
    lexer.whitespace_split = True
    lexer.commenters = ""
    words = []
    try:
        for word in lexer:
            words.append(word)
    except ValueError:
        words.append(lexer.token)
    return words


def _complete_bash() -> None:
    words = _split_words(os.environ.get("COMP_WORDS", ""))
    try:
        cword = int(os.environ.get("COMP_CWORD", ""))
    except ValueError:
        cword = len(words) - 1
    for candidate in _completions(words, cword):
        print(candidate)


# ─── Entry point ──────────────────────────────────────────────────────────────


def main() -> None:
    sys.argv[0] = "pbook"
    if os.environ.get(_COMPLETE_VAR):
        if os.environ[_COMPLETE_VAR] == "complete_bash":
            _complete_bash()
        sys.exit(0)
    args = _build_parser().parse_args(_attach_option_values(_rewrite_legacy_kwargs(sys.argv[1:])))
    try:
        _main(args)
    except KeyboardInterrupt:
        print("\nAborted!", file=sys.stderr)
        sys.exit(1)
