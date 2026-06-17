"""
pbook CLI — PanDA task bookkeeper with typer-based shell autocompletion.
"""

from __future__ import annotations

import atexit
import code
import os
import signal
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import typer

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

    import readline
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
    Client.setGlobalTmpDir(_tmp_dir)

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
        import readline
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
    commands_get_output(f"kill -9 -- -{os.getpgrp()}")


# ─── Interactive REPL namespace ───────────────────────────────────────────────

_RETRY_ALLOWED_OPTS = [
    "site", "excludedSite", "includedSite", "nFilesPerJob", "nMaxFilesPerJob",
    "nGBPerJob", "nFiles", "nEvents", "loopingCheck", "maxNFilesPerJob",
    "memory", "ramCount", "avoidVP", "ignoreMissingInDS", "forceStaged", "maxCore",
]


def _build_namespace(core) -> dict:
    import pydoc

    def help(*arg):
        """Show the help doc."""
        if arg:
            try:
                func = ns[arg[0]] if isinstance(arg[0], str) else arg[0]
                print(pydoc.plain(pydoc.render_doc(func)))
                return
            except Exception:
                print(f"Unknown command: {arg[0]}")
            return
        print("""
Available commands:
  help  show  showl  kill  finish  retry  debug
  get_user_job_metadata  recover_lost_files  reload_input
  show_workflow  kill_workflow  retry_workflow  finish_workflow
  pause_workflow  resume_workflow
  set_secret  list_secrets  delete_secret  delete_all_secrets
  generate_credential

Usage: help(show)  or  pbook show --help
""")

    def show(*args, **kwargs):
        """Print task records. Args: [taskID|'run'|'fin']. Kwargs: username, limit, taskname, days, jeditaskid, reqid, status, superstatus, format."""
        return core.show(*args, **kwargs)

    def showl(*args, **kwargs):
        """Print task records in long format (shortcut for show(..., format='long'))."""
        kwargs["format"] = "long"
        return core.show(*args, **kwargs)

    def kill(taskIDs):
        """Kill tasks. taskIDs: int, [int,...], or 'all'."""
        if taskIDs == "all":
            return _parallel(lambda t: core.kill(t.jeditaskid), core.get_active_tasks())
        elif isinstance(taskIDs, (list, tuple)):
            return _parallel(core.kill, taskIDs)
        elif isinstance(taskIDs, int):
            return [core.kill(taskIDs)]
        print("Error: Invalid argument")

    def finish(taskIDs, soft=False):
        """Finish tasks. taskIDs: int, [int,...], or 'all'. soft=True waits for running jobs."""
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

    def retry(taskIDs, newOpts=None, days=14, limit=1000, **kwargs):
        """Retry failed/cancelled tasks. taskIDs: int, [int,...], or 'all'."""
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

    ns = {k: v for k, v in locals().items() if callable(v)}
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
        core = _make_core(verbose)
        ns = _build_namespace(core)
        if command_string:
            core.init()
            exec(command_string, {}, ns)  # noqa: S102
            from pandaclient import PBookCore as _PBC
            raise typer.Exit(0 if _PBC.func_return_value else 1)
        core.init()
        code.interact(banner=f"\nStart pBook {PandaToolsPkgInfo.release_version}", local=ns)
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
    for k, v in [("username", username), ("taskname", taskname), ("jeditaskid", jeditaskid),
                  ("reqid", reqid), ("status", status), ("superstatus", superstatus)]:
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
    for k, v in [("username", username), ("taskname", taskname), ("jeditaskid", jeditaskid),
                  ("reqid", reqid), ("status", status), ("superstatus", superstatus)]:
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
        k: v for k, v in {
            "site": site, "excludedSite": excluded_site, "includedSite": included_site,
            "nFilesPerJob": n_files_per_job, "nMaxFilesPerJob": n_max_files_per_job,
            "nGBPerJob": n_gb_per_job, "nFiles": n_files, "nEvents": n_events,
            "loopingCheck": looping_check, "ramCount": memory, "avoidVP": avoid_vp,
            "ignoreMissingInDS": ignore_missing_in_ds, "forceStaged": force_staged,
            "maxCoreCount": max_core,
        }.items() if v is not None
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