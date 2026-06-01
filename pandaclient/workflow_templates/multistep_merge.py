"""
multistep_merge template
Chain of prun merge steps that reduces a dataset to a single output file.
The number of steps is computed automatically from the file count of the
input dataset via Rucio.

Optional --prunFlags keys: nGBPerJob (default 10), maxNFilesPerJob (default 200)
"""

import math
import os
import shutil
import stat

from pandaclient.workflow_description import WorkflowDescription
from pandaclient.workflow_utils import extract_scope

DEFAULT_FLAGS = {"nGBPerJob": "10", "maxNFilesPerJob": "200"}

DEFAULT_OUTPUT_FILE = "merge.root"
DEFAULT_INPUT_FILE = "input.lis"
DEFAULT_EXECUTABLE = "merge.sh"

_FIXED_ARGS_TEMPLATE = "--rootVer recommended --noBuild --notExpandInDS --respectSplitRule --avoidVP"

_WORKFLOW_SCRIPTS_SUBPATH = os.path.join("etc", "panda", "share", "workflow_scripts")


def _locate_workflow_script(script_name):
    """Return the absolute path of script_name from the installed or source-tree location."""
    panda_sys = os.environ.get("PANDA_SYS", "")
    if panda_sys:
        candidate = os.path.join(panda_sys, _WORKFLOW_SCRIPTS_SUBPATH, script_name)
        if os.path.exists(candidate):
            return candidate
    # Development / editable install: share/ lives two package levels above this file
    repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    candidate = os.path.join(repo_root, "share", "workflow_scripts", script_name)
    if os.path.exists(candidate):
        return candidate
    raise FileNotFoundError(
        f"Cannot find workflow script '{script_name}'. "
        f"Checked $PANDA_SYS/{_WORKFLOW_SCRIPTS_SUBPATH}/ and {os.path.join(repo_root, 'share', 'workflow_scripts')}/"
    )


def build(in_ds, prun_flags, verbose=False, output_file=DEFAULT_OUTPUT_FILE, input_file=DEFAULT_INPUT_FILE, executable=DEFAULT_EXECUTABLE):
    """
    Build a multistep merge :class:`WorkflowDescription`.

    Parameters
    ----------
    in_ds : str
        Input dataset (``scope:name`` or plain name).
    prun_flags : dict[str, str]
        Key/value pairs forwarded as ``--key value`` to every prun step.
        ``nGBPerJob`` defaults to 10 and ``maxNFilesPerJob`` defaults to 200;
        the latter also drives the step-count calculation.
    verbose : bool
        When True, print progress information to stdout.
    output_file : str
        Name of the merged output file (default ``merge.root``).
    input_file : str
        Name of the per-job input filename written by prun (default ``input.lis``).
    executable : str
        Merge script to run on the worker node (default ``merge.sh``).

    Returns
    -------
    WorkflowDescription
    """
    effective_flags = {**DEFAULT_FLAGS, **prun_flags}

    try:
        max_n_files_per_job = int(effective_flags["maxNFilesPerJob"])
    except (ValueError, TypeError):
        raise ValueError(f"maxNFilesPerJob must be an integer, got {effective_flags['maxNFilesPerJob']!r}")
    if max_n_files_per_job < 2:
        raise ValueError(f"maxNFilesPerJob must be at least 2 (got {max_n_files_per_job}); values < 2 cannot reduce a dataset to a single output")

    n_files = _count_rucio_files(in_ds)
    if n_files == 0:
        raise ValueError(f"Input dataset '{in_ds}' contains no files.")

    n_steps = _compute_n_steps(n_files, max_n_files_per_job)

    if verbose:
        print(f"multistep_merge: found {n_files} file(s) in '{in_ds}', using {n_steps} merge step(s)")

    user_flags_str = " ".join(f"--{k} {v}" for k, v in effective_flags.items())
    merge_args = f"--outputs {output_file} --writeInputToTxt IN:{input_file} {_FIXED_ARGS_TEMPLATE} {user_flags_str}"

    # Copy the merge script into cwd so the prun sandbox tarball includes it.
    # Skip if the script already exists in cwd so users can pre-place a custom version.
    exec_basename = os.path.basename(executable)
    script_dest = os.path.join(os.getcwd(), exec_basename)
    if os.path.exists(script_dest):
        if not os.access(script_dest, os.X_OK):
            os.chmod(script_dest, os.stat(script_dest).st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        if verbose:
            print(f"multistep_merge: '{exec_basename}' already exists in cwd, skipping copy")
    else:
        script_src = executable if (os.path.isabs(executable) or os.path.exists(executable)) else _locate_workflow_script(exec_basename)
        shutil.copy2(script_src, script_dest)
        os.chmod(script_dest, os.stat(script_dest).st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        if verbose:
            print(f"multistep_merge: copied '{script_src}' -> '{script_dest}'")

    wf = WorkflowDescription(name=f"multistep_merge_{n_steps}steps")
    wf.add_input("input_ds", in_ds)

    for i in range(1, n_steps + 1):
        step_name = f"step{i}"
        in_ref = wf.input_ref("input_ds") if i == 1 else wf.step_output(f"step{i - 1}")
        wf.add_prun_step(step_name, in_ds=in_ref, args=merge_args, executable=f"{exec_basename} {output_file} {input_file}")

    last_step = f"step{n_steps}"
    wf.add_output("final_output", from_ref=wf.step_output(last_step), output_types=[output_file])

    return wf


def _count_rucio_files(in_ds):
    try:
        from rucio.client import Client as RucioClient
    except ImportError as exc:
        raise ImportError("The 'rucio-clients' package is required for the multistep_merge template. " "Install it with: pip install rucio-clients") from exc

    scope, name = extract_scope(in_ds, strip_slash=True)

    client = RucioClient()
    return sum(1 for _ in client.list_files(scope, name))


def _compute_n_steps(n_files, max_n_files_per_job):
    n_steps = 0
    n_output = n_files
    for _ in range(int(math.log2(n_files)) + 1):
        n_steps += 1
        n_output = math.ceil(n_output / max_n_files_per_job)
        if n_output <= 1:
            break
    return n_steps
