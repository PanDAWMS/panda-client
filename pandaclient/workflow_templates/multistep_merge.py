"""
multistep_merge template
Chain of prun merge steps that reduces a dataset to a single output file.
The number of steps is computed automatically from the file count of the
input dataset via Rucio.

Required --prunFlags keys: nGBPerJob, maxNFilesPerJob
"""

import math

from pandaclient.workflow_description import WorkflowDescription
from pandaclient.workflow_utils import extract_scope

REQUIRED_FLAGS = frozenset({"nGBPerJob", "maxNFilesPerJob"})

_FIXED_ARGS = "--outputs merge.root --rootVer recommended --noBuild --notExpandInDS" " --respectSplitRule --writeInputToTxt IN:input.lis --avoidVP"


def build(in_ds, prun_flags, verbose=False):
    """
    Build a multistep merge :class:`WorkflowDescription`.

    Parameters
    ----------
    in_ds : str
        Input dataset (``scope:name`` or plain name).
    prun_flags : dict[str, str]
        Key/value pairs forwarded as ``--key value`` to every prun step.
        Must include ``nGBPerJob`` and ``maxNFilesPerJob``; the latter also
        drives the step-count calculation.
    verbose : bool
        When True, print progress information to stdout.

    Returns
    -------
    WorkflowDescription
    """
    missing = REQUIRED_FLAGS - set(prun_flags)
    if missing:
        raise ValueError(f"multistep_merge template requires the following --prunFlags keys: {', '.join(sorted(missing))}")

    max_n_files_per_job = int(prun_flags["maxNFilesPerJob"])

    n_files = _count_rucio_files(in_ds)
    if n_files == 0:
        raise ValueError(f"Input dataset '{in_ds}' contains no files.")

    n_steps = _compute_n_steps(n_files, max_n_files_per_job)

    if verbose:
        print(f"multistep_merge: found {n_files} file(s) in '{in_ds}', using {n_steps} merge step(s)")

    user_flags_str = " ".join(f"--{k} {v}" for k, v in prun_flags.items())
    merge_args = f"{_FIXED_ARGS} {user_flags_str}"

    wf = WorkflowDescription(name=f"multistep_merge_{n_steps}steps")
    wf.add_input("input_ds", in_ds)

    for i in range(1, n_steps + 1):
        step_name = f"step{i}"
        in_ref = wf.input_ref("input_ds") if i == 1 else wf.step_output(f"step{i - 1}")
        wf.add_prun_step(step_name, in_ds=in_ref, args=merge_args, executable="merge.sh")

    last_step = f"step{n_steps}"
    wf.add_output("final_output", from_ref=wf.step_output(last_step), output_types=["merge.root"])

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
