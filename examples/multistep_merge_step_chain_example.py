"""
multistep_merge_step_chain_example.py
Demonstrates building a dynamic chain of prun merge steps using a for-loop.

The number of steps is calculated automatically from the number of files in
the input dataset via Rucio: each step merges up to MAX_N_FILES_PER_JOB files,
and the chain continues until a single output file remains.

Run:
    python examples/multistep_merge_step_chain_example.py
"""

import math
import os
import sys

from rucio.client import Client as RucioClient

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pandaclient.workflow_description import WorkflowDescription

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

N_GB_PER_JOB = 10
MAX_N_FILES_PER_JOB = 50

MERGE_ARGS = (
    "--outputs merge.root --rootVer recommended --noBuild --notExpandInDS"
    f" --nGBPerJob {N_GB_PER_JOB} --maxNFilesPerJob {MAX_N_FILES_PER_JOB} --respectSplitRule"
    " --writeInputToTxt IN:input.lis --avoidVP"
)

INPUT_DATASET = "user.sgaid:user.sgaid.periodAllYear.physics_Main.DAOD_PHYS.grp23_v01_p6700.CNF_EMuJSMu_B_7cbc64d_output"

# ---------------------------------------------------------------------------
# Count files in the input dataset via Rucio
# ---------------------------------------------------------------------------

scope, dataset_name = INPUT_DATASET.split(":", 1)

client = RucioClient()
n_files = sum(1 for _ in client.list_files(scope, dataset_name))

if n_files == 0:
    raise ValueError(f"Input dataset '{INPUT_DATASET}' contains no files.")

# n_steps is determined by how many times we need to merge the input dataset until we end up with a single output file
n_steps = 0
n_output_files = n_files
for _ in range(int(math.log2(n_files)) + 1):  # rough upper bound on the number of steps needed
    n_steps += 1
    n_output_files = math.ceil(n_output_files / MAX_N_FILES_PER_JOB)
    if n_output_files <= 1:
        break

# ---------------------------------------------------------------------------
# Build the chain
# ---------------------------------------------------------------------------

wf = WorkflowDescription(name=f"step_chain_{n_steps}")
wf.add_input("input_ds", INPUT_DATASET)

for i in range(1, n_steps + 1):
    step_name = f"step{i}"
    if i == 1:
        in_ds = wf.input_ref("input_ds")
    else:
        in_ds = wf.step_output(f"step{i - 1}")

    wf.add_prun_step(step_name, in_ds=in_ds, args=MERGE_ARGS, exec_str="merge.sh")

last_step = f"step{n_steps}"
wf.add_output("final_output", from_ref=wf.step_output(last_step), output_types=["merge.root"])

# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

print("=== YAML output ===")
print(wf.to_yaml())

print("=== Validation ===")
wf.validate()
print("OK")
