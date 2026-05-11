"""
workflow_description_example.py
Demonstrates how to build native PanDA workflow descriptions programmatically.

Reproduces examples/multistep_merge_wfd.yaml and also shows JSON output,
round-trip loading, and validation.

Run:
    python examples/workflow_description_example.py
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pandaclient.workflow_description import WorkflowDescription

# ---------------------------------------------------------------------------
# Example 1: reproduce multistep_merge_wfd.yaml
# ---------------------------------------------------------------------------

N_GB_PER_JOB = 10
MAX_N_FILES_PER_JOB = 50

MERGE_ARGS = (
    "--outputs merge.root --rootVer recommended --noBuild --notExpandInDS"
    f" --nGBPerJob {N_GB_PER_JOB} --maxNFilesPerJob {MAX_N_FILES_PER_JOB} --respectSplitRule"
    " --writeInputToTxt IN:input.lis --avoidVP"
)

INPUT_DATASET = "user.sgaid:user.sgaid.periodAllYear.physics_Main.DAOD_PHYS.grp23_v01_p6700.CNF_EMuJSMu_B_7cbc64d_output"

wf = (
    WorkflowDescription(name="multistep_merge_chain")
    .add_input("input_to_merge", INPUT_DATASET)
    .add_prun_step(
        "first",
        in_ds=WorkflowDescription.input_ref("input_to_merge"),
        args=MERGE_ARGS,
        executable="merge.sh",
    )
    .add_prun_step(
        "second",
        in_ds=WorkflowDescription.step_output("first"),
        args=MERGE_ARGS,
        executable="merge.sh",
    )
    .add_prun_step(
        "third",
        in_ds=WorkflowDescription.step_output("second"),
        args=MERGE_ARGS,
        executable="merge.sh",
    )
    .add_output(
        "final_output",
        from_ref=WorkflowDescription.step_output("third"),
        output_types=["merge.root"],
    )
)

print("=== YAML output ===")
print(wf.to_yaml())

print("=== JSON output ===")
print(wf.to_json())

# ---------------------------------------------------------------------------
# Example 2: save and round-trip load
# ---------------------------------------------------------------------------

wf.save("/tmp/multistep_merge_chain.yaml")
wf.save("/tmp/multistep_merge_chain.json")

wf_from_yaml = WorkflowDescription.load("/tmp/multistep_merge_chain.yaml")
wf_from_json = WorkflowDescription.load("/tmp/multistep_merge_chain.json")

print("=== Round-trip from YAML ===")
print(repr(wf_from_yaml))

print("=== Round-trip from JSON ===")
print(repr(wf_from_json))

# ---------------------------------------------------------------------------
# Example 3: validation
# ---------------------------------------------------------------------------

print("\n=== Validation (valid workflow) ===")
wf.validate()
print("OK")

print("\n=== Validation (broken reference) ===")
wf_bad = WorkflowDescription().add_prun_step("step1", in_ds="nonexistent/outDS")
try:
    wf_bad.validate()
except ValueError as exc:
    print(exc)

# ---------------------------------------------------------------------------
# Example 4: workflow with options
# ---------------------------------------------------------------------------

wf_partial = (
    WorkflowDescription(name="partial_inputs_example")
    .add_input("raw", INPUT_DATASET)
    .add_prun_step(
        "process",
        in_ds=WorkflowDescription.input_ref("raw"),
        args="--outputs out.root --nGBPerJob 5",
        executable="process.sh",
    )
    .set_option("allow_partial_inputs", True)
    .set_option("min_input_files", 50)
)

print("\n=== Workflow with options ===")
print(wf_partial.to_yaml())
