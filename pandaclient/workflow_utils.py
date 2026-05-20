"""
workflow_utils.py
Pure utility helpers for PanDA workflow operations.
No imports from other local pandaclient modules.
"""

import re


def extract_scope(dataset_name: str, strip_slash: bool = False) -> tuple:
    """
    Extract the Rucio scope from a dataset name.

    Parameters
    ----------
    dataset_name : str
        Dataset name, optionally in ``scope:name`` form.
    strip_slash : bool
        When True, strip a trailing ``/`` (used to denote container collections)
        from *dataset_name* before processing.

    Returns
    -------
    tuple
        ``(scope, dataset_name)`` pair derived from the input.
    """
    if strip_slash and dataset_name.endswith("/"):
        dataset_name = re.sub("/$", "", dataset_name)
    if ":" in dataset_name:
        return tuple(dataset_name.split(":")[:2])
    scope = dataset_name.split(".")[0]
    if dataset_name.startswith("user") or dataset_name.startswith("group"):
        scope = ".".join(dataset_name.split(".")[:2])
    return scope, dataset_name
