"""
workflow_utils.py
Dispatcher for native PanDA workflow templates.

To add a new template:
  1. Create pandaclient/workflow_templates/<name>.py exposing a ``build(**kwargs)`` function.
  2. Import and register it in ``_TEMPLATE_REGISTRY`` below.
"""

import re

from pandaclient.workflow_templates import multistep_merge as _multistep_merge


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


_TEMPLATE_REGISTRY = {
    "multistep_merge": _multistep_merge,
}

AVAILABLE_TEMPLATES = list(_TEMPLATE_REGISTRY)


def build_workflow_from_template(template_name, **kwargs):
    """
    Build a :class:`~pandaclient.workflow_description.WorkflowDescription`
    from a named template.

    Parameters
    ----------
    template_name : str
        One of :data:`AVAILABLE_TEMPLATES`.
    **kwargs
        Forwarded verbatim to the template's ``build()`` function.

    Returns
    -------
    WorkflowDescription
    """
    if template_name not in _TEMPLATE_REGISTRY:
        raise ValueError(f"Unknown template '{template_name}'. " f"Available templates: {', '.join(AVAILABLE_TEMPLATES)}")
    return _TEMPLATE_REGISTRY[template_name].build(**kwargs)
