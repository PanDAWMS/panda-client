"""
workflow_native_utils.py
Dispatcher for native PanDA workflow templates.

To add a new template:
  1. Create pandaclient/templates/<name>.py exposing a ``build(**kwargs)`` function.
  2. Import and register it in ``_TEMPLATE_REGISTRY`` below.
"""

from pandaclient.templates import multistep_merge as _multistep_merge

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
