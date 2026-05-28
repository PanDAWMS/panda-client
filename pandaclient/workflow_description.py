"""
workflow_description.py
Generate native PanDA workflow descriptions in YAML or JSON format.

Typical usage:
    from pandaclient.workflow_description import WorkflowDescription

    wf = WorkflowDescription(name="my_chain")
    wf.add_input("raw", "user.me:my.input.dataset")
    wf.add_prun_step(
        "step1",
        in_ds=WorkflowDescription.input_ref("raw"),
        args="--outputs out.root --nGBPerJob 10",
        executable="run.sh",
    )
    wf.add_prun_step("step2", in_ds=WorkflowDescription.step_output("step1"), args="...", executable="run.sh")
    wf.add_output("result", from_ref=WorkflowDescription.step_output("step2"), output_types=["out.root"])
    wf.save("my_chain.yaml")

    # prun step with container, secondary datasets, and file-type filters:
    wf2 = WorkflowDescription(name="signal_background_combine")
    wf2.add_input("signal", "user.me:signal.dataset")
    wf2.add_input("background", "user.me:background.dataset")
    wf2.add_prun_step(
        "make_signal",
        in_ds=WorkflowDescription.input_ref("signal"),
        args="--outputs abc.dat,def.zip --nFilesPerJob 5",
        executable="echo %IN > abc.dat; echo 123 > def.zip",
        container_image="docker://busybox",
    )
    wf2.add_prun_step(
        "premix",
        in_ds=WorkflowDescription.step_output("make_signal"),
        in_ds_type="def.zip",
        secondary_dss=[WorkflowDescription.step_output("make_background_1")],
        secondary_ds_types=["xyz.pool"],
        args="--outputs klm.root --secondaryDSs IN2:13:%{SECDS1}",
        executable="echo %IN %IN2 > klm.root",
    )
    wf2.add_output("outDS", from_ref=WorkflowDescription.step_output("premix"), output_types=["klm.root"])
    wf2.save("signal_background_combine.yaml")
"""

import json
import re

try:
    import yaml as _yaml

    _YAML_AVAILABLE = True
except ImportError:
    _YAML_AVAILABLE = False


# ---------------------------------------------------------------------------
# Internal YAML helpers (no external dependency)
# ---------------------------------------------------------------------------


def _yaml_needs_quotes(s):
    if not s:
        return True
    if s[0] in "-?:,[]{}#&*!|>'\"`@%{":
        return True
    if any(c in s for c in (":", "#", "\n")):
        return True
    if s.lower() in ("true", "false", "null", "yes", "no", "on", "off"):
        return True
    try:
        float(s)
        return True
    except ValueError:
        pass
    return False


def _yaml_scalar(s):
    """Return a YAML scalar string, quoted if necessary."""
    if _yaml_needs_quotes(s):
        escaped = s.replace("\\", "\\\\").replace('"', '\\"')
        return f'"{escaped}"'
    return s


def _yaml_folded_block(value, indent, line_width=80):
    """Produce the body of a >- folded block scalar, word-wrapped to line_width."""
    words = value.split()
    lines = []
    current = indent
    for word in words:
        extended = current + (" " if current != indent else "") + word
        if len(extended) > line_width and current != indent:
            lines.append(current)
            current = indent + word
        else:
            current = extended
    if current != indent:
        lines.append(current)
    return "\n".join(lines)


class _YAMLWriter:
    """Minimal YAML writer that produces readable output for workflow dicts."""

    # Keys whose string values are emitted as >- folded block scalars
    _FOLDED_KEYS = frozenset({"args"})
    _INDENT = "  "

    def dump(self, d):
        lines = []
        self._mapping(d, lines, 0)
        return "\n".join(lines) + "\n"

    def _ind(self, level):
        return self._INDENT * level

    def _mapping(self, d, lines, level):
        for k, v in d.items():
            self._kv(k, v, lines, level)

    def _kv(self, k, v, lines, level):
        ind = self._ind(level)
        if isinstance(v, dict):
            lines.append(f"{ind}{k}:")
            self._mapping(v, lines, level + 1)
        elif isinstance(v, list):
            lines.append(f"{ind}{k}:")
            item_ind = self._ind(level + 1)
            for item in v:
                scalar = _yaml_scalar(item) if isinstance(item, str) else str(item)
                lines.append(f"{item_ind}- {scalar}")
        elif isinstance(v, str) and k in self._FOLDED_KEYS and len(v) > 40:
            lines.append(f"{ind}{k}: >-")
            lines.append(_yaml_folded_block(v, self._ind(level + 1)))
        elif isinstance(v, str):
            lines.append(f"{ind}{k}: {_yaml_scalar(v)}")
        elif isinstance(v, bool):
            lines.append(f"{ind}{k}: {'true' if v else 'false'}")
        elif v is None:
            lines.append(f"{ind}{k}: ~")
        else:
            lines.append(f"{ind}{k}: {v}")


_default_writer = _YAMLWriter()


# ---------------------------------------------------------------------------
# Public classes
# ---------------------------------------------------------------------------


class WorkflowStep:
    """A single step in a PanDA workflow."""

    __slots__ = ("type", "in_ds", "args", "exec", "in_ds_type", "secondary_dss", "secondary_ds_types", "use_athena_packages", "container_image")

    @staticmethod
    def _check(val, name, *types):
        if not isinstance(val, types):
            expected = " or ".join(t.__name__ for t in types)
            raise TypeError(f"{name} must be {expected}, got {type(val).__name__}")

    def __init__(
        self,
        step_type,
        in_ds,
        args=None,
        executable=None,
        in_ds_type=None,
        secondary_dss=None,
        secondary_ds_types=None,
        use_athena_packages=False,
        container_image=None,
    ):
        self._check(step_type, "type", str)
        self._check(in_ds, "inDS", str)
        if args is not None:
            self._check(args, "args", str)
        if executable is not None:
            self._check(executable, "exec", str)
        if in_ds_type is not None:
            self._check(in_ds_type, "inDsType", str)
        if secondary_dss is not None:
            self._check(secondary_dss, "secondaryDSs", list)
            for i, item in enumerate(secondary_dss):
                self._check(item, f"secondaryDSs[{i}]", str)
        if secondary_ds_types is not None:
            self._check(secondary_ds_types, "secondaryDsTypes", list)
            for i, item in enumerate(secondary_ds_types):
                self._check(item, f"secondaryDsTypes[{i}]", str)
        self._check(use_athena_packages, "useAthenaPackages", bool)
        if container_image is not None:
            self._check(container_image, "containerImage", str)

        self.type = step_type
        self.in_ds = in_ds
        self.args = args
        self.exec = executable  # parameter named 'executable' to avoid shadowing the built-in exec()
        self.in_ds_type = in_ds_type
        self.secondary_dss = list(secondary_dss) if secondary_dss else []
        self.secondary_ds_types = list(secondary_ds_types) if secondary_ds_types else []
        self.use_athena_packages = use_athena_packages
        self.container_image = container_image

    def to_dict(self):
        d = {"type": self.type, "inDS": self.in_ds}
        if self.in_ds_type is not None:
            d["inDsType"] = self.in_ds_type
        if self.container_image is not None:
            d["containerImage"] = self.container_image
        if self.secondary_dss:
            d["secondaryDSs"] = self.secondary_dss
        if self.secondary_ds_types:
            d["secondaryDsTypes"] = self.secondary_ds_types
        if self.args is not None:
            d["args"] = self.args
        if self.exec is not None:
            d["exec"] = self.exec
        if self.use_athena_packages:
            d["useAthenaPackages"] = self.use_athena_packages
        return d


class WorkflowOutput:
    """Workflow output definition referencing a step's output dataset."""

    __slots__ = ("from_ref", "output_types")

    def __init__(self, from_ref, output_types=None):
        self.from_ref = from_ref
        self.output_types = list(output_types) if output_types else []

    def to_dict(self):
        d = {"from": self.from_ref}
        if self.output_types:
            d["output_types"] = self.output_types
        return d


class WorkflowDescription:
    """
    Builder for native PanDA workflow descriptions.

    Produces YAML or JSON files compatible with ``pchain --wfd``.
    All mutating methods return ``self`` to allow method chaining.
    """

    def __init__(self, name=None):
        self.name = name
        self.inputs = {}  # str → dataset string
        self.outputs = {}  # str → WorkflowOutput
        self.steps = {}  # str → WorkflowStep  (insertion order preserved)
        self.options = {}  # str → value

    # ------------------------------------------------------------------
    # Builder methods
    # ------------------------------------------------------------------

    def add_input(self, name, dataset):
        """Register a named workflow input dataset."""
        self.inputs[name] = dataset
        return self

    def add_step(
        self,
        name,
        step_type,
        in_ds,
        args=None,
        executable=None,
        in_ds_type=None,
        secondary_dss=None,
        secondary_ds_types=None,
        use_athena_packages=False,
        container_image=None,
    ):
        """Add a workflow step of any type."""
        self.steps[name] = WorkflowStep(step_type, in_ds, args, executable, in_ds_type, secondary_dss, secondary_ds_types, use_athena_packages, container_image)
        return self

    def add_prun_step(
        self,
        name,
        in_ds,
        args=None,
        executable=None,
        in_ds_type=None,
        secondary_dss=None,
        secondary_ds_types=None,
        use_athena_packages=False,
        container_image=None,
    ):
        """Add a ``prun`` step (convenience wrapper around :meth:`add_step`)."""
        return self.add_step(name, "prun", in_ds, args, executable, in_ds_type, secondary_dss, secondary_ds_types, use_athena_packages, container_image)

    def add_output(self, name, from_ref, output_types=None):
        """Register a named workflow output."""
        self.outputs[name] = WorkflowOutput(from_ref, output_types)
        return self

    def set_option(self, key, value):
        """Set a workflow-level option (e.g. ``allow_partial_inputs``)."""
        self.options[key] = value
        return self

    # ------------------------------------------------------------------
    # Reference helpers
    # ------------------------------------------------------------------

    @staticmethod
    def step_output(step_name):
        """Return ``<step_name>/outDS`` — the output dataset reference for a step."""
        return f"{step_name}/outDS"

    @staticmethod
    def input_ref(input_name):
        """Return ``{input_name}`` — the reference syntax for a workflow input."""
        return "{" + input_name + "}"

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self):
        """
        Check that all inDS and output references resolve.

        Raises :class:`ValueError` listing all problems found.
        Returns ``True`` when valid.
        """
        errors = []
        if not self.steps:
            errors.append("workflow has no steps")

        step_names = set(self.steps)
        input_names = set(self.inputs)

        for sname, step in self.steps.items():
            for ref in re.findall(r"\{(\w+)\}", step.in_ds):
                if ref not in input_names:
                    errors.append(f"step '{sname}': undefined input reference '{{{ref}}}'")
            m = re.match(r"^(\w+)/outDS$", step.in_ds)
            if m and m.group(1) not in step_names:
                errors.append(f"step '{sname}': unknown step reference '{m.group(1)}/outDS'")
            if step.secondary_ds_types and len(step.secondary_ds_types) != len(step.secondary_dss):
                errors.append(f"step '{sname}': secondaryDsTypes has {len(step.secondary_ds_types)} entries " f"but secondaryDSs has {len(step.secondary_dss)}")
            for ds_ref in step.secondary_dss:
                for ref in re.findall(r"\{(\w+)\}", ds_ref):
                    if ref not in input_names:
                        errors.append(f"step '{sname}': secondaryDSs: undefined input reference '{{{ref}}}'")
                m2 = re.match(r"^(\w+)/outDS$", ds_ref)
                if m2 and m2.group(1) not in step_names:
                    errors.append(f"step '{sname}': secondaryDSs: unknown step reference '{m2.group(1)}/outDS'")

        for oname, out in self.outputs.items():
            m = re.match(r"^(\w+)/outDS$", out.from_ref)
            if m and m.group(1) not in step_names:
                errors.append(f"output '{oname}': unknown step reference '{m.group(1)}/outDS'")

        if errors:
            msg = "workflow validation errors:\n" + "\n".join(f"  - {e}" for e in errors)
            raise ValueError(msg)
        return True

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self):
        """Return the workflow as a plain Python dict."""
        d = {}
        if self.name:
            d["name"] = self.name
        if self.inputs:
            d["inputs"] = dict(self.inputs)
        if self.outputs:
            d["outputs"] = {k: v.to_dict() for k, v in self.outputs.items()}
        d["steps"] = {k: v.to_dict() for k, v in self.steps.items()}
        if self.options:
            d["options"] = dict(self.options)
        return d

    def to_json(self, indent=2):
        """Return the workflow serialized as a JSON string."""
        return json.dumps(self.to_dict(), indent=indent)

    def to_yaml(self):
        """
        Return the workflow serialized as a YAML string.

        Uses PyYAML when available, otherwise falls back to the built-in writer.
        """
        if _YAML_AVAILABLE:
            return self._to_yaml_pyyaml()
        return _default_writer.dump(self.to_dict())

    def _to_yaml_pyyaml(self):
        """Serialize with PyYAML, using >- folded blocks for long args strings."""

        class _FoldedStr(str):
            pass

        class _Dumper(_yaml.Dumper):
            pass

        _Dumper.add_representer(
            _FoldedStr,
            lambda dumper, data: dumper.represent_scalar("tag:yaml.org,2002:str", data, style=">"),
        )

        def _wrap(obj):
            if isinstance(obj, dict):
                return {k: _FoldedStr(v) if k == "args" and isinstance(v, str) and len(v) > 40 else _wrap(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [_wrap(i) for i in obj]
            return obj

        return _yaml.dump(
            _wrap(self.to_dict()),
            Dumper=_Dumper,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    @classmethod
    def from_dict(cls, d):
        """Create a :class:`WorkflowDescription` from a plain dict."""
        wf = cls(name=d.get("name"))
        for k, v in d.get("inputs", {}).items():
            wf.add_input(k, v)
        for sname, sdata in d.get("steps", {}).items():
            raw_sec = sdata.get("secondaryDSs")
            if isinstance(raw_sec, dict):
                secondary_dss = list(raw_sec.keys())
                secondary_ds_types = list(raw_sec.values())
            else:
                secondary_dss = raw_sec
                secondary_ds_types = sdata.get("secondaryDsTypes")
            wf.add_step(
                sname,
                step_type=sdata["type"],
                in_ds=sdata.get("inDS", ""),
                args=sdata.get("args"),
                executable=sdata.get("exec"),
                in_ds_type=sdata.get("inDsType"),
                secondary_dss=secondary_dss,
                secondary_ds_types=secondary_ds_types,
                use_athena_packages=sdata.get("useAthenaPackages", False),
                container_image=sdata.get("containerImage"),
            )
        for oname, odata in d.get("outputs", {}).items():
            wf.add_output(oname, from_ref=odata["from"], output_types=odata.get("output_types"))
        for k, v in d.get("options", {}).items():
            wf.set_option(k, v)
        return wf

    @classmethod
    def from_yaml(cls, yaml_str):
        """Create from a YAML string.  Requires PyYAML (``pip install pyyaml``)."""
        if not _YAML_AVAILABLE:
            raise ImportError("PyYAML is required to parse YAML; install it with: pip install pyyaml")
        return cls.from_dict(_yaml.safe_load(yaml_str))

    @classmethod
    def from_json(cls, json_str):
        """Create from a JSON string."""
        return cls.from_dict(json.loads(json_str))

    # ------------------------------------------------------------------
    # File I/O
    # ------------------------------------------------------------------

    def save(self, filepath, fmt=None):
        """
        Write the workflow description to *filepath*.

        The format is inferred from the file extension (``.json`` → JSON,
        anything else → YAML) unless *fmt* is explicitly ``'json'`` or ``'yaml'``.
        """
        if fmt is None:
            fmt = "json" if filepath.endswith(".json") else "yaml"
        content = self.to_json() if fmt == "json" else self.to_yaml()
        with open(filepath, "w") as fh:
            fh.write(content)

    @classmethod
    def load(cls, filepath):
        """Load a workflow description from a YAML or JSON file."""
        with open(filepath, "r") as fh:
            content = fh.read()
        if filepath.endswith(".json"):
            return cls.from_json(content)
        return cls.from_yaml(content)

    # ------------------------------------------------------------------
    # Misc
    # ------------------------------------------------------------------

    def __repr__(self):
        return f"WorkflowDescription(name={self.name!r}, steps={list(self.steps)})"
