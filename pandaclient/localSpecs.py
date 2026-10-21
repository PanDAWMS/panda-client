task_active_superstatus_list = ["running", "submitting", "registered", "ready"]
task_final_superstatus_list = ["finished", "failed", "done", "broken", "aborted"]

import json

from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text

# The purpose of the long width is to soft wrap the lines, instead of e.g. shrinking table columns
_console = Console(width=10000)

_STATUS_STYLE = {
    "running": "bold cyan",
    "submitting": "cyan",
    "registered": "magenta",
    "ready": "magenta",
    "done": "bold green",
    "finished": "yellow",
    "failed": "red",
    "broken": "bold red",
    "aborted": "bold red",
    "exhausted": "magenta",
    "paused": "dark_orange",
    "throttled": "dark_orange",
}

# Workflow and step statuses. Deliberately standalone rather than derived from _STATUS_STYLE:
# a workflow, a step and a task are three different vocabularies that happen to share some names,
# and inheriting meant a colour chosen for a task silently became the colour of a step. Every
# status either class can hold is listed here, so changing one changes nothing else. The lists are
# WorkflowStatus and WFStepStatus in pandaserver/workflow/workflow_base.py; a status missing here
# renders unstyled.
_WORKFLOW_STATUS_STYLE = {
    # before anything runs
    "registered": "magenta",
    "parsed": "magenta",
    "checking": "magenta",
    "ready": "magenta",
    # deciding whether the step runs at all
    "checked": "orange1",
    "checked_true": "orange1",
    "checked_false": "dark_orange",
    # waiting on inputs, then on its way
    "pending": "yellow",
    "starting": "cyan",
    "running": "bold cyan",
    # final statuses, all bold
    "done": "bold green",
    "failed": "bold red",
    "closed": "bold steel_blue",
    "cancelled": "bold light_coral",
}


class LocalTaskSpec:
    _attributes_hidden = (
        "_timestamp",
        "_sourceurl",
        "_weburl",
        "_fulldict",
    )

    _attributes_direct = (
        "jeditaskid",
        "reqid",
        "taskname",
        "username",
        "creationdate",
        "modificationtime",
        "superstatus",
        "status",
    )

    _attributes_dsinfo = (
        "pctfinished",
        "pctfailed",
        "nfiles",
        "nfilesfinished",
        "nfilesfailed",
    )

    __slots__ = _attributes_hidden + _attributes_direct + _attributes_dsinfo

    def __init__(self, task_dict, source_url=None, timestamp=None):
        self._timestamp = timestamp
        self._sourceurl = source_url
        # normalize the PanDA server's camelCase keys to lowercase attribute names
        self._fulldict = {k.lower(): v for k, v in task_dict.items()}
        for aname in self._attributes_direct:
            value = self._fulldict.get(aname)
            # datetimes are decoded as datetime objects; stringify for display
            if aname in ("creationdate", "modificationtime") and value is not None:
                value = str(value)
            setattr(self, aname, value)
        for aname in self._attributes_dsinfo:
            if aname.startswith("pct"):
                setattr(self, aname, f"{self._fulldict.get(aname)}%")
            else:
                setattr(self, aname, f"{self._fulldict.get(aname)}")
        self._weburl = f"https://bigpanda.cern.ch/task/{self.jeditaskid}/"

    def is_terminated(self):
        return self.superstatus in task_final_superstatus_list

    @staticmethod
    def _status_text(status):
        return Text(status or "", style=_STATUS_STYLE.get(status or "", ""))

    @staticmethod
    def make_table_standard():
        # Columns with numeric values (JediTaskID, ReqID, Progress, Status) are right-justified - it's conventional to right-align numbers so their digits line up vertically.
        # Text columns (CreationDate, ModificationTime, TaskName, etc.) use the default left alignment.
        t = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold")
        t.add_column("JediTaskID", justify="right")
        t.add_column("ReqID", justify="right")
        t.add_column("Status", justify="right")
        t.add_column("Progress", justify="right")
        t.add_column("TaskName")
        return t

    def add_row_standard(self, table):
        url = str(self._weburl)
        table.add_row(
            Text(str(self.jeditaskid), style=f"link {url}"),
            str(self.reqid),
            self._status_text(self.status),
            str(self.pctfinished),
            str(self.taskname),
        )

    @staticmethod
    def make_table_long():
        # Columns with numeric values (JediTaskID, ReqID, Progress, Status) are right-justified - it's conventional to right-align numbers so their digits line up vertically.
        # Text columns (CreationDate, ModificationTime, TaskName, etc.) use the default left alignment.
        t = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold")
        t.add_column("JediTaskID", justify="right")
        t.add_column("Status", justify="right")
        t.add_column("CreationDate")
        t.add_column("ModificationTime")
        t.add_column("ReqID", justify="right")
        t.add_column("Progress", justify="right")
        t.add_column("Files (done|failed|total)")
        t.add_column("TaskName")
        t.add_column("URL")
        return t

    def add_row_long(self, table):
        url = str(self._weburl)
        table.add_row(
            Text(str(self.jeditaskid), style=f"link {url}"),
            self._status_text(self.status),
            str(self.creationdate),
            str(self.modificationtime),
            str(self.reqid),
            str(self.pctfinished),
            f"{self.nfilesfinished}|{self.nfilesfailed}|{self.nfiles}",
            str(self.taskname),
            Text(url, style=f"link {url}"),
        )

    def print_plain(self):
        t = Table(box=box.SIMPLE, show_header=True, header_style="bold", title=f"Task {self.jeditaskid}")
        t.add_column("Attribute")
        t.add_column("Value")
        for aname in self.__slots__:
            if aname == "_fulldict":
                continue
            t.add_row(aname, str(getattr(self, aname, None)))
        _console.print(t)


class LocalNativeWorkflowSpec(object):
    """A PanDA native workflow as the server reports it, rendered for pbook

    The server answers with one dictionary holding the workflow, its steps and its data, so this
    holds that dictionary rather than flattening it into attributes: the parts are tables, not
    fields of one record.
    """

    def __init__(self, workflow_dict):
        self._dict = workflow_dict or {}
        self._step_name_by_id = {step.get("step_id"): step.get("name") for step in self._dict.get("steps", [])}
        # set while rendering the plain format, where a terminal hyperlink is as unwanted as colour
        self._plain = False

    @staticmethod
    def _status_text(status):
        return Text(status or "", style=_WORKFLOW_STATUS_STYLE.get(status or "", ""))

    @staticmethod
    def _task_url(target_id):
        return f"https://bigpanda.cern.ch/task/{target_id}/"

    def _target_text(self, step):
        """The step's target, linked to BigPanDA when it is a task and plain when it is not"""
        target_id = step.get("target_id")
        if not target_id:
            return "-"
        # a sub-workflow step targets a child workflow, and an unsubmitted step can still hold the
        # ${TASKID} placeholder; neither is a task to link to
        if not self._plain and step.get("flavor") == "panda_task" and str(target_id).isdigit():
            url = self._task_url(target_id)
            return Text(str(target_id), style=f"link {url}")
        return str(target_id)

    def _parents_text(self, step):
        names = [self._step_name_by_id.get(parent_id) or str(parent_id) for parent_id in step.get("parent_step_ids") or []]
        return ", ".join(sorted(names)) or "-"

    @staticmethod
    def _progress_text(step):
        progress = step.get("progress")
        if not progress:
            # nothing to measure: not a task, or not submitted yet
            return "-"
        return f"{progress.get('pct_finished')}%"

    @staticmethod
    def _files_text(step):
        progress = step.get("progress")
        if not progress:
            return "-"
        return f"{progress.get('n_files_finished')}|{progress.get('n_files_failed')}|{progress.get('n_files')}"

    @staticmethod
    def _quoted(value):
        """Quote a free-text field, since a workflow name or a user name can contain spaces

        Without it, "user=Rody Wee" leaves no way to see where the value ends, and a workflow name
        with a space in it would break the line in the middle.
        """
        return f'"{value}"' if value else "-"

    def _header_text(self, separator="  "):
        """The header line, with the workflow's status coloured as the steps' statuses are"""
        header = Text()
        header.append(f"workflow_id={self._dict.get('workflow_id')}{separator}name={self._quoted(self._dict.get('name'))}{separator}status=")
        header.append_text(self._status_text(self._dict.get("status")))
        header.append(f"{separator}submitted_as={self._dict.get('submitted_as')}{separator}user={self._quoted(self._dict.get('user'))}")
        return header

    def _print_header(self, console, separator="  "):
        console.print(self._header_text(separator), highlight=False)
        if self._dict.get("parent_id"):
            console.print(f"  sub-workflow of workflow_id={self._dict['parent_id']}", highlight=False)
        if self._dict.get("reason"):
            # a failed workflow should say why without needing a second command
            console.print(f"  reason: {self._dict['reason']}", highlight=False)

    def _make_steps_table(self, long=False):
        t = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold", title_justify="left")
        t.add_column("#", justify="right")
        t.add_column("Name")
        t.add_column("Status", justify="right")
        t.add_column("Flavor")
        t.add_column("PSLabel")
        t.add_column("Progress", justify="right")
        if long:
            t.add_column("Files (done|failed|total)")
        t.add_column("TargetID", justify="right")
        t.add_column("Parents")
        if long:
            t.add_column("Reason")
        for step in self._dict.get("steps", []):
            row = [
                str(step.get("member_id") if step.get("member_id") is not None else "-"),
                str(step.get("name")),
                self._status_text(step.get("status")),
                str(step.get("flavor")),
                str(step.get("prod_source_label") or "-"),
                self._progress_text(step),
            ]
            if long:
                row.append(self._files_text(step))
            row.append(self._target_text(step))
            row.append(self._parents_text(step))
            if long:
                row.append(str(step.get("reason") or "-"))
            t.add_row(*row)
        return t

    def _make_data_table(self):
        t = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold", title_justify="left")
        t.add_column("Name")
        t.add_column("Status", justify="right")
        t.add_column("Type")
        t.add_column("TargetID")
        for data in self._dict.get("data", []):
            t.add_row(
                str(data.get("name")),
                self._status_text(data.get("status")),
                str(data.get("type")),
                str(data.get("target_id") or "-"),
            )
        return t

    def _print_sections(self, console, long=False, show_data=False):
        steps = self._dict.get("steps", [])
        console.print(f"\n steps ({len(steps)}):", highlight=False)
        # an empty section is said, not drawn: a table of headings with no rows under it reads as
        # though something failed to load
        if steps:
            console.print(self._make_steps_table(long=long))
        data = self._dict.get("data", [])
        if not show_data:
            # the dataset names are long enough to dominate the output, and the steps are what is
            # usually being looked at, so the count is reported and the table left out
            console.print(f" data ({len(data)}), not shown; pass --show_data to see it", highlight=False)
            return
        console.print(f" data ({len(data)}):", highlight=False)
        if data:
            console.print(self._make_data_table())

    def print_standard(self, long=False, show_data=False):
        self._print_header(_console)
        self._print_sections(_console, long=long, show_data=show_data)

    def print_json(self):
        print(json.dumps(self._dict, indent=2))

    def print_plain(self, show_data=False):
        # no colour and no links, for piping somewhere that cannot render them
        plain_console = Console(width=10000, no_color=True, highlight=False)
        self._plain = True
        try:
            # the console strips the colour; a single space keeps the line machine-readable
            self._print_header(plain_console, separator=" ")
            self._print_sections(plain_console, show_data=show_data)
        finally:
            self._plain = False
