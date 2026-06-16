task_active_superstatus_list = ["running", "submitting", "registered", "ready"]
task_final_superstatus_list = ["finished", "failed", "done", "broken", "aborted"]

from rich import box
from rich.console import Console
from rich.table import Table
from rich.text import Text

_console = Console()

_STATUS_STYLE = {
    "running": "bold cyan",
    "submitting": "dark_orange",
    "registered": "dark_orange",
    "ready": "dark_orange",
    "done": "bold green",
    "finished": "green",
    "failed": "red",
    "broken": "bold red",
    "aborted": "bold red",
    "exhausted": "magenta",
    "paused": "dark_orange",
    "throttled": "dark_orange",
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
        t = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold")
        t.add_column("JediTaskID", justify="right")
        t.add_column("ReqID", justify="right")
        t.add_column("Status", justify="right")
        t.add_column("Progress", justify="right")
        t.add_column("TaskName")
        return t

    @staticmethod
    def make_table_long():
        t = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold")
        t.add_column("JediTaskID", justify="right", no_wrap=True)
        t.add_column("Status", justify="right", no_wrap=True)
        t.add_column("CreationDate", no_wrap=True)
        t.add_column("ModificationTime", no_wrap=True)
        t.add_column("ReqID", justify="right", no_wrap=True)
        t.add_column("Progress", justify="right", no_wrap=True)
        t.add_column("Files (done|failed|total)", no_wrap=True)
        t.add_column("TaskName")
        t.add_column("URL", no_wrap=True, overflow="ellipsis", max_width=40)
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
