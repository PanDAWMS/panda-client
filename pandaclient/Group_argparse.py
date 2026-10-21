import argparse
import re
import shutil
import sys
import textwrap
from collections import OrderedDict

# cap the help width on wide terminals, since very long lines are hard to read
MAX_HELP_WIDTH = 120

# bash completion script filled in by GroupArgParser.format_bash_completion. The @@NAME@@ placeholders
# (as in templates/) are replaced instead of using str.format, so that the bash braces need no escaping
BASH_COMPLETION_TEMPLATE = """# bash tab-completion for @@PROG@@
# Generated from the argument parser by packages/hatch_build.py at build time. Do not edit by hand.

@@FUNC@@() {
    local cur prev opts
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    # bash splits --opt=value at "=" into separate words
    if [[ "$cur" == "=" ]]; then
        cur=""
    elif [[ "$prev" == "=" ]]; then
        prev="${COMP_WORDS[COMP_CWORD-2]}"
    fi
    opts="@@OPTIONS@@"

    case "$prev" in
@@CASES@@
    *)
        if [[ "$cur" == -* ]]; then
            COMPREPLY=($(compgen -W "$opts" -- "$cur"))
        else
@@POSITIONAL@@
        fi
        ;;
    esac
    return 0
}
complete -F @@FUNC@@ @@PROG@@
"""


# check string args if they can be encoded with utf-8
class ActionWithUnicodeCheck(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if isinstance(values, str):
            try:
                values.encode()
            except Exception:
                parser.exit(1, message=f"ERROR: argument --{self.dest} cannot be encoded with utf-8: '{values}'\n")
        setattr(namespace, self.dest, values)


class GroupHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """Help formatter that follows the terminal width and never splits long words such as URLs

    Descriptions and epilogs are printed as written (usage texts and examples are pre-formatted),
    while the option help texts are wrapped to the terminal width.
    """

    def __init__(self, prog, indent_increment=2, max_help_position=24, width=None):
        if width is None:
            width = min(shutil.get_terminal_size().columns - 2, MAX_HELP_WIDTH)
        super().__init__(prog, indent_increment, max_help_position, width)

    def _split_lines(self, text, width):
        text = self._whitespace_matcher.sub(" ", text).strip()
        return textwrap.wrap(text, width, break_long_words=False, break_on_hyphens=False)


class GroupArgParser(argparse.ArgumentParser):
    def __init__(self, usage, conflict_handler):
        self.groups_dict = OrderedDict()
        self.examples = ""
        super().__init__(usage=usage.rstrip(), conflict_handler=conflict_handler, formatter_class=GroupHelpFormatter)

    def set_examples(self, examples):
        # shown at the end of both the brief and the full help
        self.examples = examples
        self.epilog = self.examples

    def add_group(self, name, desc=None, usage=None):
        # the usage is appended to the description so that it is shown in the full help too
        full_desc = desc
        if usage:
            full_desc = f"{desc}\n\n{usage.strip()}" if desc else usage.strip()
        group = self.MyArgGroup(self, name, full_desc)
        group.short_desc = desc
        self.groups_dict[name.upper()] = group
        return group

    def update_action_groups(self):
        for group in self.groups_dict.values():
            self._action_groups.append(group)

    def add_helpGroup(self, addHelp=None):
        help = 'Print the help of one option group (listed by --help; not case-sensitive), or of all groups with "ALL"'
        if addHelp:
            help += ". " + addHelp
        choices_m = self.MyList(list(self.groups_dict.keys()) + ["ALL"])
        self.add_argument("--helpGroup", choices=choices_m, metavar="GROUP", action=self.print_groupHelp, help=help)
        self.update_action_groups()
        self.add_argument("-h", "--help", action=self.print_briefHelp, nargs=0, help="Print this help")

    def format_brief_help(self):
        """Format the short help: usage, list of option groups, the help options and the examples"""
        formatter = self._get_formatter()
        formatter.add_usage(self.usage, self._actions, self._mutually_exclusive_groups)
        formatter.add_text(self.description)

        name_width = max(len(name) for name in list(self.groups_dict) + ["ALL"])
        lines = [f"{name.ljust(name_width)}  {group.short_desc or ''}" for name, group in self.groups_dict.items()]
        lines.append(f"{'ALL'.ljust(name_width)}  all of the above")
        formatter.start_section("option groups (use --helpGroup GROUP to see their options)")
        formatter.add_text("\n".join(lines))
        formatter.end_section()

        formatter.start_section("help options")
        formatter.add_arguments([action for action in self._actions if action.dest in ("helpGroup", "help")])
        formatter.end_section()

        formatter.add_text(self.examples)
        return formatter.format_help()

    def format_bash_completion(self, prog, positional_glob=None):
        """Generate a standalone bash completion script for the options of this parser

        Options with choices complete their choices, options whose metavar is FILE (or a list of FILE)
        complete file names, options whose metavar ends with DIR complete directories, and other options
        taking a value get no suggestions. Hidden options are not offered.

        args:
            prog: name of the command to complete
            positional_glob: glob of files to suggest for positional arguments, e.g. "*.py", or None
        returns:
            bash script as a string
        """
        options, file_opts, dir_opts, value_opts = [], [], [], []
        choice_cases = []
        for action in self._actions:
            if not action.option_strings or action.help == argparse.SUPPRESS:
                continue
            options += action.option_strings
            if action.nargs == 0:
                continue
            opts = "|".join(action.option_strings)
            metavar = action.metavar if isinstance(action.metavar, str) else ""
            if action.choices:
                words = " ".join(str(choice) for choice in action.choices)
                choice_cases.append(f'        {opts}) COMPREPLY=($(compgen -W "{words}" -- "$cur")) ;;')
            elif metavar == "FILE" or metavar.startswith("FILE["):
                file_opts += action.option_strings
            elif metavar.endswith("DIR"):
                dir_opts += action.option_strings
            else:
                value_opts += action.option_strings

        cases = choice_cases[:]
        if file_opts:
            cases.append(f'        {"|".join(file_opts)}) compopt -o filenames 2>/dev/null; COMPREPLY=($(compgen -f -- "$cur")) ;;')
        if dir_opts:
            cases.append(f'        {"|".join(dir_opts)}) compopt -o filenames 2>/dev/null; COMPREPLY=($(compgen -d -- "$cur")) ;;')
        if value_opts:
            cases.append(f'        {"|".join(value_opts)}) COMPREPLY=() ;;')
        if positional_glob:
            positional = f"""            compopt -o filenames 2>/dev/null
            COMPREPLY=($(compgen -d -- "$cur") $(compgen -f -X '!{positional_glob}' -- "$cur"))"""
        else:
            positional = '            COMPREPLY=($(compgen -W "$opts" -- "$cur"))'
        params = {
            "PROG": prog,
            "FUNC": "_" + re.sub(r"\W", "_", prog) + "_completion",
            "OPTIONS": " ".join(options),
            "CASES": "\n".join(cases),
            "POSITIONAL": positional,
        }
        script = BASH_COMPLETION_TEMPLATE
        for key, value in params.items():
            script = script.replace(f"@@{key}@@", value)
        return script

    def shareWithGroup(self, action, group):
        # share option action to another group
        if action and group:
            if action not in group._group_actions:
                group._group_actions.append(action)

    class MyArgGroup(argparse._ArgumentGroup):
        def shareWithMe(self, action):
            self._group_actions.append(action)

    class MyList(list):
        # list subclass that uses upper() when testing for 'in'
        def __contains__(self, other):
            return super().__contains__(other.upper())

    class print_briefHelp(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            parser._print_message(parser.format_brief_help(), sys.stdout)
            sys.exit(0)

    class print_groupHelp(argparse.Action):
        def __init__(self, option_strings, dest, nargs=None, **kwargs):
            if nargs is not None:
                raise ValueError("nargs not allowed")
            super().__init__(option_strings, dest, **kwargs)

        def __call__(self, parser, namespace, values, option_string=None):
            values = values.upper()
            groups = parser.groups_dict
            if values == "ALL":
                parser.print_help()
            elif values in groups.keys():
                group = groups[values]
                formatter = parser._get_formatter()
                formatter.start_section(group.title)
                formatter.add_text(group.description)
                formatter.add_arguments(group._group_actions)
                formatter.end_section()
                print(formatter.format_help())
            else:
                raise Exception("!!!ERROR!!! Unknown group name=%s" % values)
            sys.exit(0)


# factory method
def get_parser(usage, conflict_handler):
    p = GroupArgParser(usage, conflict_handler)
    p.register("action", "store", ActionWithUnicodeCheck)
    return p
