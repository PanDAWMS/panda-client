import argparse
import sys
from collections import OrderedDict

class GroupArgParser(argparse.ArgumentParser):
    def __init__(self, usage, conflict_handler):
        self.groups_dict = OrderedDict()
        self.briefHelp = None
        self.examples =  ""
        super(GroupArgParser, self).__init__(usage=usage, conflict_handler=conflict_handler)

    def set_examples(self, examples):
        self.examples = examples

    def add_group(self, name, desc=None, usage=None):
        # group = argparse._ArgumentGroup(self, name, desc)
        group = self.MyArgGroup(self, name, desc)
        group.usage = usage
        self.groups_dict[name.upper()] = group
        return group

    def update_action_groups(self):
        for group in self.groups_dict.values():
            self._action_groups.append(group)

    def add_helpGroup(self, addHelp=None):
        help='Print individual group help (the group name is not case-sensitive), where "ALL" will print all groups together.'
        if addHelp:
           help += ' ' + addHelp
        choices_m = self.MyList(list(self.groups_dict.keys()) + ['ALL'])
        self.add_argument('--helpGroup', choices=choices_m, action=self.print_groupHelp, help=help)

        try:
            from cStringIO import StringIO
        except Exception:
            from io import StringIO
        old_stdout = sys.stdout
        sys.stdout = self.briefHelp = StringIO()
        self.print_help()
        sys.stdout = old_stdout

        self.update_action_groups()
        self.add_argument('-h', '--help', action=self.print_briefHelp, nargs=0, help="Print this help")

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
            return super(GroupArgParser.MyList,self).__contains__(other.upper())

    class print_briefHelp(argparse.Action):
         def __call__(self, parser, namespace, values, option_string=None):
             briefHelp = parser.briefHelp
             if briefHelp != None:
                briefHelp.seek(0)
                print(''.join(briefHelp.readlines()))
             print(parser.examples)
             sys.exit(0)

    class print_groupHelp(argparse.Action):
         def __init__(self, option_strings, dest, nargs=None, **kwargs):
             if nargs is not None:
                 raise ValueError("nargs not allowed")
             super(GroupArgParser.print_groupHelp, self).__init__(option_strings, dest, **kwargs)

         def __call__(self, parser, namespace, values, option_string=None):
             values = values.upper()
             groups = parser.groups_dict
             if values == 'ALL':
                parser.print_help()
             elif values in groups.keys():
                group = groups[values]
                formatter = parser._get_formatter()
                formatter.start_section(group.title)
                formatter.add_text(group.description)
                formatter.add_arguments(group._group_actions)
                formatter.end_section()
                print(formatter.format_help())
                if group.usage:
                   print(group.usage)
             else:
                raise Exception("!!!ERROR!!! Unknown group name=%s" % values)
             sys.exit(0)
