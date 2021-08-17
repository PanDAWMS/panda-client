import os
import sys
import getpass
import subprocess
from IPython.core.magic import register_line_magic

try:
    import ConfigParser
except ImportError:
    import configparser as ConfigParser
from . import PLogger


# setup
def setup():
    tmp_log = PLogger.getPandaLogger()
    # parse config file
    conf_file = os.path.expanduser('~/.panda/panda_setup.cfg')
    if not os.path.exists(conf_file):
        tmp_log.error('panda conifg file is missing at {}'.format(conf_file))
        return False
    parser = ConfigParser.ConfigParser()
    parser.read(conf_file)
    section = parser['main']

    # variables
    panda_install_scripts = section['PANDA_INSTALL_SCRIPTS']
    panda_install_purelib = section['PANDA_INSTALL_PURELIB']
    panda_install_dir = section['PANDA_INSTALL_DIR']

    # PATH
    paths = os.environ['PATH'].split(':')
    if not panda_install_scripts in paths:
        paths.insert(0, panda_install_scripts)
        os.environ['PATH'] = ':'.join(paths)

    # PYTHONPATH
    if 'PYTHONPATH' not in os.environ:
        os.environ['PYTHONPATH'] = panda_install_purelib
    else:
        paths = os.environ['PYTHONPATH'].split(':')
        if panda_install_purelib not in paths:
            paths.insert(0, panda_install_scripts)
            os.environ['PYTHONPATH'] = ':'.join(paths)

    # env
    panda_env = {'PANDA_CONFIG_ROOT': '~/.pathena',
                 'PANDA_SYS': panda_install_dir,
                 "PANDA_PYTHONPATH": panda_install_purelib,
                 "PANDA_VERIFY_HOST": "off",
                 "PANDA_JUPYTER": "1",
                 }
    for i in ['PANDA_AUTH',
              'PANDA_AUTH_VO',
              'PANDA_URL_SSL',
              'PANDA_URL',
              'PANDAMON_URL',
              'X509_USER_PROXY',
              'PANDA_USE_NATIVE_HTTPLIB',
              'PANDA_NICKNAME',
              ]:
        try:
            panda_env[i] = section[i]
        except Exception:
            pass
    os.environ.update(panda_env)


# magic commands

GETPASS_STRINGS = ['Enter GRID pass phrase for this identity:']
RAWINPUT_STRINGS = ['>>> \n', "[y/n] \n"]


def _execute(command):
    with subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          stdin=subprocess.PIPE, universal_newlines=True) as p:
        while True:
            nextline = p.stdout.readline()
            if nextline == '' and p.poll() is not None:
                break
            # check if uses getpass or raw_input
            is_getpass = False
            is_raw_input = False
            for one_str in GETPASS_STRINGS:
                if one_str in nextline:
                    is_getpass = True
                    break
            for one_str in RAWINPUT_STRINGS:
                if one_str == nextline:
                    is_raw_input = True
                    break
            if not is_raw_input:
                sys.stdout.write(nextline)
                sys.stdout.flush()
            # need to call getpass or input since jupyter notebook doesn't pass stdin from subprocess
            st = None
            if is_getpass:
                st = getpass.getpass()
                p.stdin.write(st)
                p.stdin.flush()
            elif is_raw_input:
                st = input('\n' + one_str.strip())
            # feed stdin
            if st is not None:
                p.stdin.write(st + '\n')
                p.stdin.flush()

        output = p.communicate()[0]
        exit_code = p.returncode
        if exit_code == 0:
            if output:
                print(output)
        return exit_code


@register_line_magic
def pathena(line):
    _execute('pathena ' + line + ' -3')
    return


@register_line_magic
def prun(line):
    _execute('prun ' + line + ' -3')
    return


@register_line_magic
def phpo(line):
    _execute('phpo ' + line + ' -3')
    return


@register_line_magic
def pbook(line):
    _execute('pbook ' + line + ' --prompt_with_newline')
    return


del pathena, prun, phpo, pbook
