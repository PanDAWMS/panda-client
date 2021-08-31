import os
import tempfile
import subprocess
import glob
import base64


# check workflow description
def check(cwl_file, yaml_file, output_name, verbose, log_stream):
    # create dummy yaml if empty since cwl-runner doesn't like it
    tmp_yaml = None
    if not os.path.getsize(yaml_file):
        tmp_yaml = tempfile.NamedTemporaryFile(delete=False)
        yaml_file = tmp_yaml.name
        tmp_yaml.write('a: \n'.encode())
        tmp_yaml.close()

    # links to common CWL files since cwl-runner doesn't respect XDG_DATA_DIRS or XDG_DATA_HOME
    linked_cwl = []
    for tmp_cwl in glob.glob(os.path.join(os.environ['PANDA_SYS'], 'etc/panda/share/cwl/*.cwl')):
        tmp_base = os.path.basename(tmp_cwl)
        if not os.path.exists(tmp_base):
            os.symlink(tmp_cwl, tmp_base)
        linked_cwl.append(tmp_base)

    # run cwl-runner
    new_env = os.environ.copy()
    new_env['WORKFLOW_OUTPUT_BASE'] = output_name
    new_env['WORKFLOW_HOME'] = os.getcwd()
    proc = subprocess.Popen(['cwl-runner', '--disable-color', '--preserve-entire-environment', cwl_file, yaml_file],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            universal_newlines=True,
                            env=new_env)
    stdout, stderr = proc.communicate()
    is_important = True
    for line in stderr.split('\n'):
        if verbose:
            print(line)
            continue
        # decode base64-encoded raw message
        if ':::<base64>:::' in line:
            msg_level = line.split()[0]
            line = line.split(':')[-1]
            line = '{} : {}'.format(msg_level, base64.b64decode(line.encode()).decode().replace('<br>', '\n'))
        tags = line.split()
        if not tags:
            continue
        if line.startswith('INFO [job') or line.startswith('Traceback'):
            is_important = False
        elif tags[0] in ['INFO', 'DEBUG', 'WARNING', 'ERROR'] or is_important:
            is_important = True
        if is_important:
            print(line)
    if verbose:
        for line in stdout.split('\n'):
            print(line)

    if proc.returncode != 0:
        err_str = 'Failed to parse the workflow description'
        if not verbose:
            err_str += '. --debug might give more info if the error message is unclear'
        log_stream.error(err_str)
    else:
        log_stream.info('Successfully verified the workflow description')
    # remove dummy yaml
    if tmp_yaml:
        os.remove(tmp_yaml.name)

    # remove links
    for tmp_base in linked_cwl:
        os.remove(tmp_base)


# construct message using <br> since \n is sometimes converted to n when python is executed through cwl-runner
def make_message(new_msg, old_msg=None):
    if old_msg is None:
        old_msg = ''
    return "{}{}<br>".format(old_msg, new_msg)


# encode message
def encode_message(msg_str):
    return ':::<base64>:::' + base64.b64encode(msg_str.encode()).decode()


# emphasize a single message
def emphasize_single_message(msg_str):
    msg_str = make_message(msg_str)
    return encode_message(msg_str)
