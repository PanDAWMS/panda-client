import os
import tempfile
import subprocess
import glob
import base64


def check(cwl_file, yaml_file, output_name, verbose, log_stream):
    # create dummy yaml if empty since cwl-runner doesn't like it
    tmp_yaml = None
    if not os.path.getsize(yaml_file):
        tmp_yaml = tempfile.NamedTemporaryFile(delete=False)
        yaml_file = tmp_yaml.name
        tmp_yaml.write('a: \n')
        tmp_yaml.close()

    # links to common CWL files since cwl-runner doesn't respect XDG_DATA_DIRS or XDG_DATA_HOME
    linked_cwl = []
    for tmp_cwl in glob.glob(os.path.join(os.environ['PANDA_SYS'], 'etc/panda/share/cwl/*.cwl')):
        tmp_base = os.path.basename(tmp_cwl)
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
        if line.startswith('INFO : <base64>:'):
            line = line.split(':')[-1]
            line = 'INFO : {}'.format(base64.b64decode(line.encode()).replace('<br>', '\n'))
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
            err_str += '. --debug would give more info'
        log_stream.error(err_str)
    else:
        log_stream.info('Successfully verified the workflow description')
    # remove dummy yaml
    if tmp_yaml:
        os.remove(tmp_yaml.name)

    # remove links
    for tmp_base in linked_cwl:
        os.remove(tmp_base)

