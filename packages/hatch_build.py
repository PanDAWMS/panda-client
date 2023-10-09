from hatchling.builders.hooks.plugin.interface import BuildHookInterface

import os
import re
import sys
import stat
import glob
import sysconfig
import distutils


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        # chmod +x
        for f in glob.glob("./scripts/*"):
            st = os.stat(f)
            os.chmod(f, st.st_mode | stat.S_IEXEC)

        # parameters to be resolved
        self.params = {}
        self.params['install_dir'] = os.environ.get('PANDA_INSTALL_TARGET')
        if self.params['install_dir']:
            # non-standard installation path
            self.params['install_purelib'] = self.params['install_dir']
            self.params['install_scripts'] = os.path.join(self.params['install_dir'], 'bin')
        else:
            self.params['install_dir'] = sys.prefix
            try:
                # python3.2 or higher
                self.params['install_purelib'] = sysconfig.get_path('purelib')
                self.params['install_scripts'] = sysconfig.get_path('scripts')
            except Exception:
                # old python
                self.params['install_purelib'] = distutils.sysconfig.get_python_lib()
                self.params['install_scripts'] = os.path.join(sys.prefix, 'bin')
        for k in self.params:
            path = self.params[k]
            self.params[k] = os.path.abspath(os.path.expanduser(path))

        # instantiate templates
        for in_f in glob.glob("./templates/*"):
            if not in_f.endswith('.template'):
                continue
            with open(in_f) as in_fh:
                file_data = in_fh.read()
                # replace patterns
                for item in re.findall(r'@@([^@]+)@@', file_data):
                    if item not in self.params:
                        raise RuntimeError('unknown pattern %s in %s' % (item, in_f))
                    # get pattern
                    patt = self.params[item]
                    # convert to absolute path
                    if item.startswith('install'):
                        patt = os.path.abspath(patt)
                    # remove build/*/dump for bdist
                    patt = re.sub('build/[^/]+/dumb', '', patt)
                    # remove /var/tmp/*-buildroot for bdist_rpm
                    patt = re.sub('/var/tmp/.*-buildroot', '', patt)
                    # replace
                    file_data = file_data.replace('@@%s@@' % item, patt)
                out_f = re.sub(r'\.template$', '', in_f)
                with open(out_f, 'w') as out_fh:
                    out_fh.write(file_data)

        # post install only for client installation
        if not os.path.exists(os.path.join(self.params['install_purelib'], 'pandacommon')):
            target = 'pandatools'
            if not os.path.exists(os.path.join(self.params['install_purelib'], target)):
                os.symlink('pandaclient', target)
