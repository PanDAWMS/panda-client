import distutils
import glob
import os
import re
import shutil
import stat
import sys
import sysconfig

from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    def initialize(self, version, build_data):
        # track build-time artifacts created below so finalize() can remove them
        self._generated_files = []
        self._created_symlinks = []

        # chmod +x
        for f in glob.glob("./scripts/*"):
            st = os.stat(f)
            os.chmod(f, st.st_mode | stat.S_IEXEC)

        # parameters to be resolved
        self.params = {}
        self.params["install_dir"] = os.environ.get("PANDA_INSTALL_TARGET")
        if self.params["install_dir"]:
            # non-standard installation path
            self.params["install_purelib"] = os.path.join(
                self.params["install_dir"], re.sub(sysconfig.get_path("data"), "", sysconfig.get_path("purelib")).lstrip("/")
            )
            self.params["install_scripts"] = os.path.join(
                self.params["install_dir"], re.sub(sysconfig.get_path("data"), "", sysconfig.get_path("scripts")).lstrip("/")
            )
        else:
            try:
                # python3.2 or higher
                self.params["install_dir"] = sysconfig.get_path("data")
                self.params["install_purelib"] = sysconfig.get_path("purelib")
                self.params["install_scripts"] = sysconfig.get_path("scripts")
            except Exception:
                # old python
                self.params["install_dir"] = sys.prefix
                self.params["install_purelib"] = distutils.sysconfig.get_python_lib()
                self.params["install_scripts"] = os.path.join(sys.prefix, "bin")
        for k in self.params:
            path = self.params[k]
            self.params[k] = os.path.abspath(os.path.expanduser(path))

        # instantiate templates
        for in_f in glob.glob("./templates/*"):
            if not in_f.endswith(".template"):
                continue
            with open(in_f) as in_fh:
                file_data = in_fh.read()
                # replace patterns
                for item in re.findall(r"@@([^@]+)@@", file_data):
                    if item not in self.params:
                        raise RuntimeError("unknown pattern %s in %s" % (item, in_f))
                    # get pattern
                    patt = self.params[item]
                    # convert to absolute path
                    if item.startswith("install"):
                        patt = os.path.abspath(patt)
                    # remove build/*/dump for bdist
                    patt = re.sub("build/[^/]+/dumb", "", patt)
                    # remove /var/tmp/*-buildroot for bdist_rpm
                    patt = re.sub("/var/tmp/.*-buildroot", "", patt)
                    # replace
                    file_data = file_data.replace("@@%s@@" % item, patt)
                out_f = re.sub(r"\.template$", "", in_f)
                with open(out_f, "w") as out_fh:
                    out_fh.write(file_data)
                self._generated_files.append(out_f)

        # post install only for client installation
        if not os.path.exists(os.path.join(self.params["install_purelib"], "pandacommon")):
            target = "pandatools"
            if not os.path.exists(os.path.join(self.params["install_purelib"], target)) and not os.path.exists(target):
                os.symlink("pandaclient", target)
                self._created_symlinks.append(target)

    def finalize(self, version, build_data, artifact_path):
        # remove build-time artifacts created by initialize() so in-place builds
        # (hatch build / pip install .) don't leave files in the working tree.
        # finalize() runs after the wheel is assembled, so they are already packaged.
        for f in self._generated_files:
            try:
                os.remove(f)
            except Exception:
                pass
        for link in self._created_symlinks:
            try:
                os.remove(link)  # unlinks the symlink itself, not its target (POSIX)
            except Exception:
                pass
        # remove the byte-cache hatchling created when it imported THIS hook module.
        # Safe: the module is already loaded in memory, so deleting its .pyc is fine.
        # Scoped to this dir only, so an in-place build never touches __pycache__ that a
        # developer created elsewhere (e.g. pandaclient/__pycache__).
        try:
            shutil.rmtree(os.path.join(os.path.dirname(os.path.abspath(__file__)), "__pycache__"))
        except Exception:
            pass
