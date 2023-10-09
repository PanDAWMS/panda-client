# set PYTHONPATH to use the current directory first
import sys
import os
import re
import site
from io import open

sys.path.insert(0,'.')

# get release version
from pandaclient import PandaToolsPkgInfo
release_version = PandaToolsPkgInfo.release_version

from setuptools import setup
from setuptools.command.install import install as install_org
# import distutils after setuptools to tweak sys.modules so that the distutils module in setuptools is used
import distutils
from distutils.command.install_data import install_data as install_data_org


# custom install to disable egg
class install_panda (install_org):
    def finalize_options (self):
        install_org.finalize_options(self)
        self.single_version_externally_managed = True


# generates files using templates and install them
class install_data_panda (install_data_org):
    def initialize_options (self):
        install_data_org.initialize_options (self)
        self.prefix = None
        self.root   = None
        self.install_purelib = None
        self.install_scripts = None

    def finalize_options (self):
        # set install_purelib
        self.set_undefined_options('install',
                                   ('prefix','prefix'))
        self.set_undefined_options('install',
                                   ('root','root'))
        self.set_undefined_options('install',
                                   ('install_purelib','install_purelib'))
        self.set_undefined_options('install',
                                   ('install_scripts','install_scripts'))
                                            
    def run (self):
        rpmInstall = False
        # set install_dir
        if not self.install_dir:
            if self.root:
                # rpm or wheel
                self.install_dir = self.prefix
                self.install_purelib = distutils.sysconfig.get_python_lib()
                self.install_scripts = os.path.join(self.prefix, 'bin')
                rpmInstall = True
            else:
                # sdist
                if not self.prefix:
                    if '--user' in self.distribution.script_args:
                        self.install_dir = site.USER_BASE
                    else:
                        self.install_dir = site.PREFIXES[0]
                else:
                    self.install_dir = self.prefix
        #raise Exception, (self.install_dir, self.prefix, self.install_purelib)
        self.install_dir = os.path.expanduser(self.install_dir)
        self.install_dir = os.path.abspath(self.install_dir)
        # remove /usr for bdist/bdist_rpm
        match = re.search('(build/[^/]+/dumb)/usr',self.install_dir)
        if match is not None:
            self.install_dir = re.sub(match.group(0),match.group(1),self.install_dir)
        # remove /var/tmp/*-buildroot for bdist_rpm
        match = re.search('(/var/tmp/.*-buildroot)/usr',self.install_dir)
        if match is not None:
            self.install_dir = re.sub(match.group(0),match.group(1),self.install_dir)
        # create tmp area
        tmpDir = 'build/tmp'
        self.mkpath(tmpDir)
        new_data_files = []
        autoGenFiles = []
        for destDir,dataFiles in self.data_files:
            newFilesList = []
            for srcFile in dataFiles:
                # dest filename
                destFile = re.sub('\.template$','',srcFile)
                # append
                newFilesList.append(destFile)
                if destFile == srcFile:
                    continue
                autoGenFiles.append(destFile)
                # open src
                inFile = open(srcFile)
                # read
                filedata=inFile.read()
                # close
                inFile.close()
                # replace patterns
                for item in re.findall('@@([^@]+)@@',filedata):
                    if not hasattr(self,item):
                        raise RuntimeError('unknown pattern %s in %s' % (item,srcFile))
                    # get pattern
                    patt = getattr(self,item)
                    # convert to absolute path
                    if item.startswith('install'):
                        patt = os.path.abspath(patt)
                    # remove build/*/dump for bdist
                    patt = re.sub('build/[^/]+/dumb','',patt)
                    # remove /var/tmp/*-buildroot for bdist_rpm
                    patt = re.sub('/var/tmp/.*-buildroot','',patt)
                    # replace
                    filedata = filedata.replace('@@%s@@' % item, patt)
                # write to dest
                oFile = open(destFile,'w')
                oFile.write(filedata)
                oFile.close()
            # replace dataFiles to install generated file
            new_data_files.append((destDir,newFilesList))
        # install
        self.data_files = new_data_files
        install_data_org.run(self)
        # post install only for client installation
        if not os.path.exists(os.path.join(self.install_purelib, 'pandacommon')):
            target = os.path.join(self.install_purelib, 'pandatools')
            if not os.path.exists(target):
                os.symlink('pandaclient', target)
        # delete
        for autoGenFile in autoGenFiles:
            try:
                os.remove(autoGenFile)
            except Exception:
                pass

with open('README.md', 'r', encoding='utf-8') as description_file:
    long_description = description_file.read()

setup(
    name="panda-client",
    version=release_version,
    description='PanDA Client Package',
    long_description=long_description,
    long_description_content_type='text/markdown',
    license='GPL',
    author='PanDA Team',
    author_email='atlas-adc-panda@cern.ch',
    url='https://panda-wms.readthedocs.io/en/latest/',
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*',
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],

    # optional pip dependencies
    extras_require={
        'jupyter': ['pandas', 'jupyter-dash'],
    },

    packages = [ 'pandaclient',
                 ],
    scripts = [ 'scripts/prun', 
                'scripts/pcontainer',
                'scripts/pbook',
                'scripts/pathena',
                'scripts/phpo',
                'scripts/pchain',
                ],
    data_files = [ ('etc/panda', ['templates/panda_setup.sh.template',
                                  'templates/panda_setup.csh.template',
                                  'templates/panda_setup.example.cfg.template',
                                  'templates/site_path.sh.template',
                                  'glade/pbook.glade',
                                  ]
                    ),
                   ('etc/panda/icons', ['icons/retry.png',
                                        'icons/update.png',
                                        'icons/kill.png',
                                        'icons/pandamon.png',
                                        'icons/savannah.png',
                                        'icons/config.png',
                                        'icons/back.png',
                                        'icons/sync.png',
                                        'icons/forward.png',
                                        'icons/red.png',
                                        'icons/green.png',
                                        'icons/yellow.png',
                                        'icons/orange.png',                                        
                                        ]
                    ),
                   ('etc/panda/share', ['share/FakeAppMgr.py',
                                        'share/ConfigExtractor.py',
                                        'share/functions.sh'
                                        ]
                    ),
                   ],
    cmdclass={
        'install': install_panda,
        'install_data': install_data_panda
    }
)
