# How to install                                                                                                                                                                                                                                                                                                  
```                                                                                                                                                                                                                                                                                                               
pip install panda-client
```
Then set up the environment (the path is the prefix of the Python environment you installed into, e.g. your virtualenv):
```
source <prefix>/etc/panda/panda_setup.[c]sh
prun -h
pathena -h
```

## Installing from source
For development, or to install into a custom location:
```                                                                                                                                                                                                                                                            
git clone https://github.com/PanDAWMS/panda-client.git
cd panda-client
PANDA_INSTALL_TARGET=/path/to/install/dir pip install .
```

# Release Notes
https://github.com/PanDAWMS/panda-client/releases

# ATLAS CVMFS deployment 
Request atlas-adc-tier3sw-install to install the new specific version on CVMFS. They will download the package from the github release page.

# Uploading to pip
Publishing to PyPI is handled automatically by the `Upload Python Package` GitHub
Actions workflow when a release is published. To build the distributions locally:
```
python -m build -s
```
Uploading source so that the wheel generates setup files locally.