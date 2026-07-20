# How to install

Download the code
```
git clone https://github.com/PanDAWMS/panda-client.git
cd panda-client
```
and install it
```
PANDA_INSTALL_TARGET=/path/to/install/dir pip install .
```
or create the tar ball
```
python -m build -s
pip install dist/panda-client-*.tar.gz
```

# How to use
```
source /path/to/install/dir/etc/panda/panda_setup.[c]sh
prun -h
pathena -h
```

# Release Notes

https://github.com/PanDAWMS/panda-client/releases

# CVMFS deployment
Request atlas-adc-tier3sw-install to install the new specific version on CVMFS. They will download the package from the github release page.

# Uploading to pip
Publishing to PyPI is handled automatically by the `Upload Python Package` GitHub
Actions workflow when a release is published. To build the distributions locally:
```
python -m build -s
```
Uploading source so that the wheel generates setup files locally.
