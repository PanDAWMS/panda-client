# How to install

Download the code
```
git clone https://github.com/PanDAWMS/panda-client.git
cd panda-client
```
and install it
```
python setup.py install --prefix=/path/to/install/dir
```
or create the tar ball
```
python setup.py sdist
pip install dist/panda-client-*.tar.gz
```

# How to use
```
source /path/to/install/dir/etc/panda/panda_setup.[c]sh
prun -h
pathena -h
```

# Release Notes

See ChangeLog.txt

# CVMFS deployment
Request atlas-adc-tier3sw-install to install the new specific version on CVMFS. They will download the package from the github release page.

# Uploading to pip
```
python setup.py sdist upload
```
Uploading source so that wheel generates setup files locally.
 
# Acknowledge
Icons from http://iconka.com
