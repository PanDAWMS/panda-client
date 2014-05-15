** How to install

git clone https://github.com/PanDAWMS/panda-client.git
cd panda-client

python setup.py install --prefix=/path/to/install/dir

or 

echo "%_unpackaged_files_terminate_build 0" >> ~/.rpmmacros
QA_SKIP_BUILD_ROOT=1 python setup.py bdist_rpm
rm ~/.rpmmacros


** How to use

source /path/to/install/dir/etc/panda/panda_setup.[c]sh
prun -h
pathena -h


** Release Notes

See ChangeLog.txt


** Acknowledge

   Icons from http://iconka.com
