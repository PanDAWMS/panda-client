** How to install

svn co https://svn.usatlas.bnl.gov/svn/panda/panda-client/current panda-client
cd panda-client
python setup.py install --prefix=/path/to/install/dir


** How to use

source /path/to/install/dir/etc/panda/panda_setup.[c]sh
prun -h


** Release Note

current
  * added pathena
  * modified pathena to use pandatools modules
  * removed dependence from pathena on Athena runtime-structure
  * removed database stuff from pathena/prun
  * added RSS feed reader

0.1.5 (12/17/2008)
  * escape $ in pass phrase
  * added _XROOTD to AUTO sites
  * migrated to the latest DB schema
  * added pbook
  * added logger
  * added GUI stuff
  * automatic adjustment for nFilesPerJob in prun
  * unified config file
	
0.1.4 (11/25/2008)
  * fixed --match in prun

0.1.3 (11/24/2008)
  * added PkgInfo
  * moved psequencer from PandaTools CVS repo
  * make python unbuffered
  * increment jobID using local cache
  * introduced PANDA_CONFIG_ROOT

0.1.2 (11/22/2008) 
  * added --bexec. See C++ ROOT example 
  * fixed templates generating setup scripts 
  * support for proxy delegation 

0.1.1 (11/19/2008) 
  * fixed --libDS 
  * increment file index for existing outDS 

0.1.0 (11/16/2008) 
  * first release


** Acknowledge

   Icons from http://iconka.com
