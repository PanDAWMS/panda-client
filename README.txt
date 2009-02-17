** How to install

svn co http://svn.usatlas.bnl.gov/svn/panda/panda-client/current panda-client
cd panda-client
python setup.py install --prefix=/path/to/install/dir


** How to use

source /path/to/install/dir/etc/panda/panda_setup.[c]sh
prun -h
pathena -h


** Release Note

0.1.16 (2/16/2009)
  * fixed the order of voms arguments in voms-proxy-init

0.1.15 (2/16/2009)
  * added --voms to pathena/prun

0.1.14 (2/12/2009)
  * fixed unpack failure in pathena

0.1.13 (2/11/2009)
  * fixed index calcuration when reusing outDS

0.1.12 (2/5/2009)
  * fixed nFilesPerJob in pathena
  * limited dataset names for the official option

0.1.11 (2/4/2009)
  * fixed http://savannah.cern.ch/bugs/?46617

0.1.10 (2/3/2009)
  * added --dbRunNumber to pathena to save disk usage of DBRelease

0.1.9 (2/2/2009)
  * fixed BS extraction when multiple streams exist

0.1.8 (1/29/2009)
  * fixed LFC looup 
  * more diagnostic message for LFC failures
  * added retry for LFC failures

0.1.7 (1/29/2009)
  * fixed error message when files are unavailable at the site

0.1.6 (1/23/2009)
  * added pathena
  * modified pathena to use pandatools modules
  * freed pathena from Athena runtime-structure
  * removed database stuff from pathena/prun
  * added RSS feed reader
  * copy constructor of steps in psequencer
  * capability to give env variables to steps in psequencer

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
