** How to install

svn co http://svn.usatlas.bnl.gov/svn/panda/panda-client/current panda-client
cd panda-client
python setup.py install --prefix=/path/to/install/dir


** How to use

source /path/to/install/dir/etc/panda/panda_setup.[c]sh
prun -h
pathena -h


** Release Note

current
  * fixed prun for direct input
  * added --workingGroup option to pathena/prun
  * added a warning message for some special XMLs
 
0.1.64 (6/1/2009)
  * set default processingType
  * changed dev URLs
  * put size limit for file uploading with --libDS/--noBuild

0.1.63 (5/28/2009)
  * fixed variable pollution for --noRandom in pathena

0.1.62 (5/28/2009)
  * fixed convSrmV2ID for DESY-*

0.1.61 (5/25/2009)
  * fixed AthenaUtils.archiveInstallArea for --noBuild

0.1.60 (5/24/2009)
  * fixed resubmission of pathena/prun to skip input files which are being used by active jobs

0.1.59 (5/24/2009)
  * fixed Panda SiteID lookup with DQ2 SiteID to skip long/test sites

0.1.58 (5/19/2009)
  * fixed no-output checking in pathena

0.1.57 (5/13/2009)
  * use /opt/panda for rpm to avoid mixture of site-packages
  * protection against too long output datasetname

0.1.56 (5/13/2009)
  * fixed zero division in pathena

0.1.55 (5/12/2009)
  * fixed pathena for UserDataSvc

0.1.54 (5/11/2009)
  * fixed Athena ver extraction for non-AtlasOffline env
  * changed URLs of dev server

0.1.53 (5/11/2009)
  * changed default server to CERN

0.1.52 (5/8/2009)
  * fixed Athena version check in pathena

0.1.51 (5/6/2009)
  * try cmt in a sub dir when it is executed in top dir

0.1.50 (5/4/2009)
  * fixed file indexing for --individualOutDS

0.1.49 (5/3/2009)
  * added retry(X,Y) to pbook

0.1.48 (5/2/2009)
  * fixed pollution of built-in function in pathena

0.1.47 (4/30/2009)
  * fixed pathena for pileup
  * added message to prun when site is redirected

0.1.46 (4/28/2009)
  * improved messages when site is skipped due to bad status
  * fixed error message when input files are unavailable

0.1.45 (4/27/2009)
  * fixed location lookup for _PERF_XYZ

0.1.44 (4/25/2009)
  * fixed pathena to increment file index for --trf
  * added show('running') to pbook

0.1.43 (4/24/2009)
  * set a list of DNs for proxy delegation

0.1.42 (4/17/2009)
  * protection against redundant filename in --extOutFile 

0.1.41 (4/15/2009)
  * fixed --removeBurstLimit in pathena

0.1.40 (4/13/2009)
  * added --processingType and --seriesLabel

0.1.39 (4/10/2009)
  * fixed extraction of cache ver in prun

0.1.38 (4/9/2009)
  * improved performance of show() in pbook
  * fixed location lookup for _PHYS_XYZ

0.1.37 (4/8/2009)
  * added --removeBurstLimit to pathena
  * fixed prun to contain empty directories in tar archive

0.1.36 (4/7/2009)
  * set sites online when they are allowed in siteaccess
  * cache jobID for --burstSubmit 
  * use UTC timestamp in libDS to avoid duplication
  * removed unicode from puserinfo
	
0.1.35 (4/3/2009)
  * fixed --individualOutDS in pathena

0.1.34 (4/2/2009)
  * fixed pathena to allow a full pathname for jobO

0.1.33 (4/2/2009)
  * protection against VOMS cert expiration

0.1.32 (3/31/2009)
  * use panda.cern.ch:25980 for now	
  * added puserinfo

0.1.31 (3/19/2009)
  * randomize G4RandomSeeds
  * AthSequence support 

0.1.30 (3/12/2009)
  * disabled location lookup for --burstSubmit in pathena

0.1.29 (3/12/2009)
  * fixed --burstSubmit in pathena

0.1.28 (3/11/2009)
  * warning messages for site status in pathena/prun

0.1.27 (3/10/2009)
  * fixed psequencer for CERN relocation
  * introduced archiveXYZ in AthenaUtils

0.1.26 (3/5/2009)
  * resolve relative path in InstallArea

0.1.25 (3/5/2009)
  * ignore cache when it is a base release

0.1.24 (3/4/2009)
  * fixed pathena for CollListFileGUID.exe

0.1.23 (3/4/2009)
  * fixed collRefName 

0.1.22 (3/3/2009)
  * read RAW via TAG

0.1.21 (2/26/2009)
  * protection against rc loading messages in pbook

0.1.20 (2/25/2009)
  * fixed http://savannah.cern.ch/bugs/?47396

0.1.19 (2/23/2009)
  * fixed --noBuild in pathena
  * added crossSite to pathena/prun	

0.1.18 (2/19/2009)
  * allowed downstream job to use upstream output as input in --trf

0.1.17 (2/17/2009)
  * added --update to pathena/prun
  * fixed renaming error for --trf
  * fixed --libDS=LAST  

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
