** How to install

svn co http://svn.usatlas.bnl.gov/svn/panda/panda-client/current panda-client
cd panda-client
python setup.py install --prefix=/path/to/install/dir


** How to use

source /path/to/install/dir/etc/panda/panda_setup.[c]sh
prun -h
pathena -h


** Release Note

0.1.96 (10/24/2009)
  * added --parentDS to pathena

0.1.95 (10/20/2009)
  * enabled %SKIPEVENTS for evgen trf
  * fixed compact parameters in --trf of pathena for changes to T0 naming convention
	
0.1.94 (10/13/2009)
  * changed the default site of CERN to ANALY_CERN 
  * changed parameter formatting of DB and RNDM in --trf of pathena 

0.1.93 (10/8/2009)
  * enabled --destSE in pathena/prun
  * added pbook.show(-N) to show last N jobs
  * fixed pbook.retry() to correct short/long mixture of destSE

0.1.92 (10/3/2009)
  * fixed fakeProperty for serialization by RecExCommon/PrintRecoSummary
  * added --noBuild to prun

0.1.91 (9/16/2009)
  * fixed site lookup for ANALY_CERN when outDS is reused

0.1.90 (9/16/2009)
  * changed the default MyProxy for ANALY_ARC etc

0.1.89 (9/15/2009)
  * check LFC module for grid source
  * added -s to pathena

0.1.88 (9/10/2009)
  * fixed ownership of dataset replicas
  * added --prodSourceLabel to pathena

0.1.87 (8/31/2009)
  * fixed archiving for --noBuild to ignore symlinks in InstallArea/include

0.1.86 (8/31/2009)
  * fixed --pfnList of pathena
  * fixed pbook to update buildStatus
  * added forceUpdate to pbook.show()

0.1.85 (8/19/2009)
  * fixed ArchiveSource not to resolve symlink in run dir
  * allowed wildcard and/or comma in --inDS of pathena/prun to concatenate multiple datasets
  * updated pbook for the above change

0.1.84 (8/12/2009)
  * added askUser to Client.nEvents
  * enabled peer verification in registerProxyKey to protect against pharming
  * fixed libDS in pbook
  * fixed --trf of pathena for remortIO sites
  * fixed --update of pathena/prun
  * added convertConfToOutput to AthenaUtils
  * fixed index incrementation of --individualOutDS for active jobs
  * changed starting JobID to 1
  * fixed looping on BeamGas files
  * changed matching pattern for log files
  * allowed to use --dbRelease together with --trf in pathena
  * use LFC as primary replica catalog
  * fixed pbook for SL5/64 + Athena rel15
  
0.1.83 (7/28/2009)
  * exclude TAPE in brokerage

0.1.82 (7/28/2009)
  * removed size limitation for direct access sites
  * increased default extensions to be picked up by prun

0.1.81 (7/22/2009)
  * fixed --trf of pathena for duplicated DBRelease 
  * fixed --trf of pathena for AMI=tag
  * fixed version check
  * fixed resetting of sites which have a duplicated DQ2 ID

0.1.80 (7/21/2009)
  * fixed POOL-ref extraction for old Athena to work with new CollectionTree name
  * added a protection to runBrokerage to avoid too many lookup
  * support rpm installation in --update of pathena/prun
  * fixed proxy delegation for expiring certificate

0.1.79 (7/17/2009)
  * dropped the default to use US cloud

0.1.78 (7/15/2009)
  * added error message when common location is not found for --secondaryDSs in prun  
  * added --useAthenaPackages to compile Athena packages in build step of prun
  * ignore log.tgz in prun
  * ignore .svn in AthenaUtils.getPackages

0.1.77 (7/14/2009)
  * added --secondaryDSs to prun

0.1.76 (7/10/2009)
  * added --gluePackages to pathena

0.1.75 (7/10/2009)
  * fixed checkSiteAccessPermission for --burstSubmit
  * fixed result() of PStep in non-blocking mode
  * fixed getRunningPandaJobs in psequencer

0.1.74 (7/9/2009)
  * removed urllib2 and md5 to avoid the hashlib problem in Athena on SL5
  * added -c to pbook	
  * added getPandaJob and getRunningPandaJobs to psequencer
  * fixed AthenaUtils for Lhapdf

0.1.73 (6/29/2009)
  * improved --update in pathena/prun
  * added version check to pathena/prun 
  * added site permission check to pathena/prun

0.1.72 (6/18/2009)
  * added --nSkipFiles to prun

0.1.71 (6/15/2009)
  * changed the maximum length of the output datasetname to 200

0.1.70 (6/11/2009)
  * added trustIS to runBrokerage

0.1.69 (6/11/2009)
  * added set to puserinfo
  * added the longFormat option to puserinfo

0.1.68 (6/10/2009)
  * fixed site selection algorithm in pathena/prun

0.1.67 (6/10/2009)
  * fixed pbook.show() to show jobs in numeric order

0.1.66 (6/9/2009)
  * fixed location lookup for UNI-,RU-,LIP-,RO-
  * added methods to puserinfo for privileged users

0.1.65 (6/8/2009)
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
