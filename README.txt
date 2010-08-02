** How to install

svn co svn+ssh://svn.cern.ch/reps/panda/panda-client/current panda-client
cd panda-client

python setup.py install --prefix=/path/to/install/dir

or 

echo "%_unpackaged_files_terminate_build 0" >> ~/.rpmmacros
python setup.py bdist_rpm
rm ~/.rpmmacros


** How to use

source /path/to/install/dir/etc/panda/panda_setup.[c]sh
prun -h
pathena -h


** Release Note

0.2.73 (8/2/2010)
  * changed pathena/prun to use output container by default
  * changed --outDS to append / when / is missing
  * shortened LFN format to follow new DQ2 limitation on LFN length
  * added --useOldStyleOutput to pathena/prun to allow output dataset and old LFN convention
  * fixed --individualOutDS of pathena to create separate containers
  * fixed pbook to work with JobsetID instead if JobID
  * added longFormat option to pbook.show() to show jobs individually
  * fixed pbook.retry() to check input duplication
  * increased the minimum number of complete replicas for the latest DBR
  * made LFC lookup faster by increasing the number of GUIDs per cycle
  * added --nGBPerJob to prun

0.2.72 (7/29/2010)
  * fixed the range check in --goodRunList for LFNs with finer-grained LBs

0.2.71 (7/26/2010)
  * added --antiMatch to prun
     
0.2.70 (7/23/2010)
  * fixed pbook.retry() to check VOMS role when retrying group jobs
  * fixed local job conversion to set jobType using processingType
  * fixed a crash in pathena when no file is available at a site
  * added noSubmit to pbook.retry()

0.2.69 (7/23/2010)
  * fixed --trf of pathena to override DBR when DBR is explicitly specified in the argument

0.2.68 (7/23/2010)
  * improved --shipInput of pathena to find parentDS using GUIDs in TAG
  * added --useChirpServer to pathena/prun

0.2.67 (7/21/2010)
  * added a protection against unregistered GUID lookup in eventPicking 

0.2.66 (7/21/2010)
  * fixed --trf of pathena and --exec of prun to allow %DB:LATEST 	
  * fixed misleading errors when inDS is empty
  * fixed DBR lookup to ignore CDRelease

0.2.65 (7/20/2010)
  * added more warning for migration of naming convention
  * updated --destSE to accept a list of destinations
  * added showPandaIDinState to pbook.show()

0.2.64 (7/16/2010)
  * fixed --goodRunListXML to parse multiple LBRange items in a single LumiBlockCollection

0.2.63 (7/15/2010)
  * added a protection against looping on empty datasets for input/output containers

0.2.62 (7/15/2010)
  * fixed --nGBPerJob of pathena to take the number of files into account

0.2.61 (7/7/2010)
  * added a protection against deleted DS lookup in eventPicking 

0.2.60 (7/5/2010)
  * added a protection to DS lookup against unmapped GUID in eventPicking 

0.2.59 (6/25/2010)
  * changed --eventPickStreamName to be optional for MC

0.2.58 (6/25/2010)
  * use non-certificate authentication in pyAMI

0.2.57 (6/24/2010)
  * made --dbRelease=LATEST the default in pathena

0.2.56 (6/23/2010)
  * fixed unsupported cloud error due to OSG

0.2.55 (6/18/2010)
  * fixed cmtConfig

0.2.54 (6/18/2010)
  * added newSite option to pbook.retry()	
  * send .cpp and .hpp to WN by default

0.2.53 (6/15/2010)
  * set prodDBlock appropriately for GRL

0.2.52 (6/11/2010)
  * added protection for quotes in --trf of pathena

0.2.51 (6/9/2010)
  * verify spaceToken for ANALY_ARC
  * fixed a wrong option in tar archiving

0.2.50 (6/4/2010)
  * fixed slow DQ2 lookup for --dbRelease=LATEST

0.2.49 (6/4/2010)
  * use LBs for the range check in --goodRunList if they are available in LFN
  * changed the brokerage policy when container is used for output, to send
    jobs to the site where many CPUs are available rather than many files

0.2.48 (6/1/2010)
  * fixed --match in prun so that . matches \.
  * fixed LFN duplication check for event picking
  * added a protection for wrong case in outDS
  * added a protection against too many input files for ANALY_ARC

0.2.47 (5/26/2010)
  * fixed replica registration when --destSE is used for existing outDS

0.2.46 (5/25/2010)
  * fixed unused file check when --nFiles is used in pathena

0.2.45 (5/21/2010)
  * added safety margin to --nGBPerJob in pathena

0.2.44 (5/21/2010)
  * fixed insufficient skipping of CERN-PROD_TZERO and CERN-PROD_DAQ

0.2.43 (5/20/2010)
  * changed TRF URLs for SVN repository migration

0.2.42 (5/18/2010)
  * added a protection to pbook.retry for lib.tgz with GUID=NULL
  * fixed status() in pbook to set buildStatus correctly

0.2.41 (5/18/2010)
  * fixed error message in prun when files are on tape
  * fixed file check when outDS is reused by avoiding dirty-reading  

0.2.40 (5/14/2010)
  * added a protection against location mismatch between outDS and libDS
  * improved error reporting about pyAMI import

0.2.39 (5/12/2010)
  * increased the default nFilesPerJob in prun
  * fixed for --destSE
  * added event picking to prun

0.2.38 (5/12/2010)
  * fixed --eventPickDS
  * ignore irrelevant datasets for event-picking more

0.2.37 (5/11/2010)
  * fixed warning for event-picking + trf
  * ignore panda internal datasets for event-picking 
  * added warning for unmerged dataset usage
  * fixed site lookup for TRIG-DAQ
  * fixed input size calculation to take DBR into account

0.2.36 (5/9/2010)
  * fixed CollListFileGUID to remove duplication
  * added event-picking stuff

0.2.35 (5/7/2010)
  * added protection against invalid characters in --outDS
  * fixed --goodRunListXML to remove duplicated datasets for the same run number
  
0.2.34 (5/5/2010)
  * added warning message for missing nicknames
  * enabled --destSE for any DQ2 destination

0.2.33 (4/30/2010)
  * to follow new DQ2 naming convention

0.2.32 (4/29/2010)
  * fixed fakeAppMgr for allConfigurables access

0.2.31 (4/29/2010)
  * fixed site lookup for composit sites when outDS/libDS is reused

0.2.30 (4/28/2010)
  * removed .svn from archiving
  * ignore CERN-PROD_TZERO and CERN-PROD_DAQ to avoid tape access
  * fixed archiving in prun to skip pseudo empty dirs

0.2.29 (4/20/2010)
  * fixed --dbRelease=LATEST to exclude reprocessing DBR
  * added an automatic scrollbar to summary window in pbook --gui

0.2.28 (4/14/2010)
  * fixed --goodRunListXML to use pyAMI module	
  * added error message when wrong LFNs are given for input
 
0.2.27 (4/9/2010)
  * fixed --gui of pbook

0.2.26 (4/6/2010)
  * added --maxCpuCount and --memory to prun

0.2.25 (4/2/2010)
  * fixed file duplication check when --supStream is used

0.2.24 (3/31/2010)
  * fixed pathena for --minDS

0.2.23 (3/27/2010)
  * fixed for DQ2 DNS change

0.2.22 (3/23/2010)
  * updated --secondaryDSs in prun for nFiles=0 to use all files
  * added --dbRelease,dbRunNumber,notExpandDBR to prun

0.2.21 (3/16/2010)
  * fixed --secondaryDSs in prun

0.2.20 (3/15/2010)
  * fixed --supStream
  * added siteType to getSiteSpecs
  * added --useAMIAutoConf to pathena

0.2.19 (3/12/2010)
  * added --goodRunListXML,goodRunListDataType,goodRunListDataType,goodRunListDS to pathena/prun
  * added processingType to runBrokerage for HC	
  * added sequencer.mail_dirs to panda.cfg
  * increased the default value of --crossSite to 5

0.2.18 (3/5/2010)
  * fixed ImapFetcher for apostrophe in directory names

0.2.17 (3/4/2010)
  * updated pathena/prun to send jobs to multiple sites when the input dataset container
    splits over multiple sites and an output dataset container is used as --outDS
  * fixed outputPath for T3  
  * fixed libDS for T3

0.2.16 (3/1/2010)
  * fixed for analysis projects

0.2.15 (2/26/2010)
  * added --dbRelease=LATEST to pathena  	
  * updated pbook for canceled state
  * added --excludeFile to pathena/prun

0.2.14 (2/11/2010)
  * added warning message for busy sites
  * check production role for group production

0.2.13 (2/3/2010)
  * changed max input size to a site parameter

0.2.12 (1/28/2010)
  * removed cloud constraint using VOMS attributes from brokerage

0.2.11 (1/27/2010)
  * fixed the default value in getCloudUsingFQAN
  * fixed the brokerage to check DBRelease locations

0.2.10 (1/22/2010)
  * set cmtConfig for old releases for SL5 sites

0.2.9 (1/21/2010)
  * added killAndRetry to pbook
  * fixed error message in pathena for wrong DBR filename
  * set cmtConfig for 15.6.3 or higher for now
  * added retry in database sessions in pbook

0.2.8 (1/12/2010)
  * fixed prun to allow submission with unready libDS

0.2.7 (1/9/2010)
  * fixed the brokerage to check minBias/CavernBeamGas/Halo DSs

0.2.6 (1/8/2010)
  * removed strict number checking on output dataset name

0.2.5 (1/3/2010)
  * added --nGBPerJob to pathena

0.2.4 (12/17/2009)
  * fixed --excludedSite to treat composite sites properly
  * supported TAGCOMM in pathena
  * changed --site to allow a list of sites

0.2.3 (12/6/2009)
  * updated brokerage to use cache version if applicable
  * fixed location lookup to ignore TAPE when checking available files
  * fixed pathena for --burstSubmit

0.2.2 (11/20/2009)
  * protection against unchecked replica info in DQ2

0.2.1 (11/19/2009)
  * added schema evolution to pbook
  * added release/cache info to pbook.show
  * set version info to lockedby
  * added clean() to pbook  

0.2.0 (11/18/2009)
  * fixed dataset lookup

0.1.99 (11/15/2009)
  * removed case sensitivity from dataset lookup

0.1.98 (11/13/2009)
  * added kill(X,Y) to pbook
  * added rebrokerage() to pbook 
  * added --outputPath for DQ2-free sites like T3
  * fixed pathena for UserDataSvc
	
0.1.97 (11/4/2009)
  * fixed PANDA_SYS in rpm
  * fixed pathena/prun to give warning message when the number of sub-jobs exceeds the limit
  * added --long to prun
  * fixed long queue brokerage
  * changed parameter format in --exec of prun to be consistent with pathena
  * allowed to set the start number to %SKIPEVENTS in --trf of pathena
  * fixed parameter replacement in --trf/pathena and --exec/prun to take delimiter into account
  * added protection to pbook against orphan directories created by distutils
  * fixed pbook not to show duplicated datasets in inDS/outDS

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
