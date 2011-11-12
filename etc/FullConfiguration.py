"""
This is a test example of configuration file for CRAB-3 client
"""

from WMCore.Configuration import Configuration
import os

config = Configuration()

## General options for the client
config.section_("General")
config.General.requestName   = 'MyAnalysis_1'
#config.General.workArea   = '/path/to/workarea'
#config.General.serverUrl     = 'yourserver:0000'

## Specific option of the job type
## these options are directly readable from the job type plugin
config.section_("JobType")
config.JobType.pluginName  = 'Cmssw'
##As an alternative to pluginName. Used for test purposes
#config.JobType.externalPluginFile  = '/some/Plugin.py'
config.JobType.psetName    = 'pset.py'
#config.JobType.inputFiles  = ['/tmp/input_file']
## Does the job write any output files that need to be collected BESIDES those in output modules or TFileService
#config.JobType.outputFiles  = ['output_file']

## Specific data options
config.section_("Data")
config.Data.inputDataset = '/cms/data/set'
#config.Data.publishDataName = 'MyReskimForTwo'
config.Data.processingVersion = 'v1'
#config.Data.splitting = 'LumiBased'
#config.Data.splitting = 'EventBased'
#config.Data.splitting = 'FileBased'
#config.Data.unitsPerJob = 10
##For lumiMask http and https urls are also allowed
#config.Data.lumiMask = 'lumi.json'
#config.Data.blockWhitelist = [1000000]
#config.Data.blockBlacklist = [1000000,200000]
#config.Data.runWhitelist = [1,2]
#config.Data.runBlacklist = [1,2]
#config.Data.dbsUrl = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"


## User options
config.section_("User")
#config.User.voRole  = 't1access'
#config.User.voGroup = 'integration'
#config.User.team  = 'Analysis'
#config.User.group = 'Analysis'
config.User.email = ''

config.section_("Site")
config.Site.storageSite = 'T2_XX_XXX'
#config.Site.whitelist = ['T2_XY_XXY']
#config.Site.blacklist = ['T2_XZ_XXZ']
