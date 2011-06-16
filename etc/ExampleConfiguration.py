"""
This is a test example of configuration file for CRAB-3 client
"""

from WMCore.Configuration import Configuration
import os

config = Configuration()

## General options for the client
config.section_("General")
config.General.server_url    = 'wmcrabserver.cern.ch:8080'
config.General.requestName   = 'MyAnalysis_1'

## Specific option of the job type
## these options are directly readable from the job type plugin
config.section_("JobType")
config.JobType.pluginName  = 'Cmssw'
config.JobType.psetName    = 'pset.py'
#config.JobType.inputFiles  = ['/tmp/input_file']

## Specific data options
config.section_("Data")
config.Data.inputDataset = '/cms/data/set'
#config.Data.publishDataName = 'MyReskimForTwo'
config.Data.processingVersion = 'v1'
#config.Data.splitting = 'RunBased'
#config.Data.splitting = 'EventBased'
#config.Data.blockWhitelist = [1000000]
#config.Data.blockBlacklist = [1000000,200000]
#config.Data.runWhitelist = [1,2]
#config.Data.runBlacklist = [1,2]
#config.Data.filesPerJob = 10
#config.Data.eventsPerJob = 100
#config.Data.dbsUrl = "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet"


## User options
config.section_("User")
#config.User.vorole  = ''
#config.User.vogroup = '/cms/integration'
config.User.team  = 'Analysis'
config.User.group = 'Analysis'
config.User.email = ''

config.section_("Site")
config.Site.storageSite = 'T2_XX_XXX'
#config.Site.whitelist = "T2_XY_XXY"
#config.Site.blacklist = "T2_XZ_XXZ"
