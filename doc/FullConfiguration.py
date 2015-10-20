"""
This is an example configuration file for CRAB3 client, covering
most of the available configuration options.
"""

from WMCore.Configuration import Configuration
import os

config = Configuration()

## General options for the client
config.section_("General")

# Request name must be specified
#config.General.requestName   = 'MyAnalysis_1'
#config.General.workArea   = '/path/to/workarea'

# Specify a custom server.  This is almost never used; the defaults will point at the central server.
## https schema is used. If the port is not specified default 443 will be used
#config.General.serverUrl     = 'yourserver[:0000]'
# This identify which type of server you're using. If it is a private instance you must set 'private'
# (other options are prod/preprod)
#config.General.instance = 'private'

## Specific option of the job type
## these options are directly readable from the job type plugin
config.section_("JobType")
#config.JobType.pluginName  = 'Analysis'
## The plugin for MC Private Production
#config.JobType.pluginName  = 'PrivateMC'
config.JobType.psetName    = 'pset.py'
## Does the job read any additional private file:
#config.JobType.inputFiles  = ['/tmp/input_file']
## Does the job write any output files that need to be collected BESIDES those in output modules or TFileService
#config.JobType.outputFiles  = ['output_file']


## Specific data options
config.section_("Data")
#config.Data.inputDataset = '/cms/data/set'
#config.Data.outputDatasetTag = 'MyReskimForTwo'
## Splitting Algorithms
#config.Data.splitting = 'LumiBased'
#config.Data.splitting = 'EventBased'
#config.Data.splitting = 'FileBased'
#config.Data.unitsPerJob = 10

## For lumiMask http and https urls are also allowed
#config.Data.lumiMask = 'lumi.json'

## If you are splitting a Private MC Production task
## you must specify the total amount of data to generate
#config.Data.splitting = 'EventBased'
#config.Data.unitsPerJob = 10
#config.Data.totalUnits = 100


## To publish produced data there are 3 parameters to set:
#config.Data.publication = True
#config.Data.inputDBS = "https://cmsweb.cern.ch/dbs/prod/global/DBSReader"
#config.Data.publishDBS = "https://cmsweb.cern.ch/dbs/prod/prod/phys03/DBSWriter"

## User options
config.section_("User")
#config.User.voRole  = 't1access'
#config.User.voGroup = 'integration'
#config.User.team  = 'Analysis'
#config.User.group = 'Analysis'
#config.User.email = ''

config.section_("Site")
config.Site.storageSite = 'T2_XX_XXX'
#config.Site.whitelist = ['T2_XY_XXY']
#config.Site.blacklist = ['T2_XZ_XXZ']
#config.Site.removeT1Blacklisting = False

