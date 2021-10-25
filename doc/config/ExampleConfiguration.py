"""
This is a test example of configuration file for CRAB-3 client
"""

from CRABClient.Configuration import Configuration
import os

config = Configuration()

## General options for the client
config.section_("General")
config.General.requestName   = 'MyAnalysis_1'
#config.General.restHost     = 'yourserver'
#config.General.dbInstance = 'dev'
#config.General.instance = 'other'

## Specific option of the job type
## these options are directly readable from the job type plugin
config.section_("JobType")
config.JobType.pluginName  = 'Analysis'
config.JobType.psetName    = 'pset.py'

## Specific data options
config.section_("Data")
config.Data.inputDataset = '/cms/data/set'
config.Data.splitting = 'LumiBased'
config.Data.unitsPerJob = 20

## User options
config.section_("User")
config.User.email = ''

config.section_("Site")
config.Site.storageSite = 'T2_XX_XXX'
