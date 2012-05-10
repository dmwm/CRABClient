"""
This is a test example of configuration file for CRAB-3 client
"""

from WMCore.Configuration import Configuration
import os

config = Configuration()

## General options for the client
config.section_("General")
config.General.requestName   = 'MyAnalysis_1'
config.General.delegateTo   = 'AgentDN'
#config.General.serverUrl     = 'yourserver:0000'

## Specific option of the job type
## these options are directly readable from the job type plugin
config.section_("JobType")
config.JobType.pluginName  = 'Cmssw'
config.JobType.psetName    = 'pset.py'

## Specific data options
config.section_("Data")
config.Data.inputDataset = '/cms/data/set'
#config.Data.processingVersion = 'v1'
config.Data.splitting = 'LumiBased'
config.Data.unitsPerJob = 20

## User options
config.section_("User")
config.User.email = ''

config.section_("Site")
config.Site.storageSite = 'T2_XX_XXX'
