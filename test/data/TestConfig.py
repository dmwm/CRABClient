"""
This is a test example of configuration file for CRAB-3 client
"""

from WMCore.Configuration import Configuration
import os

config = Configuration()

## General options for the client
config.section_("General")
config.General.serverUrl     = '127.0.0.1:8518'
config.General.requestName   = 'MyAnalysis'

## Specific option of the job type
## these options are directly readable from the job type plugin
config.section_("JobType")
config.JobType.pluginName  = 'TestPlugin'

## Specific data options
config.section_("Data")
config.Data.inputDataset = '/RelValProdTTbar/JobRobot-MC_3XY_V24_JobRobot-v1/GEN-SIM-DIGI-RECO'
config.Data.splitting = 'FileBased'
config.Data.processingVersion = 'v1'
config.Data.unitsPerJob = 100


## User options
config.section_("User")
config.User.team  = 'Analysis'
config.User.group = 'Analysis'
config.User.email = ''

config.section_("Site")
config.Site.storageSite = 'T2_IT_Pisa'
config.Site.blacklist = ["T2_ES_CIEMAT"]
