"""
This is an example of a minimal CRAB3 configuration file.
"""

from CRABClient.Configuration import Configuration
import os

config = Configuration()

## General options for the client
config.section_("General")
config.General.requestName   = 'MyAnalysis'
config.General.instance = 'prod'

## Specific option of the job type
## these options are directly readable from the job type plugin
config.section_("JobType")
config.JobType.pluginName  = 'Analysis'
config.JobType.psetName    = 'pset.py'

## Specific data options
config.section_("Data")
config.Data.inputDataset = '/GenericTTbar/HC-CMSSW_5_3_1_START53_V5-v1/GEN-SIM-RECO'
config.Data.splitting = 'LumiBased'
config.Data.unitsPerJob = 20

## User options
config.section_("User")
config.User.email = ''

config.section_("Site")
config.Site.storageSite = 'T2_US_Nebraska'

