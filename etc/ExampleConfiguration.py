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
config.JobType.inputFiles  = ['/tmp/input_file']

## Specific data options
config.section_("Data")
config.Data.inputDataset = '/cms/data/set'
config.Data.lumiSectionFile  = '/file/path/name'

## User options
config.section_("User")
config.User.role = '/cms/integration'
config.User.team  = 'Analysis'
config.User.group = 'Analysis'
config.User.email = ''
config.User.storageSite = 'T2_XX_XXX'
