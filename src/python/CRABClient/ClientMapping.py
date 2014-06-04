#!/usr/bin/env python
"""
_ClientMapping_

This allows to have an agnostic client.
For each client command it is possible to define
the path of the REST request, the map between
the client configuration and the final request
to send to the server. It includes type of the parameter
to have the client doing a basic sanity check on
the input data type.
"""

mapping = {
    'submit' :  { 'map': {  "jobtype"           : {"default": "Analysis",       "config": None,                     "type": "StringType",  "required": True },
                            "workflow"          : {"default": None,             "config": None,                     "type": "StringType",  "required": True },
                            "savelogsflag"      : {"default": False,            "config": 'General.saveLogs',       "type": "BooleanType", "required": True },
                            "asyncdest"         : {"default": None,             "config": 'Site.storageSite',       "type": "StringType",  "required": True },
                            "saveoutput"        : {"default": None,             "config": 'General.transferOutput', "type": "BooleanType", "required": False},
                            "publishname"       : {"default": '',               "config": 'Data.publishDataName',   "type": "StringType",  "required": True },
                            "dbsurl"            : {"default": "https://cmsweb.cern.ch/dbs/prod/global/DBSReader", "config": 'Data.dbsUrl', "type": "StringType",  "required": True },
                            "publishdbsurl"     : {"default": None,             "config": 'Data.publishDbsUrl',     "type": "StringType",  "required": False},
                            "publication"       : {"default": False,            "config": 'Data.publication',       "type": "BooleanType", "required": False},
                            "lfnprefix"         : {"default": None,             "config": 'Data.prefix',            "type": "StringType",  "required": False},
                            "sitewhitelist"     : {"default": None,             "config": 'Site.whitelist',         "type": "ListType",    "required": False},
                            "siteblacklist"     : {"default": None,             "config": 'Site.blacklist',         "type": "ListType",    "required": False},
                            "splitalgo"         : {"default": None,             "config": 'Data.splitting',         "type": "StringType",  "required": False},
                            "algoargs"          : {"default": None,             "config": 'Data.unitsPerJob',       "type": "IntType",     "required": True},
                            "totalunits"        : {"default": 0,                "config": 'Data.totalUnits',        "type": "IntType",     "required": False},
                            "ignorelocality"    : {"default": False,            "config": 'Data.ignoreLocality',    "type": "BooleanType", "required": False},
                            "addoutputfiles"    : {"default": [],               "config": 'JobType.outputFiles',    "type": "ListType",    "required": False},
                            "vorole"            : {"default": None,             "config": 'User.voRole',            "type": "StringType",  "required": False},
                            "vogroup"           : {"default": None,             "config": 'User.voGroup',           "type": "StringType",  "required": False},
                            "maxjobruntime"     : {"default": None,             "config": "JobType.maxjobruntime",  "type": "IntType",     "required": False},
                            "numcores"          : {"default": None,             "config": "JobType.numcores",       "type": "IntType",     "required": False},
                            "maxmemory"         : {"default": None,             "config": "JobType.maxmemory",      "type": "IntType",     "required": False},
                            "priority"          : {"default": None,             "config": "JobType.priority",       "type": "IntType",     "required": False},
                            "faillimit"         : {"default": None,               "config": "General.failureLimit",   "type": "IntType",     "required": False},
                            "nonprodsw"         : {"default": False,            "config": "JobType.allowNonProductionCMSSW", "type": "BooleanType", "required": False},
                         },
                  'other-config-params' : ["General.serverUrl", "General.requestName", "General.workArea",
                                           "JobType.pluginName", "JobType.externalPluginFile", "JobType.psetName",
                                           "JobType.inputFiles", "JobType.pyCfgParams", "Data.primaryDataset",
                                           "Data.unitsPerJob", "Data.splitting", "Data.inputDataset", "Data.lumiMask", "Data.runRange",
                                           "User.email", "General.instance", "Debug.oneEventMode", "Data.userInputFile"],
                  'requiresTaskOption' : False,
                },
    'getlog': {'requiresTaskOption' : True},
    'getoutput': {'requiresTaskOption' : True},
    'remote_copy': {'requiresTaskOption' : True, 'initializeProxy' : False},#proxy already inited by the calling command
    'status': {'requiresTaskOption' : True},
    'report': {'requiresTaskOption': True},
    'kill': {'requiresTaskOption' : True},
    'resubmit': {'requiresTaskOption' : True},
    'request_type': {'requiresTaskOption': True},
    'uploadlog' : {'requiresTaskOption' : True},
    'checkwrite': {'requiresTaskOption': False, 'requiresREST': False},
    'purge':{'requiresTaskOption': True}
}
