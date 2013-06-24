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
    'submit' :  { 'map': {  "jobtype"       : {"default": "Analysis",       "config": None,                     "type": "StringType",  "required": True },
                            "workflow"       : {"default": None,             "config": None,                     "type": "StringType",  "required": True },
                            "savelogsflag"      : {"default": False,            "config": 'General.saveLogs',       "type": "BooleanType", "required": True },
                            "asyncdest"         : {"default": None,             "config": 'Site.storageSite',       "type": "StringType",  "required": True },
                            "publishname"       : {"default": '', "config": 'Data.publishDataName',   "type": "StringType",  "required": True },
                            #TODO
                            "dbsurl"            : {"default": "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet", "config": 'Data.dbsUrl', "type": "StringType",  "required": True },
                            "publishdbsurl"     : {"default": None,             "config": 'Data.publishDbsUrl',     "type": "StringType",  "required": False},
                            "publication"       : {"default": False,            "config": 'Data.publication',       "type": "BooleanType", "required": False},
                            "sitewhitelist"     : {"default": None,             "config": 'Site.whitelist',         "type": "ListType",    "required": False},
                            "siteblacklist"     : {"default": None,             "config": 'Site.blacklist',         "type": "ListType",    "required": False},
                            "blockwhitelist"    : {"default": None,             "config": 'Data.blockWhitelist',    "type": "ListType",    "required": False},
                            "blockblacklist"    : {"default": None,             "config": 'Data.blockBlacklist',    "type": "ListType",    "required": False},
                            "splitalgo"         : {"default": None,             "config": 'Data.splitting',         "type": "StringType",  "required": False},
                            "algoargs"          : {"default": None,             "config": 'Data.unitsPerJob',       "type": "IntType",  "required": True},
                            "totalunits"        : {"default": 0,                "config": 'Data.totalUnits',        "type": "IntType",  "required": False},
                            "addoutputfiles"    : {"default": [],               "config": 'JobType.outputFiles',    "type": "ListType",    "required": False},
                            "blacklistT1"       : {"default": True,             "config": None,                     "type": "BooleanType", "required": False},
                            "vorole"            : {"default": None,             "config": 'User.voRole',            "type": "StringType",  "required": False},
                            "vogroup"           : {"default": None,             "config": 'User.voGroup',           "type": "StringType",  "required": False},},
                  'other-config-params' : ["General.serverUrl", "General.requestName", "General.workArea",
                                           "JobType.pluginName", "JobType.externalPluginFile", "JobType.psetName",
                                           "JobType.inputFiles", "JobType.pyCfgParams",
                                           "Data.unitsPerJob", "Data.splitting", "Data.inputDataset", "Data.lumiMask", "Data.runRange",
                                           "User.email", "Site.removeT1Blacklisting", "General.instance"],
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
}
