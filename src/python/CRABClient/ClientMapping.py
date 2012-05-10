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

import time

mapping = {
    'submit' :  { 'uri': '/crabserver/workflow',
                  'map':  {
                            "jobtype"       : {"default": "Analysis",       "config": None,                     "type": "StringType",  "required": True },
                            "workflow"       : {"default": None,             "config": None,                     "type": "StringType",  "required": True },
                            "savelogsflag"      : {"default": False,            "config": 'General.saveLogs',       "type": "BooleanType", "required": True },
                            "asyncdest"         : {"default": None,             "config": 'Site.storageSite',       "type": "StringType",  "required": True },
                            "publishname"       : {"default": str(int(time.time())), "config": 'Data.publishDataName',   "type": "StringType",  "required": True },
#                            "ProcessingVersion" : {"default": "v1",             "config": 'Data.processingVersion', "type": "StringType",  "required": True },
                            #TODO
#                            "DbsUrl"            : {"default": "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet", "config": 'Data.dbsUrl', "type": "StringType",  "required": True },
                            "sitewhitelist"     : {"default": None,             "config": 'Site.whitelist',         "type": "ListType",    "required": False},
                            "siteblacklist"     : {"default": None,             "config": 'Site.blacklist',         "type": "ListType",    "required": False},
                            "runwhitelist"      : {"default": None,             "config": 'Data.runWhitelist',      "type": "StringType",    "required": False},
                            "runblacklist"      : {"default": None,             "config": 'Data.runBlacklist',      "type": "StringType",    "required": False},
                            "blockwhitelist"    : {"default": None,             "config": 'Data.blockWhitelist',    "type": "ListType",    "required": False},
                            "blockblacklist"    : {"default": None,             "config": 'Data.blockBlacklist',    "type": "ListType",    "required": False},
                            "splitalgo"         : {"default": None,             "config": 'Data.splitting',         "type": "StringType",  "required": False},
                            "algoargs"          : {"default": None,             "config": 'Data.unitsPerJob',       "type": "IntType",  "required": False},
                            "addoutputfiles"    : {"default": [],               "config": 'JobType.outputFiles',    "type": "ListType",    "required": False},
                            "blacklistT1"       : {"default": True,             "config": None,                     "type": "BooleanType", "required": False},
                          },

                  'other-config-params' : [
                                           "General.serverUrl", "General.ufccacheUrl", "General.requestName", "General.workArea",
                                           "JobType.pluginName", "JobType.externalPluginFile", "JobType.psetName",
                                           "JobType.inputFiles", "JobType.pyCfgParams",
                                           "Data.unitsPerJob", "Data.splitting", "Data.inputDataset", "Data.lumiMask", "General.delegateTo",
                                           "User.email", "Data.publishDbsUrl", "Site.removeT1Blacklisting", "General.configcacheUrl" , "General.configcacheName"
                                          ],
                  'requiresTaskOption' : False
                },
#    'get-log' :  {'uri': '/crabinterface/crab/log/'},
    'getoutput'   : {'uri': '/crabserver/workflow', 'requiresTaskOption' : True},
#    'reg_user'    : {'uri': '/crabinterface/crab/user/'},
#    'server_info' : {'uri': '/crabinterface/crab/info/'},
    'status' : {'uri': '/crabserver/campaign', 'requiresTaskOption' : True},
#    'upload' : {'uri': '/crabinterface/crab/uploadUserSandbox'},
#    'get-error': {'uri': '/crabinterface/crab/jobErrors/'},
#    'report': {'uri': '/crabinterface/crab/goodLumis/'},
#    'kill':   {'uri': '/crabinterface/crab/task/'},
#    'resubmit': {'uri': '/crabinterface/crab/reprocessTask/'},
#    'publish': {'uri': '/crabinterface/crab/publish/'},
}
