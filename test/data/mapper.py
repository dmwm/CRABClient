#!/usr/bin/env python
"""
_mapper_

parallel to CRABServer/configuration/ClientMapping.py
"""

import time

defaulturi = {
    'submit' :  { 'uri': '/unittests/rest/task/',
                  'map':  {
                            "RequestType"       : {"default": "Analysis",       "config": None,                     "type": "StringType",  "required": True },
                            "Group"             : {"default": "Analysis",       "config": 'User.group',             "type": "StringType",  "required": True },
                            "Team"              : {"default": "Analysis",       "config": 'User.group',             "type": "StringType",  "required": True },
                            "Requestor"         : {"default": None,             "config": None,                     "type": "StringType",  "required": True },
                            "Username"          : {"default": None,             "config": None,                     "type": "StringType",  "required": True },
                            "RequestName"       : {"default": None,             "config": None,                     "type": "StringType",  "required": True },
                            "RequestorDN"       : {"default": None,             "config": None,                     "type": "StringType",  "required": True },
                            "SaveLogs"          : {"default": False,            "config": 'General.saveLogs',       "type": "BooleanType", "required": True },
                            "asyncDest"         : {"default": None,             "config": 'Site.storageSite',       "type": "StringType",  "required": True },
                            "PublishDataName"   : {"default": str(time.time()), "config": 'Data.publishDataName',   "type": "StringType",  "required": True },
                            "ProcessingVersion" : {"default": "v1",             "config": 'Data.processingVersion', "type": "StringType",  "required": True },
                            "DbsUrl"            : {"default": "http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet", "config": 'Data.dbsUrl', "type": "StringType",  "required": True },
                            "SiteWhitelist"     : {"default": None,             "config": 'Site.whitelist',         "type": "ListType",    "required": False},
                            "SiteBlacklist"     : {"default": None,             "config": 'Site.blacklist',         "type": "ListType",    "required": False},
                            "RunWhitelist"      : {"default": None,             "config": 'Data.runWhitelist',      "type": "ListType",    "required": False},
                            "RunBlacklist"      : {"default": None,             "config": 'Data.runBlacklist',      "type": "ListType",    "required": False},
                            "BlockWhitelist"    : {"default": None,             "config": 'Data.blockWhitelist',    "type": "ListType",    "required": False},
                            "BlockBlacklist"    : {"default": None,             "config": 'Data.blockBlacklist',    "type": "ListType",    "required": False},
                            "JobSplitAlgo"      : {"default": None,             "config": 'Data.splitting',         "type": "StringType",  "required": False}
                            #"JobSplitArgs"      : {"default": None,             "config": 'Data.filesPerJob',       "type": IntType,    "required": False},
                            #"JobSplitArgs"      : {"default": None,             "config": 'Data.eventPerJob',       "type": IntType,    "required": False},
                          },
                    'other-config-params' : ['General.serverUrl', 'General.requestName', 'JobType.pluginName', 'JobType.externalPluginFile', 'Data.unitsPerJob', 'Data.splitting', \
                                               "JobType.psetName", "JobType.inputFiles", "Data.inputDataset", "User.email", "Data.lumiMask", "General.workArea"]

                },
            'get-log' : {'uri': '/unittests/rest/log/'},
            'get-output' : {'uri': '/unittests/rest/data/'},
            'reg_user' : {'uri': '/unittests/rest/user/'},
            'server_info' : {'uri': '/unittests/rest/info/'},
            'status' : {'uri': '/unittests/rest/task/'},
            'report' :    {'uri': '/unittests/rest/goodLumis/'},
            'get_client_mapping': {'uri': '/unittests/rest/requestmapping/'},
            'get-error': {'uri': '/unittests/rest/jobErrors/'},
            'kill': {'uri': '/unittests/rest/task/'},
            'resubmit': {'uri': '/unittests/rest/resubmit/'},
}
