#!/usr/bin/env python
"""
_ClientMapping_

This allows to have an agnostic client.
For each client command it is possible to define the path of the REST request, the map between
the client configuration and the final request to send to the server. It includes type of the
parameter so that the client can do a basic sanity check on the input data type.
For each server parameter, there can be more than one parameters in the CRAB configuration file.
If that is the case then the meaning is that any of the parameters in the CRAB configuration
file is used to set the same server parameter.
"""

parameters_mapping = {
    'on-server': {'workflow'       : {'default': None,       'config': ['General.requestName'],             'type': 'StringType',  'required': False},
                  'activity'       : {'default': None,       'config': ['General.activity'],                'type': 'StringType',  'required': False},
                  'saveoutput'     : {'default': True,       'config': ['General.transferOutputs'],         'type': 'BooleanType', 'required': False},
                  'savelogsflag'   : {'default': False,      'config': ['General.transferLogs'],            'type': 'BooleanType', 'required': False},
                  'faillimit'      : {'default': None,       'config': ['General.failureLimit'],            'type': 'IntType',     'required': False},
                  'inputdata'      : {'default': None,       'config': ['Data.inputDataset',
                                                                        'Data.primaryDataset'],             'type': 'StringType',  'required': False},
                  'userfiles'      : {'default': None,       'config': ['Data.userInputFile'],              'type': 'StringType',  'required': False},
                  'dbsurl'         : {'default': 'global',   'config': ['Data.inputDBS'],                   'type': 'StringType',  'required': False},
                  'useparent'      : {'default': None,       'config': ['Data.useParent'],                  'type': 'BooleanType', 'required': False},
                  'ignorelocality' : {'default': False,      'config': ['Data.ignoreLocality'],             'type': 'BooleanType', 'required': False},
                  'splitalgo'      : {'default': None,       'config': ['Data.splitting'],                  'type': 'StringType',  'required': True },
                  'algoargs'       : {'default': None,       'config': ['Data.unitsPerJob'],                'type': 'IntType',     'required': True },
                  'totalunits'     : {'default': 0,          'config': ['Data.totalUnits'],                 'type': 'IntType',     'required': False},
                  'lfn'            : {'default': None,       'config': ['Data.outLFN'],                     'type': 'StringType',  'required': False},
                  'publication'    : {'default': True,       'config': ['Data.publication'],                'type': 'BooleanType', 'required': False},
                  'publishdbsurl'  : {'default': 'phys03',   'config': ['Data.publishDBS'],                 'type': 'StringType',  'required': False},
                  'publishname'    : {'default': '',         'config': ['Data.publishDataName'],            'type': 'StringType',  'required': False},
                  'jobtype'        : {'default': 'Analysis', 'config': ['JobType.pluginName',
                                                                        'JobType.externalPluginFile'],      'type': 'StringType',  'required': False},
                  'generator'      : {'default': None,       'config': ['JobType.generator'],               'type': 'StringType',  'required': False},
                  'eventsperlumi'  : {'default': None,       'config': ['JobType.eventsPerLumi'],           'type': 'IntType',     'required': False},
                  'adduserfiles'   : {'default': [],         'config': ['JobType.inputFiles'],              'type': 'ListType',    'required': False},
                  'addoutputfiles' : {'default': [],         'config': ['JobType.outputFiles'],             'type': 'ListType',    'required': False},
                  'maxjobruntime'  : {'default': None,       'config': ['JobType.maxJobRuntimeMin'],        'type': 'IntType',     'required': False},
                  'numcores'       : {'default': None,       'config': ['JobType.numCores'],                'type': 'IntType',     'required': False},
                  'maxmemory'      : {'default': None,       'config': ['JobType.maxMemoryMB'],             'type': 'IntType',     'required': False},
                  'priority'       : {'default': None,       'config': ['JobType.priority'],                'type': 'IntType',     'required': False},
                  'nonprodsw'      : {'default': False,      'config': ['JobType.allowNonProductionCMSSW'], 'type': 'BooleanType', 'required': False},
                  'scriptexe'      : {'default': None,       'config': ['JobType.scriptExe'],               'type': 'StringType',  'required': False},
                  'scriptargs'     : {'default': None,       'config': ['JobType.scriptArgs'],              'type': 'ListType',    'required': False},
                  'asyncdest'      : {'default': None,       'config': ['Site.storageSite'],                'type': 'StringType',  'required': False},
                  'sitewhitelist'  : {'default': None,       'config': ['Site.whitelist'],                  'type': 'ListType',    'required': False},
                  'siteblacklist'  : {'default': None,       'config': ['Site.blacklist'],                  'type': 'ListType',    'required': False},
                  'vorole'         : {'default': None,       'config': ['User.voRole'],                     'type': 'StringType',  'required': False},
                  'vogroup'        : {'default': None,       'config': ['User.voGroup'],                    'type': 'StringType',  'required': False},
                  'oneEventMode'   : {'default': False,      'config': ['Debug.oneEventMode'],              'type': 'BooleanType', 'required': False},
                  'asourl'         : {'default': None,       'config': ['Debug.ASOURL'],                    'type': 'StringType',  'required': False},
                  'scheddname'     : {'default': None,       'config': ['Debug.scheddName'],                'type': 'StringType',  'required': False},
                  'extrajdl'       : {'default': None,       'config': ['Debug.extraJDL'],                  'type': 'ListType',    'required': False},
                  'collector'      : {'default': None,       'config': ['Debug.collector'],                 'type': 'StringType',  'required': False},
                 },
    'other-config-params': ['General.workArea', 'General.instance', 'Data.lumiMask', 'Data.runRange', 'JobType.psetName', 'JobType.pyCfgParams']
}

renamed_params = {'General.transferOutput' : 'General.transferOutputs',
                  'General.saveLogs'       : 'General.transferLogs',
                  'Data.outlfn'            : 'Data.outLFN',
                  'Data.dbsUrl'            : 'Data.inputDBS',
                  'Data.publishDbsUrl'     : 'Data.publishDBS',
                  'JobType.numcores'       : 'JobType.numCores',
                  'JobType.maxmemory'      : 'JobType.maxMemoryMB',
                  'JobType.maxjobruntime'  : 'JobType.maxJobRuntimeMin'
}

commands_configuration = {
    'submit'        : {'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': False, 'useCache': False},
    'checkusername' : {'requiresREST': False, 'initializeProxy': True,  'requiresTaskOption': False, 'useCache': False},
    'checkwrite'    : {'requiresREST': False, 'initializeProxy': True,  'requiresTaskOption': False, 'useCache': False},
    'getlog'        : {'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': True },
    'getoutput'     : {'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': True },
    'kill'          : {'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': False},
    'purge'         : {'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': False},
    'remake'        : {'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': False, 'useCache': False},
    'remote_copy'   : {'requiresREST': True,  'initializeProxy': False, 'requiresTaskOption': True,  'useCache': True },
    'report'        : {'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': True },
    'request_type'  : {'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': True },
    'resubmit'      : {'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': True },
    'status'        : {'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': True },
    'uploadlog'     : {'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': False, 'useCache': False},
    'tasks'         : {'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': False, 'useCache': False}
}


def getParamServerName(param_config_name):
    for param_server_name in parameters_mapping['on-server'].keys():
        if param_config_name in parameters_mapping['on-server'][param_server_name]['config']:
            return param_server_name
    return None


def getParamDefaultValue(param_config_name):
    param_server_name = getParamServerName(param_config_name)
    if param_server_name:
        return parameters_mapping['on-server'][param_server_name]['default']
    return None

