#!/usr/bin/env python
"""
_ClientMapping_

This allows to have an agnostic client.
For each client command it is possible to define the path of the REST request, the map between
the client configuration and the final request to send to the server. It includes type of the
parameter so that the client can do a basic sanity check on the input data type.
For each server parameter, there can be more than one parameter in the CRAB configuration file.
If that is the case, then the meaning is that any of the parameters in the CRAB configuration
file is used to set the same server parameter.
"""

## In this dictionary, the definitions of 'type', 'required' and 'default'
## refer to the parameters in the CRAB configuration file.
parametersMapping = {
    'on-server': {'workflow'       : {'default': None,       'config': ['General.requestName'],             'type': 'StringType',  'required': False},
                  'activity'       : {'default': None,       'config': ['General.activity'],                'type': 'StringType',  'required': False},
                  'saveoutput'     : {'default': True,       'config': ['General.transferOutputs'],         'type': 'BooleanType', 'required': False},
                  'savelogsflag'   : {'default': False,      'config': ['General.transferLogs'],            'type': 'BooleanType', 'required': False},
                  'faillimit'      : {'default': None,       'config': ['General.failureLimit'],            'type': 'IntType',     'required': False},
                  'inputdata'      : {'default': None,       'config': ['Data.inputDataset',
                                                                        'Data.primaryDataset'],             'type': 'StringType',  'required': False},
                  'userfiles'      : {'default': None,       'config': ['Data.userInputFiles'],             'type': 'ListType',    'required': False},
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
                  'generator'      : {'default': 'pythia',   'config': ['JobType.generator'],               'type': 'StringType',  'required': False},
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
    'other-config-params': [         {'default': None,       'config': ['General.workArea'],                'type': 'StringType',  'required': False},
                                     {'default': 'prod',     'config': ['General.instance'],                'type': 'StringType',  'required': True },
                                     {'default': None,       'config': ['Data.lumiMask'],                   'type': 'StringType',  'required': False},
                                     {'default': None,       'config': ['Data.runRange'],                   'type': 'StringType',  'required': False},
                                     {'default': None,       'config': ['JobType.psetName'],                'type': 'StringType',  'required': False},
                                     {'default': False,      'config': ['JobType.sendPythonFolder'],        'type': 'BooleanType', 'required': False},
                                     {'default': None,       'config': ['JobType.pyCfgParams'],             'type': 'ListType',    'required': False},
                           ]
}

renamedParams = {
    'General.transferOutput' : 'General.transferOutputs',
    'General.saveLogs'       : 'General.transferLogs',
    'Data.outlfn'            : 'Data.outLFN',
    'Data.dbsUrl'            : 'Data.inputDBS',
    'Data.publishDbsUrl'     : 'Data.publishDBS',
    'Data.userInputFile'     : 'Data.userInputFiles',
    'JobType.numcores'       : 'JobType.numCores',
    'JobType.maxmemory'      : 'JobType.maxMemoryMB',
    'JobType.maxjobruntime'  : 'JobType.maxJobRuntimeMin'
}


"""
---------------------------------------------------------------------------------------------------------------
Parameter Name      |   Parameter Meaning
---------------------------------------------------------------------------------------------------------------
requiresTaskOption  -  Whether the command requires a CRAB project directory as input.
useCache            -  Whether to use the CRAB cache file (~/.crab3).
                       Currently only used to get the CRAB project directory in case the command requires it
                       but no directory was given in the -d/--dir option.
requiresREST        -  Whether the command has to contact the CRAB Server REST Interface.
acceptsArguments    -  Whether the command accepts arguments. (To not confuse with options.)
                       For commands requiring the task option, which can be actually given as an argument,
                       do not count it here as an argument. Same thing for the 'submit' command which can take
                       the CRAB configuration file name from the arguments.
initializeProxy     -  Whether the command needs to create a proxy if there is not a (valid) one already.
---------------------------------------------------------------------------------------------------------------
WARNING: Don't set at the same time acceptsArguments = True and requiresTaskOption = True. This is because
         we don't have a way to distinghish the CRAB project directory  name from the other arguments,
         so there is a protection when requiresTaskOption = True to not accept more that 1 argument.
---------------------------------------------------------------------------------------------------------------
"""
commandsConfiguration = {
    'checkusername' : {'acceptsArguments': False, 'requiresREST': False, 'initializeProxy': True,  'requiresTaskOption': False, 'useCache': False},
    'checkwrite'    : {'acceptsArguments': False, 'requiresREST': False, 'initializeProxy': True,  'requiresTaskOption': False, 'useCache': False},
    'getlog'        : {'acceptsArguments': False, 'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': True },
    'getoutput'     : {'acceptsArguments': False, 'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': True },
    'kill'          : {'acceptsArguments': False, 'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': False},
    'proceed'       : {'acceptsArguments': False, 'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': True },
    'purge'         : {'acceptsArguments': False, 'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': False},
    'remake'        : {'acceptsArguments': False, 'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': False, 'useCache': False},
    'remote_copy'   : {'acceptsArguments': False, 'requiresREST': True,  'initializeProxy': False, 'requiresTaskOption': True,  'useCache': True },
    'report'        : {'acceptsArguments': False, 'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': True },
    'request_type'  : {'acceptsArguments': False, 'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': True },
    'resubmit'      : {'acceptsArguments': False, 'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': True },
    'status'        : {'acceptsArguments': False, 'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': True },
    'submit'        : {'acceptsArguments': True,  'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': False, 'useCache': False},
    'tasks'         : {'acceptsArguments': False, 'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': False, 'useCache': False},
    'uploadlog'     : {'acceptsArguments': False, 'requiresREST': True,  'initializeProxy': True,  'requiresTaskOption': True,  'useCache': True }
}


def revertParamsMapping():
    import copy
    revertedMapping = {}
    for serverParamName, paramInfo in parametersMapping['on-server'].iteritems():
        info = copy.deepcopy(paramInfo)
        info.pop('config')
        for clientParamName in paramInfo['config']:
            revertedMapping[clientParamName] = {'server': serverParamName}
            revertedMapping[clientParamName].update(info)
    for paramInfo in parametersMapping['other-config-params']:
        info = copy.deepcopy(paramInfo)
        info.pop('config')
        for clientParamName in paramInfo['config']:
            revertedMapping[clientParamName] = {'server': None}
            revertedMapping[clientParamName].update(info)
    return revertedMapping

## This mapping looks like this:
## {'General.requestName'     : {'server': 'workflow',   'default': None,  'type': 'StringType',  'required': False},
##  'General.activity'        : {'server': 'activity',   'default': None,  'type': 'StringType',  'required': False},
##  'General.transferOutputs' : {'server': 'saveoutput', 'default': True,  'type': 'BooleanType', 'required': False},
##  etc, for all CRAB configuration parameters
## }
configParametersInfo = revertParamsMapping()


def getParamServerName(paramName):
    return configParametersInfo.get(paramName, {}).get('server')


def getParamDefaultValue(paramName):
    return configParametersInfo.get(paramName, {}).get('default')

