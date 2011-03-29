"""
Module to handle CMSSW _cfg.py file
"""

import imp
import json
import os

from FWCore.ParameterSet.Modules import OutputModule
from ServerInteractions import HTTPRequests
from PSetTweaks.WMTweak import makeTweak

class CMSSWConfig(object):
    """
    Class to handle CMSSW _cfg.py file
    """
    def __init__(self, config, userConfig=None, logger=None):
        self.config = config
        self.logger = logger

        self.fullConfig = None
        self.outputFile = None
        #TODO: Deal with user parameters (pycfg_params)

        if userConfig:
            cfgBaseName = os.path.basename(userConfig).replace(".py", "")
            cfgDirName = os.path.dirname(userConfig)
            self.logger.debug("Importing CMSSW config %s" % userConfig)

            modPath = imp.find_module(cfgBaseName, [cfgDirName])

            self.fullConfig = imp.load_module(cfgBaseName, modPath[0],
                                              modPath[1],  modPath[2])

            self.tweakJson = makeTweak(self.fullConfig.process).jsonise()


    def upload(self, requestConfig):
        """
        Upload the config file to the server
        """

        if not self.outputFile:
            raise RuntimeError('You must write out the config before uploading it')

        url = self.config.General.server_url
        server = HTTPRequests(url)
        if not server:
            raise RuntimeError('No server specified for config upload')

        with open(self.outputFile) as cfgFile:
            configString = cfgFile.read()

        group  = self.config.User.group
        userDN = requestConfig['RequestorDN']

        data = {'ConfFile'   : configString, 'PsetHash'    : '',
                'Group'      : group,        'UserDN'    : userDN,
                'Label'      : '',           'Description' : '',
                'PsetTweaks' : self.tweakJson,
               }
        jsonString = json.dumps(data, sort_keys=False)

        result = server.post(uri='/crabinterface/crab/config/', data=jsonString)
        self.logger.debug('Result of POST: %s' % str(result))
        return result


    def writeFile(self, filename= 'CMSSW.py'):
        """
        Persist fully expanded _cfg.py file
        """

        self.outputFile = filename
        self.logger.debug("Writing CMSSW config to %s" % self.outputFile)
        outFile = open(filename, "wb")
        outFile.write("import FWCore.ParameterSet.Config as cms\n")
        outFile.write(self.fullConfig.process.dumpConfig())
        outFile.close()

        # Would like to store as a single pickle string rather than dumpPython
        #  which has been unreliable at times.
        #outFile.write("import pickle\n")
        #outFile.write('process = pickle.loads("""\n')
        #outFile.write(pickle.dumps(self.fullConfig.process))
        #outFile.write('\n"""')

        return


    def outputFiles(self):
        """
        Returns a tuple of lists of output files. First element is TFileService files,
        second is PoolOutput files
        """

        tFiles = []
        poolFiles = []

        # Find TFileService
        if self.fullConfig.process.services.has_key('TFileService'):
            tFileService = self.fullConfig.process.services['TFileService']
            if "fileName" in tFileService.parameterNames_():
                tFiles.append(getattr(tFileService, 'fileName', None).value())

        # Find all PoolOutputModule's
        outputFinder = PoolOutputFinder()
        for p  in self.fullConfig.process.endpaths.itervalues():
            p.visit(outputFinder)
        poolFiles = outputFinder.getList()

        return tFiles, poolFiles



class PoolOutputFinder(object):
    """
    _PoolOutputFinder_

    Helper class to find PoolOutputModules
    """

    def __init__(self):
        self.poolList = []
        self.poolDict = {}

    def enter(self, visitee):
        """
        Enter for vistor pattern
        """

        if isinstance(visitee, OutputModule) and visitee.type_() == "PoolOutputModule":
            filename = visitee.fileName.value().split(":")[-1]
            self.poolList.append(filename)

            try:
                selectEvents = visitee.SelectEvents.SelectEvents.value()
            except AttributeError:
                selectEvents = None
            try:
                dataset = visitee.dataset.filterName.value()
            except AttributeError:
                dataset = None
            self.poolDict.update({filename:{'dataset':dataset, 'selectEvents':selectEvents}})

    def leave(self, visitee):
        """
        Leave for vistor pattern
        """
        pass

    def getList(self):
        """
        Get the list of filenames
        """
        return self.poolList

    def getDict(self):
        """
        Get a dictionary of filenames and their output data sets
        """
        return self.poolDict
